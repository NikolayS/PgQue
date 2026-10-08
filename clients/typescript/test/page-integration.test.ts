// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
import { expect, it } from 'vitest';
import type { Page } from '../src/index.js';
import { TEST_DSN, setupTestQueue, teardownTestQueue, advanceQueue } from './helpers.js';

(TEST_DSN ? it : it.skip)('pages through live SQL with failure routing and receipt replay', async () => {
  const env = await setupTestQueue();
  const { client, queue, consumer } = env;
  try {
    const ids: bigint[] = [];
    for (let i = 0; i < 2; i++) ids.push(await client.send(queue, {type: 'page.live', payload: {i}}));
    const maxId = 9223372036854775807n;
    await client.rawPool.query(
      `select pgque.event_retry_raw($1,$2,now() - interval '1 second',$3,
         now(),0,'page.live','{"i":"max"}',null,null,null,null)`,
      [queue, consumer, maxId.toString()],
    );
    await client.rawPool.query(`select pgque.maint_retry_events()`);
    await advanceQueue(client, queue);
    await client.rawPool.query(
      `update pgque.tick set tick_event_seq = $1 where tick_id =
         (select max(tick_id) from pgque.tick where tick_queue =
           (select queue_id from pgque.queue where queue_name = $2))`,
      [maxId.toString(), queue],
    );
    await expect(client.processPage(queue, consumer, 'live-worker', () => {
      throw new Error('boom');
    }, 2)).rejects.toThrow('boom');
    const page = await client.receivePage(queue, consumer, 'live-worker', 2);
    expect(page.status).toBe('page');
    expect(page.isLast).toBe(false);
    expect(page.messages.map(m => m.msgId)).toEqual(ids);
    expect((await client.renewPage(page.pageToken!, 'live-worker')).getTime()).toBeGreaterThanOrEqual(page.leaseUntil!.getTime());
    const seen: bigint[] = [];
    const result = await client.processPage(queue, consumer, 'live-worker', m => { seen.push(m.msgId); }, 2);
    expect(result).toMatchObject({processedCount: 2, batchFinished: false});
    expect(seen).toEqual(ids);
    expect(await client.ackPage(page.pageToken!, 'live-worker')).toEqual({status: 'already_acked', batchFinished: false});
    const maxPage = await client.receivePage(queue, consumer, 'live-worker', 2);
    expect(maxPage.messages.map(m => m.msgId)).toEqual([maxId]);
    const failures = [{msgId: maxId.toString(), retryAfterSeconds: 0}];
    expect(await client.ackPage(maxPage.pageToken!, 'live-worker', failures)).toEqual({status: 'acked', batchFinished: true});
    expect(await client.ackPage(maxPage.pageToken!, 'live-worker', failures)).toEqual({status: 'already_acked', batchFinished: true});
    const retry = await client.rawPool.query<{ev_id: string}>(
      `select ev_id::text from pgque.retry_queue where ev_queue =
         (select queue_id from pgque.queue where queue_name = $1)`, [queue]);
    expect(retry.rows.map(row => row.ev_id)).toContain(maxId.toString());
    const idle = await client.receivePage(queue, consumer, 'live-worker');
    expect(['idle', 'advanced']).toContain(idle.status);
    expect(idle.messages).toEqual([]);
    expect(idle.pageToken).toBeNull();
  } finally { await teardownTestQueue(env); }
});

(TEST_DSN ? it : it.skip)('binds cooperative and partitioned page adapters to live SQL', async () => {
  const env = await setupTestQueue();
  const { client, queue, consumer } = env;
  const coop = `${consumer}_group`, member = 'member-one', coopWorker = 'coop-process';
  const partitioned = `${consumer}_partitioned`, partitionWorker = 'partition-process';
  try {
    await client.rawPool.query('select pgque.register_subconsumer($1,$2,$3)', [queue, coop, member]);
    await client.rawPool.query('select pgque.subscribe_partitioned($1,$2,3)', [queue, partitioned]);
    const claim = await client.rawPool.query<{epoch: bigint}>(
      "select pgque.claim_slot($1,$2,2,$3,interval '3 minutes') as epoch",
      [queue, partitioned, partitionWorker],
    );
    const epoch = claim.rows[0]!.epoch;
    expect(typeof epoch).toBe('bigint');
    const keys = new Map<number, string>();
    for (const slot of [0, 2]) {
      const result = await client.rawPool.query<{key: string}>(
        `select 'adapter-key-' || i as key from generate_series(1,1000) as i
         where (hashtextextended('adapter-key-' || i,0) % 3 + 3) % 3 = $1
         order by i limit 1`, [slot],
      );
      keys.set(slot, result.rows[0]!.key);
    }
    const payloads = ['target-first', 'other-slot', 'target-last'];
    const eventKeys = [keys.get(2)!, keys.get(0)!, keys.get(2)!];
    const ids: bigint[] = [];
    for (let i = 0; i < payloads.length; i++) {
      const result = await client.rawPool.query<{id: bigint}>(
        "select pgque.send($1,'adapter.mode',$2::text,$3::text) as id",
        [queue, payloads[i], eventKeys[i]],
      );
      ids.push(result.rows[0]!.id);
    }
    await advanceQueue(client, queue);
    const checkPage = (page: Page, indexes: number[]) => {
      expect(page.status).toBe('page');
      expect(page.pageToken).toBeTruthy();
      expect(page.batchId).not.toBeNull();
      expect(page.pageNumber).toBe(1n);
      expect(page.isLast).toBe(true);
      expect(page.leaseUntil).toBeInstanceOf(Date);
      expect(page.messages.map(m => m.msgId)).toEqual(indexes.map(i => ids[i]));
      expect(page.messages.map(m => m.payload)).toEqual(indexes.map(i => payloads[i]));
      expect(page.messages.map(m => m.extra1)).toEqual(indexes.map(i => eventKeys[i]));
      expect(page.messages.every(m => m.batchId === page.batchId && m.type === 'adapter.mode')).toBe(true);
    };
    const checkAck = async (page: Page, worker: string) => {
      await expect(client.ackPage(page.pageToken!, 'not-the-owner')).rejects.toThrow('wrong worker');
      expect(await client.ackPage(page.pageToken!, worker)).toEqual({status: 'acked', batchFinished: true});
      expect(await client.ackPage(page.pageToken!, worker)).toEqual({status: 'already_acked', batchFinished: true});
    };
    const coopPage = await client.receivePageCoop(queue, coop, member, coopWorker, 3, null, '3 minutes');
    checkPage(coopPage, [0, 1, 2]);
    expect(coopPage.fenceEpoch).toBeNull();
    await checkAck(coopPage, coopWorker);
    const coopEmpty = await client.receivePageCoop(queue, coop, member, coopWorker, 3, '5 minutes', '3 minutes');
    expect(['idle', 'advanced']).toContain(coopEmpty.status);
    expect(coopEmpty.messages).toEqual([]);
    expect(coopEmpty.pageToken).toBeNull();

    const partitionPage = await client.receivePagePartitioned(queue, partitioned, 2, 3, partitionWorker, 2);
    checkPage(partitionPage, [0, 2]);
    expect(partitionPage.fenceEpoch).toBe(epoch);
    await checkAck(partitionPage, partitionWorker);
    const partitionEmpty = await client.receivePagePartitioned(queue, partitioned, 2, 3, partitionWorker, 2);
    expect(['idle', 'advanced']).toContain(partitionEmpty.status);
    expect(partitionEmpty.messages).toEqual([]);
    expect(partitionEmpty.pageToken).toBeNull();
  } finally { await teardownTestQueue(env); }
});
