// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
import { expect, it } from 'vitest';
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
