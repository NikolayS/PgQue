#!/usr/bin/env bun
// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * Manual end-to-end evidence runner for cooperative consumers.
 *
 * Walks four scenarios in sequence and prints labelled output:
 *
 *   (1) Two subconsumers split a known set of events disjointly.
 *   (2) `subscribeSubconsumer` is idempotent (1 then 0).
 *   (3) `unsubscribeSubconsumer` rejects an active batch by default and
 *       succeeds with `{ batchHandling: 1 }`.
 *   (4) Stale takeover via `deadInterval`.
 *
 * Run with the dedicated coop database:
 *
 *   PGQUE_TEST_DSN=postgres://nik@localhost/pgque_coop_ts \
 *     bun run clients/typescript/bench/coop_manual_evidence.ts
 */

import { connect, type Client } from '../src/index.js';

const DSN = process.env.PGQUE_TEST_DSN;
if (!DSN) {
  console.error('PGQUE_TEST_DSN not set');
  process.exit(1);
}

function suffix(): string {
  return Math.random().toString(36).slice(2, 8);
}

async function freshQueue(client: Client, prefix: string): Promise<string> {
  const queue = `${prefix}_${suffix()}`;
  await client.rawPool.query('select pgque.create_queue($1)', [queue]);
  return queue;
}

async function destroyQueue(client: Client, queue: string): Promise<void> {
  await client.rawPool.query(
    `delete from pgque.subscription
      where sub_queue = (select queue_id from pgque.queue where queue_name = $1)`,
    [queue],
  );
  await client.rawPool.query(
    `delete from pgque.retry_queue
      where ev_queue = (select queue_id from pgque.queue where queue_name = $1)`,
    [queue],
  );
  await client.rawPool.query(
    `delete from pgque.dead_letter
      where dl_queue_id = (select queue_id from pgque.queue where queue_name = $1)`,
    [queue],
  );
  await client.rawPool.query('select pgque.drop_queue($1, true)', [queue]);
}

function header(label: string): void {
  console.log('');
  console.log(`=== ${label} ===`);
}

async function scenarioPartition(client: Client): Promise<void> {
  header('Scenario 1 — two subconsumers partition disjointly');
  const queue = await freshQueue(client, 'evid_part');
  const consumer = `c_${suffix()}`;
  try {
    await client.subscribeSubconsumer(queue, consumer, 'worker-1');
    await client.subscribeSubconsumer(queue, consumer, 'worker-2');

    const N = 12;
    for (let i = 0; i < N; i++) {
      await client.send(queue, { type: 'job', payload: { i } });
      // Tick after every 3 events so multiple distinct batch windows exist.
      if ((i + 1) % 3 === 0) {
        await client.forceNextTick(queue);
        await client.ticker(queue);
      }
    }
    await client.forceNextTick(queue);
    await client.ticker(queue);

    const seenA = new Set<string>();
    const seenB = new Set<string>();
    let drained = 0;
    // Alternate workers polling small batches under fresh tick windows.
    for (let round = 0; round < 12 && drained < N; round++) {
      const which = round % 2 === 0 ? 'worker-1' : 'worker-2';
      const seen = which === 'worker-1' ? seenA : seenB;
      const msgs = await client.receiveCoop(queue, consumer, which, { maxMessages: 100 });
      if (msgs.length > 0) {
        for (const m of msgs) {
          seen.add(m.msgId.toString());
          drained += 1;
        }
        await client.ack(msgs[0]!.batchId);
      }
      await client.forceNextTick(queue);
      await client.ticker(queue);
    }

    const overlap = [...seenA].filter((id) => seenB.has(id));
    console.log(JSON.stringify({
      total_sent: N,
      worker_1_count: seenA.size,
      worker_2_count: seenB.size,
      sum: seenA.size + seenB.size,
      overlap_count: overlap.length,
      disjoint: overlap.length === 0,
    }, null, 2));
  } finally {
    await destroyQueue(client, queue);
  }
}

async function scenarioIdempotent(client: Client): Promise<void> {
  header('Scenario 2 — subscribeSubconsumer is idempotent');
  const queue = await freshQueue(client, 'evid_idem');
  const consumer = `c_${suffix()}`;
  try {
    const first = await client.subscribeSubconsumer(queue, consumer, 'worker-1');
    const second = await client.subscribeSubconsumer(queue, consumer, 'worker-1');
    console.log(JSON.stringify({
      first_call_returned: first,
      second_call_returned: second,
      idempotent: first === 1 && second === 0,
    }, null, 2));
  } finally {
    await destroyQueue(client, queue);
  }
}

async function scenarioActiveBatch(client: Client): Promise<void> {
  header('Scenario 3 — unsubscribeSubconsumer with an active batch');
  const queue = await freshQueue(client, 'evid_act');
  const consumer = `c_${suffix()}`;
  try {
    const sub = 'worker-stuck';
    await client.subscribeSubconsumer(queue, consumer, sub);
    await client.send(queue, { type: 'job', payload: { i: 1 } });
    await client.forceNextTick(queue);
    await client.ticker(queue);

    const msgs = await client.receiveCoop(queue, consumer, sub, { maxMessages: 100 });
    console.log(JSON.stringify({ received_count: msgs.length, batch_id: msgs[0]?.batchId.toString() }, null, 2));

    let defaultRejected = false;
    let defaultErrorMsg = '';
    try {
      await client.unsubscribeSubconsumer(queue, consumer, sub);
    } catch (err) {
      defaultRejected = true;
      defaultErrorMsg = (err as Error).message;
    }
    console.log(JSON.stringify({
      default_unsubscribe_rejected: defaultRejected,
      default_error_includes_active_batch: /active batch|batch_handling|cooperative/i.test(defaultErrorMsg),
    }, null, 2));

    const removed = await client.unsubscribeSubconsumer(queue, consumer, sub, { batchHandling: 1 });
    console.log(JSON.stringify({ batch_handling_1_returned: removed }, null, 2));

    const retry = await client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [queue],
    );
    console.log(JSON.stringify({ retry_queue_rows_after_handling_1: retry.rows[0]!.count }, null, 2));
  } finally {
    await destroyQueue(client, queue);
  }
}

async function scenarioStaleTakeover(client: Client): Promise<void> {
  header('Scenario 4 — stale takeover via deadInterval');
  const queue = await freshQueue(client, 'evid_stale');
  const consumer = `c_${suffix()}`;
  try {
    const subA = 'worker-a';
    const subB = 'worker-b';
    await client.subscribeSubconsumer(queue, consumer, subA);
    await client.subscribeSubconsumer(queue, consumer, subB);

    await client.send(queue, { type: 'job', payload: { i: 1 } });
    await client.send(queue, { type: 'job', payload: { i: 2 } });
    await client.forceNextTick(queue);
    await client.ticker(queue);

    // worker-a grabs the batch, then "dies" (never acks).
    const msgsA = await client.receiveCoop(queue, consumer, subA, { maxMessages: 100 });
    const ownedBatchId = msgsA[0]?.batchId.toString();
    console.log(JSON.stringify({
      worker_a_received: msgsA.length,
      worker_a_batch_id: ownedBatchId,
    }, null, 2));

    // Force worker-a's heartbeat into the past so the stale-takeover guard
    // fires regardless of wall-clock latency in the harness. Cooperative
    // members are stored as `consumer.subconsumer` rows with sub_role
    // 'coop_member'.
    await client.rawPool.query(
      `update pgque.subscription s
          set sub_active = now() - interval '1 hour'
         from pgque.consumer c
        where s.sub_consumer = c.co_id
          and c.co_name = $1
          and s.sub_role = 'coop_member'
          and s.sub_queue = (select queue_id from pgque.queue where queue_name = $2)`,
      [`${consumer}.${subA}`, queue],
    );

    // worker-b polls with a short deadInterval — should steal worker-a's batch.
    const msgsB = await client.receiveCoop(queue, consumer, subB, {
      maxMessages: 100,
      deadInterval: '1 second',
    });
    const stolenBatchId = msgsB[0]?.batchId.toString();
    console.log(JSON.stringify({
      worker_b_received: msgsB.length,
      worker_b_batch_id: stolenBatchId,
      fresh_batch_id: stolenBatchId !== ownedBatchId,
    }, null, 2));

    // worker-a's old batch token is now invalid; ack of the stolen batch wins.
    if (msgsB.length > 0) {
      const acked = await client.ack(msgsB[0]!.batchId);
      console.log(JSON.stringify({ worker_b_ack_returned: acked }, null, 2));
    }
  } finally {
    await destroyQueue(client, queue);
  }
}

async function main(): Promise<void> {
  const client = await connect(DSN!);
  try {
    await scenarioPartition(client);
    await scenarioIdempotent(client);
    await scenarioActiveBatch(client);
    await scenarioStaleTakeover(client);
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
