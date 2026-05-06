// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * Manual end-to-end check for cooperative consumers: two workers under one
 * logical consumer split a fixed number of events with disjoint msg_id sets.
 *
 * Run:
 *   PGQUE_TEST_DSN=postgres://nik@localhost/pgque_coop_ts \
 *     bun src/coop_e2e.ts
 */

import { connect } from './index.js';

async function main(): Promise<void> {
  const dsn = process.env.PGQUE_TEST_DSN;
  if (!dsn) throw new Error('PGQUE_TEST_DSN not set');
  const N = 50;
  const sfx = Math.random().toString(36).slice(2, 8);
  const queue = `coop_e2e_${sfx}`;
  const consumer = `coop_e2e_${sfx}`;
  const client = await connect(dsn);
  try {
    await client.rawPool.query(`select pgque.create_queue($1)`, [queue]);
    await client.subscribeSubconsumer(queue, consumer, 'worker-1');
    await client.subscribeSubconsumer(queue, consumer, 'worker-2');

    // Emit events in 5 small waves with a tick between each so multiple
    // distinct batch windows form. Otherwise the first worker could grab
    // the entire single tick window and the second sees nothing.
    const waves = 5;
    const perWave = N / waves;
    for (let w = 0; w < waves; w++) {
      for (let i = 0; i < perWave; i++) {
        await client.send(queue, { type: 'job', payload: { i: w * perWave + i } });
      }
      await client.forceNextTick(queue);
      await client.ticker(queue);
    }

    const seenA = new Set<string>();
    const seenB = new Set<string>();
    let totalA = 0;
    let totalB = 0;

    const consA = client.newConsumer(queue, consumer, {
      pollInterval: 25,
      subconsumer: 'worker-1',
    });
    consA.handle('job', async (msg) => {
      // Slow handler so the other worker has a chance to claim a batch.
      await new Promise((r) => setTimeout(r, 30));
      seenA.add(msg.msgId.toString());
      totalA += 1;
    });
    const consB = client.newConsumer(queue, consumer, {
      pollInterval: 25,
      subconsumer: 'worker-2',
    });
    consB.handle('job', async (msg) => {
      await new Promise((r) => setTimeout(r, 30));
      seenB.add(msg.msgId.toString());
      totalB += 1;
    });

    const ac = new AbortController();
    const runs = Promise.all([consA.start(ac.signal), consB.start(ac.signal)]);

    const tickerHandle = setInterval(() => {
      // Reissue ticks so workers keep getting fresh batches even with the
      // tick threshold un-forced after the first batch.
      void client.ticker(queue).catch(() => undefined);
    }, 100);

    const deadline = Date.now() + 8000;
    while (Date.now() < deadline && totalA + totalB < N) {
      await new Promise((r) => setTimeout(r, 25));
      // Ensure new batches become available even after handlers run fast.
      await client.forceNextTick(queue);
    }
    clearInterval(tickerHandle);
    ac.abort();
    await runs;

    const overlap = [...seenA].filter((id) => seenB.has(id));

    const out = {
      total_sent: N,
      worker_1_processed: totalA,
      worker_2_processed: totalB,
      sum: totalA + totalB,
      overlap_count: overlap.length,
      disjoint: overlap.length === 0,
    };
    console.log(JSON.stringify(out, null, 2));
    if (totalA + totalB !== N) {
      throw new Error(`expected ${N} total, got ${totalA + totalB}`);
    }
    if (overlap.length !== 0) {
      throw new Error(`expected disjoint ids, got ${overlap.length} overlap`);
    }
    if (totalA === 0 || totalB === 0) {
      throw new Error('one worker received no events; expected both to share');
    }
  } finally {
    try {
      await client.rawPool.query(
        `delete from pgque.subscription
          where sub_queue = (select queue_id from pgque.queue where queue_name = $1)`,
        [queue],
      );
      await client.rawPool.query(`select pgque.drop_queue($1, true)`, [queue]);
    } catch (err) {
      console.error('cleanup error:', err);
    }
    await client.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
