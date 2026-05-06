#!/usr/bin/env bun
// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * Cooperative-consumer demo: two subconsumers under one logical consumer
 * share a fixed event set, each printing the events it processes. Designed
 * to be runnable verbatim from the README's experimental coop section.
 *
 * Run:
 *
 *   PGQUE_TEST_DSN=postgres://nik@localhost/pgque_coop_ts \
 *     bun run clients/typescript/bench/coop_demo.ts
 */

import { connect, type Client } from '../src/index.js';

interface Counts {
  total: number;
  byWorker: Record<string, number>;
}

async function main(): Promise<void> {
  const dsn = process.env.PGQUE_TEST_DSN;
  if (!dsn) {
    console.error('PGQUE_TEST_DSN not set');
    process.exit(1);
  }

  const N = 30;
  const sfx = Math.random().toString(36).slice(2, 8);
  const queue = `coop_demo_${sfx}`;
  const consumer = `coop_demo_${sfx}`;
  const workers = ['worker-1', 'worker-2'] as const;

  const client = await connect(dsn);
  try {
    await client.rawPool.query('select pgque.create_queue($1)', [queue]);
    for (const w of workers) {
      await client.subscribeSubconsumer(queue, consumer, w);
    }

    // Pre-publish a known set of events with a tick boundary every 5 events
    // so multiple batch windows form (one batch per tick window).
    const payloads = Array.from({ length: N }, (_, i) => ({ i }));
    for (let i = 0; i < N; i += 5) {
      await client.sendBatch(queue, 'demo.job', payloads.slice(i, i + 5));
      await client.forceNextTick(queue);
      await client.ticker(queue);
    }
    console.log(`published ${N} events across ${N / 5} tick windows on queue ${queue}`);

    const counts: Counts = { total: 0, byWorker: { 'worker-1': 0, 'worker-2': 0 } };
    const ac = new AbortController();

    const runs = workers.map((sub) => {
      const c = client.newConsumer(queue, consumer, {
        pollInterval: 25,
        subconsumer: sub,
      });
      c.handle('demo.job', async (msg) => {
        const data = JSON.parse(msg.payload) as { i: number };
        counts.byWorker[sub] = (counts.byWorker[sub] ?? 0) + 1;
        counts.total += 1;
        console.log(`[${sub}] msg_id=${msg.msgId} payload.i=${data.i}`);
      });
      return c.start(ac.signal);
    });

    // Keep ticking so workers see fresh batch windows even after handlers run.
    const ticker = setInterval(() => {
      void client.ticker(queue).catch(() => undefined);
    }, 50);

    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline && counts.total < N) {
      await new Promise((r) => setTimeout(r, 25));
      await client.forceNextTick(queue);
    }
    clearInterval(ticker);
    ac.abort();
    await Promise.all(runs);

    console.log('---');
    console.log(`total processed: ${counts.total} / ${N}`);
    for (const w of workers) {
      console.log(`  ${w}: ${counts.byWorker[w] ?? 0}`);
    }
    if (counts.total !== N) {
      throw new Error(`expected ${N} processed, got ${counts.total}`);
    }
  } finally {
    await cleanup(client, queue);
    await client.close();
  }
}

async function cleanup(client: Client, queue: string): Promise<void> {
  try {
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
    await client.rawPool.query('select pgque.drop_queue($1, true)', [queue]);
  } catch (err) {
    console.error('cleanup error:', err);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
