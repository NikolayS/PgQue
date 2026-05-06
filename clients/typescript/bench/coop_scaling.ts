#!/usr/bin/env bun
// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * Cooperative consumer scaling benchmark for the TypeScript client.
 *
 * Sweeps the number of subconsumers under one logical consumer and measures
 * end-to-end events/sec for a fixed event corpus.
 *
 * Args:
 *   --subconsumers N         (one or many, comma-list also accepted; default 1)
 *   --events K               (default 5000)
 *   --payload B              (approximate JSON payload size in bytes, default 64)
 *   --runs R                 (default 3; reports the median)
 *   --handler-work-ms MS     (per-message handler delay in ms; default 1)
 *
 * `--handler-work-ms` simulates per-message handler work. With a no-op
 * handler the cooperative `FOR UPDATE` row lock dominates from N=1 (one
 * worker is fastest), which is honest but unrepresentative — real
 * workloads do something with each message. A small per-message wait
 * lets parallel workers overlap handler time so the chart shows that
 * coop subconsumers share work.
 *
 * Output (stdout, stable schema):
 *   subconsumers,events_per_sec,seconds
 *
 * Run for a single N:
 *   PGQUE_TEST_DSN=postgres://nik@localhost/pgque_coop_ts \
 *     bun run clients/typescript/bench/coop_scaling.ts --subconsumers 4 \
 *     --events 5000 --payload 64 --runs 3
 *
 * The shell wrapper coop_scaling.sh sweeps {1, 2, 4, 8, 16}.
 */

import { performance } from 'node:perf_hooks';
import { connect, type Client } from '../src/index.js';

interface Args {
  subconsumers: number[];
  events: number;
  payload: number;
  runs: number;
  handlerWorkMs: number;
}

function parseArgs(argv: string[]): Args {
  const out: Args = {
    subconsumers: [1],
    events: 5000,
    payload: 64,
    runs: 3,
    handlerWorkMs: 1,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = argv[i + 1];
    if (!a) continue;
    if (a === '--subconsumers' && v) {
      out.subconsumers = v.split(',').map((s) => Number.parseInt(s.trim(), 10));
      i += 1;
    } else if (a === '--events' && v) {
      out.events = Number.parseInt(v, 10);
      i += 1;
    } else if (a === '--payload' && v) {
      out.payload = Number.parseInt(v, 10);
      i += 1;
    } else if (a === '--runs' && v) {
      out.runs = Number.parseInt(v, 10);
      i += 1;
    } else if (a === '--handler-work-ms' && v) {
      out.handlerWorkMs = Number.parseFloat(v);
      i += 1;
    }
  }
  for (const n of out.subconsumers) {
    if (!Number.isFinite(n) || n < 1) throw new Error(`invalid --subconsumers value: ${n}`);
  }
  if (!Number.isFinite(out.events) || out.events < 1) {
    throw new Error(`invalid --events value: ${out.events}`);
  }
  if (!Number.isFinite(out.payload) || out.payload < 1) {
    throw new Error(`invalid --payload value: ${out.payload}`);
  }
  if (!Number.isFinite(out.runs) || out.runs < 1) {
    throw new Error(`invalid --runs value: ${out.runs}`);
  }
  if (!Number.isFinite(out.handlerWorkMs) || out.handlerWorkMs < 0) {
    throw new Error(`invalid --handler-work-ms value: ${out.handlerWorkMs}`);
  }
  return out;
}

/**
 * Simulate per-message handler work. A no-op handler makes the cooperative
 * row-lock the bottleneck from N=1 (1 worker fastest, more workers worse).
 * Real workloads do meaningful work per message; a small per-message wait
 * lets parallel workers overlap so coop subconsumers can demonstrate
 * work-sharing.
 *
 * Uses an async sleep that doesn't block the event loop. Node/Bun
 * `setTimeout` clamps to ~1ms minimum; for sub-millisecond handler work
 * we spin on `performance.now()` so each message still spends the
 * intended time.
 */
async function handlerWork(ms: number): Promise<void> {
  if (ms <= 0) return;
  if (ms >= 1) {
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
    return;
  }
  // Spin for sub-ms targets. Yields once at the end so the event loop
  // can service other workers' I/O.
  const end = performance.now() + ms;
  while (performance.now() < end) {
    /* spin */
  }
  await new Promise<void>((resolve) => setImmediate(resolve));
}

function makePayload(targetBytes: number, i: number): { i: number; pad: string } {
  // JSON.stringify({ i: 0, pad: "..." }) overhead is roughly 14 bytes for
  // small i. We size the pad string so the encoded length is close to
  // targetBytes. Padding shorter than the floor is just an empty string.
  const overhead = 14 + String(i).length;
  const padLen = Math.max(0, targetBytes - overhead);
  return { i, pad: 'x'.repeat(padLen) };
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid]!;
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
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

async function preload(
  client: Client,
  queue: string,
  events: number,
  payloadBytes: number,
): Promise<void> {
  // Publish events in chunks and tick after each chunk so multiple distinct
  // batch windows form before workers start. Without this, all events end
  // up in one tick window — the cooperative allocator hands the whole
  // window to one worker and `--subconsumers > 1` can only steal it when
  // the first batch is acked, defeating the chart's purpose.
  //
  // CHUNK sets the per-batch event count. Smaller CHUNK → more tick
  // windows per K events → more `receive_coop` allocator calls per
  // second → coop FOR UPDATE contention shows up earlier as N grows.
  // Larger CHUNK → handlers dominate, scaling looks flat until very high N.
  // 10 keeps the allocator on the hot path so {1..16} brackets the
  // realistic region (rise → plateau → regression) for typical
  // sub-millisecond per-event work.
  const CHUNK = 10;
  for (let i = 0; i < events; i += CHUNK) {
    const batch = [];
    const end = Math.min(events, i + CHUNK);
    for (let j = i; j < end; j++) {
      batch.push(makePayload(payloadBytes, j));
    }
    await client.sendBatch(queue, 'bench.job', batch);
    await client.forceNextTick(queue);
    await client.ticker(queue);
  }
}

async function workerLoop(
  client: Client,
  queue: string,
  consumer: string,
  subconsumer: string,
  remaining: { count: number },
  handlerWorkMs: number,
  signal: AbortSignal,
): Promise<number> {
  let processed = 0;
  let emptyPolls = 0;
  while (!signal.aborted && remaining.count > 0) {
    let msgs;
    try {
      msgs = await client.receiveCoop(queue, consumer, subconsumer, { maxMessages: 1000 });
    } catch (err) {
      if (signal.aborted) break;
      throw err;
    }
    if (msgs.length === 0) {
      // No batch available right now. The benchmark pre-loads all events
      // and ticks while loading, so empty polls are caused by allocator
      // contention with another worker, not a missing tick. Yield briefly
      // and try again. After many empty polls give up — the bench's
      // remaining counter exit condition is the real source of truth.
      emptyPolls += 1;
      if (emptyPolls > 200) break;
      await new Promise((r) => setTimeout(r, 1));
      continue;
    }
    emptyPolls = 0;
    // Simulate per-message handler work. With handlerWorkMs > 0 multiple
    // workers can overlap their handlers while the coop allocator hands
    // out the next batch.
    if (handlerWorkMs > 0) {
      for (let i = 0; i < msgs.length; i++) {
        await handlerWork(handlerWorkMs);
      }
    }
    processed += msgs.length;
    remaining.count -= msgs.length;
    await client.ack(msgs[0]!.batchId);
  }
  return processed;
}

async function runOnce(
  client: Client,
  args: Args,
  subN: number,
): Promise<{ events: number; seconds: number; eventsPerSec: number }> {
  const sfx = Math.random().toString(36).slice(2, 8);
  const queue = `coop_bench_${subN}_${sfx}`;
  const consumer = `bench_${sfx}`;

  await client.rawPool.query('select pgque.create_queue($1)', [queue]);
  try {
    const subconsumers = Array.from({ length: subN }, (_, i) => `worker-${i + 1}`);
    for (const s of subconsumers) {
      await client.subscribeSubconsumer(queue, consumer, s);
    }

    await preload(client, queue, args.events, args.payload);

    const remaining = { count: args.events };
    const ac = new AbortController();

    const start = performance.now();
    const tasks = subconsumers.map((s) =>
      workerLoop(client, queue, consumer, s, remaining, args.handlerWorkMs, ac.signal),
    );
    const counts = await Promise.all(tasks);
    const elapsedMs = performance.now() - start;
    ac.abort();

    const totalProcessed = counts.reduce((a, b) => a + b, 0);
    if (totalProcessed !== args.events) {
      throw new Error(
        `processed ${totalProcessed} but expected ${args.events} (subconsumers=${subN})`,
      );
    }

    const seconds = elapsedMs / 1000;
    return {
      events: args.events,
      seconds,
      eventsPerSec: args.events / seconds,
    };
  } finally {
    await destroyQueue(client, queue).catch(() => undefined);
  }
}

async function main(): Promise<void> {
  const dsn = process.env.PGQUE_TEST_DSN;
  if (!dsn) {
    console.error('PGQUE_TEST_DSN not set');
    process.exit(1);
  }
  const args = parseArgs(process.argv.slice(2));
  // Pool max must be >= largest subconsumer fanout + a small buffer for
  // ticker / housekeeping queries; otherwise high-N runs queue on the pool
  // and the chart measures pg.Pool contention instead of coop scaling.
  const maxSubN = Math.max(...args.subconsumers);
  const poolMax = Math.max(20, maxSubN * 2 + 4);
  const client = await connect(dsn, { max: poolMax });
  try {
    // Header on stderr only — stdout is reserved for CSV (header included).
    process.stderr.write(
      `# coop_scaling: subconsumers=${args.subconsumers.join(',')} events=${args.events} payload=${args.payload}B runs=${args.runs} handlerWorkMs=${args.handlerWorkMs}\n`,
    );
    process.stdout.write('subconsumers,events_per_sec,seconds\n');
    for (const subN of args.subconsumers) {
      const samples: Array<{ eventsPerSec: number; seconds: number }> = [];
      for (let r = 0; r < args.runs; r++) {
        const result = await runOnce(client, args, subN);
        samples.push({ eventsPerSec: result.eventsPerSec, seconds: result.seconds });
        process.stderr.write(
          `# run subN=${subN} run=${r + 1}/${args.runs} eps=${result.eventsPerSec.toFixed(0)} seconds=${result.seconds.toFixed(3)}\n`,
        );
      }
      const eps = median(samples.map((s) => s.eventsPerSec));
      const secs = median(samples.map((s) => s.seconds));
      process.stdout.write(`${subN},${eps.toFixed(0)},${secs.toFixed(3)}\n`);
    }
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
