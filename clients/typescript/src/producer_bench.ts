#!/usr/bin/env node
// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/** Producer microbenchmarks for the TypeScript client.
 *
 * Compares the default client API paths:
 * - loop over client.send(queue, event)
 * - one client.sendBatch(queue, type, payloads) call
 *
 * Batch sizes are fixed at 1, 100, and 1000. Each measurement creates a fresh
 * queue, publishes N events, verifies the inserted row count, and drops the
 * queue. Output is Markdown plus CSV-friendly rows.
 *
 * Usage:
 *   PGQUE_TEST_DSN=postgres://... npx tsx src/producer_bench.ts
 */

import { randomBytes } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { connect, type Client } from './client.js';

const dsn = process.env.PGQUE_TEST_DSN;
const batchSizes = [1, 100, 1000] as const;
const repeats = Number.parseInt(process.env.PGQUE_BENCH_REPEATS ?? '3', 10);

type Method = 'send_loop' | 'send_batch';
type Payload = { i: number; lang: 'typescript'; method: Method };

interface Result {
  language: 'typescript';
  method: Method;
  batchSize: number;
  medianMs: number;
  eventsPerSec: number;
  repeats: number;
}

async function main(): Promise<void> {
  if (!dsn) {
    console.log('PGQUE_TEST_DSN not set; skipping pgque TypeScript producer benchmarks');
    return;
  }

  const client = await connect(dsn);
  try {
    const results: Result[] = [];
    for (const n of batchSizes) {
      results.push(await measure(client, 'send_loop', n, sendLoop));
      results.push(await measure(client, 'send_batch', n, sendBatch));
    }

    console.log('# pgque TypeScript producer benchmark');
    console.log();
    console.log('| method | batch_size | median_ms | events_per_sec | repeats |');
    console.log('|---|---:|---:|---:|---:|');
    for (const r of results) {
      console.log(`| ${displayMethod(r.method)} | ${r.batchSize} | ${r.medianMs.toFixed(3)} | ${r.eventsPerSec.toFixed(0)} | ${r.repeats} |`);
    }

    console.log();
    console.log('```csv');
    console.log('language,method,batch_size,median_ms,events_per_sec,repeats');
    for (const r of results) {
      console.log(`${r.language},${r.method},${r.batchSize},${r.medianMs.toFixed(3)},${r.eventsPerSec.toFixed(0)},${r.repeats}`);
    }
    console.log('```');
  } finally {
    await client.close();
  }
}

async function measure(
  client: Client,
  method: Method,
  n: number,
  fn: (client: Client, queue: string, payloads: Payload[]) => Promise<void>,
): Promise<Result> {
  const durations: number[] = [];
  for (let r = 0; r < repeats; r++) {
    const queue = `tsbench_${method}_${n}_${randomBytes(4).toString('hex')}`;
    const payloads = Array.from({ length: n }, (_, i) => ({ i, lang: 'typescript' as const, method }));
    await client.rawPool.query('select pgque.create_queue($1)', [queue]);
    try {
      const start = performance.now();
      await fn(client, queue, payloads);
      const elapsedMs = performance.now() - start;
      await verifyCount(client, queue, n);
      durations.push(elapsedMs);
    } finally {
      await client.rawPool.query('select pgque.drop_queue($1, true)', [queue]).catch(() => undefined);
    }
  }

  const medianMs = median(durations);
  return {
    language: 'typescript',
    method,
    batchSize: n,
    medianMs,
    eventsPerSec: medianMs > 0 ? n / (medianMs / 1000) : Number.POSITIVE_INFINITY,
    repeats,
  };
}

function displayMethod(method: Method): string {
  return method === 'send_loop' ? 'loop over send()' : 'sendBatch()';
}

async function sendLoop(client: Client, queue: string, payloads: Payload[]): Promise<void> {
  for (const payload of payloads) {
    await client.send(queue, { type: 'bench.producer', payload });
  }
}

async function sendBatch(client: Client, queue: string, payloads: Payload[]): Promise<void> {
  await client.sendBatch(queue, 'bench.producer', payloads);
}

async function verifyCount(client: Client, queue: string, expected: number): Promise<void> {
  const tableResult = await client.rawPool.query<{ current_event_table: string }>(
    'select pgque.current_event_table($1)',
    [queue],
  );
  const table = tableResult.rows[0]?.current_event_table;
  if (!table) throw new Error(`current_event_table returned no row for ${queue}`);
  const countResult = await client.rawPool.query<{ count: string }>(`select count(*) from ${table}`);
  const got = Number.parseInt(countResult.rows[0]?.count ?? 'NaN', 10);
  if (got !== expected) {
    throw new Error(`${queue}: expected ${expected} events, got ${got}`);
  }
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid]!;
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
}

main().catch((err) => {
  console.error('pgque TypeScript producer benchmark: FAIL', err);
  process.exit(1);
});
