#!/usr/bin/env bun
// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
//
// smoke.ts — env-gated end-to-end smoke test.
//
// Usage (from clients/typescript/):
//   bun src/smoke.ts
//
// If PGQUE_TEST_DSN is unset the script exits 0 immediately. When the var is
// set it runs a minimal send → tick → receive → ack round-trip and exits 0 on
// success, 1 on failure. This file exists as a temporary bridge until the
// `client-tests` CI step (PR #84) lands and replaces this script entirely.

import { connect } from './client.js';

const dsn = process.env.PGQUE_TEST_DSN;
if (!dsn) {
  // No DSN configured — skip silently. CI treats exit 0 as success.
  process.exit(0);
}

async function run(): Promise<void> {
  const queue = `smoke_ts_${Date.now()}`;
  const consumer = `smoke_consumer_${Date.now()}`;
  const client = await connect(dsn as string);
  try {
    await client.rawPool.query(`select pgque.create_queue($1)`, [queue]);
    await client.subscribe(queue, consumer);

    const id = await client.send(queue, { type: 'smoke', payload: { ok: true } });
    if (typeof id !== 'bigint' || id <= 0n) {
      throw new Error(`send returned unexpected id: ${id}`);
    }

    // forceTick bumps the event-seq threshold; ticker actually creates the tick
    // that makes newly sent events visible to receive(). Both calls are required
    // in manual/demo mode.
    await client.forceTick(queue);
    await client.ticker(queue);

    const msgs = await client.receive(queue, consumer, 1);
    if (msgs.length !== 1) {
      throw new Error(`expected 1 message, got ${msgs.length}`);
    }
    const msg = msgs[0]!;
    if (msg.payload === null) {
      throw new Error('unexpected null payload from Client.send');
    }
    const parsed = JSON.parse(msg.payload) as { ok: boolean };
    if (!parsed.ok) {
      throw new Error(`unexpected payload: ${msg.payload}`);
    }

    await client.ack(msg.batchId);
    console.log('pgque TypeScript smoke test: OK');
  } finally {
    await client.rawPool.query(`select pgque.drop_queue($1, true)`, [queue]).catch(() => undefined);
    await client.close();
  }
}

run().catch((err) => {
  console.error('pgque TypeScript smoke test: FAIL', err);
  process.exit(1);
});
