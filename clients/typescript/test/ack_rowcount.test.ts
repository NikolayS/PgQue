// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

// ack_rowcount.test.ts -- red/green tests for issue #148
//
// Red: client.ack() returns Promise<void>, so the return-value assertions
//   below will fail at type-check and at runtime.
// Green: client.ack() is widened to Promise<number> (0 or 1).

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Client } from '../src/client.js';
import { connect } from '../src/index.js';
import {
  TEST_DSN,
  setupTestQueue,
  teardownTestQueue,
  advanceQueue,
  type TestEnv,
} from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

// ---------------------------------------------------------------------------
// Pure-vitest unit tests with a stubbed pool (no live database)
// ---------------------------------------------------------------------------

describe('Client.ack returns rowcount (unit stubs)', () => {
  it('returns 1 when SQL returns { ack: 1 }', async () => {
    // Construct a Client with a stubbed pool that returns ack=1.
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ ack: 1n }] }),
      end: vi.fn().mockResolvedValue(undefined),
    } as unknown as import('pg').Pool;

    // Access the internal constructor via a small cast; Client is exported
    // but connect() is the documented entry point. The unit test bypasses
    // connect() to inject the stub pool.
    const { Client } = await import('../src/client.js');
    const client = new Client(fakePool);

    const result = await client.ack(42n);
    expect(result).toBe(1);
  });

  it('returns 0 when SQL returns { ack: 0 } (stale/double ack)', async () => {
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ ack: 0n }] }),
      end: vi.fn().mockResolvedValue(undefined),
    } as unknown as import('pg').Pool;

    const { Client } = await import('../src/client.js');
    const client = new Client(fakePool);

    const result = await client.ack(99n);
    expect(result).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Live-database integration tests (gated on PGQUE_TEST_DSN)
// ---------------------------------------------------------------------------

describe('Client.ack live-db rowcount (requires PGQUE_TEST_DSN)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('first ack returns 1, second ack (double) returns 0', async () => {
    await env.client.send(env.queue, { type: 'ack.rowcount', payload: { v: 1 } });
    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receive(env.queue, env.consumer, 10);
    expect(msgs.length).toBeGreaterThan(0);
    const batchId = msgs[0]!.batchId;

    // First ack — the batch is active: should return 1.
    const first = await env.client.ack(batchId);
    expect(first).toBe(1);

    // Second ack — the batch is already finished: should return 0.
    const second = await env.client.ack(batchId);
    expect(second).toBe(0);
  });
});
