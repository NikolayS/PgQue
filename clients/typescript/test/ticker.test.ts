// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
//
// Tests for ticker(queue), tickerAll(), and forceNextTick(queue) return values.
// These tests enforce issue #151: the TS client must surface SQL return values
// rather than discarding them as void.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { connect } from '../src/index.js';
import { TEST_DSN, setupTestQueue, teardownTestQueue, type TestEnv } from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

// ---------------------------------------------------------------------------
// Unit tests — stub pool.query so no DB needed
// ---------------------------------------------------------------------------

describe('ticker/forceNextTick unit tests (stubbed pool)', () => {
  it('ticker(queue) returns bigint | null from canned row', async () => {
    const { Client } = await import('../src/client.js');
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ ticker: BigInt(42) }] }),
      end: vi.fn(),
    } as any;
    const client = new Client(fakePool);
    const result = await client.ticker('myqueue');
    expect(result).toBe(BigInt(42));
  });

  it('ticker(queue) returns null when SQL returns null', async () => {
    const { Client } = await import('../src/client.js');
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ ticker: null }] }),
      end: vi.fn(),
    } as any;
    const client = new Client(fakePool);
    const result = await client.ticker('myqueue');
    expect(result).toBeNull();
  });

  it('tickerAll() returns number from canned row', async () => {
    const { Client } = await import('../src/client.js');
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ ticker: BigInt(3) }] }),
      end: vi.fn(),
    } as any;
    const client = new Client(fakePool);
    const result = await client.tickerAll();
    expect(typeof result).toBe('number');
    expect(result).toBe(3);
  });

  it('forceNextTick(queue) returns bigint from canned row', async () => {
    const { Client } = await import('../src/client.js');
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ force_next_tick: BigInt(7) }] }),
      end: vi.fn(),
    } as any;
    const client = new Client(fakePool);
    const result = await client.forceNextTick('myqueue');
    expect(typeof result).toBe('bigint');
    expect(result).toBe(BigInt(7));
  });

  it('forceNextTick(queue) returns null when SQL returns null (no ticks yet)', async () => {
    const { Client } = await import('../src/client.js');
    const fakePool = {
      query: vi.fn().mockResolvedValue({ rows: [{ force_next_tick: null }] }),
      end: vi.fn(),
    } as any;
    const client = new Client(fakePool);
    const result = await client.forceNextTick('myqueue');
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Integration tests — gated on PGQUE_TEST_DSN
// ---------------------------------------------------------------------------

describe('ticker/forceNextTick integration (requires PGQUE_TEST_DSN)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('forceNextTick(queue) returns a bigint or null', async () => {
    const result = await env.client.forceNextTick(env.queue);
    // Before any tick, result may be null (no ticks exist yet) or bigint.
    expect(result === null || typeof result === 'bigint').toBe(true);
  });

  skipIfNoDb('ticker(queue) after forceNextTick returns a non-null bigint tick id', async () => {
    // Force threshold so ticker will fire.
    await env.client.forceNextTick(env.queue);
    const tickId = await env.client.ticker(env.queue);
    expect(typeof tickId).toBe('bigint');
    expect(tickId).toBeGreaterThan(0n);
  });

  skipIfNoDb('ticker(queue) with no new events returns null', async () => {
    // Advance once so a tick exists.
    await env.client.forceNextTick(env.queue);
    await env.client.ticker(env.queue);
    // Immediately again with no new events: ticker returns NULL (no tick needed).
    const second = await env.client.ticker(env.queue);
    expect(second).toBeNull();
  });

  skipIfNoDb('tickerAll() returns a number >= 1', async () => {
    // At least one queue (env.queue) exists and is eligible.
    await env.client.forceNextTick(env.queue);
    const count = await env.client.tickerAll();
    expect(typeof count).toBe('number');
    expect(count).toBeGreaterThanOrEqual(1);
  });

  skipIfNoDb('forceNextTick then ticker returns new tick id, second ticker returns null', async () => {
    await env.client.forceNextTick(env.queue);
    const tick1 = await env.client.ticker(env.queue);
    expect(typeof tick1).toBe('bigint');

    // Immediately again — no new events, so should return null.
    const tick2 = await env.client.ticker(env.queue);
    expect(tick2).toBeNull();
  });
});
