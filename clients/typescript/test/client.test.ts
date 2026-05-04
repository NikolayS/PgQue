// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  PgqueConnectionError,
  PgqueQueueNotFoundError,
  PgqueSqlError,
  connect,
} from '../src/index.js';
import { TEST_DSN, setupTestQueue, teardownTestQueue, advanceQueue, type TestEnv } from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

describe('Client (env-gated, requires PGQUE_TEST_DSN)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  // ---------------------------------------------------------------------------
  // Connection lifecycle
  // ---------------------------------------------------------------------------

  it('rejects bad DSN with PgqueConnectionError', async () => {
    await expect(connect('postgres://nobody:wrong@127.0.0.1:1/nope')).rejects.toBeInstanceOf(
      PgqueConnectionError,
    );
  });

  // ---------------------------------------------------------------------------
  // send / receive / ack — the canonical happy path
  // ---------------------------------------------------------------------------

  skipIfNoDb('round-trips a simple event', async () => {
    const eid = await env.client.send(env.queue, {
      type: 'created',
      payload: { id: 42, hello: 'world' },
    });
    expect(typeof eid).toBe('bigint');
    expect(eid).toBeGreaterThan(0n);

    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receive(env.queue, env.consumer, 10);
    expect(msgs).toHaveLength(1);
    const m = msgs[0]!;
    expect(m.type).toBe('created');
    expect(JSON.parse(m.payload!)).toEqual({ id: 42, hello: 'world' });
    expect(typeof m.msgId).toBe('bigint');
    expect(typeof m.batchId).toBe('bigint');
    expect(m.createdAt).toBeInstanceOf(Date);
    expect(m.retryCount).toBeNull();

    await env.client.ack(m.batchId);

    // Empty after ack.
    const after = await env.client.receive(env.queue, env.consumer, 10);
    expect(after).toEqual([]);
  });

  skipIfNoDb('sendBatch publishes multiple payloads atomically', async () => {
    const ids = await env.client.sendBatch(env.queue, 'batch.test', [
      { n: 1 },
      { n: 2 },
      { n: 3 },
    ]);
    expect(ids).toHaveLength(3);
    expect(ids[0]).toBeLessThan(ids[1]!);
    expect(ids[1]).toBeLessThan(ids[2]!);

    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receive(env.queue, env.consumer, 10);
    expect(msgs).toHaveLength(3);
    expect(msgs.map((m) => m.type)).toEqual(['batch.test', 'batch.test', 'batch.test']);
    expect(msgs.map((m) => JSON.parse(m.payload!))).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    await env.client.ack(msgs[0]!.batchId);
  });

  skipIfNoDb('sendBatch accepts an empty array without inserting messages', async () => {
    await expect(env.client.sendBatch(env.queue, 'batch.empty', [])).resolves.toEqual([]);
    await advanceQueue(env.client, env.queue);
    await expect(env.client.receive(env.queue, env.consumer, 10)).resolves.toEqual([]);
  });

  skipIfNoDb('sendBatch defaults empty event type to "default"', async () => {
    await env.client.sendBatch(env.queue, '', [{ x: 1 }]);
    await advanceQueue(env.client, env.queue);
    const [msg] = await env.client.receive(env.queue, env.consumer, 10);
    expect(msg).toBeDefined();
    expect(msg!.type).toBe('default');
    await env.client.ack(msg!.batchId);
  });

  skipIfNoDb('defaults event type to "default" when omitted', async () => {
    await env.client.send(env.queue, { payload: { x: 1 } });
    await advanceQueue(env.client, env.queue);
    const [msg] = await env.client.receive(env.queue, env.consumer, 10);
    expect(msg).toBeDefined();
    expect(msg!.type).toBe('default');
    await env.client.ack(msg!.batchId);
  });

  skipIfNoDb('receive returns empty array when no batch is ready', async () => {
    const msgs = await env.client.receive(env.queue, env.consumer, 10);
    expect(msgs).toEqual([]);
  });

  // ---------------------------------------------------------------------------
  // Input validation
  // ---------------------------------------------------------------------------

  skipIfNoDb('rejects empty queue name on send', async () => {
    await expect(env.client.send('', { payload: {} })).rejects.toBeInstanceOf(PgqueSqlError);
  });

  skipIfNoDb('rejects empty queue name on sendBatch', async () => {
    await expect(env.client.sendBatch('', 'x', [{}])).rejects.toBeInstanceOf(PgqueSqlError);
  });

  skipIfNoDb('rejects empty queue name on receive', async () => {
    await expect(env.client.receive('', env.consumer, 10)).rejects.toBeInstanceOf(PgqueSqlError);
  });

  skipIfNoDb('rejects empty consumer name on receive', async () => {
    await expect(env.client.receive(env.queue, '', 10)).rejects.toBeInstanceOf(PgqueSqlError);
  });

  skipIfNoDb('rejects non-bigint batchId on ack', async () => {
    await expect(env.client.ack(1 as unknown as bigint)).rejects.toBeInstanceOf(PgqueSqlError);
  });

  skipIfNoDb('rejects non-positive maxMessages', async () => {
    await expect(env.client.receive(env.queue, env.consumer, 0)).rejects.toBeInstanceOf(
      PgqueSqlError,
    );
    await expect(env.client.receive(env.queue, env.consumer, -1)).rejects.toBeInstanceOf(
      PgqueSqlError,
    );
  });

  // ---------------------------------------------------------------------------
  // Error mapping
  // ---------------------------------------------------------------------------

  skipIfNoDb('send to nonexistent queue raises PgqueQueueNotFoundError', async () => {
    await expect(
      env.client.send('does_not_exist_xyz', { payload: {} }),
    ).rejects.toBeInstanceOf(PgqueQueueNotFoundError);
  });

  skipIfNoDb('sendBatch to nonexistent queue raises PgqueQueueNotFoundError', async () => {
    await expect(
      env.client.sendBatch('does_not_exist_xyz', 'x', [{}]),
    ).rejects.toBeInstanceOf(PgqueQueueNotFoundError);
  });

  skipIfNoDb('sendBatch rejects non-serializable payloads', async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    await expect(env.client.sendBatch(env.queue, 'x', [circular])).rejects.toBeInstanceOf(
      PgqueSqlError,
    );
  });

  // ---------------------------------------------------------------------------
  // Payload edge cases
  // ---------------------------------------------------------------------------

  skipIfNoDb('round-trips empty / null / unicode / nested payloads', async () => {
    const cases: Array<[string, unknown]> = [
      ['empty.obj', {}],
      ['empty.arr', []],
      ['null', null],
      ['unicode', { msg: 'héllo 🚀 世界' }],
      ['nested', { a: { b: { c: { d: [1, 2, { e: true }] } } } }],
      ['number.boundary', { big: 9007199254740993, small: -9007199254740993 }],
    ];
    for (const [type, payload] of cases) {
      await env.client.send(env.queue, { type, payload });
    }
    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receive(env.queue, env.consumer, 100);
    expect(msgs).toHaveLength(cases.length);
    const byType = new Map(msgs.map((m) => [m.type, m]));
    for (const [type, payload] of cases) {
      const m = byType.get(type);
      expect(m, `missing message of type ${type}`).toBeDefined();
      expect(JSON.parse(m!.payload!)).toEqual(payload);
    }
    await env.client.ack(msgs[0]!.batchId);
  });

  skipIfNoDb('top-level undefined payload becomes JSON null (explicit)', async () => {
    // `payload: undefined` must round-trip as the JSON literal `null`, not
    // as a SQL NULL bind (which would fail the JSONB cast) and not as the
    // JS `undefined` (which JSON.parse would reject).
    await env.client.send(env.queue, { type: 'undef.explicit', payload: undefined });
    await advanceQueue(env.client, env.queue);
    const [m] = await env.client.receive(env.queue, env.consumer, 10);
    expect(m).toBeDefined();
    expect(m!.type).toBe('undef.explicit');
    // Strong assertion: the stored value is the JSON literal `null`.
    expect(m!.payload).toBe('null');
    expect(JSON.parse(m!.payload!)).toBeNull();
    await env.client.ack(m!.batchId);
  });

  skipIfNoDb('omitted payload field becomes JSON null', async () => {
    // Same coercion when `payload` is absent from the Event object.
    await env.client.send(env.queue, { type: 'undef.omitted' });
    await advanceQueue(env.client, env.queue);
    const [m] = await env.client.receive(env.queue, env.consumer, 10);
    expect(m).toBeDefined();
    expect(m!.type).toBe('undef.omitted');
    expect(m!.payload).toBe('null');
    expect(JSON.parse(m!.payload!)).toBeNull();
    await env.client.ack(m!.batchId);
  });

  skipIfNoDb('drops nested undefined object properties per JSON.stringify', async () => {
    await env.client.send(env.queue, {
      type: 'undef.nested',
      payload: { a: 1, b: undefined },
    });
    await advanceQueue(env.client, env.queue);
    const [m] = await env.client.receive(env.queue, env.consumer, 10);
    expect(m).toBeDefined();
    expect(m!.type).toBe('undef.nested');
    expect(JSON.parse(m!.payload!)).toEqual({ a: 1 });
    await env.client.ack(m!.batchId);
  });

  skipIfNoDb('rejects top-level function payloads', async () => {
    await expect(
      env.client.send(env.queue, { type: 'bad.function', payload: () => undefined }),
    ).rejects.toBeInstanceOf(PgqueSqlError);
  });

  skipIfNoDb('preserves bigint batchId across the API surface', async () => {
    await env.client.send(env.queue, { payload: { x: 1 } });
    await advanceQueue(env.client, env.queue);
    const [m] = await env.client.receive(env.queue, env.consumer, 10);
    expect(m).toBeDefined();
    // bigint should round-trip without precision loss.
    expect(m!.batchId.toString()).toMatch(/^\d+$/);
    await env.client.ack(m!.batchId);
  });

  // ---------------------------------------------------------------------------
  // subscribe / unsubscribe
  // ---------------------------------------------------------------------------

  skipIfNoDb('subscribe is idempotent', async () => {
    // env.consumer is already subscribed in setup; subscribing again returns 0.
    const second = await env.client.subscribe(env.queue, env.consumer);
    expect(second).toBe(0);
  });

  skipIfNoDb('unsubscribe removes the consumer', async () => {
    const first = await env.client.unsubscribe(env.queue, env.consumer);
    expect(first).toBeGreaterThan(0);
    // Second unsubscribe is a no-op.
    const second = await env.client.unsubscribe(env.queue, env.consumer);
    expect(second).toBe(0);
  });

  // ---------------------------------------------------------------------------
  // Concurrency
  // ---------------------------------------------------------------------------

  // Pool size note (P1): pg.Pool defaults to 10 connections; 50 concurrent
  // producers will queue 5-deep while waiting for a free connection. The test
  // is intentionally not time-bound — it only asserts uniqueness and total
  // count, so it passes correctly regardless of serialization depth. Keeping
  // pool size at the default avoids inflating connection counts in CI.
  skipIfNoDb('handles concurrent producers via the pool', async () => {
    const N = 50;
    const ids = await Promise.all(
      Array.from({ length: N }, (_, i) =>
        env.client.send(env.queue, { type: 'concurrent', payload: { i } }),
      ),
    );
    expect(new Set(ids).size).toBe(N); // all unique event ids
    await advanceQueue(env.client, env.queue);

    let total = 0;
    while (total < N) {
      const batch = await env.client.receive(env.queue, env.consumer, 100);
      if (batch.length === 0) break;
      total += batch.length;
      await env.client.ack(batch[0]!.batchId);
    }
    expect(total).toBe(N);
  });
});
