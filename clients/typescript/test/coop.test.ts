// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Consumer } from '../src/consumer.js';
import type { Client } from '../src/client.js';
import type { Message } from '../src/types.js';
import {
  TEST_DSN,
  advanceQueue,
  setupTestQueue,
  teardownCoopTestQueue,
  type TestEnv,
} from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

describe('Cooperative consumers (env-gated)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
    // The default helper subscribes a normal consumer, but cooperative tests
    // need the same logical name to start as a fresh cooperative group. Drop
    // the auto-created normal subscription up front; receive_coop will
    // auto-register coop_main + coop_member on first call.
    await env.client.unsubscribe(env.queue, env.consumer);
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownCoopTestQueue(env);
  });

  // --------------------------------------------------------------------------
  // subscribeSubconsumer / unsubscribeSubconsumer
  // --------------------------------------------------------------------------

  skipIfNoDb('subscribeSubconsumer returns 1 first call, 0 second', async () => {
    const sub = 'worker-1';
    const first = await env.client.subscribeSubconsumer(env.queue, env.consumer, sub);
    expect(first).toBe(1);
    const second = await env.client.subscribeSubconsumer(env.queue, env.consumer, sub);
    expect(second).toBe(0);
  });

  // --------------------------------------------------------------------------
  // receiveCoop happy path
  // --------------------------------------------------------------------------

  skipIfNoDb('receiveCoop receives messages and ack finishes the batch', async () => {
    const sub = 'worker-a';
    // Register coop main + member first so they observe the tick that
    // contains the events. Auto-registration on the first receiveCoop call
    // would mark `last_tick = current_tick`, missing the events sent before
    // that call.
    await env.client.subscribeSubconsumer(env.queue, env.consumer, sub);
    await env.client.send(env.queue, { type: 'coop.test', payload: { i: 1 } });
    await env.client.send(env.queue, { type: 'coop.test', payload: { i: 2 } });
    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receiveCoop(env.queue, env.consumer, sub, {
      maxMessages: 100,
    });
    expect(msgs.length).toBe(2);
    expect(typeof msgs[0]!.batchId).toBe('bigint');
    const acked = await env.client.ack(msgs[0]!.batchId);
    expect(acked).toBe(1);

    // After ack the batch is finished; a fresh poll returns empty.
    await advanceQueue(env.client, env.queue);
    const after = await env.client.receiveCoop(env.queue, env.consumer, sub, {
      maxMessages: 100,
    });
    expect(after).toEqual([]);
  });

  // --------------------------------------------------------------------------
  // Two subconsumers split batches without duplicate delivery
  // --------------------------------------------------------------------------

  skipIfNoDb('two subconsumers split batches without duplicate delivery', async () => {
    const subA = 'worker-a';
    const subB = 'worker-b';
    await env.client.subscribeSubconsumer(env.queue, env.consumer, subA);
    await env.client.subscribeSubconsumer(env.queue, env.consumer, subB);

    const total = 5;
    for (let i = 0; i < total; i++) {
      await env.client.send(env.queue, { type: 't', payload: { i } });
    }
    await advanceQueue(env.client, env.queue);

    const msgsA = await env.client.receiveCoop(env.queue, env.consumer, subA, {
      maxMessages: 100,
    });
    expect(msgsA.length).toBeGreaterThan(0);

    // While A still owns its batch, B requests one too. Force a second tick
    // so a fresh batch window is available, otherwise the cooperative
    // allocator returns no rows (one tick window per batch allocation).
    for (let i = 0; i < 3; i++) {
      await env.client.send(env.queue, { type: 't', payload: { j: i } });
    }
    await advanceQueue(env.client, env.queue);

    const msgsB = await env.client.receiveCoop(env.queue, env.consumer, subB, {
      maxMessages: 100,
    });

    // Two distinct ownership tokens.
    if (msgsB.length > 0) {
      expect(msgsB[0]!.batchId).not.toBe(msgsA[0]!.batchId);
    }
    // No msg_id appears under both subconsumers.
    const idsA = new Set(msgsA.map((m) => m.msgId));
    for (const m of msgsB) {
      expect(idsA.has(m.msgId)).toBe(false);
    }

    if (msgsA.length > 0) await env.client.ack(msgsA[0]!.batchId);
    if (msgsB.length > 0) await env.client.ack(msgsB[0]!.batchId);
  });

  // --------------------------------------------------------------------------
  // unsubscribeSubconsumer with active batch
  // --------------------------------------------------------------------------

  skipIfNoDb('unsubscribeSubconsumer default raises on active batch', async () => {
    const sub = 'worker-stuck';
    await env.client.subscribeSubconsumer(env.queue, env.consumer, sub);
    await env.client.send(env.queue, { type: 't', payload: { i: 1 } });
    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receiveCoop(env.queue, env.consumer, sub, {
      maxMessages: 100,
    });
    expect(msgs.length).toBe(1);

    await expect(
      env.client.unsubscribeSubconsumer(env.queue, env.consumer, sub),
    ).rejects.toThrow();

    // Cleanup: ack so teardown doesn't trip on dangling batch state.
    await env.client.ack(msgs[0]!.batchId);
  });

  skipIfNoDb('unsubscribeSubconsumer batchHandling=1 routes through retry', async () => {
    const sub = 'worker-flush';
    await env.client.subscribeSubconsumer(env.queue, env.consumer, sub);
    await env.client.send(env.queue, { type: 't', payload: { i: 1 } });
    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receiveCoop(env.queue, env.consumer, sub, {
      maxMessages: 100,
    });
    expect(msgs.length).toBe(1);

    const removed = await env.client.unsubscribeSubconsumer(env.queue, env.consumer, sub, {
      batchHandling: 1,
    });
    expect(removed).toBeGreaterThan(0);

    // Active batch was force-routed through retry; verify a row exists.
    const retry = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(retry.rows[0]!.count).toBe('1');
  });

  // --------------------------------------------------------------------------
  // touchSubconsumer
  // --------------------------------------------------------------------------

  skipIfNoDb('touchSubconsumer returns 1 on registered row', async () => {
    const sub = 'worker-heartbeat';
    await env.client.subscribeSubconsumer(env.queue, env.consumer, sub);
    const touched = await env.client.touchSubconsumer(env.queue, env.consumer, sub);
    expect(touched).toBe(1);
  });
});

// ----------------------------------------------------------------------------
// High-level Consumer with subconsumer
// ----------------------------------------------------------------------------

describe('Consumer with subconsumer (env-gated)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
    await env.client.unsubscribe(env.queue, env.consumer);
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownCoopTestQueue(env);
  });

  skipIfNoDb('dispatches handler and acks under cooperative path', async () => {
    await env.client.subscribeSubconsumer(env.queue, env.consumer, 'worker-1');
    await env.client.send(env.queue, { type: 'job', payload: { v: 1 } });
    await env.client.send(env.queue, { type: 'job', payload: { v: 2 } });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
      subconsumer: 'worker-1',
    });
    const seen: number[] = [];
    consumer.handle('job', async (msg) => {
      const p = JSON.parse(msg.payload) as { v: number };
      seen.push(p.v);
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && seen.length < 2) {
      await sleep(50);
    }
    ac.abort();
    await start;

    expect(seen.sort()).toEqual([1, 2]);
  });
});

// ----------------------------------------------------------------------------
// Construction-time argument validation (no DB)
// ----------------------------------------------------------------------------

describe('newConsumer cooperative options', () => {
  it('throws when deadInterval is set without subconsumer', () => {
    const fakeClient = {
      receive: vi.fn(async () => []),
      receiveCoop: vi.fn(async () => []),
      ack: vi.fn(async () => 1),
      nack: vi.fn(async () => undefined),
      newConsumer: undefined as unknown,
    };
    expect(() =>
      new Consumer(fakeClient as unknown as Client, 'q', 'c', {
        deadInterval: '5 minutes',
      }),
    ).toThrow(/deadInterval requires subconsumer/i);
  });
});

// ----------------------------------------------------------------------------
// Consumer with subconsumer routes via receiveCoop (in-memory mock)
// ----------------------------------------------------------------------------

describe('Consumer with subconsumer (in-memory mocks)', () => {
  it('calls receiveCoop instead of receive when subconsumer is set', async () => {
    const msg: Message = {
      msgId: 1n,
      batchId: 99n,
      type: 't',
      payload: '{}',
      retryCount: null,
      createdAt: new Date(),
      extra1: null,
      extra2: null,
      extra3: null,
      extra4: null,
    };
    let receiveCoopCalls = 0;
    const receiveCoop = vi.fn(
      async (
        _queue: string,
        _consumer: string,
        _subconsumer: string,
        _opts?: { maxMessages?: number; deadInterval?: string },
      ): Promise<Message[]> => {
        receiveCoopCalls += 1;
        return receiveCoopCalls === 1 ? [msg] : [];
      },
    );
    const fakeClient = {
      receive: vi.fn(async () => [] as Message[]),
      receiveCoop,
      ack: vi.fn(async () => 1),
      nack: vi.fn(async () => undefined),
    };
    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 10,
      subconsumer: 'w1',
      deadInterval: '5 minutes',
      logger: { warn: () => undefined, error: () => undefined },
    });
    let seen = 0;
    consumer.handle('t', async () => {
      seen += 1;
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);
    const deadline = Date.now() + 2000;
    while (Date.now() < deadline && seen < 1) {
      await sleep(10);
    }
    ac.abort();
    await start;

    expect(seen).toBe(1);
    expect(fakeClient.receive).not.toHaveBeenCalled();
    expect(fakeClient.receiveCoop).toHaveBeenCalled();
    // Args: (queue, name, subconsumer, { maxMessages, deadInterval })
    const call = fakeClient.receiveCoop.mock.calls[0]!;
    expect(call[0]).toBe('q');
    expect(call[1]).toBe('c');
    expect(call[2]).toBe('w1');
    expect(call[3]).toMatchObject({ deadInterval: '5 minutes' });
    expect(fakeClient.ack).toHaveBeenCalledWith(99n);
  });

  it('falls back to receive when subconsumer is unset', async () => {
    const fakeClient = {
      receive: vi.fn(async () => []),
      receiveCoop: vi.fn(async () => []),
      ack: vi.fn(async () => 1),
      nack: vi.fn(async () => undefined),
    };
    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 10,
      logger: { warn: () => undefined, error: () => undefined },
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);
    const deadline = Date.now() + 2000;
    while (Date.now() < deadline && fakeClient.receive.mock.calls.length === 0) {
      await sleep(10);
    }
    ac.abort();
    await start;

    expect(fakeClient.receive).toHaveBeenCalled();
    expect(fakeClient.receiveCoop).not.toHaveBeenCalled();
  });
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
