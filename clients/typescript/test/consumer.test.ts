// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { Consumer } from '../src/consumer.js';
import type { Client } from '../src/client.js';
import type { Message } from '../src/types.js';
import { TEST_DSN, setupTestQueue, teardownTestQueue, advanceQueue, type TestEnv } from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

describe('Consumer (env-gated)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('dispatches messages to the matching handler', async () => {
    await env.client.send(env.queue, { type: 'a', payload: { v: 1 } });
    await env.client.send(env.queue, { type: 'b', payload: { v: 2 } });
    await env.client.send(env.queue, { type: 'a', payload: { v: 3 } });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
    });
    const seen: Array<{ type: string; v: number }> = [];
    consumer.handle('a', async (msg) => {
      const p = JSON.parse(msg.payload) as { v: number };
      seen.push({ type: 'a', v: p.v });
    });
    consumer.handle('b', async (msg) => {
      const p = JSON.parse(msg.payload) as { v: number };
      seen.push({ type: 'b', v: p.v });
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);

    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && seen.length < 3) {
      await sleep(50);
    }
    ac.abort();
    await startPromise;

    expect(seen).toHaveLength(3);
    expect(seen.filter((s) => s.type === 'a')).toHaveLength(2);
    expect(seen.filter((s) => s.type === 'b')).toHaveLength(1);
  });

  skipIfNoDb('handler error nacks just that message; batch still acks', async () => {
    await env.client.send(env.queue, { type: 'fail', payload: { i: 0 } });
    await env.client.send(env.queue, { type: 'fail', payload: { i: 1 } });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
      logger: { warn: () => undefined, error: () => undefined },
    });
    let calls = 0;
    consumer.handle('fail', async () => {
      calls += 1;
      if (calls === 1) throw new Error('synthetic');
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && calls < 2) {
      await sleep(50);
    }
    ac.abort();
    await start;

    expect(calls).toBeGreaterThanOrEqual(2);

    const retry = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(retry.rows[0]!.count).toBe('1'); // exactly the failing message
  });

  skipIfNoDb('AbortSignal stops the poll loop promptly', async () => {
    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 60_000, // would block forever if abort were ignored
    });
    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    setTimeout(() => ac.abort(), 100);
    const t0 = Date.now();
    await start;
    const elapsed = Date.now() - t0;

    expect(elapsed).toBeLessThan(2000);
  });

  skipIfNoDb('LISTEN/NOTIFY wakes consumer before pollInterval elapses', async () => {
    // Use a very long pollInterval so only a NOTIFY can wake the consumer in time.
    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 60_000,
    });
    const seen: Message[] = [];
    consumer.handle('notify.test', async (msg) => {
      seen.push(msg);
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);

    // Give the consumer a moment to set up its LISTEN connection.
    await sleep(200);

    // Send a message and advance the queue so the ticker emits a NOTIFY.
    await env.client.send(env.queue, { type: 'notify.test', payload: { x: 1 } });
    await advanceQueue(env.client, env.queue);

    // Assert delivery within 2 s — clearly via NOTIFY, not the 60 s poll.
    const deadline = Date.now() + 2000;
    while (Date.now() < deadline && seen.length === 0) {
      await sleep(50);
    }

    ac.abort();
    await startPromise;

    expect(seen).toHaveLength(1);
  }, 10_000);

  skipIfNoDb('unhandled message types are nacked, not silently consumed', async () => {
    await env.client.send(env.queue, { type: 'unknown', payload: { v: 1 } });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
      logger: { warn: () => undefined, error: () => undefined },
    });
    // No handlers registered.
    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    // give it a couple of poll cycles
    await sleep(400);
    ac.abort();
    await start;

    const retry = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(retry.rows[0]!.count).toBe('1');
  });
});

describe('Consumer (in-memory mocks)', () => {
  it('does not call ack when nack fails for a handler error', async () => {
    const msg: Message = {
      msgId: 1n,
      batchId: 99n,
      type: 'will_fail',
      payload: '{}',
      retryCount: null,
      createdAt: new Date(),
      extra1: null,
      extra2: null,
      extra3: null,
      extra4: null,
    };

    let receiveCalls = 0;
    const fakeClient = {
      receive: vi.fn(async () => {
        receiveCalls += 1;
        // First poll returns the message; subsequent polls return empty so
        // the loop idles until aborted.
        return receiveCalls === 1 ? [msg] : [];
      }),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => {
        throw new Error('synthetic nack failure');
      }),
    };

    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 10,
      logger: { warn: () => undefined, error: () => undefined },
    });
    consumer.handle('will_fail', async () => {
      throw new Error('handler boom');
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);
    // Wait until the consumer has observed the failing nack at least once.
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && fakeClient.nack.mock.calls.length === 0) {
      await sleep(10);
    }
    ac.abort();
    await startPromise;

    expect(fakeClient.nack).toHaveBeenCalledTimes(1);
    // Strong assertion: ack must NEVER be called for the batch when its
    // nack failed — the batch should be redelivered on the next poll.
    expect(fakeClient.ack).toHaveBeenCalledTimes(0);
    expect(fakeClient.ack.mock.calls.length).toBe(0);
  });

  it('passes the safe default maxMessages to receive', async () => {
    const fakeClient = {
      receive: vi.fn(async () => []),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => undefined),
    };
    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 10,
      logger: { warn: () => undefined, error: () => undefined },
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && fakeClient.receive.mock.calls.length === 0) {
      await sleep(10);
    }
    ac.abort();
    await startPromise;

    expect(fakeClient.receive).toHaveBeenCalled();
    expect(fakeClient.receive.mock.calls[0]).toEqual(['q', 'c', 2_147_483_647]);
  });

  it('NOTIFY wakes the poll loop before the long sleep elapses (stub pg.Client)', async () => {
    // Build a fake pg.Client stub that emits 'notification' events.
    const notifyEmitter = new EventEmitter();
    let listenCalled = false;
    let unlistenCalled = false;

    const fakePgClient = {
      connect: vi.fn(async () => undefined),
      end: vi.fn(async () => undefined),
      query: vi.fn(async (sql: string) => {
        if (/^\s*LISTEN/i.test(sql)) listenCalled = true;
        if (/^\s*UNLISTEN/i.test(sql)) unlistenCalled = true;
        return { rows: [] };
      }),
      on: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
        notifyEmitter.on(event, handler);
      }),
      once: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
        notifyEmitter.once(event, handler);
      }),
      removeListener: vi.fn((event: string, handler: (...args: unknown[]) => void) => {
        notifyEmitter.removeListener(event, handler);
      }),
    };

    // Consumer with a long pollInterval — only NOTIFY should trigger a cycle.
    const fakeClient = {
      receive: vi.fn(async () => []),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => undefined),
    };

    const consumer = new Consumer(
      fakeClient as unknown as Client,
      'orders',
      'worker',
      {
        pollInterval: 60_000,
        logger: { warn: () => undefined, error: () => undefined },
        _listenClientFactory: async () => fakePgClient as unknown as import('pg').Client,
      },
    );

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);

    // Wait for LISTEN to be registered.
    const listenDeadline = Date.now() + 1000;
    while (!listenCalled && Date.now() < listenDeadline) {
      await sleep(10);
    }
    expect(listenCalled).toBe(true);

    // Record how many receive() calls happened before we fire the notification.
    const callsBefore = fakeClient.receive.mock.calls.length;

    // Fire a simulated NOTIFY from the server.
    notifyEmitter.emit('notification', { channel: 'pgque_orders', payload: '42' });

    // The consumer should wake up and call receive() again within 500 ms.
    const wakeDeadline = Date.now() + 500;
    while (fakeClient.receive.mock.calls.length <= callsBefore && Date.now() < wakeDeadline) {
      await sleep(10);
    }

    ac.abort();
    await startPromise;

    expect(fakeClient.receive.mock.calls.length).toBeGreaterThan(callsBefore);
    expect(unlistenCalled).toBe(true);
  });

  it('passes configured maxMessages to receive', async () => {
    const fakeClient = {
      receive: vi.fn(async () => []),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => undefined),
    };
    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      maxMessages: 123,
      pollInterval: 10,
      logger: { warn: () => undefined, error: () => undefined },
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && fakeClient.receive.mock.calls.length === 0) {
      await sleep(10);
    }
    ac.abort();
    await startPromise;

    expect(fakeClient.receive).toHaveBeenCalled();
    expect(fakeClient.receive.mock.calls[0]).toEqual(['q', 'c', 123]);
  });

  it('does not call ack when nack fails for an unknown event type', async () => {
    const msg: Message = {
      msgId: 2n,
      batchId: 100n,
      type: 'unknown_type',
      payload: '{}',
      retryCount: null,
      createdAt: new Date(),
      extra1: null,
      extra2: null,
      extra3: null,
      extra4: null,
    };

    let receiveCalls = 0;
    const fakeClient = {
      receive: vi.fn(async () => {
        receiveCalls += 1;
        return receiveCalls === 1 ? [msg] : [];
      }),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => {
        throw new Error('synthetic nack failure');
      }),
    };

    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 10,
      logger: { warn: () => undefined, error: () => undefined },
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && fakeClient.nack.mock.calls.length === 0) {
      await sleep(10);
    }
    ac.abort();
    await startPromise;

    expect(fakeClient.nack).toHaveBeenCalledTimes(1);
    expect(fakeClient.ack).toHaveBeenCalledTimes(0);
  });
});

describe('Consumer.unknownHandlerPolicy=ack (env-gated)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('acks unknown event types via the batch when policy is "ack"', async () => {
    const unknownId = await env.client.send(env.queue, {
      type: 'unhandled.kind',
      payload: { v: 7 },
    });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
      unknownHandlerPolicy: 'ack',
      logger: { warn: () => undefined, error: () => undefined },
    });
    // Intentionally no handlers registered.

    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    // Allow the consumer to drain at least one batch.
    await sleep(400);
    ac.abort();
    await start;

    // No retry rows: opt-in 'ack' must NOT route to retry_queue.
    const retry = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(retry.rows[0]!.count).toBe('0');

    // No DLQ rows either.
    const dlq = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.dead_letter dl
         join pgque.queue q on q.queue_id = dl.dl_queue_id
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(dlq.rows[0]!.count).toBe('0');

    // Batch advanced: a fresh receive() must not return the unknown msg_id.
    await advanceQueue(env.client, env.queue);
    const after = await env.client.receive(env.queue, env.consumer, 100);
    for (const m of after) {
      expect(m.msgId).not.toBe(unknownId);
    }
  });
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
