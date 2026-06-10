// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Consumer, DEFAULT_MAX_MESSAGES } from '../src/consumer.js';
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

  // Coverage gap (#a): the formatted handler-error reason must reach
  // pgque.dead_letter.dl_reason. We force `queue_max_retries=0` so the
  // first nack routes straight to the DLQ, then assert the stored reason.
  skipIfNoDb('handler-error reason lands in dead_letter.dl_reason', async () => {
    await env.client.rawPool.query(
      `update pgque.queue set queue_max_retries = 0 where queue_name = $1`,
      [env.queue],
    );

    await env.client.send(env.queue, { type: 'boom', payload: { v: 1 } });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
      logger: { warn: () => undefined, error: () => undefined },
    });
    consumer.handle('boom', async () => {
      throw new Error('kaboom');
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    const deadline = Date.now() + 4000;
    while (Date.now() < deadline) {
      const dlq = await env.client.rawPool.query<{ count: string }>(
        `select count(*)::text as count
           from pgque.dead_letter dl
           join pgque.queue q on q.queue_id = dl.dl_queue_id
          where q.queue_name = $1`,
        [env.queue],
      );
      if (dlq.rows[0]!.count !== '0') break;
      await sleep(50);
    }
    ac.abort();
    await start;

    const dlq = await env.client.rawPool.query<{ dl_reason: string }>(
      `select dl.dl_reason
         from pgque.dead_letter dl
         join pgque.queue q on q.queue_id = dl.dl_queue_id
        where q.queue_name = $1
        order by dl.dl_id desc
        limit 1`,
      [env.queue],
    );
    expect(dlq.rows.length).toBe(1);
    expect(dlq.rows[0]!.dl_reason).toBe('handler error: kaboom');
  });

  // Coverage gap (#b): the default Consumer must drain the entire underlying
  // PgQ batch in one poll. Mirrors Go's
  // TestConsumer_WithMaxMessages_FetchesEntireBatch.
  skipIfNoDb('default Consumer drains the whole batch in one poll', async () => {
    const total = 105;
    for (let i = 0; i < total; i++) {
      await env.client.send(env.queue, { type: 'bulk', payload: { i } });
    }
    await advanceQueue(env.client, env.queue);

    // Default options: no maxMessages override, no pollInterval override.
    // pollInterval defaults to 30s, so a second poll is effectively
    // impossible within the test window.
    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      logger: { warn: () => undefined, error: () => undefined },
    });
    let seen = 0;
    consumer.handle('bulk', async () => {
      seen += 1;
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    const deadline = Date.now() + 8000;
    while (Date.now() < deadline && seen < total) {
      await sleep(25);
    }
    ac.abort();
    await start;

    expect(seen).toBe(total);
  });

  // Coverage gap (#c): partial-batch success+failure. Three messages (ok,
  // boom, ok); the failing one should reappear on the next poll while the
  // succeeding ones are acked away.
  skipIfNoDb('partial-batch failure: only the failing message reappears', async () => {
    await env.client.send(env.queue, { type: 'ok', payload: { i: 0 } });
    await env.client.send(env.queue, { type: 'boom', payload: { i: 1 } });
    await env.client.send(env.queue, { type: 'ok', payload: { i: 2 } });
    await advanceQueue(env.client, env.queue);

    const consumer = env.client.newConsumer(env.queue, env.consumer, {
      pollInterval: 50,
      logger: { warn: () => undefined, error: () => undefined },
    });

    let okSeen = 0;
    let boomSeen = 0;
    consumer.handle('ok', async () => {
      okSeen += 1;
    });
    consumer.handle('boom', async () => {
      boomSeen += 1;
      throw new Error('boom-err');
    });

    const ac = new AbortController();
    const start = consumer.start(ac.signal);

    // Wait until the first batch has been processed: both oks ran and the
    // initial nack/ack cycle for boom has completed (retry row exists).
    const firstDeadline = Date.now() + 6000;
    while (Date.now() < firstDeadline && (okSeen < 2 || boomSeen < 1)) {
      await sleep(25);
    }
    expect(okSeen).toBe(2);
    expect(boomSeen).toBeGreaterThanOrEqual(1);

    // The retry queue defaults to ev_retry_after = now() + 60s, so the
    // boom message is not yet redeliverable. Pull it forward, ask PgQ to
    // move the retry row back into the main queue, then force_tick +
    // ticker so the next consumer poll observes it.
    await env.client.rawPool.query(
      `update pgque.retry_queue
          set ev_retry_after = now() - interval '1 second'
        where ev_queue = (select queue_id from pgque.queue where queue_name = $1)`,
      [env.queue],
    );
    await env.client.rawPool.query(`select pgque.maint_retry_events()`);
    await advanceQueue(env.client, env.queue);

    // Now wait for redelivery of `boom`.
    const redeliveredDeadline = Date.now() + 6000;
    while (Date.now() < redeliveredDeadline && boomSeen < 2) {
      await sleep(25);
    }
    ac.abort();
    await start;

    expect(okSeen).toBe(2); // both `ok` messages acked, never redelivered
    expect(boomSeen).toBeGreaterThanOrEqual(2); // initial + at least one retry

    // Any rows still in retry_queue for this queue must be the `boom`
    // message — `ok` must never have been redirected to retry/DLQ.
    const retryRows = await env.client.rawPool.query<{ ev_type: string }>(
      `select rq.ev_type
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [env.queue],
    );
    for (const r of retryRows.rows) {
      expect(r.ev_type).toBe('boom');
    }
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
    expect(fakeClient.receive.mock.calls[0]).toEqual(['q', 'c', DEFAULT_MAX_MESSAGES]);
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

  it('passes configured retryAfter to consumer-issued nack', async () => {
    const msg: Message = {
      msgId: 3n,
      batchId: 101n,
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
      nack: vi.fn(async (_batchId: bigint, _msg: Message, _opts?: unknown) => undefined),
    };

    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 10,
      retryAfter: 5,
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
    expect(fakeClient.nack.mock.calls[0]?.[2]).toMatchObject({ retryAfter: 5 });
  });

  it('rejects negative retryAfter at construction', () => {
    const fakeClient = {
      receive: vi.fn(async () => []),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => undefined),
    };

    expect(
      () =>
        new Consumer(fakeClient as unknown as Client, 'q', 'c', {
          retryAfter: -1,
        }),
    ).toThrow(/retryAfter/);
  });

  it('sleeps pollInterval before re-polling after a nack failure', async () => {
    const msg: Message = {
      msgId: 4n,
      batchId: 102n,
      type: 'will_fail',
      payload: '{}',
      retryCount: null,
      createdAt: new Date(),
      extra1: null,
      extra2: null,
      extra3: null,
      extra4: null,
    };

    // receive always returns the same batch (PgQ redelivers an unfinished
    // batch instantly) and nack always fails, so an unfixed loop re-polls
    // at full speed. With a 60s pollInterval, a backoff-respecting loop
    // must call receive exactly once within the observation window.
    const fakeClient = {
      receive: vi.fn(async () => [msg]),
      ack: vi.fn(async () => undefined),
      nack: vi.fn(async () => {
        throw new Error('synthetic persistent nack failure');
      }),
    };

    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 60_000,
      logger: { warn: () => undefined, error: () => undefined },
    });
    consumer.handle('will_fail', async () => {
      throw new Error('handler boom');
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && fakeClient.nack.mock.calls.length === 0) {
      await sleep(10);
    }
    // Give an unfixed (hot-looping) consumer time to re-poll.
    await sleep(300);
    ac.abort();
    await startPromise;

    expect(fakeClient.receive).toHaveBeenCalledTimes(1);
    expect(fakeClient.nack).toHaveBeenCalledTimes(1);
    expect(fakeClient.ack).toHaveBeenCalledTimes(0);
  });

  it('sleeps pollInterval before re-polling after an ack error', async () => {
    const msg: Message = {
      msgId: 5n,
      batchId: 103n,
      type: 'fine',
      payload: '{}',
      retryCount: null,
      createdAt: new Date(),
      extra1: null,
      extra2: null,
      extra3: null,
      extra4: null,
    };

    let handlerCalls = 0;
    const fakeClient = {
      receive: vi.fn(async () => [msg]),
      ack: vi.fn(async () => {
        throw new Error('synthetic ack failure');
      }),
      nack: vi.fn(async () => undefined),
    };

    const consumer = new Consumer(fakeClient as unknown as Client, 'q', 'c', {
      pollInterval: 60_000,
      logger: { warn: () => undefined, error: () => undefined },
    });
    consumer.handle('fine', async () => {
      handlerCalls += 1;
    });

    const ac = new AbortController();
    const startPromise = consumer.start(ac.signal);
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && fakeClient.ack.mock.calls.length === 0) {
      await sleep(10);
    }
    // Give an unfixed (hot-looping) consumer time to re-poll and re-run
    // the handler with duplicate side effects.
    await sleep(300);
    ac.abort();
    await startPromise;

    expect(fakeClient.receive).toHaveBeenCalledTimes(1);
    expect(fakeClient.ack).toHaveBeenCalledTimes(1);
    expect(handlerCalls).toBe(1);
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
