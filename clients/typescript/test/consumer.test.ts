// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { TEST_DSN, setupTestQueue, teardownTestQueue, type TestEnv } from './helpers.js';

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
    await env.client.flush(env.queue);

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
    await env.client.flush(env.queue);

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
    await env.client.flush(env.queue);

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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
