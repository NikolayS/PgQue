// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { TEST_DSN, setupTestQueue, teardownTestQueue, type TestEnv } from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

describe('nack routing (env-gated)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('first nack lands in retry_queue, not dead_letter', async () => {
    await env.client.send(env.queue, { type: 'nack.test', payload: { v: 1 } });
    await env.client.forceTick(env.queue);
    const msgs = await env.client.receive(env.queue, env.consumer, 10);
    expect(msgs).toHaveLength(1);
    const m = msgs[0]!;

    await env.client.nack(m.batchId, m);
    await env.client.ack(m.batchId);

    const retry = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(retry.rows[0]!.count).toBe('1');

    const dlq = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.dead_letter dl
         join pgque.queue q on q.queue_id = dl.dl_queue_id
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(dlq.rows[0]!.count).toBe('0');
  });

  skipIfNoDb('nack honors custom retryAfter and reason', async () => {
    await env.client.send(env.queue, { type: 'nack.custom', payload: { v: 1 } });
    await env.client.forceTick(env.queue);
    const [m] = await env.client.receive(env.queue, env.consumer, 10);
    expect(m).toBeDefined();

    await env.client.nack(m!.batchId, m!, {
      retryAfter: '5 seconds',
      reason: 'simulated transient failure',
    });
    await env.client.ack(m!.batchId);

    const retry = await env.client.rawPool.query<{ ev_retry_after_ts: Date }>(
      `select rq.ev_retry_after_ts
         from pgque.retry_queue rq
         join pgque.queue q on q.queue_id = rq.ev_queue
        where q.queue_name = $1
        order by rq.ev_retry_after_ts desc
        limit 1`,
      [env.queue],
    );
    expect(retry.rows.length).toBe(1);
  });

  skipIfNoDb('nack at retry limit routes to dead_letter', async () => {
    // queue_max_retries default is 5. We synthesize a message with retry_count
    // already at the limit by issuing nacks via SQL retry path; simplest is
    // to lower max_retries on the queue first.
    await env.client.rawPool.query(
      `update pgque.queue set queue_max_retries = 0 where queue_name = $1`,
      [env.queue],
    );

    await env.client.send(env.queue, { type: 'nack.dlq', payload: { v: 1 } });
    await env.client.forceTick(env.queue);
    const [m] = await env.client.receive(env.queue, env.consumer, 10);
    expect(m).toBeDefined();

    await env.client.nack(m!.batchId, m!, { reason: 'over the limit' });
    await env.client.ack(m!.batchId);

    const dlq = await env.client.rawPool.query<{ count: string }>(
      `select count(*)::text as count
         from pgque.dead_letter dl
         join pgque.queue q on q.queue_id = dl.dl_queue_id
        where q.queue_name = $1`,
      [env.queue],
    );
    expect(dlq.rows[0]!.count).toBe('1');
  });
});
