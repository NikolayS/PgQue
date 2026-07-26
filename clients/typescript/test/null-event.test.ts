// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from 'vitest';
import { connect, type Message } from '../src/index.js';
import {
  TEST_DSN,
  advanceQueue,
  randomSuffix,
  setupTestQueue,
  teardownCoopTestQueue,
  teardownTestQueue,
  type TestEnv,
} from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

describe('nullable message contract', () => {
  it('declares type and payload as nullable', () => {
    expectTypeOf<Message['type']>().toEqualTypeOf<string | null>();
    expectTypeOf<Message['payload']>().toEqualTypeOf<string | null>();
  });
});

describe('nullable messages (normal receive)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('preserves SQL NULL type and payload', async () => {
    const inserted = await env.client.rawPool.query<{ insert_event: bigint }>(
      'select pgque.insert_event($1, null, null) as insert_event',
      [env.queue],
    );
    await advanceQueue(env.client, env.queue);

    const [msg] = await env.client.receive(env.queue, env.consumer, 1);
    expect(msg?.msgId).toBe(inserted.rows[0]?.insert_event);
    expect(msg?.type).toBeNull();
    expect(msg?.payload).toBeNull();
    await env.client.ack(msg!.batchId);
  });
});

describe('nullable messages (cooperative receive)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    const client = await connect(TEST_DSN);
    const suffix = randomSuffix();
    env = {
      client,
      queue: `tsnull_${suffix}`,
      consumer: `tsnullconsumer_${suffix}`,
    };
    await client.rawPool.query('select pgque.create_queue($1)', [env.queue]);
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownCoopTestQueue(env);
  });

  skipIfNoDb('preserves SQL NULL type and payload', async () => {
    await env.client.subscribeSubconsumer(env.queue, env.consumer, 'worker-1');
    const inserted = await env.client.rawPool.query<{ insert_event: bigint }>(
      'select pgque.insert_event($1, null, null) as insert_event',
      [env.queue],
    );
    await advanceQueue(env.client, env.queue);

    const [msg] = await env.client.receiveCoop(env.queue, env.consumer, 'worker-1', {
      maxMessages: 1,
    });
    expect(msg?.msgId).toBe(inserted.rows[0]?.insert_event);
    expect(msg?.type).toBeNull();
    expect(msg?.payload).toBeNull();
    await env.client.ack(msg!.batchId);
  });
});
