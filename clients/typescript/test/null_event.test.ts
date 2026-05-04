// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { Message } from '../src/types.js';
import { TEST_DSN, advanceQueue, setupTestQueue, teardownTestQueue, type TestEnv } from './helpers.js';

const skipIfNoDb = TEST_DSN ? it : it.skip;

// Type-level guard: Message.type and Message.payload must accept null.
// If either field is `string` (non-nullable), assigning null below is a
// TypeScript error and `tsc --noEmit` fails -- this is the failing test
// that drives the type-shape change.
//
// Regression for NikolayS/pgque#143.
type _AssertNullableType = Message extends { type: infer T }
  ? null extends T
    ? true
    : never
  : never;
type _AssertNullablePayload = Message extends { payload: infer P }
  ? null extends P
    ? true
    : never
  : never;
const _typeNullable: _AssertNullableType = true;
const _payloadNullable: _AssertNullablePayload = true;
void _typeNullable;
void _payloadNullable;

// Regression for NikolayS/pgque#143: the low-level PgQ primitive
// pgque.insert_event(queue, null, null) can produce a row with
// SQL-NULL ev_type and ev_data. The driver's Message type and row
// mapper must tolerate that shape: type and payload come back as
// null, and ack/nack still work on the surrounding batch.
describe('Receive: NULL ev_type / ev_data (#143)', () => {
  let env: TestEnv;

  beforeEach(async () => {
    if (!TEST_DSN) return;
    env = await setupTestQueue();
  });

  afterEach(async () => {
    if (!TEST_DSN) return;
    await teardownTestQueue(env);
  });

  skipIfNoDb('returns null type and payload without throwing', async () => {
    // Bypass send() and call the low-level primitive that yields
    // SQL-NULL ev_type / ev_data.
    await env.client.rawPool.query(
      'select pgque.insert_event($1, null::text, null::text)',
      [env.queue],
    );
    await advanceQueue(env.client, env.queue);

    const msgs = await env.client.receive(env.queue, env.consumer, 10);
    expect(msgs).toHaveLength(1);
    const m = msgs[0]!;
    expect(m.type).toBeNull();
    expect(m.payload).toBeNull();

    await env.client.ack(m.batchId);
  });
});
