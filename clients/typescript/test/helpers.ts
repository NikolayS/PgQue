// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import { randomBytes } from 'node:crypto';
import { connect, type Client } from '../src/index.js';

export const TEST_DSN = process.env.PGQUE_TEST_DSN;

/** Random suffix for disposable queue/consumer names. */
export function randomSuffix(): string {
  return randomBytes(4).toString('hex');
}

export interface TestEnv {
  client: Client;
  queue: string;
  consumer: string;
}

export async function setupTestQueue(): Promise<TestEnv> {
  if (!TEST_DSN) throw new Error('PGQUE_TEST_DSN not set');
  const client = await connect(TEST_DSN);
  const sfx = randomSuffix();
  const queue = `tstest_${sfx}`;
  const consumer = `tsconsumer_${sfx}`;
  await client.rawPool.query(`select pgque.create_queue($1)`, [queue]);
  await client.subscribe(queue, consumer);
  return { client, queue, consumer };
}

export async function teardownTestQueue(env: TestEnv): Promise<void> {
  try {
    await env.client.rawPool.query(`select pgque.drop_queue($1, true)`, [env.queue]);
  } finally {
    await env.client.close();
  }
}

/** Test helper: force the tick threshold, then run the per-queue ticker. Composes two driver primitives — not a public Client API. */
export async function advanceQueue(client: Client, queue: string): Promise<void> {
  await client.forceNextTick(queue);
  await client.ticker(queue);
}

/**
 * Teardown variant for cooperative tests: nukes any cooperative subscription
 * rows under `queue` directly so `drop_queue(..., true)` does not trip on the
 * `cannot unregister cooperative main` guard. Direct table writes are
 * test-only — production code should always go through
 * `unsubscribe_subconsumer`.
 */
export async function teardownCoopTestQueue(env: TestEnv): Promise<void> {
  try {
    await env.client.rawPool.query(
      `delete from pgque.subscription
        where sub_queue = (select queue_id from pgque.queue where queue_name = $1)`,
      [env.queue],
    );
    await env.client.rawPool.query(
      `delete from pgque.retry_queue
        where ev_queue = (select queue_id from pgque.queue where queue_name = $1)`,
      [env.queue],
    );
    await env.client.rawPool.query(
      `delete from pgque.dead_letter
        where dl_queue_id = (select queue_id from pgque.queue where queue_name = $1)`,
      [env.queue],
    );
    await env.client.rawPool.query(`select pgque.drop_queue($1, true)`, [env.queue]);
  } finally {
    await env.client.close();
  }
}
