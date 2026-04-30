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
