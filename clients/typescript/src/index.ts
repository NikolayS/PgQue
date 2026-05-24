// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * pgque is the TypeScript client for PgQue, the PgQ-based universal
 * PostgreSQL queue. It is a thin, idiomatic wrapper over the `pgque-api`
 * SQL functions: `send`, `send_batch`, `subscribe`, `unsubscribe`,
 * `receive`, `ack`, `nack`, `ticker`, `ticker_all`, and
 * `force_next_tick`.
 *
 * Quick start:
 * ```ts
 * import { connect } from 'pgque';
 *
 * const client = await connect(process.env.DATABASE_URL!);
 * try {
 *   await client.subscribe('orders', 'order_worker');
 *   await client.send('orders', { type: 'order.created', payload: { id: 42 } });
 *
 *   const consumer = client.newConsumer('orders', 'order_worker');
 *   consumer.handle('order.created', async (msg) => {
 *     console.log('got', msg.type, msg.payload);
 *   });
 *
 *   const ac = new AbortController();
 *   process.on('SIGINT', () => ac.abort());
 *   await consumer.start(ac.signal);
 * } finally {
 *   await client.close();
 * }
 * ```
 *
 * **bigint columns:** `msg_id`, `batch_id`, and `send()` / `sendBatch()`
 * return values are JS `bigint`. The parser is scoped to pgque's own pool
 * and does not affect other `pg` clients in the same process.
 */

export { Client, connect, pgqueTypes } from './client.js';
export { Consumer, DEFAULT_MAX_MESSAGES } from './consumer.js';
export {
  PgqueBatchNotFoundError,
  PgqueConnectionError,
  PgqueConsumerNotFoundError,
  PgqueError,
  PgqueQueueNotFoundError,
  PgqueSqlError,
} from './errors.js';
export type { ConsumerOptions, Event, HandlerFunc, Message, NackOptions } from './types.js';
