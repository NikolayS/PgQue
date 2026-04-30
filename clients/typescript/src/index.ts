// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * pgque is the TypeScript client for PgQue, the PgQ-based universal
 * PostgreSQL queue. It is a thin, idiomatic wrapper over the `pgque-api`
 * SQL functions: `send`, `receive`, `ack`, `nack`, plus `subscribe` /
 * `unsubscribe`.
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
 * **Side effect on import:** registers a global `pg-types` parser for
 * `bigint` (oid 20) that promotes the column to JS `bigint`. This avoids
 * silent precision loss but also affects any other `pg`-using code in
 * the same process. Documented here for transparency.
 */

export { Client, connect } from './client.js';
export { Consumer } from './consumer.js';
export {
  PgqueConnectionError,
  PgqueConsumerNotFoundError,
  PgqueError,
  PgqueQueueNotFoundError,
  PgqueSqlError,
} from './errors.js';
export type { ConsumerOptions, Event, HandlerFunc, Message, NackOptions } from './types.js';
