# pgque

TypeScript client for [PgQue](https://github.com/NikolayS/pgque) — the
PgQ-based universal PostgreSQL queue. Thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `receive`, `ack`, `nack` (plus
`subscribe` / `unsubscribe`).

## Install

```bash
npm install pgque
```

Requires Node.js 20+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Quickstart

```ts
import { connect } from 'pgque';

const client = await connect(process.env.DATABASE_URL!);
try {
  // One-time setup (e.g. in a migration)
  await client.rawPool.query(`select pgque.create_queue('orders')`);
  await client.subscribe('orders', 'order_worker');

  // Producer
  await client.send('orders', {
    type: 'order.created',
    payload: { id: 42 },
  });

  // High-level consumer with per-event-type dispatch
  const consumer = client.newConsumer('orders', 'order_worker');
  consumer.handle('order.created', async (msg) => {
    console.log('got', msg.type, msg.payload);
  });

  const ac = new AbortController();
  process.on('SIGINT', () => ac.abort());
  await consumer.start(ac.signal);
} finally {
  await client.close();
}
```

## API

| Method | Description |
|---|---|
| `connect(dsn, poolOptions?)` | Connect via `pg.Pool`. Eagerly probes the connection. |
| `client.send(queue, event)` | Publish; returns event id (`bigint`). |
| `client.receive(queue, consumer, max?)` | Fetch up to `max` (default 100) messages from the next batch. |
| `client.ack(batchId)` | Finish the batch. |
| `client.nack(batchId, msg, opts?)` | Single-message retry/DLQ. |
| `client.subscribe(queue, consumer)` | Wraps `pgque.register_consumer`. |
| `client.unsubscribe(queue, consumer)` | Wraps `pgque.unregister_consumer`. |
| `client.newConsumer(queue, name, opts?)` | High-level poll loop. |
| `consumer.handle(eventType, fn)` | Register a handler. |
| `consumer.start(signal?)` | Run; resolves when `AbortSignal` aborts. |
| `client.close()` | Drain the pool. |

`Message.msgId`, `Message.batchId`, and the return value of `send()` are
JS `bigint` to match PostgreSQL `bigint` losslessly.

## Errors

All errors derive from `PgqueError`:

- `PgqueConnectionError` — connect failure
- `PgqueQueueNotFoundError` — caller forgot `pgque.create_queue`
- `PgqueConsumerNotFoundError` — consumer not subscribed
- `PgqueSqlError` — generic SQL failure (with `cause`)

## Side effect on import

Importing `pgque` registers a global `pg-types` parser for `bigint`
(oid 20) that promotes the column to JS `bigint`. This avoids silent
precision loss but also affects any other `pg`-using code in the same
process. If you cannot accept that side effect, do not import this
package.

## Tests

The integration tests need a running PostgreSQL with the PgQue schema
installed and `pgque_admin`-equivalent privileges:

```bash
PGQUE_TEST_DSN=postgres://postgres:pgque_test@localhost/pgque_test \
  npm test
```

Without `PGQUE_TEST_DSN` the integration tests skip.

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
