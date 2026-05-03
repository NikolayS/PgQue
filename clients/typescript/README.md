# pgque

TypeScript client for [PgQue](https://github.com/NikolayS/pgque) — the
PgQ-based universal PostgreSQL queue. Thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `receive`, `ack`, `nack` (plus
`subscribe` / `unsubscribe`).

## Install

For application code, install the published package with any npm-compatible
package manager:

```bash
npm install pgque
# or: bun add pgque
# or: pnpm add pgque
# or: yarn add pgque
```

Runtime requirements: Node.js 20+ and PostgreSQL 14+ with the PgQue schema
installed (`\i pgque.sql` — no extension required).

## Quickstart

```ts
import { connect } from 'pgque';

const client = await connect(process.env.DATABASE_URL!);
try {
  // One-time setup (e.g. in a migration)
  await client.rawPool.query(`select pgque.create_queue('orders')`);
  await client.subscribe('orders', 'order_worker');

  // Producer
  const eventId = await client.send('orders', {
    type: 'order.created',
    payload: { id: 42 },
  });
  const batchIds = await client.sendBatch('orders', 'order.created', [
    { id: 43 },
    { id: 44 },
  ]);
  console.log('published', eventId, batchIds);

  // High-level consumer with per-event-type dispatch.
  // msg.payload is raw JSON text — call JSON.parse() to get the object back.
  const consumer = client.newConsumer('orders', 'order_worker');
  consumer.handle('order.created', async (msg) => {
    const data = JSON.parse(msg.payload) as { id: number };
    console.log('got', msg.type, data);
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
| `client.sendBatch(queue, type, payloads)` | Publish a same-type batch atomically; returns event ids (`bigint[]`). |
| `client.receive(queue, consumer, max?)` | Fetch up to `max` (default 100) messages from the next batch. |
| `client.ack(batchId)` | Finish the batch. |
| `client.nack(batchId, msg, opts?)` | Single-message retry/DLQ. |
| `client.subscribe(queue, consumer)` | Wraps `pgque.register_consumer`. |
| `client.unsubscribe(queue, consumer)` | Wraps `pgque.unregister_consumer`. |
| `client.forceTick(queue)` | Bump the event-seq threshold so the next ticker run produces a tick. |
| `client.ticker(queue?)` | Run pgque ticker globally or for one queue; makes eligible events visible to consumers. |
| `client.newConsumer(queue, name, opts?)` | High-level poll loop. |
| `consumer.handle(eventType, fn)` | Register a handler. |
| `consumer.start(signal?)` | Run; resolves when `AbortSignal` aborts. |
| `client.close()` | Drain the pool. |

`Message.msgId`, `Message.batchId`, and the return values of `send()` /
`sendBatch()` are JS `bigint` to match PostgreSQL `bigint` losslessly.

### Consumer options

`client.newConsumer(queue, name, opts?)` accepts:

| Option | Default | Notes |
|---|---|---|
| `pollInterval` | `30000` (ms) | Sleep between empty polls. |
| `maxMessages` | `500` | Max messages returned per `pgque.receive` call. `500` matches PgQue's default `ticker_max_count`, which is the *threshold* at which the ticker fires — **not a hard ceiling on batch size**. This mitigates row loss when batches stay at or below the ticker threshold, but does **not** prevent it when batches exceed `maxMessages`: bursts that fire via `ticker_max_lag` after more than `ticker_max_count` events accumulate, or operator changes to `ticker_max_count`, can produce larger batches. The `pgque.ack(batch_id)` call finishes the whole batch (including unreturned rows), so any rows past `maxMessages` are skipped on ack. Size `maxMessages` to at least the queue's `ticker_max_count` for your workload. |
| `unknownHandlerPolicy` | `'nack'` | What to do when a message arrives whose `type` has no registered handler. `'nack'` (default) routes to retry / DLQ via `pgque.nack`. `'ack'` logs a warning and lets the batch ack absorb it (silent discard). |
| `logger` | `console` | Receives `warn` / `error` lines. |

### Payload coercion: `undefined` → JSON `null`

`client.send()` JSON-encodes `event.payload` before binding it as
`jsonb`. Because `JSON.stringify(undefined)` returns the JS literal
`undefined` (not the string `"null"`), the driver substitutes the JSON
literal `null` whenever the top-level `payload` is `undefined`:

```ts
// All three store the JSON value `null` in the queue:
await client.send('q', { type: 't', payload: null });
await client.send('q', { type: 't', payload: undefined });
await client.send('q', { type: 't' });
```

Inside an object, properties whose value is `undefined` are dropped by
`JSON.stringify` per the JSON spec. This is the standard JS behavior;
the driver does not try to override it:

```ts
await client.send('q', { type: 't', payload: { a: 1, b: undefined } });
// Stored as: {"a":1}
```

## Errors

All errors derive from `PgqueError`:

- `PgqueConnectionError` — connect failure
- `PgqueQueueNotFoundError` — caller forgot `pgque.create_queue`
- `PgqueConsumerNotFoundError` — consumer not subscribed
- `PgqueSqlError` — generic SQL failure (with `cause`)

## Caveats

### Global BIGINT parser mutation

Importing `pgque` calls `types.setTypeParser(20, ...)` at module load
time. This mutates the process-global `pg-types` parser table so that
**all** `pg.Pool` / `pg.Client` instances in the same Node.js process
will return PostgreSQL `bigint` columns as JS `bigint` instead of the
default string representation.

Practical impact:

- If other code in your process uses `pg` and relies on `bigint` coming
  back as a string (the `pg` default), those columns will silently change
  type after `pgque` is imported.
- The change is intentional: JS `bigint` is the correct representation for
  PostgreSQL `bigint` and avoids silent precision loss above
  `Number.MAX_SAFE_INTEGER`. The Go and Python pgque drivers behave the
  same way.
- If you cannot accept this side effect, do not import this package.

There is no opt-out once the module is loaded — Node.js module caches
mean the parser is set exactly once, regardless of how many times the
package is imported.

## Tests

The repository standardizes on Bun for TypeScript client development and CI
commands. The integration tests need a running PostgreSQL with the PgQue schema
installed and `pgque_admin`-equivalent privileges:

```bash
bun install --frozen-lockfile
PGQUE_TEST_DSN=postgresql://postgres:pgque_test@localhost/pgque_test \
  bun run test
```

Without `PGQUE_TEST_DSN` the integration tests skip.

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
