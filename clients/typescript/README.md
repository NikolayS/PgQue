# pgque

TypeScript client for [PgQue](https://github.com/NikolayS/pgque) — the
PgQ-based universal PostgreSQL queue. Thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `receive`, `ack`, `nack` (plus
`subscribe` / `unsubscribe`).

## Install

After the first TypeScript client release, install the published package with
any npm-compatible package manager:

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
| `client.receive(queue, consumer, max?)` | Fetch up to `max` (default 100) messages from the next batch. If you later call `ack(batchId)`, PgQue finishes the whole underlying batch, including rows beyond `max`; size `max` for your queue or use the high-level consumer default. |
| `client.ack(batchId)` | Finish the batch. Returns `1` on success, `0` if the batch was already finished or not found (stale/double ack — log at warn level, not an error). |
| `client.nack(batchId, msg, opts?)` | Single-message retry/DLQ. |
| `client.subscribe(queue, consumer)` | Wraps `pgque.register_consumer`. |
| `client.unsubscribe(queue, consumer)` | Wraps `pgque.unregister_consumer`. |
| `client.ticker(queue)` | Per-queue ticker; returns the new tick id (`bigint`) or `null` when no tick was needed. Wraps `pgque.ticker(queue text)`. |
| `client.tickerAll()` | Global ticker across all eligible queues; returns count of queues ticked (`number`). Wraps `pgque.ticker()`. |
| `client.forceTick(queue)` | Bump event-seq threshold so the next `ticker(queue)` produces a tick; returns the last tick id (`bigint`) or `null` on a brand-new queue. Wraps `pgque.force_tick(queue text)`. |
| `client.newConsumer(queue, name, opts?)` | High-level poll loop. |
| `consumer.handle(eventType, fn)` | Register a handler. |
| `consumer.start(signal?)` | Run; resolves when `AbortSignal` aborts. |
| `client.close()` | Drain the pool. |

`Message.msgId`, `Message.batchId`, and the return values of `send()`,
`sendBatch()`, `ticker(queue)`, and `forceTick(queue)` are JS `bigint` to
match PostgreSQL `bigint` losslessly.

## Errors

All errors derive from `PgqueError`:

- `PgqueConnectionError` — connect failure
- `PgqueQueueNotFoundError` — caller forgot `pgque.create_queue`
- `PgqueConsumerNotFoundError` — consumer not subscribed
- `PgqueSqlError` — generic SQL failure (with `cause`)

## Caveats

### `ack()` returns a rowcount, not void

`client.ack(batchId)` returns `Promise<number>`. The value is `1` when the
batch was active and has been finished, or `0` when the batch was not found or
had already been finished (stale/double ack). A `0` result is not a SQL error —
the promise resolves normally. Callers that need to detect double-ack should
check the return value; the high-level `Consumer` logs a warning when it sees
`0`.

### bigint columns

`Message.msgId`, `Message.batchId`, and the return values of `send()` /
`sendBatch()` are JS `bigint`. The `int8` → `bigint` parser is registered
only on pgque's internal pool via a per-pool `CustomTypesConfig` — it
does **not** touch the process-global `pg-types` table. Other `pg.Pool`
or `pg.Client` instances in the same process are unaffected.

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

## Distribution

The npm package is `pgque`. It publishes ESM JavaScript and TypeScript
declarations from `dist/`, built from the Bun-managed source tree.

See [RELEASE.md](RELEASE.md) for publishing steps.

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
