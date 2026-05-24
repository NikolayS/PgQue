# pgque

TypeScript client for [PgQue](https://github.com/NikolayS/pgque) — the
PgQ-based universal PostgreSQL queue. Thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `receive`, `ack`, `nack` (plus
`subscribe` / `unsubscribe`).

## Install

```bash
npm install pgque@rc
# or: bun add pgque@rc
# or: pnpm add pgque@rc
# or: yarn add pgque@rc
```

The `@rc` dist-tag points at the latest v0.2.0 release candidate (currently `0.2.0-rc.2`). Drop the tag once v0.2.0 ships.

Runtime requirements: Node.js 20+ and PostgreSQL 14+ with the PgQue schema
installed (`\i pgque.sql` — no extension required).

## Database permissions

The connecting database role needs `pgque_reader` to consume (`receive`, `ack`, `nack`, `subscribe`, `unsubscribe`) and `pgque_writer` to produce (`send`, `send_batch`). The two are **siblings** — neither inherits the other. An app that both produces and consumes (the typical case for code using this client) must be granted **both** roles:

```sql
grant pgque_reader to your_app_user;
grant pgque_writer to your_app_user;
```

See [`docs/reference.md` — Roles and grants](../../docs/reference.md#roles-and-grants) for the full role table.

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
| `client.forceNextTick(queue)` | Force the next `ticker(queue)` call to produce a tick; returns the last tick id (`bigint`) or `null` on a brand-new queue. Wraps `pgque.force_next_tick(queue text)`. |
| `client.newConsumer(queue, name, opts?)` | High-level poll loop. |
| `consumer.handle(eventType, fn)` | Register a handler. |
| `consumer.start(signal?)` | Run; resolves when `AbortSignal` aborts. |
| `client.close()` | Drain the pool. |

`Message.msgId`, `Message.batchId`, and the return values of `send()`,
`sendBatch()`, `ticker(queue)`, and `forceNextTick(queue)` are JS `bigint` to
match PostgreSQL `bigint` losslessly.

## Experimental: cooperative consumers

> **Experimental in PgQue 0.2.** Function names, edge-case behavior, and
> client API shape may change before this feature is marked stable. Do not
> use this as the only processing path for critical workloads without
> idempotent handlers and stale-worker takeover tests.

Cooperative consumers let several workers share **one logical consumer** by
registering as **subconsumers** under it. Each cooperative batch is handed to
exactly one subconsumer; the main row owns the group cursor.

```ts
await client.subscribeSubconsumer('orders', 'billing', 'worker-1');

const consumer = client.newConsumer('orders', 'billing', {
  subconsumer: 'worker-1',
  deadInterval: '5 minutes', // PostgreSQL interval; enables stale-batch takeover
});
consumer.handle('order.created', async (msg) => { /* ... */ });
await consumer.start(ac.signal);
```

When `subconsumer` is set, the poll loop calls `client.receiveCoop(...)`
instead of `client.receive(...)`. Without it, the consumer behaves
identically to the non-cooperative form.

| Method | Description |
|---|---|
| `client.subscribeSubconsumer(queue, consumer, subconsumer)` | Register a subconsumer. Returns `1` first call, `0` if already registered. |
| `client.unsubscribeSubconsumer(queue, consumer, subconsumer, { batchHandling? })` | Remove a subconsumer. Default raises if a batch is active; `batchHandling: 1` routes the active batch through retry/DLQ first. |
| `client.receiveCoop(queue, consumer, subconsumer, { maxMessages?, deadInterval? })` | Cooperative receive. Auto-registers main + subconsumer rows on first call. `deadInterval` enables stale-batch takeover; the new owner gets a fresh `batchId`. |
| `client.touchSubconsumer(queue, consumer, subconsumer)` | Refresh the subconsumer heartbeat so a long handler is not stolen. The high-level consumer does not call this automatically. |

Throughput note: cooperative allocation serializes on a `FOR UPDATE` of the
main subscription row. Many workers polling tiny batches contend on that
single hot row — tune ticker cadence so each batch does meaningful work.
Once a logical consumer has subconsumers, plain `receive` / `next_batch`
on that consumer raises; unregister all subconsumers to return to normal
fan-out.

If `deadInterval` is set without `subconsumer`, `newConsumer` throws at
construction.

A runnable two-worker walkthrough lives in
[`bench/coop_demo.ts`](bench/coop_demo.ts):

```bash
PGQUE_TEST_DSN=postgres://user@host/db \
  bun run clients/typescript/bench/coop_demo.ts
```

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

## Transactions

`send` → ticker → `receive` must each run in its own committed transaction (PgQue is snapshot-based). `pg.Pool#query` satisfies this transparently — every `send`/`receive`/`ack` is its own implicit tx, and the `Consumer` is pool-level.

The footgun is `client.rawPool`: for transactional enqueue, call `BEGIN` / `pgque.send` / `COMMIT` on a checked-out client. Don't mix `pgque.send` and `pgque.receive` in one shared tx; same for `pgque.maint_retry_events` + `pgque.ticker`. See [snapshot rule](https://github.com/NikolayS/pgque/blob/main/docs/pgq-concepts.md#snapshot-rule).

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
