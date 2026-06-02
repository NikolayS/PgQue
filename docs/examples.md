---
title: Examples
description: Copy-paste PgQue recipes — fan-out, exactly-once, batch send, recurring jobs, and dead-letter replay.
---

Short, copy-paste patterns for common PgQue tasks. Each recipe is goal, SQL, result. For a guided first run see [the tutorial](tutorial.md); for every function signature see [the reference](reference.md). For queue and consumer health see [monitoring](monitoring.md).

All psql snippets assume psql autocommit (one statement per transaction). Run them with the pager and startup file disabled so output is verbatim:

```bash
PAGER=cat psql --no-psqlrc -d mydb
```

A few recipes depend on a ticker turning sent events into deliverable batches. If pg_cron is running `pgque.start()`, ticking is automatic — skip the explicit `force_next_tick` / `ticker` lines. If you tick manually, keep them, and keep `send`, `force_next_tick`, `ticker`, and `receive` in separate transactions (see the snapshot note under exactly-once).

## Fan-out — many consumers, one shared log

Goal: deliver every event to several independent consumers, each at its own pace, without duplicating the event per consumer.

Fan-out is native. Every registered consumer keeps its own cursor over the same shared event log — the event is stored once, and each consumer advances independently. This is unlike a SKIP-LOCKED queue, where a row is handed to exactly one worker.

```sql
select pgque.subscribe('orders', 'audit_logger');
select pgque.subscribe('orders', 'notification_sender');
select pgque.subscribe('orders', 'analytics_pipeline');

select pgque.send('orders', 'order.created', '{"order_id": 1}'::jsonb);
select pgque.force_next_tick('orders');  -- separate transaction; skip if pg_cron ticks
select pgque.ticker();                    -- separate transaction; skip if pg_cron ticks

select * from pgque.receive('orders', 'audit_logger', 500);
select * from pgque.receive('orders', 'notification_sender', 500);
select * from pgque.receive('orders', 'analytics_pipeline', 500);
```

Result: all three `receive` calls return the same event, each through its own cursor. Acking one consumer's batch does not affect the others.

`max_return` is 500 here to match the default `ticker_max_count`: because `ack` advances past the entire underlying batch, returning fewer rows than the batch holds would silently drop the rest. Use `max_return >= ticker_max_count` whenever you ack what you received.

Late-subscriber caveat: `subscribe` (which calls `register_consumer`) starts the consumer at the most recent tick. A consumer will not see events that were sent before it subscribed. Subscribe each consumer before you start producing.

## Exactly-once processing

Goal: process a message and commit your business writes such that the message is never lost and never applied twice.

The exactly-once property comes entirely from wrapping `receive` + your writes + `ack` in a single transaction. If the transaction rolls back, all three roll back together — the batch is not acked, so the next `receive` redelivers it. Outside this wrapping, PgQue's default is at-least-once.

```sql
begin;
  with msgs as (
    select * from pgque.receive('orders', 'processor', 500)
  ),
  inserted as (
    insert into processed_orders (order_id, status)
    select (payload::jsonb->>'order_id')::int, 'done'
    from msgs
  )
  select pgque.ack((select batch_id from msgs limit 1));
commit;
```

Result: the `processed_orders` rows and the ack commit atomically. A crash before `commit` leaves the batch un-acked and it is redelivered.

Notes:

- The `inserted` CTE runs even though the final `select` does not reference it — data-modifying CTEs always execute.
- Every row in a batch shares one `batch_id`, so the scalar subquery picks any row and `pgque.ack` runs once.
- Batch-ownership caveat: `pgque.ack(batch_id)` advances the consumer past the whole underlying batch even if `receive` returned fewer rows than the batch holds. Consume the full batch before acking, or pass `max_return >= ticker_max_count` (default 500) so every row is returned.

Snapshot rule — do not extend this transaction to cover `send` / `force_next_tick` / `ticker`. The ticker's snapshot must be taken after `send` commits, or the new event is still in-progress at tick time and is excluded from the batch:

```sql
-- WRONG -- consumer sees 0 rows
begin;
  select pgque.send('orders', 'order.created', '{"id": 1}'::jsonb);
  select pgque.force_next_tick('orders');
  select pgque.ticker();
  select * from pgque.receive('orders', 'processor', 100);  -- 0 rows
commit;
```

See [concepts](concepts.md) for the snapshot mechanics.

## Batch send

Goal: enqueue many events in one round trip.

`send_batch` takes an array of payloads and returns the assigned `ev_id`s in order. There are `jsonb[]` and `text[]` overloads; the `jsonb[]` form parses and canonicalizes each payload, the `text[]` form stores verbatim.

```sql
select pgque.send_batch('orders', 'order.created', array[
  '{"order_id": 1}'::jsonb,
  '{"order_id": 2}'::jsonb,
  '{"order_id": 3}'::jsonb
]);
```

Result: a `bigint[]` of new `ev_id`s, one per payload, in array order. An empty array returns `{}`; a NULL array raises.

## Recurring jobs with pg_cron

Goal: produce an event on a schedule — a scheduled producer that feeds workers through PgQue.

```sql
select cron.schedule('daily_report',
  '0 9 * * *',
  $$select pgque.send('jobs', 'report.generate', '{"type": "daily"}'::jsonb)$$);
```

Result: every day at 09:00 the cron job sends one `report.generate` event onto the `jobs` queue, where your workers `receive` it. The producer is the schedule; PgQue decouples it from the consumers.

## Dead-letter queue — inspect and replay

Goal: see what failed past its retry budget, and put it back.

Events retry up to five times by default (`max_retries`) before `nack` routes them to the dead-letter queue. From there you can inspect, replay one, replay all, or purge.

```sql
-- inspect failed events for a queue (default limit 100)
select dl_id, dl_reason, ev_type, ev_data
from pgque.dlq_inspect('orders');

-- replay one entry by its dl_id; returns the new ev_id
select pgque.dlq_replay(42);

-- replay every dead-lettered event for a queue
select * from pgque.dlq_replay_all('orders');

-- purge entries older than 7 days (default interval is 30 days)
select pgque.dlq_purge('orders', interval '7 days');
```

Result and return shapes:

- `dlq_inspect` lists dead-letter rows; each `dl_id` identifies one entry.
- `dlq_replay(dl_id)` re-inserts the event, removes the dead-letter row, and returns the new `ev_id`.
- `dlq_replay_all(queue)` returns a record `(replayed, failed, first_error)` — how many were re-inserted, how many failed, and the first error message if any.
- `dlq_purge(queue [, older_than])` deletes old entries and returns the count removed; `older_than` defaults to `'30 days'`.

See [the tutorial](tutorial.md) for the full retry-and-nack flow that feeds the dead-letter queue.

## Client libraries

PgQue is SQL-first, so any Postgres driver works. First-party clients for Python, Go, and TypeScript wrap the same `send` / `receive` / `ack` surface. Each has its own README with the full API.

Python ([clients/python/README.md](../clients/python/README.md)):

```bash
pip install --pre pgque-py
```

```python
import pgque

with pgque.connect("postgresql://localhost/mydb") as client:
    client.send("orders", {"order_id": 42}, type="order.created")
    client.conn.commit()
    messages = client.receive("orders", "processor", 100)
    if messages:
        client.ack(messages[0].batch_id)
```

Go ([clients/go/README.md](../clients/go/README.md)):

```bash
go get github.com/NikolayS/pgque-go
```

```go
client, _ := pgque.Connect(ctx, "postgresql://localhost/mydb")
defer client.Close()

_, _ = client.Send(ctx, "orders", pgque.Event{
    Type:    "order.created",
    Payload: map[string]any{"order_id": 42},
})
msgs, _ := client.Receive(ctx, "orders", "processor", 100)
if len(msgs) > 0 {
    _, _ = client.Ack(ctx, msgs[0].BatchID)
}
```

TypeScript ([clients/typescript/README.md](../clients/typescript/README.md)):

```bash
npm install pgque@rc   # or: bun add pgque@rc
```

```ts
import { connect } from 'pgque';

const client = await connect('postgresql://localhost/mydb');
try {
  await client.send('orders', { type: 'order.created', payload: { order_id: 42 } });
  const messages = await client.receive('orders', 'processor', 100);
  if (messages.length > 0) await client.ack(messages[0]!.batchId);
} finally {
  await client.close();
}
```

## A note on payloads and roles

Payload overload: an unquoted string literal resolves to the `text` overload, which stores the payload verbatim with no JSON validation. Cast `::jsonb` to validate and canonicalize:

```sql
select pgque.send('orders', 'order.created', '{"order_id": 1}');         -- text, stored as-is
select pgque.send('orders', 'order.created', '{"order_id": 1}'::jsonb);  -- jsonb, validated
```

Roles: producing and consuming are separate privileges. `pgque_reader` can consume, `pgque_writer` can produce, and they are siblings — neither inherits the other. An application that both produces and consumes must be granted both `pgque_writer` and `pgque_reader`. See [installation](installation.md) for grant setup.

For tick-rate and latency tuning, see [latency and tuning](latency-and-tuning.md).
