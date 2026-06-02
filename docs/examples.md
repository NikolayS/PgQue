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

## Cooperative consumers (experimental)

> **Experimental in PgQue 0.2.** The cooperative-consumer functions ship in the default install but are marked experimental: names, edge-case behavior, and the client API may change before they are stable. Use idempotent handlers and test stale-worker takeover before relying on this as the only path for critical work.

Goal: split *one* subscriber's events across a pool of competing workers, so each event is handled by exactly one worker in the pool — without giving up PgQue's shared-log model.

Native fan-out gives every registered consumer its own cursor over the whole log, so every consumer sees every event (see [Fan-out](#fan-out--many-consumers-one-shared-log)). Cooperative consumers are the opposite split: one logical consumer (`workers`) owns a single cursor, and several *subconsumers* (`w1`, `w2`, …) draw *different* batches from it. Each event is delivered to one subconsumer, not all of them — competing consumers inside a single fan-out cursor. See [concepts](concepts.md#cooperative-consumers-vs-fan-out) for when to pick which.

Register the group, then send and tick as usual:

```sql
-- a tick must already exist before registering (register starts at the latest tick)
select pgque.force_next_tick('demo');  -- separate transaction; skip if pg_cron ticks
select pgque.ticker('demo');           -- separate transaction; skip if pg_cron ticks

-- register the cooperative group before producing, so the workers' shared
-- cursor starts ahead of the events you are about to send
select pgque.register_subconsumer('demo', 'workers', 'w1');
select pgque.register_subconsumer('demo', 'workers', 'w2');
```

`register_subconsumer(queue, consumer, subconsumer)` creates the `workers` main cursor on first call and a member row per subconsumer. You can skip it entirely: `receive_coop()` auto-registers a missing consumer or subconsumer on the fly, so a cold worker can call `receive_coop()` directly. Register explicitly only when you need to control *when* the cursor starts, or to convert an existing normal consumer (`register_subconsumer(..., convert_normal => true)`).

Now each worker polls with `receive_coop()`. Successive calls hand out successive batches:

```sql
-- worker w1 (its own process / connection)
select msg_id, batch_id, payload
from pgque.receive_coop('demo', 'workers', 'w1', 100);

-- worker w2 (a different process / connection)
select msg_id, batch_id, payload
from pgque.receive_coop('demo', 'workers', 'w2', 100);
```

Result (two batches of three events, one tick apart):

```
 msg_id | batch_id | payload
--------+----------+----------
   2002 |        3 | {"n": 1}     -- w1 gets batch 3
   2003 |        3 | {"n": 2}
   2004 |        3 | {"n": 3}

 msg_id | batch_id | payload
--------+----------+----------
   4006 |        4 | {"n": 4}     -- w2 gets batch 4
   4007 |        4 | {"n": 5}
   4008 |        4 | {"n": 6}
```

w1 and w2 drew *different* slices of the same `(demo, workers)` subscriber — no event went to both. Ack each batch under whichever worker received it, exactly as with normal `receive` / `ack`:

```sql
select pgque.ack(3);  -- w1's batch
select pgque.ack(4);  -- w2's batch
```

Keep idle subconsumers alive and remove them when a worker shuts down:

```sql
-- heartbeat: mark a subconsumer live without opening a batch (lets stale-worker
-- takeover via receive_coop's dead_interval leave healthy workers alone)
select pgque.touch_subconsumer('demo', 'workers', 'w1');

-- remove a subconsumer that has no open batch
select pgque.unregister_subconsumer('demo', 'workers', 'w2');
```

Gotchas (each verified against a live install):

- **A subconsumer with an open (un-acked) batch refuses to unregister.** `unregister_subconsumer` raises unless you pass `batch_handling => 1`, which routes the in-flight messages through the queue's retry/DLQ policy first.
- **Normal `receive` / `next_batch` raise on cooperative rows.** Calling `pgque.receive('demo', 'workers', …)` on the cooperative main errors with `… is a cooperative main consumer; use cooperative receive/next_batch with a subconsumer`. Member rows are reachable only through `receive_coop()` / cooperative `next_batch()`.
- **`finish_batch` (and `ack`) reject a `coop_main` batch.** Acks apply to the member-owned batch the subconsumer received, never to the group cursor directly.
- **Empty tick windows are auto-finished.** When a poll lands on a tick window with no events, `receive_coop()` finishes it internally and returns no rows and no `batch_id` — unlike `receive()`, which still hands back an empty batch token to ack.
- **One hot row.** Batch hand-out serializes on a `FOR UPDATE` of the `workers` main row, so many workers polling tiny batches contend. If you scale the pool, raise `ticker_max_count` / tick cadence so each batch is big enough to amortize the lock.

`pgque.get_consumer_info('demo')` lists the group as `workers` (the main cursor) and each member as `workers.w1`, `workers.w2`. Full signatures are in the [reference](reference.md#cooperative-consumers--subconsumers).

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

Python ([clients/python](https://github.com/NikolayS/pgque/tree/main/clients/python)):

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

Go ([clients/go](https://github.com/NikolayS/pgque/tree/main/clients/go)):

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

TypeScript ([clients/typescript](https://github.com/NikolayS/pgque/tree/main/clients/typescript)):

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
