# Examples

Common PgQue patterns. For a guided first-run, see [the tutorial](tutorial.md). For every function signature, see [the reference](reference.md).

## Send with event type

The event type is a string tag consumers can filter on.

```sql
select pgque.send('orders', 'order.created',
  '{"order_id": 42}'::jsonb);

select pgque.send('orders', 'order.shipped',
  '{"order_id": 42, "tracking": "1Z999AA10123456784"}'::jsonb);
```

## Batch send

Both `jsonb[]` and `text[]` overloads exist — the [reference](reference.md) explains the trade-off.

```sql
select pgque.send_batch('orders', 'order.created', array[
  '{"order_id": 1}'::jsonb,
  '{"order_id": 2}'::jsonb,
  '{"order_id": 3}'::jsonb
]);
```

## Fan-out with multiple consumers

Three subscribers on the same queue, each tracking its own cursor. Unlike SKIP LOCKED queues, every consumer sees every event.

Subscribe **before** producing — a new consumer starts from the latest tick and will not see events that were sent before its `subscribe` call.

```sql
select pgque.subscribe('orders', 'audit_logger');
select pgque.subscribe('orders', 'notification_sender');
select pgque.subscribe('orders', 'analytics_pipeline');

-- send / force_next_tick / ticker / receive are separate transactions in psql
-- autocommit. Do not wrap them in begin/commit — the snapshot rule applies.
select pgque.send('orders', 'order.created', '{"order_id": 1}'::jsonb);
select pgque.force_next_tick('orders');  -- separate transaction
select pgque.ticker();                    -- separate transaction

select * from pgque.receive('orders', 'audit_logger', 100);          -- separate transaction
select * from pgque.receive('orders', 'notification_sender', 100);
```

Each consumer sees the same event through its own cursor — no producer-side duplication, independent cursors on the consumer side.

## Exactly-once processing (transactional pattern)

Wrap the receive, your writes, and the ack in one transaction — either all commit or none do.

```sql
begin;
  with msgs as (
    select * from pgque.receive('orders', 'processor', 100)
  ),
  inserted as (
    insert into processed_orders (order_id, status)
    select (payload::jsonb->>'order_id')::int, 'done'
    from msgs
  )
  select pgque.ack((select batch_id from msgs limit 1));
commit;
```

The `inserted` CTE runs to completion even though the main query does not reference it (data-modifying CTEs always execute). Every row in `msgs` shares the same `batch_id`, so the scalar subquery picks any one of them and `pgque.ack` runs exactly once. **Batch-ownership caveat:** `pgque.ack(batch_id)` advances the consumer past the entire underlying batch, even if `receive()` returned fewer rows than the batch contains (due to `max_return`). Either consume the full batch before acking, or use `max_return >= ticker_max_count` (default 500) to ensure all rows are returned.

> **Anti-pattern: send + receive in one transaction.** Above merges `receive` + writes + `ack` into one tx — correct. Do **not** also merge `send` / `force_next_tick` / `ticker` into the same tx; the ticker's snapshot must be taken *after* `send` commits.
>
> ```sql
> -- WRONG -- consumer sees 0 rows
> begin;
>   select pgque.send('orders', 'order.created', '{"id": 1}'::jsonb);
>   select pgque.force_next_tick('orders');
>   select pgque.ticker();
>   select * from pgque.receive('orders', 'processor', 100);  -- 0 rows
> commit;
> ```
>
> See [pgq-concepts.md#snapshot-rule](pgq-concepts.md#snapshot-rule).

## Recurring jobs with pg_cron

```sql
select cron.schedule('daily_report',
  '0 9 * * *',
  $$select pgque.send('jobs', 'report.generate',
      '{"type": "daily"}'::jsonb)$$);
```

## Dead letter queue inspection

`pgque.dlq_inspect()` lists entries for a queue. Replay a single row by its `dl_id`, or purge rows older than a given interval.

```sql
select dl_id, dl_reason, ev_type, ev_data
from pgque.dlq_inspect('orders');

-- replay a single entry (returns the new ev_id)
select pgque.dlq_replay(42);

-- or drop entries older than 7 days
select pgque.dlq_purge('orders', interval '7 days');
```

See [the tutorial](tutorial.md) for the full DLQ flow including retry budgets and nack.

## Monitoring: queue + consumer health

Two functions inherited from PgQ read out queue and consumer health. Use `\x` in psql for the per-column layout below.

A healthy snapshot:

```
\x

select * from pgque.get_queue_info('orders');
-[ RECORD 1 ]------------+------------------------
queue_name               | orders
queue_ntables            | 3
queue_cur_table          | 2
queue_rotation_period    | 02:00:00
queue_switch_time        | 2026-04-17 08:03:11+00
queue_external_ticker    | f
queue_ticker_paused      | f
queue_ticker_max_count   | 500
queue_ticker_max_lag     | 00:00:03
queue_ticker_idle_period | 00:01:00
ticker_lag               | 00:00:01.842
ev_per_sec               | 3.40
ev_new                   | 12
last_tick_id             | 1247

select * from pgque.get_consumer_info('orders', 'processor');
-[ RECORD 1 ]--+--------------
queue_name     | orders
consumer_name  | processor
lag            | 00:00:00.520
last_seen      | 00:00:00.201
last_tick      | 1247
current_batch  |
next_tick      |
pending_events | 0
```

A stuck consumer, same queue, a couple of hours later:

```
select * from pgque.get_consumer_info('orders', 'processor');
-[ RECORD 1 ]--+----------------
queue_name     | orders
consumer_name  | processor
lag            | 02:15:33.204
last_seen      | 02:14:59.817
last_tick      | 1247
current_batch  |
next_tick      | 1389
pending_events | 847
```

The ticker is still running — `ticker_lag` stays low. But the worker stopped draining: `lag` and `last_seen` climbed to hours, and `pending_events` filled up.

Red flags to alert on:

- **`ticker_lag`** climbing past `queue_ticker_max_lag` (default 3 s) — the ticker is not running. Check `pg_cron` (or your external scheduler).
- **`lag`** climbing into minutes or longer — the consumer is not finishing batches. Check the worker.
- **`last_seen`** climbing into minutes or longer — the consumer has stopped calling `receive` at all. Check the worker process is alive.
- **`pending_events`** growing without bound while `lag` is high — a stuck consumer also blocks table rotation; event tables will grow.

`pgque.status()` rolls up cron-job state, version, queue count, and consumer count into a single diagnostic view — run it first when something looks off.
