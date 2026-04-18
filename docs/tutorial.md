# PgQue tutorial

This is a hands-on walkthrough. If you have psql access to a Postgres 14+ instance and ten minutes, you can follow every step end to end. You will type SQL, see what comes back, and build an intuition for how PgQue moves messages.

By the end, you will have a working `orders` queue with a `processor` consumer, a retry flow, a dead letter queue, and a way to check that the whole thing is healthy.

Prerequisites: Postgres 14 or newer, a database you are willing to install into, and `psql` (the tutorial uses `\i` and `\gset`, which are psql meta-commands). `pg_cron` is recommended for production but not required here — this tutorial drives the ticker manually so it works anywhere.

How to read this tutorial: each step shows the exact SQL and the expected output. The sample `msg_id` / `ev_id` numbers will not match what you see — every `pgque.force_tick` call skips the event sequence forward by about 1000, so the specific ids depend on when you call it. Treat those numbers as illustrative. When transaction boundaries matter (and they do — PgQue is snapshot-based), the text calls that out.

You can run every snippet in `psql` with `--no-psqlrc` and `PAGER=cat` if you want reproducible output. From the cloned repo, so `\i sql/pgque.sql` resolves:

```bash
cd /path/to/pgque
PAGER=cat psql --no-psqlrc -d mydb
```

For vocabulary — "batch", "tick", "rotation" — see the [concepts glossary](pgq-concepts.md).

## Step 1: Install

PgQue is a single SQL file. Install it inside a transaction so a failure leaves no half-built schema behind:

```sql
begin;
\i sql/pgque.sql
commit;
```

Verify the install by asking for the version:

```sql
select pgque.version();
```

```
    version
----------------
 [[your version]]
```

The install creates the `pgque` schema, three roles (`pgque_reader`, `pgque_writer`, `pgque_admin`), and every function you will call in the rest of this tutorial. See the [reference](reference.md) for the full surface.

## Step 2: Create the queue and the consumer

A queue is a named, shared event log. A consumer is a named cursor into that log — multiple consumers on the same queue each see every event exactly once, independently. This is fan-out by default.

```sql
select pgque.create_queue('orders');
select pgque.subscribe('orders', 'processor');
```

```
 create_queue
--------------
            1
 subscribe
-----------
         1
```

`create_queue` returns `1` when it actually created the queue (and `0` if it already existed — the call is idempotent). `subscribe` is the modern alias for `register_consumer`.

## Step 3: Send an order

Send one event to the queue. The `jsonb` overload validates and canonicalizes the payload:

```sql
select pgque.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);
```

```
 send
------
    1
```

The returned number is the event id (`ev_id`). It is unique within the queue and monotonically increasing within a rotation window.

A brief callout on overloads: `pgque.send` also accepts a raw `text` payload — useful for protobuf, msgpack, or XML that you encode yourself. Untyped string literals like `'{"x":1}'` without the `::jsonb` cast resolve to the `text` overload. This tutorial stays on `jsonb` for clarity; see the [reference](reference.md) for the full overload rules and the NUL-byte caveat for binary payloads.

## Step 4: Try to receive — and get nothing

Now try to pull that event back out:

```sql
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type | payload | retry_count | created_at | ...
--------+----------+------+---------+-------------+------------+-----
(0 rows)
```

Zero rows. This surprises every first-time user, so it is worth being explicit about why.

PgQue is **tick-based**, not row-claiming. Producers append events to the queue, but consumers do not see individual rows — they see **batches**. A batch is the set of events between two ticks. Until a tick happens, there is no batch boundary, so `pgque.receive` has nothing to return.

In normal operation, a scheduler (`pg_cron` or an external loop) calls `pgque.ticker()` every second or two and ticks happen continuously. In this tutorial we have not started a scheduler, so no tick has run yet.

See the [concepts glossary](pgq-concepts.md) for the full definitions of event, batch, tick, and consumer.

## Step 5: Force a tick, then receive

For demos and tests, PgQue provides `pgque.force_tick` to bump the event-count threshold for one queue. It does **not** create the tick by itself — you still have to call `pgque.ticker()` afterwards to actually produce the tick:

```sql
select pgque.force_tick('orders');
select pgque.ticker();
```

```
 force_tick
------------
          1

 ticker
--------
      1
```

`force_tick` returns the current tick id (the queue was seeded with tick `1` by `create_queue`). `ticker()` returns the number of queues it processed.

Now try receiving again:

```sql
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                            | retry_count | created_at
--------+----------+---------+------------------------------------+-------------+----------------------
      1 |        1 | default | {"order_id": 42, "total": 99.95}   |             | 2026-04-17 10:00:00+00
(1 row)
```

The event is back. Note `retry_count` is null — this is the first delivery attempt. The `batch_id` is the important value for the next step.

In production, `pg_cron` (or a small worker loop) calls `pgque.ticker()` every one to two seconds, and you never touch `force_tick`. It exists for exactly the situation you are in right now: you want to advance the queue without waiting for the ticker's lag threshold.

## Step 6: Ack the batch

A batch stays assigned to a consumer until the consumer calls `ack`. Until then, the same batch is returned every time you call `receive` — the consumer has not moved forward.

Capture the `batch_id` from step 5 and ack it. In psql you can use `\gset`:

```sql
select batch_id from pgque.receive('orders', 'processor', 100) limit 1 \gset
select pgque.ack(:batch_id);
```

```
 ack
-----
   1
```

Or, if you already saw `batch_id = 1` in the output, call it directly:

```sql
select pgque.ack(1);
```

`ack` is the modern alias for PgQ's `finish_batch`. It finalizes the batch and advances the consumer's cursor past it.

Call `receive` once more to confirm there is nothing left:

```sql
select * from pgque.receive('orders', 'processor', 100);
```

```
(0 rows)
```

## Step 7: Send, nack, retry

Real consumers sometimes fail. `nack` handles that: the message is scheduled for redelivery after a delay you choose. Before we demo it, lower the retry ceiling so we can drive a message to the DLQ in step 8:

```sql
select pgque.set_queue_config('orders', 'max_retries', '2');
```

The parameter is `max_retries`, not `queue_max_retries` — `set_queue_config` prepends `queue_` for you.

Send another event, tick, and receive:

```sql
select pgque.send('orders', '{"order_id": 43, "total": 10.00}'::jsonb);
select pgque.force_tick('orders');
select pgque.ticker();
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                            | retry_count | ...
--------+----------+---------+------------------------------------+-------------+----
      2 |        2 | default | {"order_id": 43, "total": 10.00}   |             |
(1 row)
```

Now pretend the handler failed. `nack` takes the full `pgque.message` row, so the natural pattern is a `do` block that receives and nacks in one place.

**Both `nack` and `ack` are needed on the same batch.** They are not alternatives: `nack` per-event schedules a retry (or routes to the DLQ if `retry_count >= max_retries`); `ack` per-batch finalizes the batch and advances the consumer cursor. Without the `ack`, the consumer never moves past the batch and the same events are redelivered forever.

```sql
do $$
declare
    v_msg pgque.message;
begin
    select * into v_msg from pgque.receive('orders', 'processor', 1) limit 1;
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'simulated failure');
    perform pgque.ack(v_msg.batch_id);
end $$;
```

The event is now in PgQ's retry queue. Moving it back into the main event stream is a separate maintenance step — `pgque.maint_retry_events()` does exactly that, and then the next tick makes it visible again:

```sql
select pgque.maint_retry_events();
select pgque.force_tick('orders');
select pgque.ticker();
select * from pgque.receive('orders', 'processor', 100);
```

In production, `pgque.start()` schedules `maint_retry_events` on its own cadence — you never call it by hand. See the reference for [`maint_retry_events`](reference.md) and the related `maint` helpers.

```
 msg_id | batch_id | type    | payload                            | retry_count | ...
--------+----------+---------+------------------------------------+-------------+----
      2 |        3 | default | {"order_id": 43, "total": 10.00}   |           1 |
(1 row)
```

Same `msg_id = 2`, new `batch_id = 3`, and `retry_count = 1`. That is the redelivery.

## Step 8: Drive the message to the dead letter queue

Keep nacking. We set `max_retries = 2` in step 7, and the message was just redelivered with `retry_count = 1`. On the next `nack` it becomes `retry_count = 2`; one more `nack` after that and `nack` sees `retry_count >= max_retries` and routes the message to `pgque.dead_letter` instead of the retry queue.

That is two more nack cycles. Run the following block — `nack` `do` block followed by `maint_retry_events` + tick — twice:

```sql
do $$
declare
    v_msg pgque.message;
begin
    select * into v_msg from pgque.receive('orders', 'processor', 1) limit 1;
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'still failing');
    perform pgque.ack(v_msg.batch_id);
end $$;

select pgque.maint_retry_events();
select pgque.force_tick('orders');
select pgque.ticker();
```

The second iteration sees `retry_count = 2` and routes to the DLQ instead of to the retry queue. After it runs, no events come back out of `receive` — the event has moved to `pgque.dead_letter`.

Inspect the DLQ:

```sql
select dl_id, dl_reason, ev_id, ev_retry, ev_data
from pgque.dlq_inspect('orders');
```

```
 dl_id | dl_reason     | ev_id | ev_retry | ev_data
-------+---------------+-------+----------+----------------------------------
     1 | still failing |     2 |        2 | {"order_id": 43, "total": 10.00}
(1 row)
```

Two useful moves from here. After you have fixed the upstream bug, put the event back on the queue:

```sql
select pgque.dlq_replay(1);
```

The event re-enters the main queue with a fresh event id and will be delivered on the next tick. The DLQ row is removed.

To empty the DLQ, use `dlq_purge` — it deletes rows older than the interval you pass (`'0 seconds'` clears everything for that queue; the default is `'30 days'`):

```sql
select pgque.dlq_purge('orders', '0 seconds'::interval);
```

## Step 9: Look at queue and consumer health

Three functions give you a quick read on the system.

```sql
select queue_name, ticker_lag, ev_per_sec, ev_new, last_tick_id
from pgque.get_queue_info('orders');
```

```
 queue_name | ticker_lag      | ev_per_sec | ev_new | last_tick_id
------------+-----------------+------------+--------+--------------
 orders     | 00:00:03.412    |       0.12 |      0 |            7
```

`ticker_lag` is the wall time since the last tick. If this grows without bound, the ticker is not running.

```sql
select queue_name, consumer_name, lag, last_seen, pending_events
from pgque.get_consumer_info('orders', 'processor');
```

```
 queue_name | consumer_name | lag          | last_seen    | pending_events
------------+---------------+--------------+--------------+----------------
 orders     | processor     | 00:00:02.11  | 00:00:01.50  |              0
```

`lag` is how far behind the consumer is — the age of its last finished batch. `last_seen` is how long ago the consumer last picked up a batch. `pending_events` is the count waiting in the current table for the next tick. For a healthy system, `lag` and `last_seen` both stay low and `ticker_lag` stays under a few seconds.

```sql
select * from pgque.status();
```

```
 component  | status      | detail
------------+-------------+----------------------------------
 postgresql | info        | PostgreSQL 17.2 on ...
 pgque      | info        | [[your version]]
 pg_cron    | unavailable | pg_cron not installed -- call ...
 queues     | info        | 1 queues configured
 consumers  | info        | 1 active subscriptions
```

`status()` is the one-stop health check. If `pg_cron` is installed and `pgque.start()` has been run, you will see `ticker` and `maintenance` rows with `scheduled` status and the cron job id.

## Next steps

### Production cadence: use pg_cron

You have been driving the ticker by hand. In production, you want a scheduler running it every one to two seconds. If the database has `pg_cron` installed:

```sql
select pgque.start();
```

That one call schedules three cron jobs: `pgque_ticker` every two seconds, `pgque_maint` every thirty seconds (retries, cleanup), and `pgque_rotate_step2` every ten seconds (table rotation). You can check them with `select * from pgque.status();` or `select * from cron.job;`.

Without `pg_cron`, call `pgque.ticker()` and `pgque.maint()` from your application or an external scheduler on the same cadence. The install is still useful — you provide the heartbeat yourself.

### Where to go from here

- [reference](reference.md) — every function with signatures, return types, and role grants.
- [examples](examples.md) — patterns: fan-out, exactly-once consumption, batch loading, recurring jobs.
- [concepts](pgq-concepts.md) — glossary of batch, tick, rotation, and the consumer loop.
- [history](pgq-history.md) — how this engine came from PgQ and why it is worth trusting.
