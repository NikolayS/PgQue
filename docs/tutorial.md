# logres tutorial

A hands-on walkthrough. With psql access to a Postgres 14+ instance and ten minutes, you can follow every step end to end. You will type SQL, see what comes back, and build an intuition for how logres moves messages.

By the end, you will have a working `orders` queue with a `processor` consumer, a retry flow, a dead letter queue, and health checks.

Prerequisites: Postgres 14 or newer, a database to install into, and `psql` (the tutorial uses `\i` and `\gset`, which are psql meta-commands). `pg_cron` is recommended for production but not required here — this tutorial drives the ticker manually so it works anywhere.

Each step shows the exact SQL and the expected output. Your `msg_id` / `ev_id` / `pending_events` / `ev_new` numbers will differ from the examples — every `logres.force_tick` call skips the event sequence forward by about 1000, so exact numeric output depends on when you call it. Treat those numbers as illustrative. When transaction boundaries matter (and they matter — logres is snapshot-based), the text calls that out.

You can run every snippet in `psql` with `--no-psqlrc` and `PAGER=cat` if you want reproducible output. From the cloned repo, so `\i sql/logres.sql` resolves:

```bash
cd /path/to/logres
PAGER=cat psql --no-psqlrc -d mydb
```

For vocabulary — "batch", "tick", "rotation" — see the [concepts glossary](pgq-concepts.md).

## Step 1: Install

logres is a single SQL file. Install it inside a transaction so a failure leaves no half-built schema behind:

```sql
begin;
\i sql/logres.sql
commit;
```

Verify the install by asking for the version:

```sql
select logres.version();
```

```
    version
----------------
 [[your version]]
```

The install creates the `logres` schema, three roles (`logres_reader`, `logres_writer`, `logres_admin`), and every function you will call in the rest of this tutorial. See the [reference](reference.md) for the full surface.

## Step 2: Create the queue and the consumer

A queue is a named, shared event log. A consumer is a named cursor into that log. Any number of producers can write to the same queue concurrently, and any number of consumers can subscribe — each sees every event through its own cursor, independently (fan-out by default). You can create as many queues as you want in the same database; this tutorial uses one.

```sql
select logres.create_queue('orders');
select logres.subscribe('orders', 'processor');
```

```
 create_queue
--------------
            1
 subscribe
-----------
         1
```

`create_queue` returns `1` when it created the queue (and `0` if it already existed — the call is idempotent). `subscribe` is the modern alias for `register_consumer`.

## Step 3: Send an order

Send one event to the queue. The `jsonb` overload validates and canonicalizes the payload:

```sql
select logres.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);
```

```
 send
------
    1
```

That is the event id (`ev_id`) — unique within the queue, monotonically increasing within a rotation window.

`logres.send` also accepts a raw `text` payload — useful for protobuf, msgpack, or XML that you encode yourself. Untyped string literals like `'{"x":1}'` without the `::jsonb` cast resolve to the `text` overload. This tutorial stays on `jsonb` for clarity; see the [reference](reference.md) for the full overload rules and the NUL-byte caveat for binary payloads.

## Step 4: Try to receive — and get nothing

Now try to pull that event back out:

```sql
select * from logres.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type | payload | retry_count | created_at | ...
--------+----------+------+---------+-------------+------------+-----
(0 rows)
```

Zero rows. This surprises every first-time user. Here is why.

logres is **tick-based**, not row-claiming. Producers append events to the queue, but consumers do not see rows directly — they see **batches**. A batch is the set of events between two ticks. Until a tick happens, there is no batch boundary, so `logres.receive` has nothing to return.

In normal operation, a scheduler (`pg_cron` or an external loop) calls `logres.ticker()` every second and ticks happen continuously. In this tutorial you have not started a scheduler, so no tick has run yet.

See the [concepts glossary](pgq-concepts.md) for the full definitions of event, batch, tick, and consumer.

## Step 5: Force a tick, then receive

For demos and tests, logres provides `logres.force_tick` to bypass the tick thresholds for one queue. It does **not** create the tick by itself — you still have to call `logres.ticker()` afterwards to produce the tick:

```sql
select logres.force_tick('orders');
select logres.ticker();
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
select * from logres.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                            | retry_count | created_at
--------+----------+---------+------------------------------------+-------------+----------------------
      1 |        1 | default | {"order_id": 42, "total": 99.95}   |             | 2026-04-17 10:00:00+00
(1 row)
```

The event is back. `retry_count` is null because this is the first delivery attempt. The `batch_id` is the important value for the next step.

In production, `pg_cron` (or a small worker loop) calls `logres.ticker()` every second. `force_tick` exists for the situation here: advancing the queue without waiting on the ticker's lag threshold.

## Step 6: Ack the batch

A batch stays assigned to a consumer until the consumer calls `ack`. Until then, the same batch is returned every time you call `receive` — the consumer has not moved forward.

Capture the `batch_id` from step 5 and ack it. In psql you can use `\gset`:

```sql
select batch_id from logres.receive('orders', 'processor', 100) limit 1 \gset
select logres.ack(:batch_id);
```

```
 ack
-----
   1
```

Or, if you already saw `batch_id = 1` in the output, call it directly:

```sql
select logres.ack(1);
```

`ack` is the modern alias for PgQ's `finish_batch`. It finalizes the batch and advances the consumer's cursor past it.

Call `receive` once more to confirm there is nothing left:

```sql
select * from logres.receive('orders', 'processor', 100);
```

```
(0 rows)
```

**What you just did, in PgQ terms.** The modern `receive`/`ack` pair wraps PgQ's canonical consumer loop:

```
batch_id = next_batch(queue, consumer)   -- NULL → sleep and retry
events   = get_batch_events(batch_id)
process(events)                           -- event_retry per event on failure
finish_batch(batch_id)
commit
```

`logres.receive` = `next_batch` + `get_batch_events`. `logres.ack` = `finish_batch`. `logres.nack` = `event_retry` (with DLQ routing when `retry_count >= max_retries`). Both surfaces ship; the primitives are available for advanced use. See the [reference](reference.md) or the [concepts glossary](pgq-concepts.md).

Every row `logres.receive` returns is a `logres.message` composite: `msg_id` (PgQ's `ev_id`), `batch_id`, `type`, `payload` (text — cast to `jsonb` for JSON access), `retry_count` (NULL on first delivery), `created_at`, and four free-form `extra1..4` text columns.

## Step 7: Send, nack, retry

Consumers sometimes fail. `nack` handles that: the message is scheduled for redelivery after a delay you choose. Before demoing it, lower the retry ceiling so you can drive a message to the DLQ in step 8:

```sql
select logres.set_queue_config('orders', 'max_retries', '2');
```

The parameter is `max_retries`, not `queue_max_retries` — `set_queue_config` prepends `queue_` for you.

Send another event, tick, and receive:

```sql
select logres.send('orders', '{"order_id": 43, "total": 10.00}'::jsonb);
select logres.force_tick('orders');
select logres.ticker();
select * from logres.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                            | retry_count | ...
--------+----------+---------+------------------------------------+-------------+----
      2 |        2 | default | {"order_id": 43, "total": 10.00}   |             |
(1 row)
```

Now pretend the handler failed. `nack` takes the full `logres.message` row, so the natural pattern is a `do` block that receives and nacks in one place.

**Both `nack` and `ack` are needed on the same batch.** They are not alternatives: `nack` per-event schedules a retry (or routes to the DLQ if `retry_count >= max_retries`); `ack` per-batch finalizes the batch and advances the consumer cursor. Without the `ack`, the consumer never moves past the batch and the same events are redelivered forever.

```sql
do $$
declare
    v_msg logres.message;
begin
    select * into v_msg from logres.receive('orders', 'processor', 1) limit 1;
    perform logres.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'simulated failure');
    perform logres.ack(v_msg.batch_id);
end $$;
```

The event is now in PgQ's retry queue. Moving it back into the main event stream is a separate maintenance step: `logres.maint_retry_events()`. After that, the next tick makes it visible again:

```sql
select logres.maint_retry_events();
select logres.force_tick('orders');
select logres.ticker();
select * from logres.receive('orders', 'processor', 100);
```

In production, `logres.start()` schedules `maint_retry_events` on its own cadence — you never call it by hand. See [`logres.maint()`](reference.md#logresmaint--integer) and the surrounding Lifecycle entries in the reference.

```
 msg_id | batch_id | type    | payload                            | retry_count | ...
--------+----------+---------+------------------------------------+-------------+----
      2 |        3 | default | {"order_id": 43, "total": 10.00}   |           1 |
(1 row)
```

Same `msg_id = 2`, new `batch_id = 3`, `retry_count = 1` — that's the redelivery.

## Step 8: Drive the message to the dead letter queue

Keep nacking. You set `max_retries = 2` in step 7, and the message was just redelivered with `retry_count = 1`. On the next `nack` it becomes `retry_count = 2`; one more `nack` after that, and `nack` sees `retry_count >= max_retries` and routes the message to `logres.dead_letter` instead of the retry queue.

Two more nack cycles. Run the following block twice — the `nack` `do` block followed by `maint_retry_events` + tick:

```sql
do $$
declare
    v_msg logres.message;
begin
    select * into v_msg from logres.receive('orders', 'processor', 1) limit 1;
    perform logres.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'still failing');
    perform logres.ack(v_msg.batch_id);
end $$;

select logres.maint_retry_events();
select logres.force_tick('orders');
select logres.ticker();
```

The second iteration sees `retry_count = 2` and routes to the DLQ instead of the retry queue. After it runs, `receive` returns nothing — the event has moved to `logres.dead_letter`.

Inspect the DLQ:

```sql
select dl_id, dl_reason, ev_id, ev_retry, ev_data
from logres.dlq_inspect('orders');
```

```
 dl_id | dl_reason     | ev_id | ev_retry | ev_data
-------+---------------+-------+----------+----------------------------------
     1 | still failing |     2 |        2 | {"order_id": 43, "total": 10.00}
(1 row)
```

From here, two moves. After you have fixed the upstream bug, put the event back on the queue:

```sql
select logres.dlq_replay(1);
```

The event re-enters the main queue with a fresh event id and will be delivered on the next tick. The DLQ row is removed.

To empty the DLQ, use `dlq_purge` — it deletes rows older than the interval you pass (`'0 seconds'` clears everything for that queue; the default is `'30 days'`):

```sql
select logres.dlq_purge('orders', '0 seconds'::interval);
```

## Step 9: Look at queue and consumer health

Three functions read out queue and consumer health.

```sql
select queue_name, ticker_lag, ev_per_sec, ev_new, last_tick_id
from logres.get_queue_info('orders');
```

```
 queue_name | ticker_lag      | ev_per_sec | ev_new | last_tick_id
------------+-----------------+------------+--------+--------------
 orders     | 00:00:03.412    |       0.12 |      0 |            7
```

`ticker_lag` is the wall time since the last tick. If this grows without bound, the ticker is not running.

```sql
select queue_name, consumer_name, lag, last_seen, pending_events
from logres.get_consumer_info('orders', 'processor');
```

```
 queue_name | consumer_name | lag          | last_seen    | pending_events
------------+---------------+--------------+--------------+----------------
 orders     | processor     | 00:00:02.11  | 00:00:01.50  |              0
```

`lag` is the age of the consumer's last finished batch — high means the consumer is falling behind. `last_seen` is the elapsed time since the consumer last processed a batch — high means the consumer has stopped calling `receive`. `pending_events` is the count waiting in the current table for the next tick. For a healthy system, `lag` and `last_seen` both stay low and `ticker_lag` stays under a few seconds.

```sql
select * from logres.status();
```

```
 component  | status      | detail
------------+-------------+----------------------------------
 postgresql | info        | PostgreSQL 17.2 on ...
 logres      | info        | [[your version]]
 pg_cron    | unavailable | pg_cron not installed -- call ...
 queues     | info        | 1 queues configured
 consumers  | info        | 1 active subscriptions
```

`status()` is the one-stop health check. If `pg_cron` is installed and `logres.start()` has been run, you will see `ticker` and `maintenance` rows with `scheduled` status and the cron job id.

## Next steps

### Production cadence: use pg_cron

You have been driving the ticker by hand. In production you want a scheduler calling it every second. The recommended default is `pg_cron` — pre-installed or one-command available on every major managed Postgres provider (RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon). For self-managed Postgres, follow the [pg_cron setup guide](https://github.com/citusdata/pg_cron#setting-up-pg_cron).

With `pg_cron` available in the same database as logres:

```sql
select logres.start();
```

That one call schedules four cron jobs: `logres_ticker` every second, `logres_retry_events` every thirty seconds (moves `nack`'d events back into the main stream), `logres_maint` every thirty seconds (rotation step 1 and vacuum), and `logres_rotate_step2` every ten seconds (rotation step 2). Check them with `select * from logres.status();` or `select * from cron.job;`.

**pg_cron in a different database.** `pg_cron` runs jobs in one designated database (`cron.database_name`, typically `postgres`). If your logres schema lives in a different database, use the [cross-database pattern](https://github.com/citusdata/pg_cron#creating-a-cron-job-in-a-different-database) to call `logres.ticker()` and `logres.maint()` across databases. *Todo: a future release will detect this and emit the correct `cron.schedule_in_database` calls from `logres.start()` automatically.*

**pg_cron log hygiene.** `pg_cron` logs every job execution to `cron.job_run_details`. At the one-second ticker cadence, that table grows by roughly 3,600 rows per hour from logres alone, with no built-in purge.

Recommended: disable successful-run logging globally.

```sql
alter system set cron.log_run = off;
-- requires a Postgres restart; errors from failed jobs still land in
-- the Postgres server log via cron.log_min_messages (default WARNING)
```

If other pg_cron jobs on the instance need run history (pg_cron has no per-job logging toggle as of 1.6), schedule a periodic purge instead:

```sql
select cron.schedule(
  'logres_purge_cron_log',
  '0 * * * *',
  $$delete from cron.job_run_details where end_time < now() - interval '1 day'$$
);
```

*Todo: a future `logres.start()` will warn about this overhead and offer to schedule the purge job.*

Without `pg_cron` at all, call `logres.ticker()` and `logres.maint()` from your application or an external scheduler (system `cron`, systemd, a worker loop) on the same cadence. The install is still useful — you provide the heartbeat yourself.

### Where to go from here

- [reference](reference.md) — every function with signatures, return types, and role grants.
- [examples](examples.md) — patterns: fan-out, exactly-once consumption, batch loading, recurring jobs.
- [concepts](pgq-concepts.md) — glossary of batch, tick, rotation, and the consumer loop.
- [history](pgq-history.md) — how this engine came from PgQ.
