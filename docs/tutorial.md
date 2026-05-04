# PgQue tutorial

A hands-on walkthrough. With psql access to a Postgres 14+ instance and ten minutes, you can follow every step end to end. You will type SQL, see what comes back, and build an intuition for how PgQue moves messages.

By the end, you will have a working `orders` queue with a `processor` consumer, a retry flow, a dead letter queue, and health checks.

Prerequisites: Postgres 14 or newer, a database to install into, and `psql` (the tutorial uses `\i` and `\gset`, which are psql meta-commands). `pg_cron` is recommended for production but not required here — this tutorial drives the ticker manually so it works anywhere.

Each step shows the exact SQL and the expected output. Your `msg_id` / `ev_id` / `pending_events` / `ev_new` numbers will differ from the examples — every `pgque.force_tick` call skips the event sequence forward by about 1000, so exact numeric output depends on when you call it. Treat those numbers as illustrative. When transaction boundaries matter (and they matter — PgQue is snapshot-based), the text calls that out.

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

The install creates the `pgque` schema, three roles (`pgque_reader`, `pgque_writer`, `pgque_admin`), and every function you will call in the rest of this tutorial. The roles are siblings: `pgque_writer` produces (`send`, `send_batch`); `pgque_reader` consumes (`subscribe`, `receive`, `ack`, `nack`); `pgque_admin` is a member of both.

**Skip the next snippet if you are following this tutorial as the install owner** — the rest of the tutorial works without any extra grants. The block below is the typical app-role setup you would do in production, where `app_orders` / `app_webhook` / `metrics` are app-role names you create yourself:

```sql
-- Produce + consume:
grant pgque_reader, pgque_writer to app_orders;

-- Pure producer:
grant pgque_writer to app_webhook;

-- Pure consumer / dashboard / metrics:
grant pgque_reader to metrics;
```

See the [reference](reference.md) for the full surface.

## Step 2: Create the queue and the consumer

A queue is a named, shared event log. A consumer is a named cursor into that log. Any number of producers can write to the same queue concurrently, and any number of consumers can subscribe — each sees every event through its own cursor, independently (fan-out by default). You can create as many queues as you want in the same database; this tutorial uses one.

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

`create_queue` returns `1` when it created the queue (and `0` if it already existed — the call is idempotent). `subscribe` is the modern alias for `register_consumer`.

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

That is the event id (`ev_id`) — unique within the queue, monotonically increasing within a rotation window.

`pgque.send` also accepts a raw `text` payload — useful for protobuf, msgpack, or XML that you encode yourself. Untyped string literals like `'{"x":1}'` without the `::jsonb` cast resolve to the `text` overload. This tutorial stays on `jsonb` for clarity; see the [reference](reference.md) for the full overload rules and the NUL-byte caveat for binary payloads.

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

Zero rows. This surprises every first-time user. Here is why.

PgQue is **tick-based**, not row-claiming. Producers append events to the queue, but consumers do not see rows directly — they see **batches**. A batch is the set of events between two ticks. Until a tick happens, there is no batch boundary, so `pgque.receive` has nothing to return.

In normal operation, a scheduler (`pg_cron` or an external loop) calls `pgque.ticker()` every second and ticks happen continuously. In this tutorial you have not started a scheduler, so no tick has run yet.

See the [concepts glossary](pgq-concepts.md) for the full definitions of event, batch, tick, and consumer.

## Step 5: Force a tick, then receive

For demos and tests, PgQue provides `pgque.force_tick` to bypass the tick thresholds for one queue. It does **not** create the tick by itself — you still have to call `pgque.ticker()` afterwards to produce the tick:

```sql
-- separate transactions (psql autocommit). Do not wrap in begin/commit:
-- ticker() must see the prior send committed before it can include it in a batch.
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
      1 |        1 | default | {"total": 99.95, "order_id": 42}   |             | 2026-04-17 10:00:00+00
(1 row)
```

The event is back. `retry_count` is null because this is the first delivery attempt. The `batch_id` is the important value for the next step.

In production, `pg_cron` (or a small worker loop) calls `pgque.ticker()` every second. `force_tick` exists for the situation here: advancing the queue without waiting on the ticker's lag threshold.

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

**What you just did, in PgQ terms.** The modern `receive`/`ack` pair wraps PgQ's canonical consumer loop:

```
batch_id = next_batch(queue, consumer)   -- NULL → sleep and retry
events   = get_batch_events(batch_id)
process(events)                           -- event_retry per event on failure
finish_batch(batch_id)
commit
```

`pgque.receive` = `next_batch` + `get_batch_events`. `pgque.ack` = `finish_batch`. `pgque.nack` = `event_retry` (with DLQ routing when `retry_count >= max_retries`). Both surfaces ship; the primitives are available for advanced use. See the [reference](reference.md) or the [concepts glossary](pgq-concepts.md).

Every row `pgque.receive` returns is a `pgque.message` composite: `msg_id` (PgQ's `ev_id`), `batch_id`, `type`, `payload` (text — cast to `jsonb` for JSON access), `retry_count` (NULL on first delivery), `created_at`, and four free-form `extra1..4` text columns.

## Step 7: Send, nack, retry

Consumers sometimes fail. `nack` handles that: the message is scheduled for redelivery after a delay you choose. Before demoing it, lower the retry ceiling so you can drive a message to the DLQ in step 8:

```sql
select pgque.set_queue_config('orders', 'max_retries', '2');
```

The parameter is `max_retries`, not `queue_max_retries` — `set_queue_config` prepends `queue_` for you.

Send another event, tick, and receive:

```sql
-- send / force_tick / ticker / receive are four separate transactions in psql
-- autocommit. Do not wrap them in begin/commit — the snapshot rule still applies.
select pgque.send('orders', '{"order_id": 43, "total": 10.00}'::jsonb);
select pgque.force_tick('orders');
select pgque.ticker();
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                            | retry_count | ...
--------+----------+---------+------------------------------------+-------------+----
      2 |        2 | default | {"total": 10.00, "order_id": 43}   |             |
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

The event is now in PgQ's retry queue. Moving it back into the main event stream is a separate maintenance step: `pgque.maint_retry_events()`. After that, the next tick makes it visible again:

```sql
-- four separate transactions (psql autocommit). Do not wrap in begin/commit.
select pgque.maint_retry_events();
select pgque.force_tick('orders');
select pgque.ticker();
select * from pgque.receive('orders', 'processor', 100);
```

In production, `pgque.start()` schedules `maint_retry_events` on its own cadence — you never call it by hand. See [`pgque.maint()`](reference.md#pgquemaint--integer) and the surrounding Lifecycle entries in the reference.

```
 msg_id | batch_id | type    | payload                            | retry_count | ...
--------+----------+---------+------------------------------------+-------------+----
      2 |        3 | default | {"total": 10.00, "order_id": 43}   |           1 |
(1 row)
```

Same `msg_id = 2`, new `batch_id = 3`, `retry_count = 1` — that's the redelivery.

## Step 8: Drive the message to the dead letter queue

Keep nacking. You set `max_retries = 2` in step 7, and the message was just redelivered with `retry_count = 1`. On the next `nack` it becomes `retry_count = 2`; one more `nack` after that, and `nack` sees `retry_count >= max_retries` and routes the message to `pgque.dead_letter` instead of the retry queue.

Two more nack cycles. Run the following block twice — the `nack` `do` block followed by `maint_retry_events` + tick:

```sql
do $$
declare
    v_msg pgque.message;
begin
    select * into v_msg from pgque.receive('orders', 'processor', 1) limit 1;
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'still failing');
    perform pgque.ack(v_msg.batch_id);
end $$;

-- the do-block above is one transaction; the three statements below are
-- three more, in psql autocommit. Do not wrap them in begin/commit.
select pgque.maint_retry_events();
select pgque.force_tick('orders');
select pgque.ticker();
```

The second iteration sees `retry_count = 2` and routes to the DLQ instead of the retry queue. After it runs, `receive` returns nothing — the event has moved to `pgque.dead_letter`.

Inspect the DLQ:

```sql
select dl_id, dl_reason, ev_id, ev_retry, ev_data
from pgque.dlq_inspect('orders');
```

```
 dl_id | dl_reason     | ev_id | ev_retry | ev_data
-------+---------------+-------+----------+----------------------------------
     1 | still failing |     2 |        2 | {"total": 10.00, "order_id": 43}
(1 row)
```

From here, two moves. After you have fixed the upstream bug, put the event back on the queue:

```sql
select pgque.dlq_replay(1);
```

The event re-enters the main queue with a fresh event id and will be delivered on the next tick. The DLQ row is removed.

To empty the DLQ, use `dlq_purge` — it deletes rows older than the interval you pass (`'0 seconds'` clears everything for that queue; the default is `'30 days'`):

```sql
select pgque.dlq_purge('orders', '0 seconds'::interval);
```

## Step 9: Look at queue and consumer health

Three functions read out queue and consumer health.

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

`lag` is the age of the consumer's last finished batch — high means the consumer is falling behind. `last_seen` is the elapsed time since the consumer last processed a batch — high means the consumer has stopped calling `receive`. `pending_events` is the count waiting in the current table for the next tick. For a healthy system, `lag` and `last_seen` both stay low and `ticker_lag` stays under a few seconds.

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

You have been driving the ticker by hand. In production you want a scheduler calling it every second. The recommended default is `pg_cron` — pre-installed or one-command available on every major managed Postgres provider (RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon). For self-managed Postgres, follow the [pg_cron setup guide](https://github.com/citusdata/pg_cron#setting-up-pg_cron).

With `pg_cron` available in the same database as PgQue:

```sql
select pgque.start();
```

That one call schedules four cron jobs: `pgque_ticker` every second, `pgque_retry_events` every thirty seconds (moves `nack`'d events back into the main stream), `pgque_maint` every thirty seconds (rotation step 1 and vacuum), and `pgque_rotate_step2` every ten seconds (rotation step 2). Check them with `select * from pgque.status();` or `select * from cron.job;`.

**pg_cron in a different database.** `pg_cron` runs jobs in one designated database (`cron.database_name`, typically `postgres`). If your PgQue schema lives in a different database, use the [cross-database pattern](https://github.com/citusdata/pg_cron#creating-a-cron-job-in-a-different-database) to call `pgque.ticker()` and `pgque.maint()` across databases. *Todo: a future release will detect this and emit the correct `cron.schedule_in_database` calls from `pgque.start()` automatically.*

**pg_cron log hygiene.** `pg_cron` logs every job execution to `cron.job_run_details`. At the one-second ticker cadence, that table grows by roughly 3,600 rows per hour from PgQue alone, with no built-in purge.

Recommended: disable successful-run logging globally.

```sql
alter system set cron.log_run = off;
-- requires a Postgres restart; errors from failed jobs still land in
-- the Postgres server log via cron.log_min_messages (default WARNING)
```

If other pg_cron jobs on the instance need run history (pg_cron has no per-job logging toggle as of 1.6), schedule a periodic purge instead:

```sql
select cron.schedule(
  'pgque_purge_cron_log',
  '0 * * * *',
  $$delete from cron.job_run_details where end_time < now() - interval '1 day'$$
);
```

*Todo: a future `pgque.start()` will warn about this overhead and offer to schedule the purge job.*

Without `pg_cron` at all, call `pgque.ticker()` and `pgque.maint()` from your application or an external scheduler (system `cron`, systemd, a worker loop) on the same cadence. The install is still useful — you provide the heartbeat yourself.

### Where to go from here

- [reference](reference.md) — every function with signatures, return types, and role grants.
- [examples](examples.md) — patterns: fan-out, exactly-once consumption, batch loading, recurring jobs.
- [concepts](pgq-concepts.md) — glossary of batch, tick, rotation, and the consumer loop.
- [history](pgq-history.md) — how this engine came from PgQ.
