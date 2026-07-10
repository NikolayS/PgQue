---
title: Tutorial
description: A hands-on PgQue walkthrough — install, send, receive, retry, dead-letter, idempotency, partition keys, and health checks.
---

A guided, hands-on walkthrough. With `psql` access to a Postgres 14+ database
and about fifteen minutes, you can follow every step end to end. You will type
SQL, see what comes back, and build an intuition for how PgQue moves messages
through an order-processing queue.

By the end you will have a working `orders` queue with a `processor` consumer,
a happy-path delivery, a retry that reappears after a delay, a message driven
all the way to the dead-letter queue, a health check, and a retry-safe keyed
event consumed through a leased partition slot.

Prerequisites: Postgres 14 or newer, a database to install into, and `psql` (the tutorial uses `\i` and `\gset`, which are psql meta-commands). `pg_cron` is recommended for production but not required here — this tutorial drives the ticker by hand, so it works on any managed or self-managed Postgres.

Each step shows the exact SQL and the expected output. Your `msg_id` / `batch_id` / `ev_new` numbers will differ from the examples — each `pgque.force_next_tick` call advances the event sequence by a large amount, so exact numeric output depends on when you call it. Treat the numbers as illustrative. Where transaction boundaries matter — and they matter, because PgQue is snapshot-based — the text calls that out.

For reproducible output, run psql with `--no-psqlrc` and `PAGER=cat`. This
tutorial follows the `main` branch development build; start from the cloned
repo so `\i devel/sql/pgque.sql` resolves. For the stable build, use the
[latest release's tagged tutorial](https://github.com/NikolayS/pgque/releases/latest).

```bash
cd /path/to/pgque
PAGER=cat psql --no-psqlrc -d mydb
```

For vocabulary — "event", "batch", "tick", "rotation" — see the [concepts glossary](concepts.md).

## Step 1: Install

PgQue is a single SQL file. Install it inside a transaction so a failure leaves no half-built schema behind:

```sql
begin;
\i devel/sql/pgque.sql
commit;
```

Verify the install by asking for the version:

```sql
select pgque.version();
```

```
 version
---------
 <installed development version>
(1 row)
```

The install creates the `pgque` schema, three roles (`pgque_reader`,
`pgque_writer`, `pgque_admin`), and every function you call in the rest of this
tutorial. The roles are siblings, not parent and child: `pgque_writer` produces
(`send`, `send_idem`, `send_batch`); `pgque_reader` owns the normal and
partitioned consume APIs; `pgque_admin` is a member of both.

Following along as the install owner needs no extra grants — skip the next snippet. In production you grant the roles to your own app roles. An app that both produces and consumes needs both roles:

```sql
-- produce and consume:
grant pgque_reader, pgque_writer to app_orders;

-- produce only:
grant pgque_writer to app_webhook;

-- consume only (dashboard, metrics):
grant pgque_reader to metrics;
```

See [Installation & operations](installation.md) for the full role and grant story.

## Step 2: Create the queue and the consumer

A queue is a named, shared event log. A consumer is a named cursor into that log. Any number of producers can write to the same queue concurrently, and any number of consumers can subscribe — each sees every event through its own cursor, independently (fan-out by default).

```sql
select pgque.create_queue('orders');
select pgque.subscribe('orders', 'processor');
```

```
 create_queue
--------------
            1
(1 row)

 subscribe
-----------
         1
(1 row)
```

`create_queue` returns `1` when it created the queue, `0` if it already existed — the call is idempotent. `subscribe` is the modern alias for PgQ's `register_consumer`.

## Step 3: Send an order

Send one event to the queue. The `jsonb` overload validates and canonicalizes the payload:

```sql
select pgque.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);
```

```
 send
------
    1
(1 row)
```

That return value is the event id — unique within the queue. `pgque.send` also accepts a raw `text` payload for formats you encode yourself (protobuf, msgpack, XML). Note that an untyped literal like `'{"x":1}'` without the `::jsonb` cast resolves to the `text` overload and is stored verbatim, with no JSON validation. This tutorial stays on `jsonb`; see the [reference](reference.md) for the full overload rules.

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

Zero rows. This surprises every first-time user, and it is not an error. Here is why.

PgQue is tick-based, not row-claiming. Producers append events, but consumers do not see rows directly — they see batches. A batch is the set of events between two ticks. Until a tick happens there is no batch boundary, so `pgque.receive` has nothing to return and reports zero rows.

In normal operation a scheduler (`pg_cron` or an external loop) drives ticks continuously — PgQue ticks 10 times per second by default (every 100 ms). In this tutorial you have not started a scheduler, so no tick has run yet. That is the next step.

## Step 5: Force the next tick, then receive

For demos and tests, `pgque.force_next_tick` lets one queue bypass the tick thresholds. It does **not** create a tick by itself — it only advances the queue's event sequence past the tick threshold so that the next ticker run is guaranteed to materialize a tick. You still have to call `pgque.ticker()` afterwards:

```sql
select pgque.force_next_tick('orders');
select pgque.ticker();
```

```
 force_next_tick
-----------------
               1
(1 row)

 ticker
--------
      1
(1 row)
```

`force_next_tick` returns the current last tick id (the queue was seeded with tick `1` by `create_queue`). `ticker()` returns the number of queues it ticked.

> Each statement above runs in its own transaction. This is required, not stylistic. PgQue marks batch boundaries with a `pg_snapshot`: the ticker captures a snapshot, and `receive` only returns events whose `send` committed before that snapshot. An event sent in the *same* transaction as the tick is still in-progress at snapshot time and is excluded from the batch. Wrapping `send` + `force_next_tick` + `ticker` + `receive` in one `begin`/`commit` returns zero rows. In psql autocommit, each `select` is already its own transaction — just do not wrap them.

Now receive again:

```sql
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                          | retry_count | created_at
--------+----------+---------+----------------------------------+-------------+------------------------
      1 |        1 | default | {"total": 99.95, "order_id": 42} |             | 2026-06-02 10:00:00+00
(1 row)
```

The event is back. `retry_count` is null because this is the first delivery attempt. The `batch_id` is the value you need for the next step. (The `jsonb` overload stored a canonical form, so the keys come back ordered by length then alphabetically — `"total"` before `"order_id"` — rather than in the order you typed them.)

In production you never call `force_next_tick`: `pg_cron` runs the ticker continuously, or an external worker loop calls `pgque.ticker()` on its own cadence. `force_next_tick` exists exactly for the situation here — advancing one queue on demand without waiting on the tick thresholds.

## Step 6: Ack the batch

A batch stays assigned to its consumer until the consumer calls `ack`. Until then the same batch is returned on every `receive` — the cursor has not moved.

Capture the `batch_id` and ack it. In psql, `\gset` saves a single-row result into a variable:

```sql
select batch_id from pgque.receive('orders', 'processor', 100) limit 1 \gset
select pgque.ack(:batch_id);
```

```
 ack
-----
   1
(1 row)
```

Or, if you already saw `batch_id = 1`, call it directly with `select pgque.ack(1);`.

`ack` is the modern alias for PgQ's `finish_batch`. It finalizes the batch and advances the consumer cursor past it. Confirm there is nothing left:

```sql
select * from pgque.receive('orders', 'processor', 100);
```

```
(0 rows)
```

Every row `pgque.receive` returns is a `pgque.message` composite: `msg_id` (PgQ's `ev_id`), `batch_id`, `type`, `payload` (text — cast to `jsonb` for JSON access), `retry_count` (null on first delivery, an integer after a retry), `created_at`, and four free-form `extra1..4` text columns. The modern `receive`/`ack`/`nack` surface reduces cleanly to PgQ primitives; see the [reference](reference.md).

## Step 7: Send a bad order, nack with a delay, watch it reappear

Consumers sometimes fail. `nack` handles that: it schedules the message for redelivery after a delay you choose, and routes it to the dead-letter queue once it has exhausted its retries. By default a message retries up to 5 times before the DLQ. To reach the DLQ quickly in step 8, lower the ceiling first:

```sql
select pgque.set_queue_config('orders', 'max_retries', '2');
```

The parameter is `max_retries`; `set_queue_config` prepends `queue_` for you.

Send another order, tick, and receive — four separate transactions in psql autocommit:

```sql
select pgque.send('orders', '{"order_id": 43, "total": 10.00}'::jsonb);
select pgque.force_next_tick('orders');
select pgque.ticker();
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                          | retry_count | created_at
--------+----------+---------+----------------------------------+-------------+------------------------
      2 |        2 | default | {"total": 10.00, "order_id": 43} |             | 2026-06-02 10:01:00+00
(1 row)
```

Now pretend the handler failed. `nack` takes the full `pgque.message` row. Note that `nack` and `ack` are **both** needed on the same batch — they are not alternatives. `nack` per-event schedules the retry (or routes to the DLQ); `ack` per-batch finalizes the batch and advances the cursor. Without the `ack`, the consumer never moves past this batch. A `do` block that receives, nacks, and acks in one place is the natural pattern:

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

The event is now in PgQ's retry queue, with a redelivery time set by the interval you passed (here `'0 seconds'`, so it is due immediately; in real code you would pass a backoff like `'60 seconds'`). Moving a due event back into the main stream is a separate maintenance step, `pgque.maint_retry_events()`. After that, the next tick makes it visible again:

```sql
select pgque.maint_retry_events();
select pgque.force_next_tick('orders');
select pgque.ticker();
select * from pgque.receive('orders', 'processor', 100);
```

```
 msg_id | batch_id | type    | payload                          | retry_count | created_at
--------+----------+---------+----------------------------------+-------------+------------------------
      2 |        3 | default | {"total": 10.00, "order_id": 43} |           1 | 2026-06-02 10:01:00+00
(1 row)
```

Same `msg_id = 2`, new `batch_id = 3`, and `retry_count = 1` — that is the redelivery. (In production, `pgque.start()` schedules `maint_retry_events` on its own cadence and you never call it by hand.)

## Step 8: Drive the bad order into the dead-letter queue

`nack` routes a message to the dead-letter queue when its stored retry count has reached the ceiling — specifically when `coalesce(ev_retry, 0) >= max_retries`. Walk the arithmetic with `max_retries = 2`:

- First nack: the event's `retry_count` is null, so `coalesce(null, 0) = 0`. `0 >= 2` is false → retry. The retry bumps the stored count to `1`. (This is the nack you did in step 7.)
- Second nack: `retry_count = 1`. `1 >= 2` is false → retry. The stored count becomes `2`.
- Third nack: `retry_count = 2`. `2 >= 2` is true → the message goes to `pgque.dead_letter` instead of the retry queue.

You have done the first nack. Run the block below twice to do the second and third. The first iteration redelivers with `retry_count = 2`; the second iteration sees `2 >= 2` and dead-letters the message:

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
select pgque.force_next_tick('orders');
select pgque.ticker();
```

After the second iteration, `receive` returns nothing — the event has left the main queue for `pgque.dead_letter`. Inspect it:

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

From here you have two moves. Once the upstream bug is fixed, replay the event onto the queue — it re-enters with a fresh event id and is delivered on the next tick, and the DLQ row is removed:

```sql
select pgque.dlq_replay(1);
```

Or empty the DLQ. `dlq_purge` deletes rows older than the interval you pass; `'0 seconds'` clears everything for the queue (the default is `'30 days'`):

```sql
select pgque.dlq_purge('orders', '0 seconds'::interval);
```

## Step 9: Check queue and consumer health

Two observability functions read out queue and consumer health; `status()` rolls them up.

```sql
select queue_name, ticker_lag, ev_new, last_tick_id
from pgque.get_queue_info('orders');
```

```
 queue_name | ticker_lag   | ev_new | last_tick_id
------------+--------------+--------+--------------
 orders     | 00:00:03.412 |      0 |            7
(1 row)
```

`ticker_lag` is the wall time since the last tick. If it grows without bound, the ticker is not running.

```sql
select queue_name, consumer_name, lag, last_seen, pending_events
from pgque.get_consumer_info('orders', 'processor');
```

```
 queue_name | consumer_name | lag          | last_seen    | pending_events
------------+---------------+--------------+--------------+----------------
 orders     | processor     | 00:00:02.110 | 00:00:01.500 |              0
(1 row)
```

`lag` is the age of the consumer's last finished batch — high means it is falling behind. `last_seen` is the elapsed time since the consumer last processed a batch — high means it has stopped calling `receive`. `pending_events` is the count waiting for the next tick. In a healthy system, `lag` and `last_seen` stay low and `ticker_lag` stays under a few seconds.

```sql
select * from pgque.status();
```

```
 component    | status      | detail
--------------+-------------+----------------------------------------------------------
 postgresql   | info        | PostgreSQL 17.2 on ...
 pgque        | info        | <installed development version>
 scheduler    | manual      | ticker_job_id=NULL, maint_job_id=NULL, tick_period_ms=100 (10.00 ticks/sec)
 ticker       | stopped     | not scheduled (tick_period_ms=100)
 maintenance  | stopped     | not scheduled
 pg_cron      | unavailable | use pgque.start_timetable() for pg_timetable, or call ...
 pg_timetable | unavailable | run pg_timetable against this database, then ...
 queues       | info        | 1 queues configured
 consumers    | info        | 1 active subscriptions
(9 rows)
```

`status()` is the one-stop health check. Here it reflects manual ticking: with no scheduler, the `scheduler` row reads `manual` and the `ticker`/`maintenance` rows read `stopped`. With `pg_cron` installed and `pgque.start()` run, the `scheduler` row would show `pg_cron` with job ids and the ticker/maintenance rows would flip to `scheduled`.

## Step 10: Try retry-safe keyed publishing

The basic consumer above sees the whole stream. This final exercise combines
the two SQL-first scaling features: producer idempotency prevents a retry from
appending twice, while a partition key routes the accepted event to one leased
slot and preserves order for that key.

Create every slot before producing. The atomic setup gives all four slots the
same starting tick:

```sql
select pgque.create_queue('keyed_orders');
select pgque.subscribe_partitioned(
  queue_name := 'keyed_orders',
  consumer := 'workers',
  n := 4
);
```

Send the same logical effect twice with the same idempotency key:

```sql
select * from pgque.send_idem(
  queue_name := 'keyed_orders',
  type_name := 'order.created',
  payload := '{"order_id": 99}'::jsonb,
  idem_key := 'tutorial:order-99:create:v1',
  ttl := interval '1 hour',
  partition_key := 'customer-42'
);

select * from pgque.send_idem(
  queue_name := 'keyed_orders',
  type_name := 'order.created',
  payload := '{"order_id": 99}'::jsonb,
  idem_key := 'tutorial:order-99:create:v1',
  ttl := interval '1 hour',
  partition_key := 'customer-42'
);
```

The first row has `deduped = false`; the second has `deduped = true` and the
same `event_id`. The TTL is a deduplication window for this effect, not a rate
limit. Different work needs a different key.

Calculate the destination slot, claim it with a process-instance worker id,
then tick and receive. These are still separate psql autocommit transactions:

```sql
select ((hashtextextended('customer-42', 0) % 4 + 4) % 4)::int as customer_slot \gset

select pgque.claim_slot(
  queue_name := 'keyed_orders',
  consumer := 'workers',
  slot := :customer_slot,
  worker := 'tutorial-worker',
  ttl := interval '2 minutes'
);

select pgque.force_next_tick('keyed_orders');
select pgque.ticker();

select msg_id, batch_id, type, payload, extra1 as partition_key
from pgque.receive_partitioned(
  queue_name := 'keyed_orders',
  consumer := 'workers',
  slot := :customer_slot,
  n := 4,
  worker := 'tutorial-worker',
  max_return := 500
);
```

One row returns, not two. Capture and ack the same batch, then release the slot
at the batch boundary:

```sql
select batch_id
from pgque.receive_partitioned(
  queue_name := 'keyed_orders',
  consumer := 'workers',
  slot := :customer_slot,
  n := 4,
  worker := 'tutorial-worker',
  max_return := 500
)
limit 1 \gset

select pgque.ack_partitioned(
  queue_name := 'keyed_orders',
  consumer := 'workers',
  slot := :customer_slot,
  n := 4,
  worker := 'tutorial-worker'
);

select pgque.release_slot(
  queue_name := 'keyed_orders',
  consumer := 'workers',
  slot := :customer_slot,
  worker := 'tutorial-worker'
);
```

Production workers repeat that lifecycle across **every** slot. A stopped slot
pins rotation, and each slot scans the full stream, so four slots mean roughly
four times the reads. Leases use transactional rows and work through
transaction-mode poolers, but the first-party clients do not yet implement the
claim/rebalance loop. See [Partition keys](partition-keys.md) and
[Producer idempotency](producer-idempotency.md) before deploying this pattern.

## Where to go from here

You have driven the ticker by hand all the way through. In production you let a scheduler do it — one call, `select pgque.start();`, schedules `pg_cron` to run the ticker (and the maintenance jobs) for you, so messages flow without manual ticks and land within roughly 50 ms median at the default 100 ms tick.

To tear down the queue you built here:

```sql
select pgque.drop_queue('orders', true);  -- force=true unregisters the consumer first
```

To remove PgQue entirely (drops the `pgque` schema; leaves the roles), see [Installation & operations](installation.md).

Next:

- [Installation & operations](installation.md) — `pg_cron` setup, the production ticker cadence, roles, and teardown.
- [Latency & tick tuning](latency-and-tuning.md) — how `tick_period_ms` trades latency against overhead.
- [Reference](reference.md) — public SQL signatures, return types, behavior,
  and role grants.
- [Examples](examples.md) — patterns: fan-out, exactly-once consumption, batch loading, recurring jobs.
- [Concepts](concepts.md) — glossary of event, batch, tick, rotation, and the consumer loop.
- [Monitoring](monitoring.md) — health metrics and what to alert on.
- [Producer idempotency](producer-idempotency.md) — key scope, TTL behavior,
  maintenance, and failure handling.
- [Partition keys](partition-keys.md) — worker leases, fencing, poolers,
  monitoring, read amplification, and recovery.
