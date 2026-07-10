---
title: Monitoring and health
description: Read PgQue's observability functions, and know what to alert on — ticker lag, consumer lag, a stuck consumer, and dead-letter depth.
---

PgQue exposes its health through a small set of read-only functions. This page
explains the columns that matter operationally, the one failure mode you must
catch early — a stuck consumer that blocks table rotation — and the queries to
wire into your monitoring.

All of the `get_*_info` functions, `pgque.version()`, and the
`pgque.partition_slot_status` view are granted to `pgque_reader`, so a
read-only monitoring role can run the operational queries here.
`pgque.status()` is admin-only. For role setup see
[Installation and operations](installation.md); for vocabulary see
[Concepts](concepts.md).

The examples assume:

```bash
PAGER=cat psql --no-psqlrc -d yourdb
```

## The observability surface

### pgque.status() — is the engine wired up

`pgque.status()` returns `(component, status, detail)` rows. It is the one-stop
check that the ticker and maintenance jobs are scheduled. If `pg_cron` is
installed and `pgque.start()` has run, you will see `ticker` and `maintenance`
rows with a `scheduled` status and their cron job ids. This function is
admin-only.

```sql
select * from pgque.status();
```

If `status()` shows nothing scheduled, no ticks are being created, and every
`pgque.receive()` returns zero rows forever. That is the first thing to rule
out.

### pgque.get_queue_info([queue]) — is the queue flowing

Call with no argument for all queues, or pass a queue name for one. The
operationally important output columns:

| column | meaning | watch for |
|---|---|---|
| `ticker_lag` | wall time since this queue's last tick | grows without bound when the ticker is not running |
| `ev_per_sec` | recent event throughput (float8, from the last ~20 ticks) | sudden drop to zero, or unexpected spikes |
| `ev_new` | events sent but not yet covered by a tick | climbs and stays high if ticking stalls |
| `last_tick_id` | id of the most recent tick | should keep advancing |
| `queue_ticker_paused` | whether ticking is paused on this queue | `true` means no delivery by design |
| `queue_ticker_max_count` / `queue_ticker_max_lag` / `queue_ticker_idle_period` | the tick-trigger thresholds | context for interpreting `ticker_lag` |
| `queue_rotation_period` / `queue_switch_time` | rotation period and last rotation time | stale `queue_switch_time` hints rotation is stuck |

```sql
select queue_name, ticker_lag, ev_per_sec, ev_new, last_tick_id
from pgque.get_queue_info('orders');
```

`ticker_lag` is the single most useful queue signal. With the default settings,
the queue ticks at least every `ticker_idle_period` (1 minute) even when idle,
so a `ticker_lag` that keeps climbing past that means the ticker has stopped.

### pgque.get_consumer_info([queue[, consumer]]) — is the consumer keeping up

Call with no arguments for every consumer on every queue, with a queue name to
scope to one queue, or with both to inspect a single consumer. Output columns:

| column | meaning | watch for |
|---|---|---|
| `lag` | age of the events the consumer is currently positioned at | grows when the consumer falls behind |
| `last_seen` | elapsed time since the consumer last processed a batch | grows when the consumer has stopped calling `receive` |
| `pending_events` | events waiting past the consumer's position, not yet consumed | a growing backlog |
| `last_tick` | tick id of the consumer's last processed tick | should advance; a frozen value is the stuck-consumer signal |
| `current_batch` | active batch id, or NULL if none open | a long-lived non-NULL value means a batch is never being acked |
| `next_tick` | final tick of the active batch, if one is open | — |

```sql
select queue_name, consumer_name, lag, last_seen, pending_events, last_tick
from pgque.get_consumer_info('orders', 'processor');
```

In a healthy system `lag` and `last_seen` both stay low and `pending_events`
stays near zero. A consumer whose `last_tick` stops advancing while `last_seen`
keeps climbing is stuck — see the next section.

### pgque.get_batch_info(batch_id) — inspect one in-flight batch

Given a batch id (the `batch_id` on a `pgque.message`, or `current_batch` from
`get_consumer_info`), this returns one row describing the batch: `queue_name`,
`consumer_name`, `batch_start`, `batch_end`, `prev_tick_id`, `tick_id`, `lag`,
`seq_start`, `seq_end`. Use it to debug a specific batch that seems stalled —
`lag` is `now()` minus the batch's end-tick time, and `seq_end - seq_start`
approximates the batch's event span.

```sql
select queue_name, consumer_name, lag, seq_start, seq_end
from pgque.get_batch_info(12345);
```

### Partitioned consumers

`pgque.partition_slot_status` returns one row for every expected slot of every
partitioned consumer, including a slot whose engine subscription is missing:

```sql
select queue_name, consumer, slot, n, subscribed, lease_owner,
       lease_until, epoch, last_tick, pending_events
from pgque.partition_slot_status
order by queue_name, consumer, slot;
```

| column | meaning | watch for |
|---|---|---|
| `subscribed` | whether the slot's engine subscription exists | `false` means incomplete setup; `last_tick` and `pending_events` are null because lag is unknown |
| `lease_owner` | current live owner; expired owners are shown as null | null while an always-on worker pool should own every slot |
| `lease_until` | stored expiry, including an expired lease | repeatedly expires during normal work when TTL/renewal is too short |
| `epoch` | fencing counter incremented on takeover | rapid growth indicates worker churn or repeated expiry |
| `last_tick` | this slot's independent cursor | freezes while the queue's latest tick advances when polling stops |
| `pending_events` | pre-hash-filter event-sequence lag | sustained growth means the slot is behind; zero means caught up exactly |

`pending_events` counts the queue stream before the slot's hash filter, so it
overstates the slot's own routed share by approximately `n`. Use it for trends
and zero/nonzero state, not as an exact number of messages the worker will
receive.

Every slot must advance. Each is an independent subscription over the full
stream, and one unpolled slot pins table rotation even when all other slots are
caught up. The first-party clients do not yet run or monitor the claim loop;
applications must cover all `0..n-1` slots themselves. See
[Partition keys](partition-keys.md) for lease renewal, fencing, pooler behavior,
and incomplete-slot recovery.

## What to alert on

### The critical one: a stuck consumer blocks rotation

This is the headline operational risk in PgQue, and it is worth understanding
before any other alert.

PgQue stores events in a set of inherited tables and reclaims space by
**rotating** them: periodically it advances to the next table in the set and
`TRUNCATE`s the one it is reusing. Rotation is the only thing that frees disk —
there are no per-row deletes.

Rotation is gated on the slowest consumer. Step one of rotation finds the lowest
`sub_last_tick` across all subscriptions on the queue; if the slowest consumer
still needs the table about to be truncated, rotation returns zero and skips.
A consumer that has stopped — crashed, deadlocked, deploy gone wrong, or simply
far too slow — pins that lowest tick and **blocks the TRUNCATE indefinitely.**
The event tables then grow without bound until the consumer recovers or is
unsubscribed.

So the alert that protects your disk is not a disk alert — it is a stuck-consumer
alert. Catch it by watching `get_consumer_info`:

- `last_seen` keeps growing for a consumer that should be active, and
- its `last_tick` is not advancing while `last_tick_id` on the queue is,
- typically with `pending_events` climbing alongside.

When you confirm a consumer is wedged and will not come back, unsubscribe it so
rotation can proceed:

```sql
select pgque.unsubscribe('orders', 'dead_consumer');
```

(Or `pgque.drop_queue('orders', true)` to unregister all consumers, if you are
tearing the queue down.) A dead consumer that you do not intend to restart must
be unsubscribed, or it will hold the queue's storage forever.

For a partitioned consumer, recover or replace the worker and resume polling
the same slot. Do not use `unsubscribe_slot` as an alert response: it leaves the
logical consumer incomplete, removes slot-owned retry/DLQ state, and a later
repair cannot recover history ticked while the subscription was missing. Use
controlled teardown/recreation only with the recovery rules in the
[partition guide](partition-keys.md#incomplete-setup-and-recovery).

### Threshold table

Frame these relatively — PgQue ships no SLA. Alert on trends across several
sampling intervals, not on a single reading, and tune absolute thresholds to
your own tick rate and traffic.

| signal | source | alert when | why it matters |
|---|---|---|---|
| ticker lag | `get_queue_info.ticker_lag` | climbs and stays above `ticker_idle_period` (default 1 minute) across intervals | ticker not running → no batches → no delivery |
| consumer lag | `get_consumer_info.lag` / `pending_events` | `lag` and `pending_events` keep growing across intervals | a consumer is falling behind real-time |
| stuck consumer | `get_consumer_info.last_seen` + frozen `last_tick` | `last_seen` grows while `last_tick` stays put and the queue's `last_tick_id` advances | pins the lowest tick → blocks `TRUNCATE` rotation → event tables grow unbounded (the critical one) |
| partition slot | `partition_slot_status` | `subscribed = false`, or one slot's `last_tick` freezes and `pending_events` grows | incomplete/unpolled slot misses its key class or pins queue rotation |
| lease churn | `partition_slot_status.lease_until` / `epoch` | leases repeatedly expire or epochs climb during ordinary processing | TTL is too short, renewals are not committing, or workers are unstable |
| DLQ depth | `dlq_inspect` row count / `pgque.dead_letter` | the dead-letter backlog grows or is non-empty when you expect zero | events are exhausting retries; a downstream is failing |

### Dead-letter depth

Events that exhaust their retries (5 by default) land in `pgque.dead_letter`.
A growing dead-letter backlog means a downstream is failing repeatedly. Count it
two ways — directly on the table, or via `dlq_inspect` (both granted to
`pgque_reader`):

```sql
-- depth per queue, straight from the table
select dl_queue_id, count(*) as dlq_depth
from pgque.dead_letter
group by dl_queue_id
order by dlq_depth desc;

-- inspect the most recent dead-lettered events for one queue
select dl_id, ev_id, dl_time, dl_reason, ev_type
from pgque.dlq_inspect('orders', 20);
```

To replay or purge dead-letter entries, see the DLQ functions in the
[Reference](reference.md) and the patterns in [Examples](examples.md).

## Read-only monitoring queries

Everything below runs as `pgque_reader`.

Confirm the installed version:

```sql
select pgque.version();
```

Queue health across all queues at a glance:

```sql
select queue_name, ticker_lag, ev_per_sec, ev_new, last_tick_id
from pgque.get_queue_info()
order by ticker_lag desc;
```

Every consumer's lag and liveness, worst first:

```sql
select queue_name, consumer_name, lag, last_seen, pending_events, last_tick
from pgque.get_consumer_info()
order by last_seen desc nulls last;
```

Partitioned slots needing attention:

```sql
select queue_name, consumer, slot, n, subscribed, lease_owner,
       lease_until, epoch, last_tick, pending_events
from pgque.partition_slot_status
where not subscribed
   or pending_events > 0
order by subscribed, pending_events desc nulls first,
         queue_name, consumer, slot;
```

Stuck-consumer hunt — join consumer position against the queue's latest tick so
a frozen `last_tick` stands out against an advancing `last_tick_id`:

```sql
select c.queue_name, c.consumer_name, c.last_seen, c.last_tick,
       q.last_tick_id, q.last_tick_id - c.last_tick as ticks_behind,
       c.pending_events
from pgque.get_consumer_info() c
join pgque.get_queue_info() q using (queue_name)
order by ticks_behind desc nulls last;
```

Dead-letter depth per queue:

```sql
select dl_queue_id, count(*) as dlq_depth, max(dl_time) as latest
from pgque.dead_letter
group by dl_queue_id
order by dlq_depth desc;
```

## Related

- [Concepts](concepts.md) — tick, batch, rotation, and the snapshot rule.
- [Installation and operations](installation.md) — `pg_cron` setup, the ticker cadence, and roles.
- [Latency and tuning](latency-and-tuning.md) — how `tick_period_ms` and the ticker thresholds trade latency against overhead.
- [Reference](reference.md) — full signatures, return columns, and role grants.
- [Examples](examples.md) — DLQ replay, fan-out, and exactly-once patterns.
- [Partition keys](partition-keys.md) — slot lifecycle, leases, operational
  limits, and recovery.
- [Producer idempotency](producer-idempotency.md) — deduplication maintenance
  and claim-table sizing.
