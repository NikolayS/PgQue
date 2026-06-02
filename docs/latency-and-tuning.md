---
title: Latency and tick tuning
description: The three queue latencies, why end-to-end delivery tracks the tick period, and how to tune tick cadence.
---

"Queue latency" is three numbers, not one. Conflating them confuses design
discussion — each reflects a different bottleneck, and PgQue's trade-offs only
make sense once they are separated. This page explains the three latencies, why
the third one is what your application actually feels, and how to tune it with
the tick period.

## The three latencies

| # | Name | What it is | PgQue | Bottleneck |
|---|------|------------|-------|------------|
| 1 | Producer | `send` / `insert_event` → durable | sub-ms | WAL flush, triggers |
| 2 | Subscriber | `next_batch` over an already-built batch | sub-ms | how "next work" is located |
| 3 | End-to-end | `send` → consumer visibility | ≈ tick period | ticker cadence (tunable) |

### 1. Producer latency

`pgque.send()` reduces to a single `insert_event()` — one INSERT into the
current event table. There is no SKIP LOCKED scan, no claim UPDATE, no row
lock. The cost is a WAL flush and any CDC triggers you have attached, so a
durable send completes in sub-millisecond time.

### 2. Subscriber latency

`pgque.receive()` opens a batch with `next_batch` and returns the events that
already belong to it. The batch boundary was computed by a prior tick, so the
read is a plain snapshot-bounded SELECT over a range of `ev_txid` values — no
scan for "claimable" rows, no locking. Returning an already-built batch is
sub-millisecond.

### 3. End-to-end delivery

End-to-end latency is the gap from `send()` to the moment a consumer can see
the event. This is the number application behavior depends on — SLAs, retry
timing, perceived staleness. In PgQue it is approximately one tick period.

The trap: #3 is bounded below by #1 + #2, but the magnitude of #1 and #2 does
not determine #3. Tick cadence does. You can drive producer and subscriber
latency to microseconds and still have end-to-end latency in the hundreds of
milliseconds because the ticker has not fired yet. The reverse is impossible: a
message cannot be visible to a consumer faster than it can be written and read.

## Why end-to-end ≈ the tick period

PgQue is tick-based. A consumer sees an event only after a tick creates a batch
boundary that includes it. Each tick stores a `pg_snapshot`, and a batch is the
set of events visible in the current tick's snapshot but not in the previous
one. An event sent between two ticks is simply waiting for the next tick's
snapshot to capture it.

So an event sent just after a tick waits almost a full period; one sent just
before the next tick waits almost nothing. Averaged over arrivals, the mean
wait is about half the period, and the worst case is about one full period.

The committed benchmark confirms this. At the default `tick_period_ms = 100`,
median end-to-end delivery is about 52 ms — almost exactly period/2 — with a
maximum of about 105 ms, roughly one period. See `benchmark/tick-rate/`.

### It does not grow with load

The key property of the tick model: end-to-end latency does not grow with
load. The ticker fires at its configured rate regardless of backlog. Under
pressure, the batch size grows (up to `queue_ticker_max_count`) — not the
delivery latency. A producer-side spike makes batches larger, not later.

## Tuning the tick cadence

PgQue ticks 10 times per second by default (every 100 ms). Tune it at runtime:

```sql
select pgque.set_tick_period_ms(50);   -- 20 ticks/sec
select pgque.set_tick_period_ms(10);   -- 100 ticks/sec
select pgque.set_tick_period_ms(1);    -- 1000 ticks/sec
```

Accepted values are exact divisors of 1000 in the 1..1000 ms range: 1, 2, 4, 5,
8, 10, 20, 25, 40, 50, 100, 125, 200, 250, 500, 1000. The change applies on the
next scheduler slot (≤1 s); no rescheduling is needed. Inspect the current rate
with `select * from pgque.status()`.

How this works without a sub-second scheduler: `pg_cron` has a 1-second
minimum granularity. PgQue still uses one cron slot per second regardless of
`tick_period_ms`. That slot calls `pgque.ticker_loop()`, a procedure that
invokes `pgque.ticker()` every `tick_period_ms` and commits between iterations.
The commit per iteration is essential — each tick gets its own transaction,
snapshot semantics are preserved, and the held xmin is bounded by
`tick_period_ms` rather than by the full second, so metadata rotation is not
blocked.

### Latency by tick period

From the committed `benchmark/tick-rate/` reproducer (single laptop,
Postgres 16, `pg_cron` with `use_background_workers = on`; 100 ev/s producer,
30 s per cell; methodology below):

| `tick_period_ms` | Effective rate | p50 e2e | p95 e2e | max e2e |
|------------------|----------------|---------|---------|---------|
| 1000 | 1 tick/sec | ≈ 503 ms | — | — |
| 100 (default) | 10 ticks/sec | ≈ 52 ms | ≈ 99 ms | ≈ 105 ms |
| 10 | 100 ticks/sec | ≈ 8 ms | — | — |
| 1 | 1000 ticks/sec | ≈ 3 ms | — | — |

At the default the distribution is clean: p50 ≈ 52 ms, p95 ≈ 99 ms, max
≈ 105 ms — tracking "wait for the next tick, mean ≈ period/2". At 10 ms and
1 ms the median drops to single digits, but the benchmark shows the tail
inflating toward the 1-second slot length: at very short periods the inner loop
may not finish all its iterations within one cron slot, so an occasional tick
window is skipped. Treat sub-10 ms periods as specialized and benchmark them on
your own hardware first.

### Idle backoff makes quiet queues cheap

The tick period is a check cadence, not a promise to write a tick row 10 times
a second for every queue. When no events are arriving, `pgque.ticker()` usually
returns nothing and PgQue backs off that queue toward its `ticker_idle_period`
(default 1 minute). Calling the ticker every 100 ms is cheap when it has
nothing to do, so an idle queue produces only occasional metadata writes.

### Per-queue tick triggers

When there is activity, a tick is created when either threshold is crossed,
both set per queue via `pgque.set_queue_config()`:

| Short name | Default | Triggers a tick when |
|------------|---------|----------------------|
| `ticker_max_count` | 500 | this many new events have accumulated |
| `ticker_max_lag` | 3 seconds | events have been waiting this long |
| `ticker_idle_period` | 1 minute | upper bound on cadence while idle |

```sql
select pgque.set_queue_config('orders', 'ticker_max_count', '1000');
select pgque.set_queue_config('orders', 'ticker_max_lag', '1 second');
```

## The cost of a higher tick rate

A higher tick rate buys lower end-to-end latency at the cost of more work per
unit time. The effects are qualitative — measure them on your own workload
before committing to an aggressive setting:

- **More WAL.** Every materialized tick writes PgQue metadata. A queue that
  materializes ticks continuously at a higher rate writes proportionally more
  WAL. Idle queues are unaffected — they back off. The exact bytes per tick
  depend on your Postgres version, `full_page_writes`, `wal_compression`,
  checkpoint cadence, and page state, so measure with `pg_current_wal_lsn()`
  deltas on your own cluster rather than relying on a single figure.
- **More metadata churn.** `pgque.tick` and `pgque.subscription` are UPDATEd on
  every tick, producing dead tuples that autovacuum reclaims. PgQue rotates
  these tables to keep the working set bounded; at sub-50 ms periods, scale the
  rotation period down so the peak dead-tuple count stays in check. Watch
  `pg_stat_user_tables` for these tables.
- **More NOTIFY traffic.** `pgque.ticker()` emits one `pg_notify` per ticked
  queue. The NOTIFY queue is a single global SLRU (8 GiB ceiling, a Postgres
  platform limit); slow LISTEN consumers can fall behind at very high tick
  rates.
- **One cron worker held per slot.** The sub-second loop occupies one pg_cron
  background worker for roughly the length of its 1-second slot. This is true
  at any `tick_period_ms` — PgQue uses one slot per second regardless — but it
  matters when many PgQue databases share one cluster's pool of cron workers.

## PgQue vs UPDATE/DELETE designs

UPDATE/DELETE-based queues use a different model: a consumer call returns
messages immediately and marks them consumed via an UPDATE (claim) and a DELETE
(ack), rather than advancing a snapshot cursor. End-to-end latency there is
about the consumer poll interval — sub-ms while a consumer is actively polling.

The trade-off is the opposite of PgQue's:

- **UPDATE/DELETE designs** return immediately but generate dead tuples on every
  claim and ack. Under MVCC pressure — a long-running transaction, an
  idle-in-transaction session, a lagging logical replication slot, or a standby
  with `hot_standby_feedback = on` — autovacuum cannot reclaim those tuples and
  the queue table bloats. The bloat mechanism and the committed evidence are in
  [concepts.md](concepts.md).
- **PgQue** trades a tick-period delay (≈ 52 ms median at the default) for zero
  bloat by construction: no per-row claim or delete, just snapshot diffs and
  TRUNCATE-based table rotation.

If you need sub-millisecond delivery more than you need bloat resistance, an
UPDATE/DELETE design may fit better. If stable latency and zero bloat under
MVCC pressure matter more, the tick-period delay is the price PgQue pays for
them.

## pg_cron log hygiene

When you run PgQue with pg_cron, `cron.job_run_details` accumulates a row per
job run. PgQue runs a fixed set of jobs (the ticker once a second plus the
maintenance jobs), so this table grows steadily over time. The sub-second tick
loop does not multiply the count — there is still one ticker slot per second
regardless of `tick_period_ms` — but on a small deployment the successful-run
history can still dominate unless you trim it.

A scoped purge keeps only PgQue's own job history bounded, leaving unrelated
jobs untouched:

```sql
select cron.schedule(
  'pgque_purge_cron_log',
  '0 * * * *',
  $$
  delete from cron.job_run_details d
  using cron.job j
  where d.jobid = j.jobid
    and j.jobname in (
      'pgque_ticker',
      'pgque_retry_events',
      'pgque_maint',
      'pgque_rotate_step2',
      'pgque_purge_cron_log'
    )
    and d.end_time < now() - interval '1 day'
  $$
);
```

If you do not need successful-run history for any pg_cron job at all,
`alter system set cron.log_run = off` disables it globally (after a restart).
With pg_timetable, configure its own execution-log retention instead.

## Methodology

The `benchmark/tick-rate/` figures measure end-to-end delivery latency as
`consumer_recv_ts - producer_send_ts`, both taken from server-side
`clock_timestamp()` so there is no client clock skew. The producer stamps the
send timestamp into the JSON payload; the consumer reads it back at
`pgque.receive()` time and subtracts. The consumer polls in a tight loop with a
1 ms sleep between empty receives and no LISTEN/NOTIFY wakeup, so the numbers
reflect the tick-driven path alone. For the idle sweep, the queue's tick
triggers are lowered to their minimum so a tick fires on essentially every
check regardless of event volume, isolating the effect of `tick_period_ms`. All
cells reported sent == received (zero loss).

## See also

- [reference.md](reference.md) — `set_tick_period_ms`, `set_queue_config`, and
  the ticker functions.
- [concepts.md](concepts.md) — snapshot batching, rotation, and the bloat
  mechanism PgQue avoids.
- [monitoring.md](monitoring.md) — reading tick lag and consumer lag in
  production.
- [tutorial.md](tutorial.md) — getting a ticker running with pg_cron.
- [installation.md](installation.md) — installing and starting PgQue.
- [examples.md](examples.md) — common produce/consume patterns.
