# PgQ: Concepts

Vocabulary adapted from the 2009 PgCon talk by Kreen & Pihlak
([slides](https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf)).

## Glossary

- **Event** — one row in a queue table. Delivered **at-least-once**.
- **Batch** — events between two ticks, served to a consumer together.
- **Queue** — named event stream; 3 rotating tables, purged by `TRUNCATE`.
  Any number of queues can coexist in one database.
- **Producer** — anything that calls `insert_event` / `pgque.send`. Any
  number of producers can write to the same queue concurrently.
- **Consumer** — subscribes, reads batches, calls `ack` (or `finish_batch`).
  Any number of consumers can subscribe to the same queue; each has its
  own cursor and independently sees every event (fan-out by default).
- **Ticker** — creates ticks, vacuums, rotates, reschedules retries.
  In PgQue: `pg_cron` calling `pgque.ticker()`.
- **Tick** — position marker in the event stream; delimits batches.

## Delivery

At-least-once. Exactly-once requires either:

- **Same DB:** process in the same transaction as `finish_batch` (or `pgque.ack`).
- **Cross DB:** target-side batch/event tracking — record the `batch_id` or per-event ids on the target side and skip duplicates. PgQue does not ship a helper for this today.

## Consumer loop

```
batch_id = next_batch(queue, consumer)   -- NULL → sleep, retry
events   = get_batch_events(batch_id)
process(events)                           -- nack individual failures
finish_batch(batch_id)
commit
```

## Event row

`ev_id`, `ev_time`, `ev_txid` (`xid8`), `ev_retry`, `ev_type`, `ev_data`,
`ev_extra1..4`. `ev_extra1` is table name by convention (triggers).
Payload format is a producer/consumer contract — PgQue does not interpret it.

## Health signals

`pgque.get_consumer_info()`:

- **lag** — age of last finished batch; high = falling behind.
- **last_seen** — time since last batch; high = consumer not running.

## Per-queue tuning

Stored on `pgque.queue`, read by `pgque.ticker()` (pg_cron). Set via
`pgque.set_queue_config(queue, param, value)` — `param` is the short name
below; the function auto-prefixes `queue_` internally.

- `ticker_max_lag` — max wall time between ticks.
- `ticker_idle_period` — tick interval when idle.
- `ticker_max_count` — force tick at N events (batch-size cap).
- `rotation_period` — table rotation period (disk vs. history).
- `max_retries` — retry ceiling before a message goes to `pgque.dead_letter`.

## Ticker rule

> Keep the ticker running. No ticks → no batches → no delivery. Long pauses
> produce huge batches consumers can't handle.

— Kreen & Pihlak, PgCon 2009

## Three latencies

"Queue latency" is three numbers, not one. Conflating them confuses
design discussion — each reflects a different bottleneck, and PgQue's
trade-offs only make sense once they are separated.

| # | Name | What it is | PgQue | Bottleneck |
|---|---|---|---|---|
| 1 | Producer | `send` / `insert_event` → durable | sub-ms (~high-µs; ~86k ev/s PL/pgSQL single-INSERT in prelim bench) | WAL flush, triggers |
| 2 | Subscriber | `next_batch` + `get_batch_events` returning an already-built batch | sub-ms (snapshot SELECT, no SKIP LOCKED scan; ~2.4M ev/s consumer read) | how "next work" is located |
| 3 | End-to-end | `send` → consumer visibility | ≈ tick period + consumer poll interval | ticker cadence (tunable) |

#3 is the one application behavior depends on (SLAs, retries, perceived
staleness). You can have #1 and #2 in microseconds and still have #3 in
seconds — or vice versa. They are independent.

### End-to-end is tunable, not floored

**The default 1-second tick is a `pg_cron` schedule, not a design floor.**
PgQue's e2e is bounded by whatever tick cadence you configure. Sub-ms
e2e is achievable with more aggressive ticking:

- **Staggered `pg_cron` jobs.** Schedule N jobs at `1 second` each, offset
  by `1/N` via a shared coordinating lock, to get effective tick periods
  down to ~10 ms (N=100) or ~1 ms (N=1000).
- **In-tick sleep loop.** Single cron callout that internally does
  `pg_sleep(0.01)` ×100 inside one invocation — same effective cadence,
  fewer scheduler wakeups.
- **Native sub-second cron.** Future `pg_cron` may support sub-second
  schedules directly, removing the workaround.

Trade-off at very high tick rates: every tick UPDATEs `pgque.tick` and
`pgque.subscription`, so more ticks = more dead tuples on those metadata
tables under held-xmin conditions. The event tables stay bloat-free
(TRUNCATE rotation); the metadata-table bloat is a separate story and
is addressed by extending the same rotation pattern to those tables —
at sufficiently high tick rates that mitigation becomes necessary.

Rough guidance:

| `pg_cron` schedule | Average e2e | Notes |
|---|---|---|
| `1 second` (default) | ~500 ms | pgqd-compatible, minimal metadata churn |
| `250 ms` | ~125 ms | 4× metadata writes, still cheap |
| `10 ms` staggered | ~5 ms | needs coordinated jobs or in-tick sleep |
| `1 ms` staggered | sub-ms | kHz-range; metadata-table rotation recommended |

Per-queue thresholds (`queue_ticker_max_lag` default `3 seconds`,
`queue_ticker_max_count` default 500, `queue_ticker_idle_period` default
`1 minute` idle-decelerator) go through `pgque.set_queue_config()`.

### Load behavior: PgQue vs. UPDATE/DELETE designs

The key property of the tick model: **e2e does not grow with load.** The
ticker fires at its configured rate regardless of backlog, so under
pressure batch size grows (up to `queue_ticker_max_count`) — not e2e.

UPDATE/DELETE-based systems use a different model: a consumer call
returns messages immediately, marking them consumed via UPDATE (claim)
and DELETE (ack) rather than advancing a snapshot cursor. So e2e ≈
consumer poll interval — sub-ms when the consumer is actively polling,
up to the poll interval otherwise. Drain rate is
`batch_size / poll_interval`; if producers outrun that, queue depth
grows and e2e grows with it until consumers scale out. Separately, those
UPDATEs and DELETEs produce dead tuples that autovacuum cannot reclaim
under MVCC pressure (long-running tx, idle-in-transaction, lagging
logical replication slot, physical standby with
`hot_standby_feedback=on`) — the bloat failure mode
[PgQue avoids by construction](../README.md#why-pgque).

### When to pick which

Pick PgQue if you want batching efficiency and bloat immunity and can
configure a tick cadence that meets your SLA (the default 1 s or a faster
one). Pick an UPDATE/DELETE-based system if you need always-hot
single-digit-ms delivery for synchronous request/response patterns, MVCC
pressure is low in your environment, and that system's API fits better.
