# Three latencies

"Queue latency" is three numbers, not one. Conflating them confuses design discussion — each reflects a different bottleneck, and PgQue's trade-offs only make sense once they are separated.

| # | Name | What it is | PgQue | Bottleneck |
|---|---|---|---|---|
| 1 | Producer | `send` / `insert_event` → durable | sub-ms (~high-µs; ~86k ev/s PL/pgSQL single-INSERT in prelim bench) | WAL flush, triggers |
| 2 | Subscriber | `next_batch` + `get_batch_events` returning an already-built batch | sub-ms (snapshot SELECT, no SKIP LOCKED scan; ~2.4M ev/s consumer read) | how "next work" is located |
| 3 | End-to-end | `send` → consumer visibility | ≈ tick period + consumer poll interval | ticker cadence (tunable) |

#3 is what application behavior depends on (SLAs, retries, perceived staleness). The trap: #3 is bounded below by #1 + #2, but **the magnitude of #1 and #2 doesn't determine #3** — tick cadence and consumer poll interval do. You can drive #1 and #2 to microseconds and still have #3 in seconds because the ticker hasn't fired yet. The reverse — sub-ms #3 while #1 or #2 takes seconds — isn't possible: a message can't be visible to a consumer faster than it can be written and read.

## End-to-end is tunable, not floored

The default 1-second tick is a `pg_cron` schedule, not a design floor. PgQue's e2e is bounded by whatever tick cadence you configure. Sub-ms e2e is achievable with more aggressive ticking:

- **Staggered `pg_cron` jobs.** Schedule N jobs at `1 second` each, offset by `1/N` via a shared coordinating lock, to get effective tick periods down to ~10 ms (N=100) or ~1 ms (N=1000).
- **In-tick sleep loop.** Single cron callout that internally does `pg_sleep(0.01)` ×100 inside one invocation — same effective cadence, fewer scheduler wakeups.
- **Native sub-second cron.** `pg_cron` does not yet support sub-second schedules natively; the staggered-job or in-tick-sleep workarounds are the current approach.

Trade-off at very high tick rates: every tick UPDATEs `pgque.tick` and `pgque.subscription`, so more ticks = more dead tuples on those metadata tables under held-xmin conditions. The event tables stay bloat-free (TRUNCATE rotation); the metadata-table bloat is a separate story. PgQue uses the same UPDATE pattern on the small subscription/tick tables as upstream PgQ — the bloat shape is bounded but real under sustained held-xmin.

Rough guidance:

| `pg_cron` schedule | Average e2e | Notes |
|---|---|---|
| `1 second` (default) | ~500 ms | pgqd-compatible, minimal metadata churn |
| `250 ms` | ~125 ms | 4× metadata writes, still cheap |
| `10 ms` staggered | ~5 ms | needs coordinated jobs or in-tick sleep |
| `1 ms` staggered | sub-ms | kHz-range; metadata-table rotation recommended |

Per-queue thresholds (`queue_ticker_max_lag` default `3 seconds`, `queue_ticker_max_count` default 500, `queue_ticker_idle_period` default `1 minute` idle-decelerator) go through `pgque.set_queue_config()`.

## Load behavior: PgQue vs. UPDATE/DELETE designs

The key property of the tick model: **e2e does not grow with load.** The ticker fires at its configured rate regardless of backlog, so under pressure batch size grows (up to `queue_ticker_max_count`) — not e2e.

UPDATE/DELETE-based systems use a different model: a consumer call returns messages immediately, marking them consumed via UPDATE (claim) and DELETE (ack) rather than advancing a snapshot cursor. So e2e ≈ consumer poll interval — sub-ms when the consumer is actively polling, up to the poll interval otherwise. Drain rate is `batch_size / poll_interval`; if producers outrun that, queue depth grows and e2e grows with it until consumers scale out. Separately, those UPDATEs and DELETEs produce dead tuples that autovacuum cannot reclaim under MVCC pressure (long-running tx, idle-in-transaction, lagging logical replication slot, physical standby with `hot_standby_feedback=on`) — the bloat failure mode [PgQue avoids by construction](../README.md#why-pgque).
