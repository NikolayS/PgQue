# Three latencies

"Queue latency" is three numbers, not one. Conflating them confuses design discussion — each reflects a different bottleneck, and PgQue's trade-offs only make sense once they are separated.

| # | Name | What it is | PgQue | Bottleneck |
|---|---|---|---|---|
| 1 | Producer | `send` / `insert_event` → durable | sub-ms (~high-µs; ~86k ev/s PL/pgSQL single-INSERT in prelim bench) | WAL flush, triggers |
| 2 | Subscriber | `next_batch` + `get_batch_events` returning an already-built batch | sub-ms (snapshot SELECT, no SKIP LOCKED scan; ~2.4M ev/s consumer read) | how "next work" is located |
| 3 | End-to-end | `send` → consumer visibility | ≈ tick period (default **100 ms / 10 Hz**) + consumer poll interval | ticker cadence (tunable via `pgque.set_tick_period_ms`) |

#3 is what application behavior depends on (SLAs, retries, perceived staleness). The trap: #3 is bounded below by #1 + #2, but **the magnitude of #1 and #2 doesn't determine #3** — tick cadence and consumer poll interval do. You can drive #1 and #2 to microseconds and still have #3 in seconds because the ticker hasn't fired yet. The reverse — sub-ms #3 while #1 or #2 takes seconds — isn't possible: a message can't be visible to a consumer faster than it can be written and read.

## End-to-end is tunable, not floored

The default cadence is **10 Hz (one tick every 100 ms)** — not the 1-second `pg_cron` floor. PgQue achieves this with a single 1-second `pg_cron` slot that calls `pgque.ticker_loop()`, a procedure that re-invokes `pgque.ticker()` every `tick_period_ms` and `commit`s between iterations. The commit is essential: each tick gets its own transaction, snapshot semantics are preserved, and held-xmin is bounded by `tick_period_ms` rather than by the 1-second slot — so PgQ's metadata rotation isn't blocked.

Tune at runtime:

```sql
select pgque.set_tick_period_ms(50);    -- 20 Hz
select pgque.set_tick_period_ms(10);    -- 100 Hz
select pgque.set_tick_period_ms(1);     -- 1 kHz
```

Range: `1`..`60000` ms. Effective on the next pg_cron slot (≤1 s); no rescheduling needed. Inspect the current rate with `select * from pgque.status();`.

Trade-offs at high tick rates:
- **WAL volume.** Every tick UPDATEs `pgque.tick` and writes to per-queue tick partitions. 10 Hz is ~10× the WAL of 1 Hz; 1 kHz is ~1000×. Bench against your workload before pushing the rate up.
- **Metadata dead tuples.** `pgque.tick` and `pgque.subscription` are UPDATEd on every tick. PgQue rotates these tables to keep peak dead-tuple counts bounded; at sub-50 ms ticks, scale the rotation period down proportionally.
- **NOTIFY pressure.** `pgque.ticker()` emits one `pg_notify` per ticked queue. The NOTIFY queue is a single global SLRU (8 GiB ceiling) — slow LISTEN consumers can fall behind at very high rates.
- **`pg_cron` log hygiene.** *Not* a new problem at high rates: PgQue still uses **one** pg_cron slot per second regardless of `tick_period_ms`, so `cron.job_run_details` grows at the same per-second rate as a 1 Hz schedule. The pre-existing log-hygiene recipe (`alter system set cron.log_run = off`, or a periodic purge job) is unchanged — see [tutorial.md](tutorial.md#production-cadence-use-pg_cron).

Rough guidance:

| `tick_period_ms` | Effective rate | Median e2e | Notes |
|---|---|---|---|
| `1000` | 1 Hz | ~500 ms | pgqd-compatible, minimal WAL/metadata churn |
| `100` (**default**) | 10 Hz | ~50 ms | sweet spot for non-LISTEN consumers |
| `25` | 40 Hz | ~12 ms | 25× WAL of default; consider rotation tuning |
| `10` | 100 Hz | ~5 ms | tighten metadata rotation cadence |
| `1` | 1 kHz | sub-ms | bench WAL + NOTIFY + metadata bloat first |

Per-queue thresholds (`queue_ticker_max_lag` default `3 seconds`, `queue_ticker_max_count` default 500, `queue_ticker_idle_period` default `1 minute` idle-decelerator) go through `pgque.set_queue_config()`.

## Load behavior: PgQue vs. UPDATE/DELETE designs

The key property of the tick model: **e2e does not grow with load.** The ticker fires at its configured rate regardless of backlog, so under pressure batch size grows (up to `queue_ticker_max_count`) — not e2e.

UPDATE/DELETE-based systems use a different model: a consumer call returns messages immediately, marking them consumed via UPDATE (claim) and DELETE (ack) rather than advancing a snapshot cursor. So e2e ≈ consumer poll interval — sub-ms when the consumer is actively polling, up to the poll interval otherwise. Drain rate is `batch_size / poll_interval`; if producers outrun that, queue depth grows and e2e grows with it until consumers scale out. Separately, those UPDATEs and DELETEs produce dead tuples that autovacuum cannot reclaim under MVCC pressure (long-running tx, idle-in-transaction, lagging logical replication slot, physical standby with `hot_standby_feedback=on`) — the bloat failure mode [PgQue avoids by construction](../README.md#why-pgque).
