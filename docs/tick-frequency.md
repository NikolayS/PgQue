# Tick frequency tuning

PgQue is tick-based. A consumer sees events only after a tick creates a batch boundary. `pgque.start()` uses `pg_cron` to run one 1-second slot, but that slot calls `pgque.ticker_loop()`, which invokes `pgque.ticker()` every `pgque.config.tick_period_ms` ms and commits between iterations.

Default: `tick_period_ms = 100`, i.e. 10 checks/sec.

## The key point: idle queues are cheap

The default 100 ms period is the **ticker check cadence**, not a guarantee that PgQue writes a new tick row 10 times/sec for every queue.

If no events are coming, `pgque.ticker()` usually returns `NULL`. PgQue then backs off idle queues toward `ticker_idle_period` (default `1 minute`). Inactive queues therefore create only occasional metadata writes. This matters for small databases — for example a free-tier database with a 1 GiB storage cap should not expect a quiet PgQue queue to burn hundreds of MiB/day just because the default check period is 100 ms.

The WAL cost comes from **materialized ticks**, not from every ticker check.

## Choosing a cadence

The tick period is the main dial for end-to-end delivery latency:

| `tick_period_ms` | Check rate | Typical median wait for next tick | When to consider it |
|---|---:|---:|---|
| `1000` | 1 check/sec | ~500 ms | small projects, WAL-constrained systems, slow logical-replication subscribers, pgqd-compatible cadence |
| `100` | 10 checks/sec | ~50 ms | default; lower latency without pushing into very high churn |
| `50` | 20 checks/sec | ~25 ms | latency-sensitive apps after checking WAL/metadata budget |
| `10` | 100 checks/sec | ~5 ms | specialized workloads; benchmark first |
| `1` | 1000 checks/sec | sub-ms average tick wait | experimental/extreme; benchmark WAL, NOTIFY, metadata churn |

Allowed values are exact divisors of `1000` in the `1`..`1000` ms range:

```sql
select pgque.set_tick_period_ms(1000);  -- 1 check/sec
select pgque.set_tick_period_ms(100);   -- 10 checks/sec, default
select pgque.set_tick_period_ms(50);    -- 20 checks/sec
```

The change applies on the next `pg_cron` slot (≤1 s); no rescheduling needed.

## WAL budget for active queues

Every materialized tick writes PgQue metadata. To estimate the unit cost, we forced a queue to materialize ticks in a simple PG18 measurement without producer writes; that isolated cost was about **280 bytes of WAL per materialized tick per queue**.

That is **not** the WAL/day of a default idle queue. The table below is only the projection for a queue that actually materializes ticks continuously at the listed rate — usually because it is continuously active, or because idle ticking was configured aggressively.

Approximate continuously-materializing queue budget:

| Materialized tick rate | Formula | Estimate |
|---:|---|---:|
| 1 tick/sec continuously | 280 B × 86,400 | ~24 MiB/day per queue |
| 10 ticks/sec continuously | 280 B × 10 × 86,400 | ~240 MiB/day per queue |
| 100 ticks/sec continuously | 280 B × 100 × 86,400 | ~2.4 GiB/day per queue |

Treat these as order-of-magnitude planning numbers, not guarantees.

Things that can move the number:

- **Full-page images (FPIs).** With PostgreSQL's default `full_page_writes = on`, the first change to a page after a checkpoint can log a full page image. Fresh clusters, short checkpoint intervals, and dirty-page patterns can make measured WAL higher than the steady-state bytes/tick number.
- **Postgres version and settings.** `wal_compression`, page layout, checkpoint cadence, and storage settings matter.
- **Table state.** Page splits, relation extension, and vacuum/rotation timing can change per-tick WAL.
- **Number of active queues.** Ticking is per queue. Ten continuously-active queues at 10 materialized ticks/sec are roughly ten times the single-queue estimate. Ten idle queues are not.
- **pg_cron logging.** `cron.job_run_details` WAL is separate. PgQue's sub-second loop does not multiply pg_cron log rows: there is still one cron slot per second regardless of `tick_period_ms`, but successful-run logging can still dominate small deployments unless disabled or purged.

## Why idle queues back off

With no producer writes, `pgque.ticker()` backs off using queue settings:

- `ticker_max_lag` — max wall time between ticks when there is activity.
- `ticker_idle_period` — upper bound for idle ticking; default `1 minute`.
- `ticker_max_count` — event-count threshold for creating a tick.

So a quiet queue tends toward occasional idle ticks rather than 10 materialized ticks/sec. Calling `pgque.ticker()` every 100 ms is cheap when it returns `NULL`; the WAL cost comes from materialized ticks.

## Practical recommendations

- Do not worry that inactive queues will consume ~240 MiB/day. That estimate is for a continuously materializing active queue at 10 ticks/sec.
- Start with the default 100 ms / 10 checks/sec if you care about low end-to-end latency and have normal WAL headroom.
- Use 1000 ms / 1 check/sec for small projects, low-throughput queues, or environments where WAL volume and logical replication lag matter more than sub-100 ms delivery.
- For many queues in one database, estimate active queues separately from idle queues; idle queues back off.
- If you raise the rate below 50 ms, monitor WAL generation, `pg_stat_user_tables` dead tuples for PgQue metadata tables, NOTIFY queue pressure, and replica/apply lag.
- Purge PgQue's `cron.job_run_details` rows if pg_cron logging noise matters, without disabling history for unrelated jobs:

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

- If you do not need successful-run history for any pg_cron job, `alter system set cron.log_run = off;` disables it globally after a restart.

## What still needs better benchmarks

The 280 B/tick number is intentionally conservative documentation from a simple measurement. We still need a dedicated benchmark matrix for:

- cold vs warm pages / full-page-image impact after checkpoints with `full_page_writes = on`;
- `wal_compression` on/off;
- 1, 10, 100, and 1000 materialized ticks/sec;
- one queue vs many queues;
- idle queues vs active queues;
- pg_cron logging on/off;
- logical replication subscriber lag under sustained ticking;
- interaction with rotation/vacuum cadence.
