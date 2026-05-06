# Benchmark: PG-Backed Queues Under Burst Fan-Out Churn

## Motivation

`BENCH_XMIN_HORIZON.md` covers the case where VACUUM is *blocked* — a long
REPEATABLE READ transaction or an idle logical slot holds the xmin horizon and
dead tuples cannot be reclaimed at all. That is one of two main failure modes
for `SELECT ... FOR UPDATE SKIP LOCKED` queues. This spec covers the other:
**steady-state churn under burst fan-out**, where autovacuum is not blocked but
still loses ground during bursts. Every enqueued job eventually becomes a dead
tuple when it is dequeued; every burst adds a spike of dead tuples that
autovacuum must chase. Over time, the queue table bloats, buffer cache fills
with dead pages, and bystander queries on co-located tables see p99 spikes even
when the queue is otherwise healthy.

Burst fan-out is a standard production pattern: a periodic job scans a source
table for pending work and enqueues a batch of detail jobs in one pass. Common
shapes include image-cleanup sweeps, expired-session reapers, bulk notification
fan-outs, background reconciliation scans, and outbox-pattern flushes. The
common structure is one trigger → 10²–10⁴ jobs added → workers drain →
repeat. Unlike a steady trickle, these bursts hit the queue table as a sudden
wall; autovacuum reacts after the fact and may not finish before the next burst.

pgque's TRUNCATE-rotation model is structurally immune: events live in a
rotating set of tables that are dropped by TRUNCATE rather than reclaimed
row-by-row. There are no dead tuples on the hot path regardless of burst size.
Tuning autovacuum more aggressively (lower `autovacuum_vacuum_scale_factor`,
higher `autovacuum_vacuum_cost_limit`) can reduce how much bloat accumulates,
but it cannot eliminate the cause — vacuum still does work proportional to the
number of dequeued jobs. Rotation eliminates the cause entirely.

This benchmark measures the gap: throughput, bloat growth, bystander latency
impact, and autovacuum activity under controlled bursts, for both workloads.
Together with BENCH_XMIN_HORIZON.md it maps the full landscape of dead-tuple
failure modes for SKIP LOCKED queues.

## Scenarios

| ID | Description | Shape |
|----|-------------|-------|
| **F1** | Baseline — low steady rate, no bursts | 50 jobs/s continuous, 30 min |
| **F2** | Single burst | 1 batch × 10,000 jobs, then drain to empty |
| **F3** | Sustained bursts | 20 bursts × 1,000 jobs every 60 s, 30 min total |
| **F4** | F3 + bystander workload | F3 concurrent with pgbench on an unrelated 1M-row table |

F1 establishes a baseline: with no burst, does autovacuum keep up? F2 isolates
the single-burst spike. F3 tests whether repeated bursts cause cumulative drift
(bloat that does not return to baseline between bursts). F4 reveals whether
bloat translates to measurable bystander latency degradation.

## Workloads

| Workload | Backing model |
|----------|---------------|
| **W-skiplocked** | Single `jobs` table; producer `INSERT ... RETURNING id` (batch via `COPY` or multi-row `VALUES`); consumer `DELETE FROM jobs WHERE id IN (SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 50) RETURNING *`. Generic single-table queue, ~30 LOC SQL. |
| **W-pgque** | Stock `pgque.sql` with `pgque.create_queue('bench')` and `pgque.send_batch('bench', ...)`. |

## Metrics (5s sampling)

1. Throughput: jobs/s (dequeue rate); burst enqueue total latency in ms
2. Bloat: `pg_stat_user_tables.n_live_tup`, `n_dead_tup`, `last_autovacuum`,
   `vacuum_count`; `pg_total_relation_size`
3. Bystander latency (F4 only): pgbench worker on unrelated 1M-row table,
   p50/p95/p99 per 5s window
4. Dequeue latency over time: per-batch p50/p95/p99
5. Autovacuum activity: `pg_stat_progress_vacuum`; time between successive
   autovacuums on the queue table

## Hardware / PG Config

Single-laptop reproducible. Docker Compose. PG17.

Aggressive autovacuum baked in (matches production-grade tuning):

```
autovacuum_naptime = 10s
autovacuum_vacuum_scale_factor = 0.005
autovacuum_vacuum_threshold = 50
autovacuum_analyze_scale_factor = 0.005
autovacuum_vacuum_cost_limit = 10000
autovacuum_vacuum_cost_delay = 2ms
log_autovacuum_min_duration = 0
```

These settings make autovacuum more aggressive than the PostgreSQL default.
The intent is to measure the failure mode even under favorable tuning, not to
exploit default laziness.

## File Layout

Implementation will live under `benchmark/fanout-churn/` in a follow-up PR.
