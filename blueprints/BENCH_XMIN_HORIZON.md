# Benchmark: PG-Backed Queues Under Blocked xmin Horizon

## Motivation

Most published Postgres-queue benchmarks measure throughput in steady state — empty buffer cache, no concurrent application load, no replication lag, autovacuum unimpeded. These conditions rarely hold in production. The most common operational failure mode of `SELECT ... FOR UPDATE SKIP LOCKED` queues is silent degradation when the **xmin horizon is blocked**: VACUUM cannot reclaim dead tuples produced by the queue's `INSERT → UPDATE → DELETE` cycle, the table and its indexes bloat, and read latency on the queue (and on co-located application tables sharing buffer cache) climbs. Causes are routine: long REPEATABLE READ transactions, `pg_dump`, idle logical replication slots, `hot_standby_feedback=on` with a slow replica.

This benchmark quantifies that failure mode and compares it against pgque's TRUNCATE-rotation model, which produces zero dead tuples on the hot path.

## Scenarios

| ID | Description | Mechanism |
|----|-------------|-----------|
| **S1** | Baseline | Queue under sustained load, no xmin holder |
| **S2** | RR transaction holds xmin | `BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1; \watch 1` |
| **S3** | Idle logical slot holds xmin | `pg_create_logical_replication_slot('bench_slot','pgoutput')`; never consume |

Each scenario runs at a fixed enqueue rate.

## Workloads

| Workload | Backing model |
|----------|---------------|
| **W-skiplocked** | Single `jobs` table; producer `INSERT ... RETURNING id`, consumer `DELETE FROM jobs WHERE id = (SELECT id FROM jobs ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *`. Generic single-table queue (~30 lines of SQL). |
| **W-pgque** | Stock `pgque.sql` with `pgque.create_queue('bench')`. |

## Metrics (5s sampling)

1. Throughput (`jobs_processed_per_sec`)
2. Bloat: `pg_stat_user_tables.n_live_tup`, `n_dead_tup`, `last_autovacuum`, `vacuum_count`; `pg_total_relation_size`
3. Application latency: bystander pgbench worker on an unrelated 1M-row table, p50/p95/p99
4. VACUUM activity: `pg_stat_progress_vacuum`, `pg_replication_slots.confirmed_flush_lsn`, oldest backend xmin from `pg_stat_activity`
5. xmin horizon: `age(datfrozenxid)`, slot xmin

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

## File Layout

`benchmark/xmin-horizon/` — see implementation.
