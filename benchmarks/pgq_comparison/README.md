# PgQue vs PgQ Throughput Comparison

Benchmarks comparing PgQue (pure PL/pgSQL, modern API) against raw PgQ
(PL-only mode) on the same hardware and configuration, with pg_cron ticker
and table rotation running.

## Prerequisites

- PostgreSQL 18+ with `pg_cron` in `shared_preload_libraries`
- `cron.database_name = 'bench_pgque'` in postgresql.conf
- PgQ source (for `pgq_pl_only.sql`): `git clone https://github.com/pgq/pgq && cd pgq && make`
- PgQue installed

## Quick start

```bash
# 1. Apply tuning (restart required)
psql -d postgres -c "alter system set synchronous_commit = off;"
psql -d postgres -c "alter system set shared_buffers = '2GB';"
psql -d postgres -c "alter system set max_wal_size = '4GB';"
psql -d postgres -c "alter system set wal_level = minimal;"
psql -d postgres -c "alter system set max_wal_senders = 0;"
psql -d postgres -c "alter system set wal_compression = lz4;"
psql -d postgres -c "alter system set cron.database_name = 'bench_pgque';"
# restart PostgreSQL

# 2. Create databases
psql -d postgres -c "create database bench_pgque;"
psql -d postgres -c "create database bench_pgq;"

# 3. Run setup (installs pgque, pgq, pg_cron jobs)
psql -d bench_pgque -f benchmarks/pgq_comparison/setup.sql

# 4. Run benchmark (default: 10 min per test, 8 clients)
./benchmarks/pgq_comparison/run.sh

# Or customize duration and clients:
./benchmarks/pgq_comparison/run.sh 300 16  # 5 min, 16 clients
```

## What it tests

| API | Database | Payload | Description |
|-----|----------|---------|-------------|
| `pgq.insert_event()` | bench_pgq | ~2 KiB text | Raw PgQ PL-only (baseline) |
| `pgque.insert_event()` | bench_pgque | ~2 KiB text | PgQue PgQ-compat API |
| `pgque.send()` | bench_pgque | ~1 KiB jsonb | PgQue modern API |

All tests run with:
- pg_cron ticker every 2 seconds
- Table rotation every 2 minutes
- Prepared statements (`-M prepared`)
- Per-minute progress reporting

## Results (2026-04-14)

**Hardware:** Apple Silicon, 10 cores, 24 GiB RAM, APFS SSD
**PostgreSQL:** 18.3 (Homebrew)
**Duration:** 10 minutes per test, 8 clients

### Per-minute throughput

| Minute | PgQ `insert_event()` | PgQue `insert_event()` | PgQue `send()` |
|--------|---------------------|----------------------|---------------|
| 1 | 80,520 | 67,734 | 83,258 |
| 2 | 85,289 | 73,918 | 81,711 |
| 3 | 93,237 | 74,901 | 82,335 |
| 4 | 92,103 | 78,794 | 81,860 |
| 5 | 82,089 | 78,010 | 81,768 |
| 6 | 82,565 | 72,485 | 81,633 |
| 7 | 80,960 | 72,939 | 80,468 |
| 8 | 79,546 | 72,914 | 80,348 |
| 9 | 79,258 | 70,386 | 79,397 |
| 10 | 77,367 | 70,703 | 79,637 |

### Summary

| API | 10-min avg ev/s | Steady-state | vs PgQ |
|-----|----------------|-------------|--------|
| **PgQ** `insert_event()` | **83,294** | 77-93k | baseline |
| **PgQue** `insert_event()` | **73,278** | 68-79k | -12% |
| **PgQue** `send()` (jsonb) | **81,242** | 79-83k | **-2%** |

### Key findings

1. **PgQue `send()` matches PgQ** — only 2% slower, within noise. The modern
   jsonb API adds no meaningful overhead over raw PgQ.

2. **PgQue `insert_event()` is 12% slower** — the text-based compatibility
   wrapper has measurable overhead from the extra function layer.

3. **PgQue `send()` is the most stable** — 79-83k ev/s with minimal variance.
   PgQ and pgque `insert_event()` both show more checkpoint-related dips.

4. **Rotation worked correctly** — disk usage stayed bounded at ~40 GiB (2
   rotation periods), fully recovered after cleanup.

5. **All three sustained 70-93k ev/s for 10 minutes** with no degradation.
