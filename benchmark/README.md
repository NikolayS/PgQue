# pgque benchmark harness

The whole bench rig: how we compared pgque against pgq, pgmq, pgmq-partitioned, river, que, pg-boss, and **awa** (R10) under held-xmin pathology conditions on AWS i4i.2xlarge.

Backs [NikolayS/pgque#61](https://github.com/NikolayS/pgque/issues/61) (the held-xmin bloat issue) and [PR #62](https://github.com/NikolayS/pgque/pull/62) (the subscription/tick rotation fix). Round-by-round results are tracked at GitLab issue [postgres-ai/postgresql-consulting/tests-and-benchmarks#77](https://gitlab.com/postgres-ai/postgresql-consulting/tests-and-benchmarks/-/issues/77).

## Start here

- **[Throughput harness](./)** — cross-system benchmark harness and results.
- **[xmin-horizon](xmin-horizon/)** — blocked-xmin/dead-tuple repro comparing `SKIP LOCKED` queues with PgQue rotation.
- **[METHODOLOGY.md](METHODOLOGY.md)** — full bench methodology, workload shape, observability stack, phase scheduler.
- **[OPS_GOTCHAS.md](OPS_GOTCHAS.md)** — every operational lesson from running this on AWS (NVMe mount, pg_partman stale rows, que function leftovers, pgboss index, etc.).
- **[HARDWARE.md](HARDWARE.md)** — i4i.2xlarge specs, PG tuning, expected microbench baselines.

## Credit

The pathology this bench exercises is the one Brandur Leach documented in [Postgres Queues (2015)](https://brandur.org/postgres-queues) — DELETE-based queues become unvacuumably bloated under held xmin, and the DB-level death spiral that follows. The bench is a controlled reproduction of that scenario across seven queue systems.

## Quick-start

```bash
# on the VM, after bootstrap:
bash install/install_<sys>.sh            # system-specific install (incl. install_awa.sh for R10)
bash runners/run_r10.sh <sys>            # R10 orchestrator: 50m per system; dispatches Python workers for awa, pgbench for the others
# (R7 orchestrator runners/run_r7.sh kept for the 90m bloat-saw-tooth schedule)

# outputs (all under /tmp/bench/):
#   producer_agg.*                       pgbench producer aggregate log
#   consumer_agg.*                       pgbench consumer aggregate log
#   consumer.log                         NOTICE-instrumented per-call event counts
#   bloat.csv                            pg_stat_user_tables every 30 s
#   sys_metrics.csv                      CPU/mem/disk every 10 s
#   pgss_timeseries.csv                  pg_stat_statements every 10 s
#   ash.csv                              pg_ash 1 Hz wait-event samples (harvested at end)
#   pgfr_snapshots.csv                   pg-flight-recorder snapshots (harvested at end)
#   pgfr_table_snapshots.csv             pgfr table-level snapshots
#   pgfr_statement_snapshots.csv         pgfr per-statement snapshots
#   events_consumed_per_sec.csv          parsed from consumer.log
```

Example instrumented consumer: [consumers/consumer_pgque.sql](consumers/consumer_pgque.sql).

ASH and pgfr output (`ash.csv`, `pgfr_snapshots.csv`) land in `/tmp/bench/` post-run when the harvest step of `run_r7.sh` completes.

## Directory layout

```
benchmark/
  README.md                          # this file
  METHODOLOGY.md                     # full methodology
  OPS_GOTCHAS.md                     # operational lessons
  HARDWARE.md                        # VM + PG tuning
  tooling/
    bloat_sampler.py                 # pg_stat_user_tables sampler
    sys_metrics_sampler.py           # /proc/{stat,meminfo,diskstats} sampler (v2)
    pg_stat_statements_snapshot.py   # pgss time-series
    parse_events_consumed.py         # NOTICE log → events_consumed_per_sec.csv
    idle_in_tx.py                    # REPEATABLE READ xmin holder (psycopg2-based, R7-R8)
    idle_in_tx.sh                    # psql-based xmin holder (no Python deps, R9-R10 default)
    pgq_ticker_daemon.py             # tight ticker loop for pgq (no built-in daemon)
    microbench.sh                    # sysbench + fio baseline
  runners/
    run_r7.sh                        # 90m phase-scheduled orchestrator (clean+TX+recovery, sized for bloat-saw-tooth)
    run_r10.sh                       # 50m phase-scheduled orchestrator (R9/R10), dispatches Python (awa) or pgbench
    clean_reinstall.sh               # reset between runs
    fix_nvme_mount.sh                # recover from NVMe-not-mounted boot
  consumers/
    consumer_pgque.sql               # instrumented (DO + NOTICE)
    consumer_pgq.sql
    consumer_pgmq.sql
    consumer_pgmq-partitioned.sql
    consumer_river.sql
    consumer_que.sql
    consumer_pgboss.sql
    consumer_awa.py                  # native awa worker (Python, R10)
  producers/
    producer_pgque.sql               # pgque.send()
    producer_pgq.sql                 # pgq.insert_event
    producer_pgmq.sql                # pgmq.send
    producer_river.sql               # INSERT INTO river_job
    producer_que.sql                 # INSERT INTO que_jobs
    producer_pgboss.sql              # INSERT INTO pgboss.job
    producer_pgmq-partitioned.sql    # same as producer_pgmq
    producer_awa.py                  # native awa async insert (Python, R10)
  install/
    README.md                        # which system uses which installer
    bootstrap.sh                     # shared PG18 + pg_cron + ash + pgfr base
    install_pgq.sh
    install_pgmq.sh                  # also used by pgmq-partitioned
    pgmq-partitioned_setup_5min.sql  # partman cron schedule
    install_river.sh
    install_pgboss.sh
    install_awa.sh                   # pip install awa-pg awa-cli + awa migrate (R10)
                                     # note: pgque + que driven from AMI userdata
  charts/
    r5_analyze.py                    # 2-panel chart (dead tuples + consumer latency)
    r6_smoke_chart.py                # smoke Solarized-Dark chart (events/s + dead tuples)
    r8_analyze.py                    # R8 main chart: throughput, lag, backlog, CPU per system
    r8_ash_analyze.py                # R8 ASH chart: wait-event breakdown across systems
    r8_pgfr_analyze.py               # R8 pgfr chart: pg-flight-recorder I/O and buffer metrics
    r10_throughput_chart.py          # R10: 8-panel events/sec timeline
    r10_sysmetrics_chart.py          # R10: CPU + NVMe write MiB/s + IOPS, all 8 systems
    r10_ash_chart.py                 # R10: 8-panel ASH (incl. awa); pg_ash color convention
    r10_summary_table.py             # R10: producer/consumer totals, true backlog, peaks
    r10_ash_table.py                 # R10: per-phase wait-event mix table
  gifs/
    r4_gif_v17_solarized.py          # dead-tuples animated GIF
    r4_gif_tps_solarized.py          # TPS/latency animated GIF
```

## Scope

Only a harness. Produces no test coverage for pgque itself — that lives under `tests/` in the main repo. This directory adds nothing to the pgque production SQL; it is pure ops tooling + docs.
