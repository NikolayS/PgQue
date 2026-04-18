# Bench methodology — the definitive reference

Adapted from GitLab issue [postgres-ai/postgresql-consulting/tests-and-benchmarks#77, note 3263767264](https://gitlab.com/postgres-ai/postgresql-consulting/tests-and-benchmarks/-/issues/77#note_3263767264).

This document is the single source of truth for how the PostgreSQL-queue bench is structured, what it measures, and every script/config that makes it work. It is written so a reviewer can reproduce the whole thing end-to-end.

Cross-links:

- Upstream fix: [pgque PR #62](https://github.com/NikolayS/pgque/pull/62) · issue [NikolayS/pgque#61](https://github.com/NikolayS/pgque/issues/61)
- GitLab rounds: R4 final narrative — note 3262968456; R5 root — note 3263000815; R5 full writeup — note 3263438287; pgque rotation PoC under held xmin — note 3263417609.

---

## 1. Goals and pathology

We reproduce Brandur Leach's "Postgres queues" death spiral (see [Brandur's 2015 post](https://brandur.org/postgres-queues)): a DELETE-based work-queue becomes unvacuumably bloated the moment an unrelated backend holds `xmin` in the past (long transactions, logical-replication slots, stuck standby feedback). Dead tuples accumulate; bitmap / index scans must traverse them; latency explodes; throughput collapses even after the holder releases.

Seven systems, each on its own VM:

| System | Version | Pattern |
|---|---|---|
| **pgque** | patched via PR #62 | batch ticker + rotating event tables (TRUNCATE) |
| pgq | v3.5.1 PL-only | same model as pgque; upstream baseline |
| pgmq | v1.11.0 | single queue table, VISIBILITY + DELETE |
| pgmq-partitioned | v1.11.0 + pg_partman | range-partitioned queue table |
| river | v0.34 | SQL-level SKIP LOCKED + DELETE (consumer emulated) |
| que | v2.4 | SKIP LOCKED + DELETE (consumer emulated) |
| pg-boss | v12.15 | SKIP LOCKED + DELETE on partitioned `pgboss.job` |

The Go/Ruby/Node workers are installed end-to-end so the schema is authentic, but the actual consumer load is driven via pgbench running each system's *SQL claim pattern*. This isolates the DB-side behaviour from runtime/GC artefacts.

Workload shape (all runs):

- Producer: 1 client, pgbench `-R 1000` rate-cap (may move to `-R 5000` in R7/R8)
- Consumer: 1 client for pgque/pgq, 4 clients for everything else
- Three 30-minute phases back-to-back = **1.5 h per run**:
  1. **Clean baseline** — no held xmin
  2. **Held xmin** — `idle_in_tx.py` holds `REPEATABLE READ` open
  3. **Clean recovery** — holder killed, observe regrowth / catchup

---

## 2. Infrastructure

- AWS us-east-2, **i4i.2xlarge** (8 vCPU, 64 GB, NVMe instance store)
- Spot where available; on-demand only as last resort (see Section 10)
- Ubuntu 24.04, **PG18 from PGDG**, `pg_cron`, `pg_stat_statements`, pg_ash, pgfr
- Data dir moved to NVMe: `/mnt/pgdata/postgresql/18/main` symlink (see [runners/fix_nvme_mount.sh](runners/fix_nvme_mount.sh) and [OPS_GOTCHAS.md §1](OPS_GOTCHAS.md))
- One VM per system so any tuning / runtime / GC behaviour is contained
- SSH key: `<your-ssh-key>` (us-east-2)

Live R7 VMs (redacted): `<pgque-ip>`, `<pgq-ip>`, `<pgmq-ip>`, `<river-ip>`, `<pgboss-ip>`, `<que-ip>`, `<pgmq-partitioned-ip>`.

---

## 3. Observability stack

Seven parallel streams run during every bench. All CSVs land in `/tmp/bench/` and are rsynced to the local `/tmp/bench_r<N>/<system>/` tree every 30 min.

### (a) bloat_sampler.py — pg_stat_user_tables every 30 s

Per-system filter covers each queue's event/metadata tables (including pg-boss partitions and pgmq-partitioned's partman partitions). Writes `bloat.csv`.

Source: [tooling/bloat_sampler.py](tooling/bloat_sampler.py).

### (b) pg_ash — 1 Hz wait-event sampling

pg_cron runs ash sampling at 1 Hz throughout; at end of bench we export:

```sql
COPY (SELECT sample_time, database_name, active_backends, wait_event, query_id
      FROM ash.samples(p_interval => '2 hour'::interval, p_limit => 2000000))
TO '/tmp/bench/ash.csv' CSV HEADER;
```

### (c) pgfr (pg-flight-recorder) — snapshot observability

pgfr writes snapshots on a pg_cron schedule into `pgfr_record.*`. At end of run we export:

```sql
COPY (SELECT * FROM pgfr_record.snapshots)           TO '/tmp/bench/pgfr_snapshots.csv'           CSV HEADER;
COPY (SELECT * FROM pgfr_record.table_snapshots)     TO '/tmp/bench/pgfr_table_snapshots.csv'     CSV HEADER;
COPY (SELECT * FROM pgfr_record.statement_snapshots) TO '/tmp/bench/pgfr_statement_snapshots.csv' CSV HEADER;
```

Full pgfr_record schema on every VM includes: `snapshots`/`snapshots_v2` (partitioned), `table_snapshots(_v2)`, `statement_snapshots(_v2)`, `index_snapshots(_v2)`, `replication_snapshots(_v2)`, `vacuum_progress_snapshots(_v2)`, `activity_samples(_archive_v2)`, `lock_samples(_archive_v2)`, `config_snapshots`, `db_role_config_snapshots`.

### (d) sys_metrics_sampler.py — CPU / mem / disk every 10 s (R7+)

Reads `/proc/stat`, `/proc/meminfo`, `/proc/diskstats` directly (psutil-optional). NVMe device is `nvme1n1` (the instance store, which is what `/mnt/pgdata` sits on). v2 adds per-device IOPS and latency columns.

Source: [tooling/sys_metrics_sampler.py](tooling/sys_metrics_sampler.py).

### (e) pg_stat_statements_snapshot.py — pgss time-series every 10 s

Polls pgss with a regex filter over our queue-related query shapes, diffs consecutive snapshots downstream. Used as a cross-check for NOTICE-based ev/s (important for pgque/pgq/pgmq where DO-block wrappers hide per-statement rows).

Source: [tooling/pg_stat_statements_snapshot.py](tooling/pg_stat_statements_snapshot.py).

### (f) pgbench `--aggregate-interval=10 --log`

Both producer and consumer run with per-10 s aggregate logs (min/max/sum/sumsq latency). Files: `producer_agg.<pid>` and `consumer_agg.<pid>.<worker>` under `/tmp/bench/`.

### (g) Consumer NOTICE instrumentation (R6+)

Each instrumented `consumer.sql` is wrapped in a DO block that emits exactly one `RAISE NOTICE 'ev ts=<epoch_s> n=<events>'` per call. This gives us an authoritative per-call consumed-events stream that is immune to the pgss DO-wrapper opacity problem.

The seven instrumented consumers are in [consumers/](consumers/). The parser for `consumer.log` NOTICE lines is [tooling/parse_events_consumed.py](tooling/parse_events_consumed.py).

### (h) idle_in_tx.py — the death-spiral inducer

Opens a `REPEATABLE READ` transaction, pins xmin, sleeps forever. Kill with SIGTERM to release the horizon. Source: [tooling/idle_in_tx.py](tooling/idle_in_tx.py).

### (i) pgq_ticker_daemon.py — tight ticker loop for pgq

pgq upstream has no built-in ticker daemon (the C one was always external). pgque runs inline; for a fair comparison we run a tight Python loop on the pgq VM that calls `pgq.ticker()` at 1 Hz and `pgq.maint_operations()` every 5 s. Source: [tooling/pgq_ticker_daemon.py](tooling/pgq_ticker_daemon.py).

---

## 4. Bench runner

Orchestrates the phase scheduler. Forks producer, consumer (both pgbench), bloat_sampler, sys_metrics_sampler, pgss snapshotter; at t=1800 s opens `idle_in_tx`; runs `VACUUM VERBOSE` at phase boundaries; at t=3600 s kills the holder; another `VACUUM VERBOSE`; at t=5400 s harvests ash/pgfr/pgss.

Source: [runners/run_r7.sh](runners/run_r7.sh).

---

## 5. Clean-slate reset

Before every run we kill stragglers, DROP the system's schema/extensions, unschedule lingering `pg_cron` jobs, re-run `install.sh`, and reset all stats. The companion `full_reset.sql` calls `pg_stat_statements_reset()`, `pg_stat_reset()`, `pg_stat_reset_shared()` for bgwriter/checkpointer/wal/io, `TRUNCATE ash.sample`, and `DELETE FROM cron.job_run_details`.

Source: [runners/clean_reinstall.sh](runners/clean_reinstall.sh). See also [OPS_GOTCHAS.md §4, §5, §7](OPS_GOTCHAS.md) for adjacent-schema pitfalls.

---

## 6. Per-system install scripts

Each VM has its own `/tmp/install.sh` (mirrored into [install/](install/)). Pattern differs by system:

- **pgque** — `git clone` the PR #62 branch, `make USE_PGXS=1 install`, run the SQL installer, `SELECT pgque.create_queue('bench_queue')`, schedule `pgque.ticker()` via `pg_cron` (also called inline in `consumer.sql`). Driven from AMI user-data; see [install/README.md](install/README.md) and [install/bootstrap.sh](install/bootstrap.sh).
- **pgq** — PGDG `postgresql-18-pgq3` or `git clone --branch v3.5.1`, then `CREATE EXTENSION pgq` and immediately apply `switch_plonly.sql` to replace the C `insert_event_raw` with PL/pgSQL (pg_proc lang check `= 'plpgsql'` as a gate). See [install/install_pgq.sh](install/install_pgq.sh).
- **pgmq** — PGDG `postgresql-18-pgmq` + `CREATE EXTENSION pgmq` + `pgmq.create('bench_queue')`. See [install/install_pgmq.sh](install/install_pgmq.sh).
- **pgmq-partitioned** — pgmq + `pg_partman` + `pgmq.create_partitioned('bench_queue', 'id', '10000')`. Reuses [install/install_pgmq.sh](install/install_pgmq.sh) plus [install/pgmq-partitioned_setup_5min.sql](install/pgmq-partitioned_setup_5min.sql) for the partman schedule.
- **river** — `go install github.com/riverqueue/river/cmd/river@v0.34.x` and `river migrate-up` (sets schema), then the consumer SQL emulates the SKIP-LOCKED claim pattern. See [install/install_river.sh](install/install_river.sh).
- **que** — Ruby + `gem install que -v 2.4.x`, `bundle exec que:install` creates `que_jobs`; consumer SQL emulates the Ruby Que SELECT/DELETE. Driven from AMI user-data — see [install/README.md](install/README.md) and [OPS_GOTCHAS.md §7](OPS_GOTCHAS.md).
- **pg-boss** — local `npm install pg-boss@12.15`, run `new PgBoss(DSN).start()` once to migrate schema; consumer SQL emulates the `SKIP LOCKED` claim on `pgboss.job`. See [install/install_pgboss.sh](install/install_pgboss.sh).

---

## 7. Per-system consumer.sql patterns

All seven instrumented consumers are in [consumers/](consumers/):

- [consumer_pgque.sql](consumers/consumer_pgque.sql) — ticker + `next_batch` + `get_batch_events` + `finish_batch`
- [consumer_pgq.sql](consumers/consumer_pgq.sql) — identical, schema swap pgque → pgq
- [consumer_pgmq.sql](consumers/consumer_pgmq.sql) — `pgmq.read(50)` + `pgmq.delete`
- [consumer_pgmq-partitioned.sql](consumers/consumer_pgmq-partitioned.sql) — identical SQL to pgmq (schema hides partitioning)
- [consumer_river.sql](consumers/consumer_river.sql) — SKIP LOCKED + DELETE on `river_job`
- [consumer_que.sql](consumers/consumer_que.sql) — SKIP LOCKED + DELETE on `que_jobs`
- [consumer_pgboss.sql](consumers/consumer_pgboss.sql) — SKIP LOCKED + DELETE on `pgboss.job`

All producers are in [producers/](producers/).

---

## 8. Analysis and chart generation

- [charts/r5_analyze.py](charts/r5_analyze.py) — R5 full 2-panel chart (dead tuples + consumer latency, linear scale, no symlog — per [MEMORY rule](../CLAUDE.md) "Never use log/symlog on charts").
- [charts/r6_smoke_chart.py](charts/r6_smoke_chart.py) — R6 smoke Solarized-Dark 2-panel chart (events/s + pgque per-table dead tuples).
- [gifs/r4_gif_v17_solarized.py](gifs/r4_gif_v17_solarized.py) — R4 dead-tuples animated GIF (7 systems, Solarized-Dark).
- [gifs/r4_gif_tps_solarized.py](gifs/r4_gif_tps_solarized.py) — R4 TPS/latency animated GIF.

---

## 9. GitLab posting style

- Threads open under `### header`; replies stay inside the same discussion.
- Every `<details>` block has **one blank line** between the opening tag and the triple-backtick fence, and **one blank line** between the closing fence and `</details>` — and another blank line after `</details>`. This is what makes them render correctly on GitLab.
- Every significant result embeds `![alt](/uploads/…)` — charts are first-class citizens. GIFs too.
- Post bodies via `curl --data-urlencode "body@/tmp/path"` to the discussions endpoint — new thread, not a reply under an existing one.

---

## 10. Cost discipline

- us-east-2 spot preferred (~$0.22/h); when spot is exhausted we hop to another region before going on-demand.
- At R7 time 2 VMs are on-demand (pgmq-partitioned and pgboss; pgque+que moved on-demand after R6 spot reclaim).
- **~$15 per 1.5 h bench round** (7 VMs).
- Total spend through R7 is ~$100.
- Rsync every 30 min pulls `/tmp/bench/` to local, so a spot reclaim loses at most a partial phase.

See [OPS_GOTCHAS.md §12](OPS_GOTCHAS.md) for spot-reclaim mitigations.

---

## Reproducing from scratch

1. Launch 7 × i4i.2xlarge in us-east-2 (spot first) with `bench_userdata.sh` (symlinks `/mnt/pgdata`, installs PGDG PG18, pg_cron, pg_ash, pgfr, pg_stat_statements). See [runners/fix_nvme_mount.sh](runners/fix_nvme_mount.sh) for the NVMe side if the userdata path breaks.
2. `scp` per-system `install.sh`, `producer.sql`, instrumented `consumer.sql`, plus the samplers in [tooling/](tooling/).
3. `bash clean_reinstall.sh <sys>` on each VM.
4. `bash run_r7.sh <sys>` — 1.5 h, writes `/tmp/bench/*.csv` + `*.log`.
5. Rsync everything back to `/tmp/bench_r7/<sys>/`.
6. Run [charts/r5_analyze.py](charts/r5_analyze.py) (or R7 successor) for the verdict table + 2-panel PNG.
7. Post results as a threaded reply with the `<details>`/blank-line style above.

That is the whole rig.
