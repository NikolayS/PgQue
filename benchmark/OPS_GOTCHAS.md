# Operational gotchas

Every operational lesson accumulated while running the pgque-vs-pgq-vs-pgmq-vs-river-vs-que-vs-pgboss-vs-pgmq-partitioned bench on AWS i4i.2xlarge. Read this before reproducing.

---

## 1. AWS i4i.2xlarge: NVMe instance store is NOT auto-mounted

The Ubuntu 24.04 AMI boots with `/dev/nvme1n1` **present but not formatted, not mounted**. You MUST format and mount it BEFORE installing Postgres data dir, or the data goes to the 8 GiB root EBS volume and the bench dies the moment you fill the page cache.

```bash
sudo mkfs.xfs /dev/nvme1n1
sudo mkdir -p /mnt/pgdata
sudo mount -o noatime,nodiratime /dev/nvme1n1 /mnt/pgdata
sudo chown postgres:postgres /mnt/pgdata
```

If PG is installed first (default path `/var/lib/postgresql/18/main`), data goes to the 8 GiB root EBS → disaster under bench load.

**Symlink pattern** after moving data:

```bash
sudo systemctl stop postgresql@18-main
sudo mv /var/lib/postgresql/18/main /mnt/pgdata/postgresql/18/main
sudo ln -s /mnt/pgdata/postgresql/18/main /var/lib/postgresql/18/main
sudo systemctl start postgresql@18-main
```

See [runners/fix_nvme_mount.sh](runners/fix_nvme_mount.sh) for the recovery procedure when this happens post-hoc.

---

## 2. `/tmp/bench/` observability dir — must be on NVMe

Default `/tmp` is on the root disk. pgbench `--log-prefix=/tmp/bench/producer_agg` + `consumer.log` with 17 k NOTICE/s + `bloat.csv` + `sys_metrics.csv` all write there.

At bench rates this is 1–5 MiB/s of root-disk I/O. Not catastrophic but adds noise you can see in the sys_metrics disk panel of the non-subject VM.

**Fix:**

```bash
sudo mkdir -p /mnt/pgdata/bench && sudo chown postgres:postgres /mnt/pgdata/bench
ln -s /mnt/pgdata/bench /tmp/bench
```

Now observability output lands on NVMe alongside PG data.

---

## 3. `logging_collector=off` means server logs go to systemd journal

We run with `logging_collector=off`, so server logs go to systemd-journald (and from there to disk) rather than a PG-managed log file. Log I/O is small relative to WAL + queue table writes at the bench's TPS, but it is not zero — quantifying its share is a follow-up (#123).

If investigating slow queries later, enable `log_statement='all'` + `logging_collector=on` with `log_directory` inside the NVMe data dir (relative path `'log'` resolves correctly since `data_dir` is symlinked to NVMe).

---

## 4. Clean-slate reset without losing adjacent schemas

`DROP SCHEMA pgque CASCADE` can cascade into objects it doesn't own if a function somewhere references pgque. This happened during testing — the `ash` schema was silently dropped on the pgque VM.

**Safer pattern for pgque:** `TRUNCATE` all `pgque.*` tables instead of drop-and-recreate. If a reinstall is actually needed:

```bash
sudo -u postgres psql -d bench -c "DROP SCHEMA pgque CASCADE"
sudo -u postgres psql -d bench -f /tmp/pgque.sql
sudo -u postgres psql -d bench -f /tmp/pg_ash/sql/ash-install.sql
sudo -u postgres psql -d bench -f /tmp/pgfr/_record/sql/install.sql
sudo -u postgres psql -d bench -f /tmp/pgfr/_analyze/sql/install.sql
sudo -u postgres psql -d bench -f /tmp/pgfr/_control/sql/install.sql
```

Re-run the ash/pgfr installers even if you think they weren't touched.

**Verify after reset:**

```sql
SELECT nspname FROM pg_namespace
 WHERE nspname IN ('ash','pgfr_record','pgfr_analyze','pgfr_control','pgque');
```

All five must appear.

---

## 5. pg_partman + pgmq-partitioned: stale `part_config` rows

After `DROP SCHEMA pgmq CASCADE`, `public.part_config` retains rows pointing to non-existent tables. `run_maintenance_proc()` then throws:

```
ERROR: Given parent table not found in system catalogs: pgmq.q_bench_queue
```

**Fix** before reinstalling pgmq:

```sql
DELETE FROM public.part_config WHERE parent_table LIKE 'pgmq.%';
```

pg_partman is installed in schema `public` (not `partman` as the docs sometimes suggest); `part_config` is `public.part_config`.

---

## 6. pg_partman premake tuning

Default `premake=4` works for bench (4 future 5-min partitions = 20 min buffer, enough for 1.5 h bench given 1-min maintenance cron cadence).

`premake=20` (24 partitions steady-state) collapsed pgmq-partitioned consumer perf to 525 TPS vs 6621 with premake=4. The dominant cost in PG's per-partition planning is the first-query-in-session penalty (Postgres caches the plan for subsequent queries in the same connection). Why this still hurt the consumer at steady state is a follow-up (#124) — likely short-lived connections (or PgBouncer transaction-mode recycling) paying the penalty repeatedly.

`infinite_time_partitions=true` is needed for the maintenance job to keep creating future partitions indefinitely:

```sql
UPDATE public.part_config
   SET premake=4, infinite_time_partitions=true
 WHERE parent_table='pgmq.q_bench_queue';
```

---

## 7. que (Ruby) reinstall: residual `que_validate_tags` function

`DROP TABLE que_jobs, que_lockers, que_values CASCADE` doesn't drop associated functions (they're in `public` schema).

Re-running `que.migrate!` fails:

```
PG::DuplicateFunction: function que_validate_tags already exists with same argument types
```

**Fix** — explicitly drop all que_* functions before migrate:

```sql
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT proname FROM pg_proc WHERE proname LIKE 'que\_%' OR proname='que_validate_tags'
  LOOP EXECUTE format('DROP FUNCTION IF EXISTS public.%I CASCADE', r.proname); END LOOP;
END $$;
```

---

## 8. river: no clean_reinstall.sh on the VM

river's `install.sh` was simpler than the others — it installs the Go binary + creates `river_job` via `river migrate up`.

Reset = `TRUNCATE river_job` + stats reset. No schema-drop needed. If you really want a nuke-and-pave, `DROP TABLE river_job, river_leader, river_migration, river_client, river_client_queue, river_queue CASCADE` then re-run migrate.

---

## 9. pgboss: covering index for bench workload

pg-boss v12.15 DELETE path scans `pgboss.job_common` which partitions by name; the `(name, state, start_after)` index isn't covering under bench-heavy DELETE.

**Add a covering index:**

```sql
CREATE INDEX bench_covering
ON pgboss.job_common (name, state, start_after, created_on, id)
WHERE state < 'active';
```

This lines up the consumer's claim path and turns the DELETE into an index-only prefilter.

---

## 10. pgq: no built-in ticker in the v3.5.1 PL-only fork

Upstream pgq relies on the C ticker binary. The PL-only fork we use for fair comparison doesn't ship one.

**Workaround:** tight Python loop calling `pgq.ticker('queue_name')` every 1 s and `pgq.maint_operations()` every 5 s — see [tooling/pgq_ticker_daemon.py](tooling/pgq_ticker_daemon.py).

Runs on the pgq VM only. Killed at bench end via `pkill -f pgq_ticker_daemon`.

---

## 11. pgque PG18 xid8 cast bug (pre-PR #62)

`batch_event_sql` function casts `xid8` to `bigint`, which fails on PG18's stricter type rules:

```
ERROR: cannot cast type xid8 to bigint
```

**Fix:** `::text::bigint` as intermediate cast. Landed in [PR #62](https://github.com/NikolayS/pgque/pull/62).

If you see this on a VM that wasn't freshly installed from the PR #62 branch, re-clone the branch and run `make USE_PGXS=1 install` followed by `psql -f build/pgque.sql` per [install/README.md](install/README.md).

---

## 12. Spot-instance termination risk

i4i.2xlarge spot price is ~$0.20–0.30/h vs on-demand $0.686/h. The us-east-2 spot market is reliable for short bench runs but DOES reclaim.

- `pgmq-partitioned` spot was reclaimed mid-run during one bench session.
- `pgque` AND `que` spots were reclaimed between runs (hours-long windows) in another.

**Mitigation:** on-demand for the primary subject under test (pgque for PR #62 work). Cost delta: ~$3–4 per 7-VM 1.5 h run. Worth it for the subject you're landing a PR on; leave the comparison set on spot.

Rsync every 30 min (see Section 10 of [METHODOLOGY.md](METHODOLOGY.md)) so a reclaim loses at most a partial phase, not a whole run.

---

## 13. ASH + pgfr install prerequisites

Install order: `pg_cron` → `pg_stat_statements` → pg_ash → pgfr (`_record` / `_analyze` / `_control`).

```
ALTER SYSTEM SET shared_preload_libraries='pg_cron,pg_stat_statements';
```

…then **restart** BEFORE installing ash.

ASH uses `pg_cron.schedule('ash_sampler','1 seconds', ...)` — requires `pg_cron.database_name` to match the bench DB:

```
cron.database_name = 'bench'
```

If you see `ash.samples()` returning zero rows with no obvious error, the cron job is almost certainly scheduled against the wrong database.

---

## 14. pgbench aggregate-interval logging

`--aggregate-interval=10 --log --log-prefix=/tmp/bench/producer_agg` writes per-10 s buckets, one line per bucket per client.

Low observer effect: one `fwrite` per 10 s per client, not per transaction.

Captures `latency min/max/sum/sumsq` → enough to reconstruct mean + stddev + approximate min/max band per window. This is how we plot "consumer latency over time" without running a separate sampler.

Log file naming: `producer_agg.<pid>` for `-c 1 -j 1`; `consumer_agg.<pid>.<worker>` for `-c N -j N` with N>1.

---

## 15. NOTICE-based event-count instrumentation

We use `RAISE NOTICE 'ev ts=% n=%'` inside DO blocks inside the consumer SQL.

At 17 k calls/sec: ~40 % CPU overhead on the **consumer client** (the pgbench process parsing NOTICE lines into stdout/log), but no observed server-side cost. Safe for bench purposes, NOT for production.

pgbench faithfully prints NOTICE lines to stdout (captured as `consumer.log`). Parser: [tooling/parse_events_consumed.py](tooling/parse_events_consumed.py). Output: `events_consumed_per_sec.csv` + `events_consumed_summary.txt`.

Why NOTICE rather than pgss: DO-wrappers hide per-statement rows from `pg_stat_statements`, so the pgss queryid reported for a consumer is the DO wrapper as a whole — not the SELECT/DELETE inside it — which makes it useless for counting consumed events when the internal shape differs across systems. NOTICE is schema-neutral and authoritative.

Caveat: `RAISE NOTICE` itself has observer effect (server→client protocol message + log subsystem write per fire). The choice was pragmatic for boundary events; high-frequency measurements would benefit from named functions + `pg_stat_statements` instead of DO-wrappers (#127).

---

## Summary checklist

Before a run:

- [ ] `/dev/nvme1n1` formatted + mounted + PG data dir symlinked
- [ ] `/tmp/bench` symlinked to `/mnt/pgdata/bench`
- [ ] `shared_preload_libraries='pg_cron,pg_stat_statements'` + restart
- [ ] `cron.database_name='bench'`
- [ ] pg_ash, pgfr (`_record`, `_analyze`, `_control`) installed
- [ ] per-system install script re-run after any schema drop
- [ ] for pgmq-partitioned: stale `public.part_config` rows deleted, `premake=4`
- [ ] for que: que_* functions dropped before migrate
- [ ] for pgq: ticker daemon started (and remembered to be killed at end)
- [ ] rsync cron scheduled on the local workstation (not the VM)
