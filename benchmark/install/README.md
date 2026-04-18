# Install scripts

All seven VMs share `bootstrap.sh` as the base — installs PG18 (PGDG) + pg_cron + pg_stat_statements + pg_ash + pgfr, applies postgresql.conf tuning, enables local trust auth, restarts PG.

Then each VM layers a system-specific install on top:

| System | Per-system installer | Notes |
|---|---|---|
| pgque | — (cloned + `make install` in userdata before bootstrap — PR #62 branch) | The pgque binary install happens in AMI user-data; bootstrap only prepares PG. Actual pgque SQL install: `git clone NikolayS/pgque -b feat/rotation`, `make USE_PGXS=1 install`, `psql -f build/pgque.sql`, `SELECT pgque.create_queue('bench_queue')`. |
| pgq | [install_pgq.sh](install_pgq.sh) | PL-only fork via `switch_plonly.sql`. |
| pgmq | [install_pgmq.sh](install_pgmq.sh) | SQL-only install from tembo-io/pgmq v1.11.0 raw URL. |
| pgmq-partitioned | [install_pgmq.sh](install_pgmq.sh) + `pgmq.create_partitioned(...)` + pg_partman | See [pgmq-partitioned_setup_5min.sql](pgmq-partitioned_setup_5min.sql) for the partman cron schedule. |
| river | [install_river.sh](install_river.sh) | `go install` the CLI, run `river migrate-up`. |
| que | — (Ruby gem installed in VM userdata) | `gem install que -v 2.4.x`, `bundle exec que:install`. The VM userdata handles this; the bench only consumes the schema. |
| pgboss | [install_pgboss.sh](install_pgboss.sh) | `npm install pg-boss@12.15` locally on the VM, run `new PgBoss(DSN).start()` once to migrate schema. |

There is **no** `install_que.sh` or `install_pgmq-partitioned.sh` as separate shell files — the que gem install is driven from the AMI user-data (Ruby + bundler), and pgmq-partitioned reuses `install_pgmq.sh` + `pg_partman` + a single-call SQL script.

Reset procedure between runs: [../runners/clean_reinstall.sh](../runners/clean_reinstall.sh). See [../OPS_GOTCHAS.md §4, §5, §7](../OPS_GOTCHAS.md) for the schema-drop pitfalls.
