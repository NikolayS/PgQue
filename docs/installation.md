---
title: Installation and operations
description: Install PgQue on managed or self-hosted Postgres, keep it ticking, grant the right roles, upgrade in place, and troubleshoot the common failures.
---

PgQue is a pure-SQL Postgres queue. There is no extension to compile and no
`shared_preload_libraries` change to make — you install it by running one SQL
file, and you keep it running by calling `pgque.ticker()` on a schedule. This
page covers the full operational loop: install, tick, grant, upgrade, uninstall,
and troubleshoot.

If you are brand new to PgQue, start with the [tutorial](tutorial.md) for a
hands-on walkthrough, then come back here to set up a durable install.

## Requirements

- Postgres 14 or newer. PgQue uses `xid8`, `pg_snapshot`, and
  `pg_current_xact_id()`, which arrived in Postgres 14.
- Something that calls `pgque.ticker()` periodically. Without a ticker,
  producers can enqueue events but consumers see nothing — see
  [ticking](#ticking) below. This is the single most common "it isn't working"
  cause.
- Optionally `pg_cron` or `pg_timetable` to drive the ticker for you. PgQue
  itself works without either; they are only used to schedule the maintenance
  calls.

PgQue runs on managed Postgres (it needs no superuser, no extension, and no
restart) as well as self-hosted clusters.

## Install

Clone the repository and run the single-file installer inside a transaction.
From `psql`:

```sql
begin;
\i sql/pgque.sql
commit;
```

Or as a shell one-liner from the repository root:

```bash
PAGER=cat psql --no-psqlrc --single-transaction -d mydb -f sql/pgque.sql
```

The installer creates the `pgque` schema, all functions and tables, and the
three roles (`pgque_reader`, `pgque_writer`, `pgque_admin`). It is idempotent —
re-running it upgrades an existing install in place (see [upgrading](#upgrading)).

Verify the install:

```sql
select pgque.version();
```

At this point the queue machinery exists but nothing is ticking yet. Set up a
ticker next.

## Ticking

A tick is the unit that makes enqueued events visible to consumers. Until a tick
covers an event, `receive()` will not return it. You therefore need
`pgque.ticker()` to run on a schedule. PgQue ticks 10 times per second by
default (one tick every 100 ms). There are three ways to drive it.

### Option A — pg_cron (recommended)

If `pg_cron` is available, one call schedules everything:

```sql
select pgque.start();
```

`pgque.start()` registers four `pg_cron` jobs:

| Job | Cadence | What it does |
| --- | --- | --- |
| `pgque_ticker` | every 1 s | runs `pgque.ticker_loop()`, which re-ticks sub-second inside each 1 s slot at the configured rate (default 10 ticks/sec) |
| `pgque_retry_events` | every 30 s | moves `nack`'d events from the retry queue back into the stream |
| `pgque_maint` | every 30 s | table-rotation step 1 (and any queue extra-maint hooks) |
| `pgque_rotate_step2` | every 10 s | table-rotation step 2, which must run in its own transaction |

`pg_cron`'s minimum granularity is one second, so the ticker job fires once per
second and `pgque.ticker_loop()` does the sub-second re-ticking within that slot.
`pgque.start()` requires `pg_cron` and raises a clear error if it is absent; PgQue
itself does not. Stop the jobs with `pgque.stop()`.

`pg_cron` runs jobs in a single designated database (`cron.database_name`,
typically `postgres`). If your PgQue schema lives in a different database,
`pgque.start()` will not reach it from the cron database — instead schedule the
jobs with `cron.schedule_in_database`, pointing each at PgQue's database:

```sql
select cron.schedule_in_database('pgque_ticker', '1 second',
  $$call pgque.ticker_loop()$$, 'mydb');
select cron.schedule_in_database('pgque_retry_events', '30 seconds',
  $$select pgque.maint_retry_events()$$, 'mydb');
select cron.schedule_in_database('pgque_maint', '30 seconds',
  $$select pgque.maint()$$, 'mydb');
```

### Option B — pg_timetable

If you run `pg_timetable`, use its variant. It schedules the same four jobs:

```sql
select pgque.start_timetable();   -- ticks_per_second defaults to 10
select pgque.start_timetable(10); -- explicit form
```

`pg_timetable` is an external, long-running scheduler process (not a Postgres
background worker), so keep its worker running against PgQue's database with
`--clientname=pgque`. Stop with `pgque.stop_timetable()`. The two schedulers are
mutually exclusive: `pgque.start_timetable()` first removes any PgQue `pg_cron`
jobs, and `pgque.start()` first removes any PgQue `pg_timetable` jobs, so
switching between them never leaves both ticking.

### Option C — manual or external scheduler

With no in-database scheduler, call the maintenance functions yourself from any
external scheduler (a sidecar loop, a cron'd `psql`, an application worker):

```sql
select pgque.ticker();              -- create ticks for eligible queues
select pgque.maint_retry_events();  -- re-queue due retry events
select pgque.maint();               -- rotation step 1 (and any queue extra-maint hooks)
```

For sub-second delivery, loop `pgque.ticker()` at your desired interval rather
than relying on a once-per-second slot. Each `pgque.ticker()` call must commit
before the next, so an external loop should commit between iterations.

> Without a working ticker, enqueue succeeds but consumers see nothing. If
> `receive()` returns no rows, check that a ticker is running first.

## Tick rate

The default tick period is 100 ms (10 ticks per second). Tune it with:

```sql
select pgque.set_tick_period_ms(50); -- 20 ticks/sec
```

Accepted values are exact divisors of 1000 in the range 1..1000 ms (for example
1, 2, 4, 5, 10, 20, 50, 100, 200, 500, 1000). A value that does not divide 1000
evenly is rejected. The new period is picked up on the next ticker slot.

A faster tick means lower delivery latency at the cost of more tick rows and
maintenance work; a slower tick is cheaper but raises latency. For the latency
and cost trade-offs, see [latency-and-tuning.md](latency-and-tuning.md).

## Roles and grants

PgQue ships three roles. `pgque_reader` and `pgque_writer` are **siblings**, not
parent and child — neither inherits the other.

| Role | For | Capabilities |
| --- | --- | --- |
| `pgque_reader` | consumers, dashboards | consume API (`subscribe`, `unsubscribe`, `receive`, `ack`, `nack`), read-only info functions, `select` on tables. Cannot produce. |
| `pgque_writer` | producers | produce API (`send`, `send_batch`, `insert_event`). Cannot consume. |
| `pgque_admin` | operators, migrations | member of both reader and writer, plus lifecycle and DDL (`create_queue`, `drop_queue`, `start`, `stop`, `maint`, `set_queue_config`, `uninstall`). |

Because the roles are siblings, an application that both produces and consumes
must be granted **both**:

```sql
grant pgque_writer to app_orders;
grant pgque_reader to app_orders;
```

A producer-only service needs only the writer role:

```sql
grant pgque_writer to app_webhook;
```

A consumer-only or monitoring service needs only the reader role:

```sql
grant pgque_reader to metrics;
```

PUBLIC can read the metadata catalog tables but cannot execute any PgQue
function. Lifecycle and DDL functions are admin-only — run them as a migration or
operator role that holds `pgque_admin`. Roles are cluster-global and not scoped
per queue; see [reference.md](reference.md) for the role-scope details.

## Upgrading

Upgrades are SQL-file upgrades: re-run `sql/pgque.sql` over the existing install,
in a single transaction, as the schema owner or a superuser. From the repository
root:

```bash
PAGER=cat psql --no-psqlrc --single-transaction -v ON_ERROR_STOP=1 -d mydb -f sql/pgque.sql
```

The installer is idempotent. It preserves queues, consumers, subscriptions, retry
rows, dead-letter rows, and existing event tables while adding any new functions,
columns, grants, and constraints the target release needs.

One grant subtlety: `create or replace function` preserves existing grants, and
Postgres does not auto-revoke role-to-role grants. The installer therefore
explicitly revokes the older `pgque_reader -> pgque_writer` grant and re-applies
function grants on the corrected sibling roles. After upgrading, confirm the
version:

```sql
select pgque.version();
```

## Uninstall

To remove PgQue, run the uninstall script. It best-effort stops the scheduler
and drops the `pgque` schema with `cascade`:

```sql
\i sql/pgque_uninstall.sql
```

The schema and all its data are dropped. The three roles are **not** dropped,
because Postgres roles are cluster-global and may be referenced elsewhere; drop
them yourself if you no longer need them.

## pg_tle packaging variant

`sql/pgque-tle.sql` installs the same function and table surface wrapped as a
[pg_tle](https://github.com/aws/pg_tle) trusted-language extension, so PgQue
appears in `pg_available_extensions` and is managed with `create extension` /
`drop extension`. It trades the zero-dependency `\i` install for a `pg_tle`
prerequisite.

Prerequisites: `pg_tle` must be in `shared_preload_libraries`, and the installer
must run as a role holding `pgtle_admin` plus `CREATEROLE` (the roles are created
outside the extension body since they are cluster-global).

First make sure `pg_tle` is preloaded. **Append** it to the existing list —
overwriting `shared_preload_libraries` would disable anything else you preload
(for example `pg_cron`):

```sql
show shared_preload_libraries;                                -- inspect the current list first
alter system set shared_preload_libraries = 'pg_cron,pg_tle'; -- keep existing entries
-- restart Postgres, then in the target database:
create extension pg_tle;
```

With `pg_tle` loaded, register and create PgQue:

```sql
\i sql/pgque-tle.sql
create extension pgque;
```

Uninstall the TLE variant with:

```sql
\i sql/pgque-tle-uninstall.sql
```

If you have no specific reason to use `pg_tle`, prefer the plain `sql/pgque.sql`
install above.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `receive()` returns no events | No ticker is running, so no ticks exist | Start a ticker: `pgque.start()`, `pgque.start_timetable()`, or a manual `pgque.ticker()` loop (see [ticking](#ticking)) |
| The same batch comes back every time | The batch was never acknowledged | Call `pgque.ack(batch_id)` after processing; an unacked batch is redelivered |
| Retried events never reappear | `maint_retry_events()` is not running | Ensure the retry job runs (`pgque.start()` schedules it every 30 s; otherwise call `pgque.maint_retry_events()` yourself) |
| Queue / event tables grow without bound | A stopped or stuck consumer pins the oldest tick and blocks table rotation | Resume or unsubscribe the wedged consumer; rotation can then truncate old tables. See [concepts.md](concepts.md) on rotation |
| `force_tick` / `force_next_tick` looks like a no-op | These only bump the event sequence; they do not insert a tick | Run the ticker afterward: `select pgque.force_next_tick('q'); select pgque.ticker('q');` |
| Permission denied calling `send` (or `ack`/`receive`) | The role lacks the matching sibling role | Grant `pgque_writer` to produce, `pgque_reader` to consume, both to do both (see [roles and grants](#roles-and-grants)) |

For consumer-side health metrics (lag, pending events, batch state) see
[monitoring.md](monitoring.md). For end-to-end examples (fan-out, exactly-once)
see [examples.md](examples.md).
