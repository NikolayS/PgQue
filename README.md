<h1 align="center">logres — event log for Postgres</h1>

<p align="center"><strong>Zero-bloat event log, PgQ heritage. One SQL file to install, <code>pg_cron</code> to tick.</strong></p>

<p align="center">
  <a href="https://github.com/NikolayS/logres/actions/workflows/ci.yml"><img src="https://github.com/NikolayS/logres/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://www.postgresql.org/"><img src="https://img.shields.io/badge/PostgreSQL-14--18-336791?logo=postgresql&logoColor=white" alt="PostgreSQL 14-18"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://github.com/citusdata/pg_cron"><img src="https://img.shields.io/badge/pg__cron-optional-336791" alt="pg_cron"></a>
  <a href="https://github.com/NikolayS/logres"><img src="https://img.shields.io/badge/anti--extension-%5Ci_and_go-orange" alt="Anti-Extension"></a>
  <a href="https://news.ycombinator.com/item?id=47817349"><img src="https://img.shields.io/badge/Hacker%20News-discussion-ff6600?logo=ycombinator&logoColor=white" alt="Discussion on Hacker News"></a>
</p>

<p align="center"><img src="docs/images/death_spiral.gif" alt="Death spiral of a SKIP LOCKED queue under sustained load — the failure mode logres avoids by construction" width="720"></p>

Discussion on [Hacker News](https://news.ycombinator.com/item?id=47817349).

*For teams who want a durable event stream inside Postgres. The model is closer to Kafka (log) than to ActiveMQ or RabbitMQ (task message queue). Shared event log, independent per-consumer cursors, zero bloat under sustained load. Pure SQL and PL/pgSQL, any Postgres 14+ — managed or self-hosted, no sidecar daemon. The rest of this README walks the history, comparison, and install paths that back up the claim.*

## Contents

- [Why logres](#why-logres)
- [Latency trade-off](#latency-trade-off)
- [Comparison](#comparison)
- [Installation](#installation)
- [Roles and grants](#roles-and-grants)
- [Project status](#project-status)
- [Docs](#docs)
- [Quick start](#quick-start)
- [Client libraries](#client-libraries)
- [Benchmarks](#benchmarks)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

logres brings back [PgQ](https://github.com/pgq/pgq) — one of the longest-running Postgres queue architectures in production — in a form that runs on any Postgres platform, managed providers included.

PgQ was designed at Skype to run messaging for hundreds of millions of users, and it ran on large self-managed Postgres deployments for over a decade. Standard PgQ depends on a C extension (`pgq`) and an external daemon (`pgqd`), neither of which run on most managed Postgres providers.

logres rebuilds that battle-tested engine in pure PL/pgSQL, so the zero-bloat queue pattern works anywhere you can run SQL — without adding another distributed system to your stack.

**The anti-extension.** Pure SQL + PL/pgSQL on any Postgres 14+ — including RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon, and most other managed providers. No C extension, no `shared_preload_libraries`, no provider approval, no restart.

Historical context, two decks:

- [Marko Kreen (Skype), PGCon 2009 — PgQ](https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf)
- [Alexander Kukushkin (Microsoft), 2026 — Rediscovering PgQ](https://speakerdeck.com/cyberdemn/rediscovering-pgq)

## Why logres

Most Postgres queues rely on `SKIP LOCKED` plus `DELETE` and/or `UPDATE`. That holds up in toy examples and then turns into dead tuples, VACUUM pressure, index bloat, and performance drift under sustained load.

logres avoids that whole class of problems. It uses **snapshot-based batching** and **TRUNCATE-based table rotation** instead of per-row deletion. The hot path stays predictable:

- **Zero bloat by design** — no dead tuples in the main queue path
- **No performance decay** — it does not get slower because it has been running for months
- **Built for heavy-loaded systems** — the sustained-load regime the original PgQ architecture was designed for
- **Real Postgres guarantees** — ACID transactions, transactional enqueue/consume, WAL, backups, replication, SQL visibility
- **Works on managed Postgres** — no custom build, no C extension, no separate daemon

logres gives you queue semantics **inside** Postgres, with Postgres durability and transactional behavior, without the bloat tax most in-database queues eventually hit.

## Latency trade-off

logres is built around **snapshot-based batching**, not row-by-row claiming. That's what gives it zero bloat in the hot path, stable behavior under sustained load, and clean ACID semantics inside Postgres.

The trade-off is **end-to-end delivery latency** — the gap between `send` and when a consumer can `receive` the event. In the default configuration, end-to-end delivery typically lands within ~1–2 seconds: up to 1 s for the next tick, plus the consumer's poll interval. Per-call latency (the `send` / `receive` / `ack` functions themselves) stays in the microsecond range.

Ways to reduce delivery latency: tune tick frequency and queue thresholds; use `force_tick()` for tests and demos or to force an immediate batch. Future versions may add logical-decoding-based wake-ups for sub-second delivery without cutting the tick interval.

If your top priority is single-digit-millisecond dispatch, logres is the wrong tool. If your priority is **stability under load without bloat**, that is where logres fits.

## Comparison

| Feature | logres | PgQ | PGMQ | River | Que | pg-boss |
|---|---|---|---|---|---|---|
| Snapshot-based batching (no row locks) | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| Zero bloat under sustained load | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| No external daemon or worker binary | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ |
| Pure SQL install, managed Postgres ready | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| Language-agnostic SQL API | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Multiple independent consumers (fan-out) | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| Built-in retry with backoff | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ |
| Built-in dead letter queue | ✅ | ❌ | ⚠️ | ⚠️ | ❌ | ✅ |

**Legend:** ✅ yes · ❌ no · ⚠️ partial / indirect

**Notes:**

- **[PgQ](https://github.com/pgq/pgq)** is the Skype-era queue engine (~2007) logres is derived from. Same snapshot/rotation architecture, but requires C extensions and an external daemon (`pgqd`) — unavailable on managed Postgres. logres removes both constraints.
- **No external daemon:** logres uses pg_cron (or your own scheduler) for ticking; PGMQ uses visibility timeouts. River, Que, and pg-boss require a Go / Ruby / Node.js worker binary.
- **[Que](https://github.com/que-rb/que)** uses advisory locks (not SKIP LOCKED) — no dead tuples from *claiming*, but completed jobs are still DELETEd. Brandur's [bloat post](https://brandur.org/postgres-queues) was about Que at Heroku. Ruby-only.
- **PGMQ retry** is visibility-timeout re-delivery (`read_ct` tracking) — no configurable backoff or max attempts.
- **pg-boss fan-out** is copy-per-queue `publish()`/`subscribe()`, not a shared event log with independent cursors.
- **Category:** River, Que, and pg-boss (and Oban, graphile-worker, solid_queue, good_job) are **job queue frameworks**. logres is an **event/message queue** optimized for high-throughput streaming with fan-out.

### What differentiates logres

**1. Zero event-table bloat, by design.** SKIP LOCKED queues (PGMQ, River, pg-boss, Oban, graphile-worker) UPDATE + DELETE rows, creating dead tuples that require VACUUM. Under sustained load this causes documented failures:

- [Brandur/Heroku (2015)](https://brandur.org/postgres-queues) — 60k backlog in one hour.
- [PlanetScale (2026)](https://planetscale.com/blog/keeping-a-postgres-queue-healthy) — death spiral at 800 jobs/sec with OLAP on the side.
- [River issue #59](https://github.com/riverqueue/river/issues/59) — autovacuum starvation.

Oban Pro shipped table partitioning to mitigate it; PGMQ ships aggressive autovacuum settings. logres's TRUNCATE rotation creates zero dead tuples by construction. No tuning. Immune to xmin horizon pinning.

**2. Native fan-out.** Each registered consumer maintains its own cursor on a shared event log and independently receives all events. That is different from competing-consumers (SKIP LOCKED) where each job goes to one worker. pg-boss has fan-out but it is copy-per-queue (one INSERT per subscriber per event). logres's model is a position on a shared log — no data duplication, atomic batch boundaries, late subscribers catch up. Closer to Kafka topics than to a job queue.

### Log, not task queue (but can serve task-queue workloads)

logres is a log. It can still serve task-queue workloads when the workload fits a log's shape.

- **Good fit:** per-key ordered processing (partition by key), high-throughput uniform tasks, replayable pipelines, multiple consumers on the same event stream, event-sourced systems where "task" and "event" collapse into one primitive.
- **Bad fit:** high-variance task duration (a slow task head-of-lines the partition), per-message retry with backoff and priority, SQS-style visibility timeouts, dynamic load balancing across heterogeneous workers.

For bad-fit workloads, use a task-queue library (River, graphile-worker, Oban, pgmq) or an external broker (RabbitMQ, ActiveMQ, SQS). For good-fit workloads, logres saves you from running a second system.

## Installation

**Requirements:** Postgres 14+, and something that calls `logres.ticker()` periodically (every 1 second by default). `pg_cron` is the recommended default — pre-installed or one-command available on all major managed Postgres providers (RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon); on self-managed Postgres, follow the [pg_cron setup guide](https://github.com/citusdata/pg_cron#setting-up-pg_cron). Any external scheduler (system `cron`, systemd, a worker loop in your app) works as an alternative — see below.

Inside a psql session:

```sql
begin;
\i sql/logres.sql
commit;
```

Or from the shell, same single-transaction guarantee via `psql --single-transaction`:

```bash
PAGER=cat psql --no-psqlrc --single-transaction -d mydb -f sql/logres.sql
```

With `pg_cron` available in the same database as logres, `logres.start()` creates the default ticker and maintenance jobs:

```sql
select logres.start();
```

**pg_cron in a different database.** `pg_cron` runs jobs in one designated database (`cron.database_name`, typically `postgres`). If your logres schema lives in a different database, use the [cross-database pattern](https://github.com/citusdata/pg_cron#creating-a-cron-job-in-a-different-database) to call `logres.ticker()` and `logres.maint()` across databases. *Todo: a future release will detect this and emit the correct `cron.schedule_in_database` calls from `logres.start()` automatically.*

**pg_cron log hygiene.** The ticker runs every second, adding ~3,600 rows per hour to `cron.job_run_details` with no built-in purge. Set `alter system set cron.log_run = off;` globally, or schedule a periodic purge — see [the tutorial](docs/tutorial.md#production-cadence-use-pg_cron) for both recipes.

Without `pg_cron`, logres still installs. Drive ticking and maintenance from your application or an external scheduler:

```bash
PAGER=cat psql --no-psqlrc -c "select logres.ticker()"   # every 1 second
PAGER=cat psql --no-psqlrc -c "select logres.maint()"    # every 30 seconds
```

**Important:** logres does not deliver messages without a working ticker. Enqueueing still works, but consumers will see nothing new because no ticks are created. If you do not use `pg_cron`, run `logres.ticker()` and `logres.maint()` yourself.

Treat installation as one-way for now — upgrade and reinstall paths are still being tightened. To uninstall: `\i sql/logres_uninstall.sql`.

## Roles and grants

The install creates three roles. Application users do not need superuser — grant them whichever role matches their access pattern.

| Role | Purpose | Granted access |
|---|---|---|
| `logres_reader` | Dashboards, metrics, debugging | `get_queue_info`, `get_consumer_info`, `get_batch_info`, `version`, plus `select` on all tables |
| `logres_writer` | Producers and consumers (most apps) | inherits `logres_reader` + the modern API (`send`, `send_batch`, `subscribe`, `unsubscribe`, `receive`, `ack`, `nack`) and the underlying PgQ primitives (`insert_event`, `next_batch`, `get_batch_events`, `finish_batch`, `event_retry`, `register_consumer`, `unregister_consumer`) |
| `logres_admin`  | Operators, migrations | inherits `logres_writer` + full schema/table/sequence access. `uninstall()` is revoked from both `logres_admin` and PUBLIC (superuser-only — it drops the schema). |

Typical app setup:

```sql
\i sql/logres.sql
select logres.start();                     -- optional pg_cron ticker + maint

create user app_orders with password '...';          -- replace with a real password
grant logres_writer to app_orders;

create user metrics with password '...';              -- replace with a real password
grant logres_reader to metrics;
```

DDL-class operations (`create_queue`, `drop_queue`, `start`, `stop`, `maint`, `ticker`, `force_tick`) are not granted to `logres_writer` and should be performed by an admin / migration role. They currently default to PUBLIC; revoking from PUBLIC and granting only to `logres_admin` is on the roadmap.

## Project status

logres is **early-stage** as a product and API layer. PgQ itself has run at Skype scale for over a decade. What's new here is the packaging, modernization, managed-Postgres compatibility, and the higher-level logres API around that core.

The default install stays small in v0.1; additional APIs live under `sql/experimental/` until they are worth promoting. See [blueprints/PHASES.md](blueprints/PHASES.md).

## Docs

- [Tutorial](docs/tutorial.md) — a hands-on walkthrough. Start here if you are new.
- [Reference](docs/reference.md) — every shipped function and role.
- [Examples](docs/examples.md) — patterns: fan-out, exactly-once, batch loading, recurring jobs.
- [Benchmarks](docs/benchmarks.md) — throughput measurements and methodology.
- [PgQ concepts](docs/pgq-concepts.md) — glossary (batch, tick, rotation) for contributors.
- [PgQ history](docs/pgq-history.md) — where this engine came from.

## Quick start

```sql
-- tx 1: create queue + consumer
select logres.create_queue('orders');
select logres.subscribe('orders', 'processor');

-- tx 2: send a message
select logres.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);

-- tx 3: advance the queue if you are not using pg_cron
-- (force_tick bypasses lag/count thresholds — handy in demos/tests)
select logres.force_tick('orders');
select logres.ticker();

-- tx 4: receive (batch_id is the same for every returned row)
select * from logres.receive('orders', 'processor', 100);

-- tx 5: acknowledge
select logres.ack(:batch_id);
```

Send, tick, and receive should be separate transactions — that's PgQ's snapshot-based design working as intended. In normal operation, `pg_cron` or an external scheduler drives `logres.ticker()`; `force_tick()` is mainly for demos, tests, and manual operation.

Longer walkthrough in the [tutorial](docs/tutorial.md); patterns like fan-out, exactly-once, and recurring jobs in [examples](docs/examples.md).

## Client libraries

logres is SQL-first, so any Postgres driver works. Example client libraries exist for **Python**, **Go**, and **TypeScript** — unpublished, still evolving, demonstrating integration patterns rather than stable SDKs. **Contributions welcome.**

### Python (`logres-py`) — psycopg 3

```python
from logres import PgqueClient, Consumer

client = PgqueClient(conn)
client.send("orders", {"order_id": 42})

consumer = Consumer(dsn, queue="orders", name="processor", poll_interval=30)

@consumer.on("order.created")
def handle_order(msg):
    process_order(msg.payload)

consumer.start()
```

### Go (`logres-go`) — pgx/v5

```go
client, _ := logres.Connect(ctx, "postgresql://localhost/mydb")

consumer := client.NewConsumer("orders", "processor")
consumer.Handle("order.created", func(ctx context.Context, msg logres.Message) error {
    return processOrder(msg)
})
consumer.Start(ctx)
```

### TypeScript (`logres-ts`) — node-postgres

```ts
const client = new PgqueClient('postgresql://localhost/mydb');
await client.connect();

await client.send('orders', { order_id: 42 }, 'order.created');
await client.subscribe('orders', 'processor');

const messages = await client.receive('orders', 'processor', 100);
if (messages.length > 0) await client.ack(messages[0].batch_id);
```

### Any language

```sql
select logres.send('orders', '{"order_id": 42}'::jsonb);
select * from logres.receive('orders', 'processor', 100);
select logres.ack(batch_id);
```

## Benchmarks

Preliminary laptop numbers: ~86k ev/s PL/pgSQL insert, ~2.4M ev/s consumer
read rate, zero dead-tuple growth under a 30-minute sustained test. See
[docs/benchmarks.md](docs/benchmarks.md) for the full table and methodology.
Server-class numbers to follow.

## Architecture

logres keeps PgQ's proven core architecture — snapshot-based batch isolation, three-table TRUNCATE rotation on the hot path, separate retry / delayed / dead-letter tables, and independent per-consumer cursors — and adds a modern API layer on top. See [blueprints/SPECx.md](blueprints/SPECx.md) for the full specification and [docs/pgq-concepts.md](docs/pgq-concepts.md) for the batch/tick/rotation glossary.

## Contributing

See [blueprints/SPECx.md](blueprints/SPECx.md) for the specification and implementation plan. New code should follow red/green TDD: write the failing test first, then fix it.

## License

Apache-2.0. See [LICENSE](LICENSE).

logres includes code derived from [PgQ](https://github.com/pgq/pgq) (ISC license, Marko Kreen / Skype Technologies OU). See [NOTICE](NOTICE).
