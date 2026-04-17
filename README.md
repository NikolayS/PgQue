# PgQue – PgQ, universal edition

**Zero-bloat PostgreSQL queue. No extensions. No daemon. One SQL file.**

[![CI](https://github.com/NikolayS/pgque/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/pgque/actions/workflows/ci.yml)
[![PostgreSQL 14-18](https://img.shields.io/badge/PostgreSQL-14--18-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![pg_cron](https://img.shields.io/badge/pg__cron-optional-336791)](https://github.com/citusdata/pg_cron)
[![Anti-Extension](https://img.shields.io/badge/anti--extension-%5Ci_and_go-orange)](https://github.com/NikolayS/pgque)

## Contents

- [Why PgQue](#why-pgque)
- [Comparison](#comparison)
- [Installation](#installation)
- [Project status](#project-status)
- [Quick start](#quick-start)
- [Usage examples](#usage-examples)
- [Client libraries](#client-libraries)
- [Function reference](#function-reference)
- [Benchmarks](#benchmarks)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

PgQue brings back [PgQ](https://github.com/pgq/pgq) — one of the most proven PostgreSQL queue architectures ever built — in a form that fits modern Postgres.

PgQ was originally designed at Skype, with architecture meant to serve **1B users**, and it was used in very large self-managed PostgreSQL installations for years. That knowledge is mostly lost art now — real database kung fu from the era when people solved brutal scale problems without cargo-culting another distributed system into the stack.

PgQue takes that battle-tested core and repackages it as an extension-free, managed-Postgres-friendly project.

**The anti-extension.** Pure SQL + PL/pgSQL that works on any Postgres 14+ — including RDS, Cloud SQL, AlloyDB, Supabase, Neon, and every other managed provider. No C extension, no `shared_preload_libraries`, no provider approval, no restart. Just `\i` and go.

If you want the historical context, two decks are worth your time:

- [Marko Kreen (Skype), PGCon 2009 — PgQ](https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf)
- [Alexander Kukushkin (Microsoft), 2026 — Rediscovering PgQ](https://speakerdeck.com/cyberdemn/rediscovering-pgq)

## Why PgQue

Most PostgreSQL queues rely on `SKIP LOCKED` plus `DELETE` or `UPDATE`. That works nicely in toy examples and then quietly turns into dead tuples, VACUUM pressure, index bloat, and performance drift under sustained load.

PgQue avoids that whole class of problems. It uses **snapshot-based batching** and **TRUNCATE-based table rotation** instead of per-row deletion. So the hot path stays predictable over time:

- **Zero bloat by design** — no dead tuples in the main queue path
- **No performance decay under sustained load** — it does not get slower just because it has been running for months
- **Built for heavy-loaded systems** — this is exactly the kind of abuse the original PgQ architecture was made for
- **Real PostgreSQL guarantees** — ACID transactions, transactional enqueue/consume patterns, WAL, backups, replication, SQL visibility, and the rest of the Postgres toolbox
- **Works on managed Postgres** — no custom server build, no C extension, no separate daemon process

This is the key point: PgQue gives you queue semantics **inside** Postgres, with Postgres durability and transactional behavior, without paying the usual bloat tax most in-database queues eventually pay.

## Latency trade-off

PgQue is built around **snapshot-based batching**, not row-by-row claiming.

That gives it the properties this project cares about most:
- zero bloat in the hot path
- stable behavior under sustained load
- clean ACID semantics inside PostgreSQL

The trade-off is latency. In the default configuration, delivery is typically measured in **seconds**, not milliseconds, because events become visible on ticks rather than immediately on insert.

Ways to reduce latency:
- tune tick frequency and queue thresholds
- use `force_tick()` for tests, demos, or explicit immediate batching
- use `LISTEN/NOTIFY` so consumers wake up as soon as a batch is created (with the usual caveats: notifications are hints, not durable delivery)
- future versions may improve wake-up behavior further, including logical-decoding-based approaches

So: if your top priority is ultra-low-latency dispatch in the single-digit millisecond range, PgQue is probably the wrong hammer. If your priority is **stability under load without bloat**, that's exactly where it gets interesting.

## Comparison

| Feature | PgQue | PgQ | PGMQ | River | Que | pg-boss |
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

- **[PgQ](https://github.com/pgq/pgq)** is the original Skype-era queue
  engine (~2007) that PgQue is derived from. Same snapshot/rotation
  architecture, but requires C extensions and an external daemon (`pgqd`) —
  unavailable on managed Postgres. PgQue removes both constraints.
- **No external daemon:** PgQue uses pg_cron for ticker/maintenance; PGMQ
  uses visibility timeouts. Both run entirely inside PostgreSQL. River
  requires a Go binary, Que requires Ruby, pg-boss requires Node.js.
- **[Que](https://github.com/que-rb/que)** uses advisory locks (not
  SKIP LOCKED) — no dead tuples from *claiming*, but completed jobs are
  still DELETEd. Brandur's famous
  [bloat post](https://brandur.org/postgres-queues) was about Que at Heroku.
  Ruby-only.
- **PGMQ retry** is via visibility timeout re-delivery (`read_ct`
  tracking) — no configurable backoff or max attempts built in.
- **pg-boss fan-out** uses `publish()`/`subscribe()` with copy-per-queue
  semantics, not a shared event log with independent cursors.
- **Category difference:** River, Que, and pg-boss are **job queue
  frameworks** with worker executors, priority queues, and per-job lifecycle
  management. PgQue is an **event/message queue** optimized for
  high-throughput streaming with fan-out. See also: Oban (Elixir),
  graphile-worker (Node.js), solid_queue (Ruby/Rails), good_job (Ruby).

### What genuinely differentiates PgQue

**1. Zero event-table bloat under sustained load (structural, not tuning-dependent)**

SKIP LOCKED queues (PGMQ, River, pg-boss, Oban, graphile-worker) all use
UPDATE + DELETE on rows, creating dead tuples that require VACUUM. Under
sustained load this causes documented, reproducible production failures:

- [Brandur/Heroku (2015)](https://brandur.org/postgres-queues): single open
  transaction caused 60k job backlog in one hour
- [PlanetScale (2026)](https://planetscale.com/blog/keeping-a-postgres-queue-healthy):
  death spiral at 800 jobs/sec with shared analytics queries
- [River issue #59](https://github.com/riverqueue/river/issues/59):
  autovacuum starvation documented at Heroku
- Oban Pro shipped table partitioning specifically to mitigate bloat
- PGMQ ships aggressive autovacuum settings in some deployment setups

PgQue's TRUNCATE rotation creates zero dead tuples in event tables by
construction. No tuning required, immune to xmin horizon pinning. This
matters most at sustained high throughput or when the queue database is
shared with OLAP workloads.

**2. Native fan-out (multiple independent consumers on a shared event log)**

Each registered consumer maintains its own cursor position and independently
receives all events. This is fundamentally different from competing-consumers
(SKIP LOCKED) where each job goes to one worker. pg-boss has fan-out but it
is copy-per-queue (one INSERT per subscriber per event). PgQue's model is
position-in-shared-log — no data duplication, atomic batch boundaries, late
subscribers catch up.

This makes PgQue more analogous to Kafka topics than to a job queue.

### When to use PgQue vs. a job queue

PgQue is an **event/message queue**. River, graphile-worker, pg-boss, and
Oban are **job queue frameworks**. They are different categories:

- **Choose PgQue** when you want event-driven fan-out, zero-maintenance
  bloat behavior, language-agnostic SQL API, and you do not need per-job
  priorities/scheduling/worker frameworks
- **Choose a job queue** when you need per-job lifecycle management,
  sub-3ms latency, priority queues, cron scheduling, unique jobs, and
  deep ecosystem integration (Elixir/Go/Node.js/Ruby)

## Installation

**Requirements:** PostgreSQL 14+. `pg_cron` is optional and recommended.

```sql
begin;
\i sql/pgque.sql
commit;
```

With `pg_cron` installed, `pgque.start()` creates the default ticker and maintenance jobs:

```sql
select pgque.start();
```

Without `pg_cron`, installation still works, but PgQue is not self-running. You must drive ticking and maintenance from an external scheduler or application process:

```bash
# every 1-2 seconds
psql -c "select pgque.ticker()"

# for demos/tests, if you need an immediate batch without waiting,
# force a tick threshold first for that queue
psql -c "select pgque.force_tick('orders')"
psql -c "select pgque.ticker()"

# every 30 seconds
psql -c "select pgque.maint()"
```

**Important:** PgQue does not deliver messages without a working ticker. By default, `pgque.start()` sets this up with `pg_cron`. If the cron job is absent, disabled, or broken, enqueueing still works, but consumers will see nothing new because ticks are not being created. If you do not use `pg_cron`, run `pgque.ticker()` yourself from an external scheduler or application process. Run `pgque.maint()` regularly as well for retries, cleanup, and rotation.

For now, treat installation as initial setup. Upgrade/reinstall guarantees are still being tightened.

To uninstall:

```sql
\i sql/pgque_uninstall.sql
```

## Roles and grants

The install creates three roles with explicit grants for the operational
API. Application users do not need superuser; grant them whichever role
matches their access pattern.

| Role | Purpose | Granted access |
|---|---|---|
| `pgque_reader` | Dashboards, metrics, debugging | `get_queue_info`, `get_consumer_info`, `get_batch_info`, `version`, plus `select` on all tables |
| `pgque_writer` | Producers and consumers (most apps) | inherits `pgque_reader` + the modern API (`send`, `send_batch`, `subscribe`, `unsubscribe`, `receive`, `ack`, `nack`) and the underlying PgQ primitives (`insert_event`, `next_batch`, `get_batch_events`, `finish_batch`, `event_retry`, `register_consumer`, `unregister_consumer`) |
| `pgque_admin`  | Operators, migrations | inherits `pgque_writer` + full schema/table/sequence access. Excludes `uninstall()` (superuser-only on purpose: it drops the schema). |

Typical app setup:

```sql
-- once, as superuser or schema owner
\i sql/pgque.sql
select pgque.start();           -- optional: pg_cron ticker + maint

-- for the application connection
create user app_orders with password '...';
grant pgque_writer to app_orders;

-- for a read-only metrics user
create user metrics with password '...';
grant pgque_reader to metrics;
```

DDL-class operations (`create_queue`, `drop_queue`, `start`, `stop`,
`maint`, `ticker`, `force_tick`) are not granted to `pgque_writer` and
should be performed by an admin / migration role. They run on PUBLIC
default for now; revoking from PUBLIC and granting only to
`pgque_admin` is on the roadmap.

## Project status

PgQue is **early-stage** as a product and API layer.

PgQ itself is **rock solid** — battle-tested in very large systems over many years. What's new here is the packaging, modernization, managed-Postgres compatibility, and the higher-level PgQue API around that core.

The default install intentionally stays small in v0.1. Additional APIs live under `sql/experimental/` until they are worth promoting into the default install. See [blueprints/PHASES.md](blueprints/PHASES.md).

## Quick start

```sql
-- transaction 1: create queue + consumer
select pgque.create_queue('orders');
select pgque.subscribe('orders', 'processor');

-- transaction 2: send a message
select pgque.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);

-- transaction 3: advance the queue if you are not using pg_cron
-- force_tick() is handy in demos/tests to avoid waiting for lag/count thresholds
select pgque.force_tick('orders');
select pgque.ticker();

-- transaction 4: receive and process messages
-- (capture batch_id from any returned row -- it's the same for the whole batch)
select * from pgque.receive('orders', 'processor', 100);

-- transaction 5: acknowledge the batch
select pgque.ack(:batch_id);  -- substitute the batch_id you saw above
```

Important: send/tick/receive should be separate transactions. That's not a PgQue quirk so much as PgQ's snapshot-based design doing exactly what it is supposed to do.
In normal operation, a scheduler or `pg_cron` job should be driving `pgque.ticker()`. `force_tick()` is mainly useful for demos, tests, and manual operation.

## Usage examples

### Send with event type

```sql
select pgque.send('orders', 'order.created',
  '{"order_id": 42}'::jsonb);

select pgque.send('orders', 'order.shipped',
  '{"order_id": 42, "tracking": "1Z999AA10123456784"}'::jsonb);
```

### Batch send

```sql
select pgque.send_batch('orders', 'order.created', array[
  '{"order_id": 1}'::jsonb,
  '{"order_id": 2}'::jsonb,
  '{"order_id": 3}'::jsonb
]);
```

### Experimental SQL

The repo also contains additional SQL under `sql/experimental/` for features
that are not part of the default v0.1 install yet:

- delayed delivery
- observability helpers

Those pieces are being kept out of the default install until the API surface settles.

### Fan-out with multiple consumers

```sql
select pgque.subscribe('orders', 'audit_logger');
select pgque.subscribe('orders', 'notification_sender');
select pgque.subscribe('orders', 'analytics_pipeline');

select * from pgque.receive('orders', 'audit_logger', 100);
select * from pgque.receive('orders', 'notification_sender', 100);
```

### Transactional exactly-once-ish processing

Do the queue read, your writes, and the ack in one transaction:

```sql
begin;
  create temp table msgs as
    select * from pgque.receive('orders', 'processor', 100);

  insert into processed_orders (order_id, status)
  select (payload::jsonb->>'order_id')::int, 'done'
  from msgs;

  select pgque.ack((select distinct batch_id from msgs limit 1));
commit;
```

If the transaction rolls back, your writes roll back and the ack rolls back too.

### Recurring jobs with pg_cron

```sql
select cron.schedule('daily_report',
  '0 9 * * *',
  $$select pgque.send('jobs', 'report.generate',
      '{"type": "daily"}'::jsonb)$$);
```

## Client libraries

PgQue currently includes example client libraries for **Go**, **Python**, and **TypeScript**.

These are **examples for now** — **unpublished**, still evolving, and meant to demonstrate integration patterns rather than promise stable SDKs yet.

**Help improving them is very much appreciated.**

PgQue is SQL-first, so any PostgreSQL driver works. On top of that, dedicated client libraries already exist or are being built around the API.

### Python (`pgque-py`)

Built on psycopg 3. Typical usage:

```python
from pgque import PgqueClient, Consumer

client = PgqueClient(conn)
client.send("orders", {"order_id": 42})

consumer = Consumer(dsn, queue="orders", name="processor", poll_interval=30)

@consumer.on("order.created")
def handle_order(msg):
    process_order(msg.payload)

consumer.start()
```

### Go (`pgque-go`)

Built on pgx/v5. Typical usage:

```go
client, _ := pgque.Connect(ctx, "postgresql://localhost/mydb")

consumer := client.NewConsumer("orders", "processor")
consumer.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
    return processOrder(msg)
})
consumer.Start(ctx)
```

### TypeScript (`pgque-ts`)

Built on node-postgres (`pg`). Typical usage:

```ts
const client = new PgqueClient('postgresql://localhost/mydb');
await client.connect();

await client.send('orders', { order_id: 42 }, 'order.created');
await client.subscribe('orders', 'processor');

const messages = await client.receive('orders', 'processor', 100);
if (messages.length > 0) {
  await client.ack(messages[0].batch_id);
}
```

### Any language

If your language can talk to PostgreSQL, you can use PgQue immediately:

```sql
select pgque.send('orders', '{"order_id": 42}'::jsonb);
select * from pgque.receive('orders', 'processor', 100);
select pgque.ack(batch_id);
```

## Function reference

### Publishing

| Function | Returns | Description |
|---|---|---|
| `pgque.send(queue, payload)` | `bigint` | Send with default type. Untyped literals resolve to the `text` overload (fast path, bytes verbatim — text only, no NUL bytes); use `::jsonb` cast to opt into validation + canonicalization. See [SPECx.md §4.1](blueprints/SPECx.md) for the NUL-byte caveat and binary-payload encoding guidance |
| `pgque.send(queue, type, payload)` | `bigint` | Send with explicit event type. Same `text` / `jsonb` overload rules as above |
| `pgque.send_batch(queue, type, payloads[])` | `bigint[]` | Batch send in a single transaction. `text[]` by default, `jsonb[]` opt-in via cast |
| `pgque.send_at(queue, type, payload, deliver_at)` | `bigint` | Delayed/scheduled delivery (experimental) |

### Consuming

| Function | Returns | Description |
|---|---|---|
| `pgque.receive(queue, consumer, max_return)` | `setof pgque.message` | Receive up to N messages from the next batch |
| `pgque.ack(batch_id)` | `integer` | Finish batch and advance consumer position (modern alias for `finish_batch`) |
| `pgque.nack(batch_id, msg, retry_after, reason)` | `integer` | Retry the message after `retry_after` (default 60s), or move to dead letter once the queue's `max_retries` is reached |
| `pgque.subscribe(queue, consumer)` | `integer` | Register a consumer (modern alias for `register_consumer`) |
| `pgque.unsubscribe(queue, consumer)` | `integer` | Unregister a consumer |

### Queue management

| Function | Returns | Description |
|---|---|---|
| `pgque.create_queue(queue)` | `integer` | Create a new queue (admin / migration role) |
| `pgque.drop_queue(queue)` | `integer` | Drop a queue (admin / migration role) |

### Observability

Granted to `pgque_reader`, so dashboards and metrics services don't need write access.

| Function | Returns | Description |
|---|---|---|
| `pgque.get_queue_info()` / `pgque.get_queue_info(queue)` | `setof record` | Per-queue stats: tick count, last tick time, event count, table sizes |
| `pgque.get_consumer_info()` / `(queue)` / `(queue, consumer)` | `setof record` | Per-consumer lag, last seen tick, pending batches |
| `pgque.get_batch_info(batch_id)` | `record` | Inspect a specific batch (consumer, queue, range) |
| `pgque.version()` | `text` | Installed PgQue version |

### Lifecycle

| Function | Returns | Description |
|---|---|---|
| `pgque.start()` | `void` | Create pg_cron ticker + maintenance jobs when pg_cron is available |
| `pgque.stop()` | `void` | Remove pg_cron jobs |
| `pgque.status()` | `setof record` | Diagnostic dashboard (cron jobs, ticker health, queue lag) |
| `pgque.ticker()` | `bigint` | Manual ticker for all queues — call from your scheduler if you do not use pg_cron |
| `pgque.force_tick(queue)` | `bigint` | Bypass tick thresholds for one queue and create a tick now. Useful in demos and tests; not for production hot paths |
| `pgque.maint()` | `integer` | Manual maintenance runner — table rotation, retry processing, cleanup |
| `pgque.uninstall()` | `void` | Stop jobs and drop schema (superuser only) |

### PgQ-native API

The original PgQ primitives are still there for advanced use:

| Function | Description |
|---|---|
| `pgque.insert_event(queue, type, data)` | Low-level event insert |
| `pgque.next_batch(queue, consumer)` | Get next batch ID |
| `pgque.get_batch_events(batch_id)` | Get all events in a batch |
| `pgque.finish_batch(batch_id)` | Mark batch complete |
| `pgque.event_retry(batch_id, event_id, seconds)` | Schedule event retry |

## Benchmarks

Preliminary results on a laptop (Apple Silicon, 10 cores, 24 GiB RAM,
PostgreSQL 18.3, `synchronous_commit=off` per-session). Full methodology:
[NikolayS/pgq#1](https://github.com/NikolayS/pgq/issues/1).

| Scenario | Throughput | Per core |
|---|---|---|
| PL/pgSQL single insert/TX, ~100 B, 16 clients | **85,836 ev/s** | ~8.6k ev/s |
| PL/pgSQL batched 100k/TX, ~100 B | 80,515 ev/s | ~8.1k ev/s |
| PL/pgSQL batched 100k/TX, ~2 KiB | 48,899 ev/s (91.5 MiB/s) | ~4.9k ev/s |
| Consumer read rate, 100k batch, ~100 B | ~2.4M ev/s | ~240k ev/s |
| Consumer read rate, 100k batch, ~2 KiB | ~305k ev/s (568 MiB/s) | ~30.5k ev/s |

Key takeaways:

- **Zero bloat under sustained load** — 30-minute sustained test showed zero dead tuple growth in event tables
- **Batching matters** — throughput jumps hard when you stop doing one tiny transaction per event
- **Consumer side is not the bottleneck** — reads are much faster than writes
- **You keep Postgres guarantees** — transactional semantics, WAL durability options, backups, replication, SQL introspection

> `synchronous_commit=off` can be set per session or per transaction for queue-heavy workloads if that trade-off makes sense for your system.

## Architecture

PgQue keeps PgQ's proven core architecture and adds a modern API layer:

- **Snapshot-based batch isolation** — each batch contains exactly the events committed between two ticks
- **Three rotating event tables** — the main queue path uses 3-table TRUNCATE rotation instead of row-by-row churn
- **Separate retry table** — retries are stored outside the hot event path and re-inserted later by maintenance
- **Separate delayed-delivery table** — scheduled messages wait outside the hot path until due
- **Separate dead letter queue** — exhausted messages move aside cleanly instead of poisoning normal flow
- **Multiple independent consumers** — each consumer tracks its own position
- **Optional pg_cron scheduler** — replaces the old external `pgqd` daemon when available; otherwise call SQL functions manually

See [SPECx.md](blueprints/SPECx.md) for the full specification and
[SPEC.md](blueprints/SPEC.md) for PgQ internals.

## Contributing

See [SPECx.md](blueprints/SPECx.md) for the specification and implementation
plan. New code should follow red/green TDD: write the failing test first, then fix it.

## License

Apache-2.0. See [LICENSE](LICENSE).

PgQue includes code derived from [PgQ](https://github.com/pgq/pgq) (ISC license, Marko Kreen / Skype Technologies OU). See [NOTICE](NOTICE).
