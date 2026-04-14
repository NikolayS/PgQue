# PgQue – PgQ, universal edition

**Zero-bloat PostgreSQL queue. No extensions. No daemon. One SQL file.**

[![CI](https://github.com/NikolayS/pgque/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/pgque/actions/workflows/ci.yml)
[![PostgreSQL 14-18](https://img.shields.io/badge/PostgreSQL-14--18-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![pg_cron](https://img.shields.io/badge/pg__cron-optional-336791)](https://github.com/citusdata/pg_cron)

PgQue brings back [PgQ](https://github.com/pgq/pgq) — one of the most proven PostgreSQL queue architectures ever built — in a form that fits modern Postgres.

PgQ was originally designed at Skype, with architecture meant to serve **1B users**, and it was used in very large self-managed PostgreSQL installations for years. That knowledge is mostly forgotten ancient arto now — real database kung fu from the era when people solved brutal scale problems without cargo-culting another distributed system into the stack.

PgQue takes that battle-tested core and repackages it as an extension-free, managed-Postgres-friendly project.

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

## Comparison

| Feature | PgQue | PGMQ | River | graphile-worker | pg-boss | Oban |
|---|---|---|---|---|---|---|
| Lockless snapshot-based claim mechanism | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Zero bloat under sustained load | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| No C extension required | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| Managed Postgres friendly | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| Language-agnostic SQL API | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| Multiple independent consumers | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |
| Built-in retry queue | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Built-in dead letter queue | ✅ | ⚠️ | ❌ | ❌ | ✅ | ⚠️ |
| Battle-tested core architecture | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |

**Legend:** ✅ yes · ❌ no · ⚠️ partial / indirect

## Installation

**Requirements:** PostgreSQL 14+. `pg_cron` is optional and recommended.

```sql
begin;
\i sql/pgque.sql
commit;
```

With `pg_cron` installed, start the built-in ticker and maintenance jobs:

```sql
select pgque.start();
```

Without `pg_cron`, installation still works. You just drive ticking and maintenance from an external scheduler or your app:

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

For now, treat installation as initial setup. Upgrade/reinstall guarantees are still being tightened.

To uninstall:

```sql
\i sql/pgque_uninstall.sql
```

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
select * from pgque.receive('orders', 'processor', 100);

-- transaction 5: acknowledge the batch
select pgque.ack(1);
```

Important: send/tick/receive should be separate transactions. That's not a PgQue quirk so much as PgQ's snapshot-based design doing exactly what it is supposed to do.

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
- dead letter queue tooling
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
| `pgque.send(queue, payload)` | `bigint` | Send a message with default type |
| `pgque.send(queue, type, payload)` | `bigint` | Send with explicit event type |
| `pgque.send_batch(queue, type, payloads[])` | `bigint[]` | Batch send in a single transaction |
| `pgque.send_at(queue, type, payload, deliver_at)` | `bigint` | Delayed/scheduled delivery |

### Consuming

| Function | Returns | Description |
|---|---|---|
| `pgque.receive(queue, consumer, max_return)` | `setof pgque.message` | Receive up to N messages from the next batch |
| `pgque.ack(batch_id)` | `integer` | Finish batch and advance consumer position |
| `pgque.subscribe(queue, consumer)` | `integer` | Register a consumer |
| `pgque.unsubscribe(queue, consumer)` | `integer` | Unregister a consumer |

### Queue management

| Function | Returns | Description |
|---|---|---|
| `pgque.create_queue(queue)` | `integer` | Create a new queue |
| `pgque.drop_queue(queue)` | `integer` | Drop a queue |

### Lifecycle

| Function | Returns | Description |
|---|---|---|
| `pgque.start()` | `void` | Create pg_cron ticker + maintenance jobs when pg_cron is available |
| `pgque.stop()` | `void` | Remove pg_cron jobs |
| `pgque.status()` | `table` | Diagnostic dashboard |
| `pgque.ticker()` | `bigint` | Manual ticker for all queues |
| `pgque.maint()` | `integer` | Manual maintenance runner |
| `pgque.uninstall()` | `void` | Stop jobs and drop schema |

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
