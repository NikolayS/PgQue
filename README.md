# PgQue -- PgQ Universal Edition

[![CI](https://github.com/NikolayS/pgque/actions/workflows/ci.yml/badge.svg)](https://github.com/NikolayS/pgque/actions/workflows/ci.yml)
[![PostgreSQL 14-17](https://img.shields.io/badge/PostgreSQL-14--17-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Pure SQL](https://img.shields.io/badge/Pure_SQL-no_extensions-brightgreen.svg)](#installation)

**Zero-bloat PostgreSQL queue. No extensions. No daemons. One SQL file.**

PgQue is a repackaging of [PgQ](https://github.com/pgq/pgq) -- the
battle-tested queue system that ran at Skype/Microsoft scale for 15+ years --
into a modern, extension-free system that works on **any managed PostgreSQL
provider**: Amazon RDS, Aurora, Google Cloud SQL, AlloyDB, Azure, Supabase,
Neon, Crunchy Bridge, Timescale, Aiven, and any other PostgreSQL 14+ instance.
Install with `\i pgque-install.sql` -- no `CREATE EXTENSION`, no `make`, no
`shared_preload_libraries`, no server restart.

## Table of Contents

- [Why PgQue](#why-pgque)
- [How It Compares](#how-it-compares)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
  - [Fan-out (Multiple Consumers)](#fan-out-multiple-consumers)
  - [Retry and Dead Letter Queue](#retry-and-dead-letter-queue)
  - [Delayed Delivery](#delayed-delivery)
  - [Batch Send](#batch-send)
  - [Exactly-once (Transactional Ack)](#exactly-once-transactional-ack)
  - [Observability](#observability)
- [Client Libraries](#client-libraries)
  - [Python](#python)
  - [Go](#go)
- [Function Reference](#function-reference)
- [Benchmarks](#benchmarks)
- [Managed Provider Compatibility](#managed-provider-compatibility)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why PgQue

Every other PostgreSQL queue uses `SKIP LOCKED` + `DELETE`, which creates dead
tuples. Under sustained load, VACUUM can't keep up, indexes bloat, and
throughput degrades. PgQue is **structurally immune** -- it uses TRUNCATE-based
3-table rotation instead of per-row deletion. Zero dead tuples, ever.

- **No C extensions** -- pure SQL/PL/pgSQL, installs with `\i pgque-install.sql`
- **No external daemons** -- pg_cron replaces the old `pgqd` ticker
- **Works everywhere** -- RDS, Aurora, AlloyDB, Cloud SQL, Supabase, Neon, Crunchy Bridge
- **Language-agnostic** -- SQL API works from any language; client libraries for Python and Go
- **Modern API** -- `send()` / `receive()` / `ack()` / `nack()`
- **Built-in DLQ** -- dead letter queue with inspect, replay, purge
- **Delayed delivery** -- schedule messages for future delivery with `send_at()`
- **Observability** -- `queue_health()`, `queue_stats()`, OTel-compatible metrics
- **Exactly-once capable** -- transactional ack pattern (application write + ack in one TX)
- **Battle-tested core** -- PgQ has processed billions of events at Skype/Microsoft for 15+ years

## How It Compares

| Feature | PgQue | PGMQ | River | graphile-worker | pg-boss | Oban |
|---|---|---|---|---|---|---|
| Claim mechanism | Snapshot isolation (lockless) | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED |
| Table bloat under sustained load | **None** (TRUNCATE rotation) | Yes | Yes | Yes | Mitigated | Yes |
| Batch processing | Native (tick-bounded) | Manual | Manual | Manual | No | No |
| DLQ | Built-in | Via archival | No | No | Built-in | Via plugin |
| Multiple consumers | Built-in | Manual | No | No | No | Via queues |
| C extension required | No | Yes (Rust) | No (Go binary) | No | No | No |
| Language | Any (SQL API) | Any (SQL API) | Go only | Node.js only | Node.js only | Elixir only |
| Managed PG compatible | Yes | Depends | Yes | Yes | Yes | Yes |
| Typical latency | 1-2s (tick interval) | Sub-100ms | Sub-100ms | Sub-3ms | ~1s | ~1s |
| Battle-tested | 15+ years | ~2 years | ~2 years | ~5 years | ~5 years | ~5 years |

### When NOT to use PgQue

- **Sub-10ms latency** -- PgQue is tick-based (1-2s default). Use
  graphile-worker or direct LISTEN/NOTIFY for sub-10ms dispatch.
- **100k+ events/sec sustained** -- at that scale, consider a dedicated broker
  (Kafka, RedPanda).
- **Complex multi-step workflows** -- that's a workflow engine problem. Use
  Temporal, Restate, or Absurd.
- **Single-language team** -- if you're pure Go, River gives better DX. Pure
  Elixir, use Oban. Rails 8, use solid_queue.

---

## Installation

### Requirements

- PostgreSQL 14+
- pg_cron >= 1.5 (optional but recommended; pre-installed on all major managed providers)

### Install

```sql
-- Download pgque-install.sql from the releases page, then:
\i pgque-install.sql

-- Start the ticker and maintenance jobs (requires pg_cron)
select pgque.start();
```

That's it. No `make`, no `CREATE EXTENSION`, no server restart.

### Verify

```sql
select pgque.version();
--  version
-- -----------
--  1.0.0-dev

select * from pgque.status();
--  component  |  status  |  detail
-- ------------+----------+-------------------------------------------
--  pgque      | ok       | 1.0.0-dev
--  ticker     | running  | pg_cron job 1, every 1 second
--  maint      | running  | pg_cron job 2, every 120 seconds
--  postgresql | ok       | 17.4
--  pg_cron    | ok       | 1.6
```

### Without pg_cron

If pg_cron is not available, run the ticker and maintenance manually or via an
external cron job:

```sql
-- Run manually
select pgque.ticker();
select pgque.maint();
```

```bash
# External cron (every second for ticker, every 2 minutes for maintenance)
* * * * * psql -c "select pgque.ticker()" your_db
*/2 * * * * psql -c "select pgque.maint()" your_db
```

### Uninstall

```sql
select pgque.uninstall();  -- stops pg_cron jobs, drops schema
```

---

## Quick Start

```sql
-- 1. Create a queue
select pgque.create_queue('orders');

-- 2. Send a message
select pgque.send('orders', '{"order_id": 42, "amount": 99.95}'::jsonb);
--  send
-- ------
--     1

-- 3. Register a consumer
select pgque.subscribe('orders', 'billing');

-- 4. Run the ticker (pg_cron does this automatically after pgque.start())
select pgque.ticker();

-- 5. Receive messages
select * from pgque.receive('orders', 'billing', 100);
--  msg_id | batch_id |  type   |              payload               | retry_count |         created_at
-- --------+----------+---------+------------------------------------+-------------+----------------------------
--       1 |        1 | default | {"order_id": 42, "amount": 99.95}  |           0 | 2026-04-13 10:00:00.123+00

-- 6. Acknowledge (marks entire batch as processed)
select pgque.ack(1);  -- batch_id from receive()
```

---

## Usage Examples

### Fan-out (Multiple Consumers)

Multiple consumers read the same queue independently. Each consumer gets every
message, tracking its own position.

```sql
select pgque.create_queue('events');
select pgque.subscribe('events', 'analytics');
select pgque.subscribe('events', 'notifications');
select pgque.subscribe('events', 'audit_log');

-- One send reaches all three consumers
select pgque.send('events', '{"user": "alice", "action": "signup"}'::jsonb);
select pgque.ticker();

-- Each consumer processes independently
select * from pgque.receive('events', 'analytics', 100);
-- ... process ...
select pgque.ack(1);

select * from pgque.receive('events', 'notifications', 100);
-- ... process ...
select pgque.ack(2);
```

### Retry and Dead Letter Queue

Failed messages are retried with configurable delays. After exceeding
`max_retries`, they land in the dead letter queue for inspection and replay.

```sql
select pgque.create_queue('jobs');
select pgque.set_queue_config('jobs', 'max_retries', '3');
select pgque.subscribe('jobs', 'worker');

select pgque.send('jobs', 'email', '{"to": "alice@example.com"}'::jsonb);
select pgque.ticker();

-- Receive a batch
select * from pgque.receive('jobs', 'worker', 100);

-- Processing failed -- nack with retry after 60 seconds
select pgque.nack(
  1,                                        -- batch_id
  (select m from pgque.receive('jobs', 'worker', 1) m limit 1),
  interval '60 seconds',                    -- retry delay
  'SMTP timeout'                            -- reason (stored in DLQ if max retries exceeded)
);

-- After max_retries exhausted, inspect the dead letter queue
select * from pgque.dlq_inspect('jobs');
--  id | queue_name | ev_type |          payload           | retry_count |   reason
-- ----+------------+---------+----------------------------+-------------+-------------
--   1 | jobs       | email   | {"to": "alice@example.com"}|           3 | SMTP timeout

-- Replay a dead letter back into the queue
select pgque.dlq_replay(1);

-- Or purge old dead letters
select pgque.dlq_purge('jobs', interval '7 days');
```

### Delayed Delivery

Schedule messages for future delivery. They sit in a holding table until
`maint_deliver_delayed()` moves them to the queue when their time arrives.

```sql
-- Send a reminder 24 hours from now
select pgque.send_at(
  'notifications',
  'reminder',
  '{"user_id": 42, "msg": "Your trial expires tomorrow"}'::jsonb,
  now() + interval '24 hours'
);
```

### Batch Send

Insert many messages in a single transaction for maximum throughput.

```sql
select pgque.send_batch(
  'analytics',
  'pageview',
  array[
    '{"url": "/home", "user": "alice"}'::jsonb,
    '{"url": "/pricing", "user": "bob"}'::jsonb,
    '{"url": "/docs", "user": "carol"}'::jsonb
  ]
);
```

### Exactly-once (Transactional Ack)

Combine message consumption with application writes in a single transaction.
If either fails, both roll back.

```sql
begin;
  -- Receive messages
  select * from pgque.receive('orders', 'processor', 100);

  -- Application write (same transaction)
  insert into processed_orders (order_id, amount)
  select (payload->>'order_id')::int, (payload->>'amount')::numeric
  from pgque.receive('orders', 'processor', 100);

  -- Ack the batch
  select pgque.ack(1);
commit;
-- If commit fails, neither the ack nor the insert persists.
```

### Observability

PgQue provides built-in functions for monitoring queue health and performance.

```sql
-- Queue overview
select * from pgque.queue_stats();
--  queue_name | depth | oldest_msg_age | consumers | events_per_sec | dlq_count
-- ------------+-------+----------------+-----------+----------------+-----------
--  orders     |   142 | 00:00:03       |         2 |           1250 |         0
--  events     |     0 | (null)         |         3 |            430 |         0

-- Consumer lag
select * from pgque.consumer_stats();
--  queue_name | consumer_name |     lag      | pending_events | batch_active
-- ------------+---------------+--------------+----------------+--------------
--  orders     | billing       | 00:00:02     |            142 | f
--  orders     | analytics     | 00:00:45     |          12340 | t

-- Health checks
select * from pgque.queue_health();
--  queue_name | check_name      | status  | detail
-- ------------+-----------------+---------+--------------------------------------
--  orders     | ticker          | ok      | last tick 1s ago
--  orders     | consumer_lag    | warning | analytics lag 45s > rotation_period/2
--  orders     | rotation        | ok      | 3 tables, current: event_1_1

-- Stuck consumers (lag exceeding threshold)
select * from pgque.stuck_consumers(interval '30 seconds');

-- Throughput over time
select * from pgque.throughput('orders', interval '1 hour', interval '5 minutes');

-- OTel-compatible metrics export
select * from pgque.otel_metrics();
```

---

## Client Libraries

### Python

Requires Python 3.10+ and [psycopg](https://www.psycopg.org/) 3.

```python
import psycopg
from pgque import PgqueClient

conn = psycopg.connect("postgresql://localhost/mydb", autocommit=True)
client = PgqueClient(conn)

# Setup
client.create_queue("tasks")
client.subscribe("tasks", "worker")

# Send
event_id = client.send("tasks", {"job": "resize_image", "url": "..."})

# Receive and process
messages = client.receive("tasks", "worker", max_messages=100)
for msg in messages:
    process(msg.payload)
client.ack(messages[0].batch_id)

# Batch send
ids = client.send_batch("tasks", "resize", [
    {"url": "/img/1.jpg"},
    {"url": "/img/2.jpg"},
    {"url": "/img/3.jpg"},
])
```

### Go

Requires Go 1.21+ and [pgx](https://github.com/jackc/pgx) v5.

```go
package main

import (
    "context"
    "fmt"

    pgque "github.com/NikolayS/pgque/clients/go"
)

func main() {
    ctx := context.Background()
    client, _ := pgque.Connect(ctx, "postgresql://localhost/mydb")
    defer client.Close()

    // Setup
    client.CreateQueue(ctx, "tasks")
    client.Subscribe(ctx, "tasks", "worker")

    // Send
    id, _ := client.Send(ctx, "tasks", pgque.Event{
        Type:    "resize",
        Payload: map[string]any{"url": "/img/1.jpg"},
    })
    fmt.Println("event id:", id)

    // Receive and process
    messages, _ := client.Receive(ctx, "tasks", "worker", 100)
    for _, msg := range messages {
        process(msg.Payload)
    }
    client.Ack(ctx, messages[0].BatchID)
}
```

---

## Function Reference

### Queue Management

| Function | Returns | Description |
|---|---|---|
| `pgque.create_queue(queue text)` | `integer` | Create a new queue |
| `pgque.create_queue(queue text, options jsonb)` | `integer` | Create queue with options |
| `pgque.drop_queue(queue text, force bool = false)` | `void` | Drop a queue (force drops even with consumers) |
| `pgque.pause_queue(queue text)` | `void` | Pause ticker for a queue |
| `pgque.resume_queue(queue text)` | `void` | Resume a paused queue |
| `pgque.set_queue_config(queue text, param text, value text)` | `void` | Set queue parameter (e.g., `max_retries`) |

### Producing

| Function | Returns | Description |
|---|---|---|
| `pgque.send(queue text, payload jsonb)` | `bigint` | Send a message (type defaults to `"default"`) |
| `pgque.send(queue text, type text, payload jsonb)` | `bigint` | Send a typed message |
| `pgque.send_batch(queue text, type text, payloads jsonb[])` | `bigint[]` | Send multiple messages atomically |
| `pgque.send_at(queue text, type text, payload jsonb, deliver_at timestamptz)` | `bigint` | Schedule a message for future delivery |

### Consuming

| Function | Returns | Description |
|---|---|---|
| `pgque.subscribe(queue text, consumer text)` | `integer` | Register a consumer on a queue |
| `pgque.unsubscribe(queue text, consumer text)` | `integer` | Remove a consumer from a queue |
| `pgque.receive(queue text, consumer text, max_return int = 100)` | `setof pgque.message` | Receive a batch of messages |
| `pgque.ack(batch_id bigint)` | `integer` | Acknowledge (finish) an entire batch |
| `pgque.nack(batch_id bigint, msg pgque.message, retry_after interval, reason text)` | `integer` | Reject a message; retry or move to DLQ |

### Dead Letter Queue

| Function | Returns | Description |
|---|---|---|
| `pgque.dlq_inspect(queue text, limit_count int = 100)` | `setof pgque.dead_letter` | View dead-lettered messages |
| `pgque.dlq_replay(dead_letter_id bigint)` | `bigint` | Re-insert a dead letter back into its queue |
| `pgque.dlq_replay_all(queue text)` | `integer` | Replay all dead letters for a queue |
| `pgque.dlq_purge(queue text, older_than interval = '30 days')` | `integer` | Delete old dead letters |

### Observability

| Function | Returns | Description |
|---|---|---|
| `pgque.queue_stats()` | `table` | Queue depth, consumer count, throughput, DLQ count |
| `pgque.consumer_stats()` | `table` | Per-consumer lag, pending events, batch status |
| `pgque.queue_health()` | `table` | Diagnostic health checks (ok / warning / critical) |
| `pgque.otel_metrics()` | `table` | OpenTelemetry-compatible metrics export |
| `pgque.stuck_consumers(threshold interval = '1 hour')` | `table` | Consumers exceeding lag threshold |
| `pgque.in_flight(queue text)` | `table` | Currently open (unacked) batches |
| `pgque.throughput(queue text, period interval, bucket_size interval)` | `table` | Historical throughput in time buckets |
| `pgque.error_rate(queue text, period interval, bucket_size interval)` | `table` | Retry and dead letter rates over time |

### Lifecycle

| Function | Returns | Description |
|---|---|---|
| `pgque.start()` | `void` | Create pg_cron jobs for ticker and maintenance |
| `pgque.stop()` | `void` | Remove pg_cron jobs |
| `pgque.status()` | `table` | Show pgque, ticker, maintenance, and PG status |
| `pgque.version()` | `text` | Return installed version |
| `pgque.uninstall()` | `void` | Stop pg_cron jobs and drop the pgque schema |

### Low-level (PgQ Core)

These functions are inherited from PgQ. The modern API (`send`/`receive`/`ack`)
wraps them, but they are available for advanced use.

| Function | Returns | Description |
|---|---|---|
| `pgque.insert_event(queue text, ev_type text, ev_data text)` | `bigint` | Insert a raw event |
| `pgque.insert_event(queue text, ev_type text, ev_data text, ev_extra1..4 text)` | `bigint` | Insert with extra fields |
| `pgque.register_consumer(queue text, consumer text)` | `integer` | Register consumer (low-level) |
| `pgque.unregister_consumer(queue text, consumer text)` | `integer` | Unregister consumer (low-level) |
| `pgque.next_batch(queue text, consumer text)` | `bigint` | Get next batch ID |
| `pgque.get_batch_events(batch_id bigint)` | `setof record` | Read events from a batch |
| `pgque.finish_batch(batch_id bigint)` | `integer` | Finish (ack) a batch |
| `pgque.event_retry(batch_id bigint, event_id bigint, retry_seconds int)` | `integer` | Retry a single event |
| `pgque.ticker()` | `bigint` | Run one tick cycle across all queues |
| `pgque.maint()` | `integer` | Run maintenance (rotation, retry, vacuum, delayed) |

### RBAC Roles

PgQue creates three roles with hierarchical inheritance:

| Role | Permissions |
|---|---|
| `pgque_reader` | `SELECT` on all tables; execute `get_queue_info()`, `get_consumer_info()`, `version()` |
| `pgque_writer` | Inherits reader + `insert_event()`, `register_consumer()`, `next_batch()`, `finish_batch()`, `event_retry()` |
| `pgque_admin` | Inherits writer + all schema privileges, all functions |

---

## Benchmarks

Preliminary results on a laptop (Apple Silicon, 10 cores, 24 GiB RAM,
PostgreSQL 18.3, `synchronous_commit=off` per-session). Full methodology and
raw data: [NikolayS/pgq#1](https://github.com/NikolayS/pgq/issues/1).

| Scenario | Throughput | Per core |
|---|---|---|
| **Single insert/TX, ~100 B, 16 clients** | **85,836 ev/s** | ~8.6k ev/s |
| Batched 100k/TX, ~100 B, 1 client | 80,515 ev/s | ~8.1k ev/s |
| Batched 100k/TX, ~2 KiB, 1 client | 48,899 ev/s (91.5 MiB/s) | ~4.9k ev/s |
| Consumer read, 100k batch, ~100 B | ~2.4M ev/s | ~240k ev/s |
| Consumer read, 100k batch, ~2 KiB | ~305k ev/s (568 MiB/s) | ~30.5k ev/s |

All PL/pgSQL mode (no C extension). Results are preliminary -- server-grade
NVMe hardware would improve throughput (57% of time was spent on I/O writes).

Key takeaways:
- **Zero dead tuples** under sustained load -- verified by `dead_tuple_check.sql`
- **Consumer is never the bottleneck** -- reading is 3-6x faster than writing
- **Tuning matters more than C vs. PL/pgSQL** -- PL/pgSQL tuned (86k ev/s) beats C untuned (52k ev/s)
- **Batching matters most** -- 1000 inserts/TX reaches 417k ev/s (3.6x over single-insert)
- **Sustained throughput is stable** -- 30-minute sustained run showed no degradation across 70 checkpoint cycles

---

## Managed Provider Compatibility

PgQue requires no C extensions and works on any PostgreSQL 14+ instance.
pg_cron is optional but recommended for automated ticker/maintenance.

| Provider | PgQue | pg_cron available |
|---|---|---|
| Amazon RDS | Yes | Yes |
| Amazon Aurora | Yes | Yes |
| Google Cloud SQL | Yes | Yes |
| Google AlloyDB | Yes | Yes |
| Azure Database for PostgreSQL | Yes | Yes |
| Supabase | Yes | Yes |
| Neon | Yes | Yes |
| Crunchy Bridge | Yes | Yes |
| Timescale | Yes | Yes |
| Aiven | Yes | Yes |
| Self-hosted | Yes | Yes |

---

## Roadmap

| Sprint | Scope | Status |
|---|---|---|
| Sprint 1 | **Core repackaging** -- PgQ rename, PG14+ modernization, single-file install, RBAC | Done |
| Sprint 2 | **pg_cron lifecycle** -- `start()`, `stop()`, `status()`, LISTEN/NOTIFY | Done |
| Sprint 3 | **Modern API** -- `send`/`receive`/`ack`/`nack`, DLQ, delayed delivery | Done |
| Sprint 4 | **Observability** -- `queue_stats`, `queue_health`, OTel metrics | Done |
| Sprint 5 | **Client libraries** -- Python (psycopg 3), Go (pgx v5) | Done |
| Sprint 6 | **Testing and docs** -- benchmarks, CI (PG 14-17 matrix), README | In progress |
| v2 | Node.js and Ruby clients, CLI, OTel export, advanced patterns | Planned |

See [SPECx.md](blueprints/SPECx.md) for the full specification and
implementation plan.

---

## License

Apache-2.0. See [LICENSE](LICENSE).

PgQue includes code derived from [PgQ](https://github.com/pgq/pgq)
(ISC license, copyright Marko Kreen, Skype Technologies OU).
See [NOTICE](NOTICE).
