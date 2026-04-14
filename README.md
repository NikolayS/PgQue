# PgQue – PgQ, universal edition

**Zero-bloat Postgres queue for heavy load. No extensions. No daemons. One SQL file.**

PgQue brings back one of the smartest queue architectures ever built for
Postgres and makes it work on modern managed platforms.

PgQ solved a hard problem unusually well: high-load queueing inside Postgres
without the long-term decay caused by row-by-row churn. Its hot path is based
on snapshot-bounded batches and TRUNCATE-based rotation, so it avoids the dead-
tuple treadmill that makes many queue tables rot over time.

What held PgQ back was not the architecture. It was packaging: custom C pieces,
an external daemon, and poor fit for managed Postgres.

PgQue fixes that completely and adds useful extras on top.

It is built for teams that want:

- managed Postgres compatibility
- a language-agnostic SQL API
- stable sustained-load behavior
- transactional queueing inside a truly ACID system

It is **not** the right tool for:

- sub-10ms dispatch latency
- workflow orchestration
- pretending batch semantics are per-message visibility semantics

## Why PgQue

Most Postgres queues create churn as they work: rows are claimed, updated,
deleted, vacuumed, and eventually degraded under sustained load.

PgQue takes a different path. It keeps the zero-bloat hot path of PgQ and the
core benefits of Postgres itself: transactional enqueue, transactional
consume-and-commit patterns, strong consistency, and durability.

## Quick start

```sql
-- Install
\i pgque-install.sql
select pgque.start();

-- Create a queue
select pgque.create_queue('orders');

-- Produce
select pgque.send('orders', '{"order_id": 42}'::jsonb);

-- Consume
select pgque.subscribe('orders', 'processor');
select * from pgque.receive('orders', 'processor', 100);
-- ... process messages ...
select pgque.ack(batch_id);
```

**Important:** `pgque.receive(queue, consumer, n)` limits how many rows are
returned to the caller, but `pgque.ack(batch_id)` finishes the **entire batch**
behind that `batch_id`. PgQue is batch-oriented.

If `pg_cron` is unavailable or unsuitable, call `pgque.ticker()` and
`pgque.maint()` from an external scheduler.

## Architecture

- **Snapshot-based batches** -- each batch contains events committed between two ticks
- **3-table TRUNCATE rotation** -- no hot-path dead tuples from normal event consumption
- **Independent consumers** -- one queue, many readers

## Managed Postgres platforms

PgQue targets major managed Postgres platforms including:

- RDS
- Aurora
- AlloyDB
- Cloud SQL
- Supabase
- Neon
- Crunchy Bridge

## Requirements

- PostgreSQL 14+
- `pg_cron >= 1.5` optional but recommended

## Documentation

- [Semantics](docs/semantics.md)
- [Architecture](docs/architecture.md)
- [Comparison notes](docs/comparison.md)
- [Observability](docs/observability.md)
- [Benchmarks](docs/benchmarks.md)
- [Full specification](blueprints/SPECx.md)

## Status

Under active development.

## License

Apache-2.0. See [LICENSE](LICENSE).

PgQue includes code derived from PgQ (ISC license). See [NOTICE](NOTICE).
