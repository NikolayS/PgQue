# PgQue -- PgQ Universal Edition

**Zero-bloat PostgreSQL queue. No extensions. No daemons. One SQL file.**

PgQue is a repackaging of [PgQ](https://github.com/pgq/pgq) -- the
battle-tested queue system that ran at Skype/Microsoft scale for 15+ years --
into a modern, extension-free system that works on any managed PostgreSQL
provider.

## Why PgQue

Every other PostgreSQL queue uses `SKIP LOCKED` + `DELETE`, which creates dead
tuples. Under sustained load, VACUUM can't keep up, indexes bloat, and
throughput degrades. PgQue is **structurally immune** -- it uses TRUNCATE-based
table rotation instead of per-row deletion. Zero dead tuples, ever.

- **No C extensions** -- pure SQL/PL/pgSQL, installs with `\i pgque-install.sql`
- **No external daemons** -- pg_cron replaces the old `pgqd` ticker
- **Works everywhere** -- RDS, Aurora, AlloyDB, Cloud SQL, Supabase, Neon, Crunchy Bridge
- **Language-agnostic** -- SQL API works from any language; client libraries for Python and Go
- **Modern API** -- `pgque.send()` / `pgque.receive()` / `pgque.ack()` / `pgque.nack()`
- **Built-in DLQ** -- dead letter queue with replay
- **Observability** -- `pgque.queue_health()`, `pgque.queue_stats()`, OTel-compatible metrics

## Quick Start

```sql
-- Install
\i pgque-install.sql
SELECT pgque.start();  -- creates pg_cron ticker + maintenance jobs

-- Create a queue
SELECT pgque.create_queue('orders');

-- Produce
SELECT pgque.send('orders', '{"order_id": 42}'::jsonb);

-- Consume
SELECT pgque.subscribe('orders', 'processor');
SELECT * FROM pgque.receive('orders', 'processor', 100);
-- ... process messages ...
SELECT pgque.ack(batch_id);
```

## Architecture

PgQue uses PgQ's proven architecture:

- **Snapshot-based batch isolation** -- each batch contains exactly the events
  committed between two ticks. No gaps, no duplicates.
- **3-table TRUNCATE rotation** -- event tables rotate via TRUNCATE (DDL, not DML).
  Zero dead tuples, zero VACUUM pressure.
- **Multiple independent consumers** -- each consumer tracks its own position.
  One queue, many readers.

See [SPECx.md](blueprints/SPECx.md) for the full specification.

## Requirements

- PostgreSQL 14+
- pg_cron >= 1.5 (optional but recommended; available on all major managed providers)

## Status

Under active development. See [SPECx.md](blueprints/SPECx.md) for the
implementation plan and current progress.

## License

Apache-2.0. See [LICENSE](LICENSE).

PgQue includes code derived from PgQ (ISC license). See [NOTICE](NOTICE).
