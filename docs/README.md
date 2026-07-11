---
title: PgQue docs
description: Tutorial, reference, examples, monitoring, and concepts for PgQue — the zero-bloat Postgres queue.
---

Short docs for users, plus a contributor primer.

> **Build contract.** The documentation on the `main` branch and pgque.dev
> describes the in-development SQL in `devel/sql/`. For the latest stable
> build, open the [latest stable release](https://github.com/NikolayS/pgque/releases/latest)
> and use its tagged README and `sql/pgque.sql`. A release promotion copies the
> tested artifacts into `sql/`, switches documentation install and source paths
> from `devel/sql/` to `sql/` (with source links pinned to the release tag), and
> removes this development-build notice before tagging.

## Get started

- **[Tutorial](tutorial.md)** — a hands-on walkthrough. Send, tick, receive,
  retry, DLQ, observability. Start here if you are new.

## Guides

- **[Installation and operations](installation.md)** — install, ticking,
  role grants, uninstall, and troubleshooting.
- **[Producer idempotency](producer-idempotency.md)** — queue-scoped keys,
  TTL windows, retry behavior, and maintenance.
- **[Partition keys](partition-keys.md)** — atomic slot setup, worker leases,
  ordered routing, poolers, operational limits, and recovery.
- **[Examples](examples.md)** — short patterns: fan-out, exactly-once,
  idempotent sends, partition workers, batch send, recurring jobs, DLQ
  inspection, and
  [cooperative consumers / subconsumers](examples.md#cooperative-consumers--subconsumers-experimental)
  (experimental).
- **[Monitoring and health](monitoring.md)** — queue, consumer, and batch
  introspection; lag and pending-event signals; what to alert on.

## Reference

- **[Function reference](reference.md)** — the public SQL surface, return
  types, behavior, and role grants in the development install.

## Explanation

- **[Latency and tick tuning](latency-and-tuning.md)** — how ticks shape
  end-to-end delivery latency, choosing `tick_period_ms`, and idle backoff.
- **[Concepts and heritage](concepts.md)** — the core vocabulary (event,
  batch, tick, rotation, ticker rule) and where PgQue comes from.

For the full specification and implementation plan, see
[`blueprints/SPECx.md`](https://github.com/NikolayS/pgque/blob/main/blueprints/SPECx.md).
For what ships in the default install vs experimental, see
[`blueprints/PHASES.md`](https://github.com/NikolayS/pgque/blob/main/blueprints/PHASES.md).
