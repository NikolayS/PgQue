---
title: PgQue docs
description: Tutorial, reference, examples, monitoring, and concepts for PgQue — the zero-bloat Postgres queue.
---

Short docs for users, plus a contributor primer.

## Get started

- **[Tutorial](tutorial.md)** — a hands-on walkthrough. Send, tick, receive,
  retry, DLQ, observability. Start here if you are new.

## Guides

- **[Installation and operations](installation.md)** — install, ticking,
  role grants, uninstall, and troubleshooting.
- **[Examples](examples.md)** — short patterns: fan-out, exactly-once,
  batch send, recurring jobs, DLQ inspection, and
  [cooperative consumers / subconsumers](examples.md#cooperative-consumers--subconsumers-experimental)
  (experimental).
- **[Monitoring and health](monitoring.md)** — queue, consumer, and batch
  introspection; lag and pending-event signals; what to alert on.

## Reference

- **[Function reference](reference.md)** — every function, return type, and
  role grant in the default install.

## Explanation

- **[Latency and tick tuning](latency-and-tuning.md)** — how ticks shape
  end-to-end delivery latency, choosing `tick_period_ms`, and idle backoff.
- **[Concepts and heritage](concepts.md)** — the core vocabulary (event,
  batch, tick, rotation, ticker rule) and where PgQue comes from.

For the full specification and implementation plan, see
[`blueprints/SPECx.md`](https://github.com/NikolayS/pgque/blob/main/blueprints/SPECx.md).
For what ships in the default install vs experimental, see
[`blueprints/PHASES.md`](https://github.com/NikolayS/pgque/blob/main/blueprints/PHASES.md).
