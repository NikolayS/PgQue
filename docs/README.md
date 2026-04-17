# PgQue docs

Five short docs for users, plus one contributor primer.

## For users

- **[Tutorial](tutorial.md)** — a hands-on walkthrough. Send, tick, receive,
  retry, DLQ, observability. Start here if you are new.
- **[Reference](reference.md)** — every function, return type, and role
  grant in the v0.1 default install.
- **[Examples](examples.md)** — short patterns: fan-out, exactly-once,
  batch send, recurring jobs, DLQ inspection.
- **[Benchmarks](benchmarks.md)** — current throughput numbers and
  methodology.
- **[PgQ concepts](pgq-concepts.md)** — glossary of the core vocabulary
  (event, batch, tick, rotation, ticker contract). Useful alongside the
  tutorial.

## For contributors

- **[PgQ history](pgq-history.md)** — short timeline from Skype to PgQue.

For the full specification and implementation plan, see
[`blueprints/SPECx.md`](../blueprints/SPECx.md). For what ships in v0.1 vs
experimental, see [`blueprints/PHASES.md`](../blueprints/PHASES.md).
