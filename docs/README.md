# PgQue docs

Short docs for users, plus a contributor primer.

## For users

- **[Tutorial](tutorial.md)** — a hands-on walkthrough. Send, tick, receive,
  retry, DLQ, observability. Start here if you are new.
- **[Reference](reference.md)** — every function, return type, and role
  grant in the default install.
- **[Upgrading](upgrading.md)** — supported SQL-file upgrade procedure,
  including v0.1.0 → v0.2.0.
- **[Examples](examples.md)** — short patterns: fan-out, exactly-once,
  batch send, recurring jobs, DLQ inspection.
- **[Benchmarks](benchmarks.md)** — current throughput numbers and
  methodology.
- **[Three latencies](three-latencies.md)** — producer latency, subscriber
  latency, and end-to-end delivery explained; tick-cadence trade-off table;
  comparison with UPDATE/DELETE-based designs.
- **[Tick frequency tuning](tick-frequency.md)** — choosing `tick_period_ms`,
  WAL planning numbers, idle backoff, and pg_cron logging caveats.
- **[PgQ concepts](pgq-concepts.md)** — glossary of the core vocabulary
  (event, batch, tick, rotation, ticker rule). Useful alongside the
  tutorial.

## For contributors

- **[PgQ history](pgq-history.md)** — short timeline from Skype to PgQue.

For the full specification and implementation plan, see
[`blueprints/SPECx.md`](../blueprints/SPECx.md). For what ships in the
default install vs experimental, see
[`blueprints/PHASES.md`](../blueprints/PHASES.md).
