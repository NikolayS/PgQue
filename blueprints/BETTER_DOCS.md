# BETTER_DOCS.md — design doc for logres documentation

> **Status (2026-04-17): superseded for v0.1 by #59.** The v0.1 docs
> shipped a **flat `docs/` layout** — `tutorial.md`, `reference.md`,
> `examples.md`, `benchmarks.md`, `pgq-concepts.md`, `pgq-history.md` —
> not the full Diátaxis tree with per-quadrant subdirectories this doc
> originally proposed. The four-quadrant tree (tutorials / how-tos /
> reference / explanation + ten how-to files) remains a roadmap target
> for a later release when the content base justifies the split. For
> what ships today see [PHASES.md](PHASES.md) and the
> [README](../README.md). This file is preserved as the thinking
> behind the eventual v1.0 layout.

## Purpose

Define what logres documentation should look like end-to-end: what stays in
`README.md`, what moves into a new `docs/` tree, how the tree is organized, and
what each file is for. The goal is that a new user can land on the README,
understand whether logres fits their problem within two minutes, and then follow
clear paths into deeper material — without the README itself trying to be a
tutorial, a reference, and an explainer all at once.

This document is the design. The actual `docs/` files are written in follow-up
work, in the order set out in section 8.

## 1. Research base

The plan below is grounded in:

- **PgQ** ([github.com/pgq/pgq](https://github.com/pgq/pgq)) — source code,
  inline comments, regression tests, README, and the full commit history
  (~80 commits, 2016–2025). PgQ is the engine logres inherits.
- **logres** — current `README.md`, `sql/logres.sql`, experimental SQL,
  `tests/`, `blueprints/SPECx.md`, `blueprints/PHASES.md`, and the project's
  commit history (including the recent `xid8` fix and the ticker-requirement
  clarification in `02f649d`).
- **Diátaxis** ([documentation.divio.com](https://documentation.divio.com/)) —
  four-quadrant model: tutorials, how-to guides, reference, explanation.
- **postgres-ai shared rules**
  ([gitlab.com/postgres-ai/rules](https://gitlab.com/postgres-ai/rules/-/tree/main/rules))
  — referenced from `CLAUDE.md`. The writing rules in particular shape tone,
  terminology, and platform-neutrality across every doc.

## 2. What end users most need to understand

These are ordered by how much pain a user feels when they miss them. They are
the *content priorities*, independent of which file a given piece ends up in.

### Tier 1 — users will fail without this

1. **The mental model.** logres is a snapshot-based shared event log with
   per-consumer position tracking. It is not `SKIP LOCKED`, not row-claiming,
   not pgmq-style. Events become visible in batches between ticks, not
   individually. Latency is seconds by design. Fan-out is native — multiple
   consumers each track their own position in the same log; events are not
   duplicated per consumer.
2. **The ticker.** Without ticking, no events are ever visible to consumers.
   This is the single largest source of "it is not working" reports for any
   PgQ-derived system. The README already calls this out (added in
   `02f649d`); the broader docs need to keep reinforcing it. Two ticking
   modes: `logres.start()` with `pg_cron`, or manual `logres.ticker()` from an
   external scheduler. Tuning knobs: `ticker_max_lag` (3s),
   `ticker_max_count` (500), `ticker_idle_period` (1m).
3. **The consumer loop.** `receive` returns the current batch. If `ack` is
   not called, the same batch is returned forever. Correct loop:
   `receive` → process → `ack` (or `nack` per event) → repeat. `receive`
   returns empty when no batch is ready; sleep and retry.
4. **The happy path.** Six lines that produce visible output:
   `create_queue` → `subscribe` → `send` → `force_tick` + `ticker` →
   `receive` → `ack`.

### Tier 2 — users will hit this within the first week

5. **Maintenance shape.** Three separate operations, each on its own cadence:
   ticker (~2s), `maint` (~30s), rotate-step-2 (~10s). Step 1 and step 2 of
   rotation must run in separate transactions — combining them defeats the
   global-visibility safety mechanism and event tables grow without bound.
   `logres.start()` configures all three when `pg_cron` is available;
   otherwise the user runs them.
6. **Retry mechanics.** `nack(batch_id, msg, '60 seconds')` schedules retry.
   `retry_count` increments per attempt. After `max_retries` (default 5),
   the message routes to the dead letter queue. Retries depend on `maint`
   running.
7. **Message shape.** Fields users see on `logres.message`: `msg_id`,
   `batch_id`, `type`, `payload`, `retry_count` (NULL on first delivery),
   `created_at`, and four `extra` text columns.
8. **Three-table rotation — why bloat is zero.** Each queue has three
   inheritance child tables. Inserts go to the hot table. The oldest is
   `TRUNCATE`d after every consumer has passed it. A consumer that stops
   consuming blocks rotation, and tables grow. Monitor consumer lag.
9. **Exactly-once pattern.** Wrap `receive` + business writes + `ack` in one
   transaction. Rollback rolls back all three atomically. This is the
   property that external brokers structurally cannot offer.

### Tier 3 — users will need this in production

10. **Configuration reference.** Per-queue: `ticker_max_count`,
    `ticker_max_lag`, `ticker_idle_period`, `rotation_period`, `max_retries`,
    `ticker_paused`, `external_ticker`. Set via
    `logres.set_queue_config(queue, key, value)`.
11. **Monitoring.** `logres.status()`, `logres.get_queue_info()`,
    `logres.get_consumer_info()`. What each column means. What to alert on:
    ticker lag, consumer lag, stuck consumers, DLQ depth.
12. **Roles.** `logres_reader`, `logres_writer`, `logres_admin`. What each can
    do. Uninstall requires superuser by design.
13. **Dead letter queue.** `dlq_inspect`, `dlq_replay`, `dlq_replay_all`,
    `dlq_purge`. Currently in `sql/experimental/`; document the surface that
    is in the default install and clearly mark experimental pieces.
14. **Managed Postgres compatibility.** Pure SQL install, no C extensions,
    no daemon. `pg_cron` availability varies; document the manual fallback.

### Tier 4 — power users and edge cases

15. **Common patterns.** Fan-out, batch send, delayed delivery (experimental),
    cursor-based streaming for large batches, custom batch timing via
    `next_batch_custom`.
16. **PgQ-native primitives.** `insert_event`, `next_batch`,
    `next_batch_info`, `next_batch_custom`, `get_batch_events`,
    `finish_batch`, `event_retry`. For users who need cursor streaming,
    custom batch timing, or direct event field control.
17. **Troubleshooting.** Symptom → cause → fix table for the recurring
    failure modes (no events returned, same batch repeatedly, retry events
    not reappearing, queue growing unbounded, `force_tick` apparently
    no-op).

### Hard-won lessons from PgQ + logres commit history

These are not a tier — they are facts that should land in whichever doc
covers the relevant area. They are easy to miss without reading the history.

- **Origin and intent.** PgQ was built at Skype (~2007) by Marko Kreen.
  The PL/pgSQL-only fallback that logres is built on was added explicitly
  because users asked for a way to run PgQ in restricted/hosted environments
  where C extensions are not allowed. That is logres's entire reason to exist.
- **`xid8` matters.** PG14+ uses `xid8` for transaction IDs. Mismatches
  silently break batch event SQL and rotation. logres handles this; users
  extending the schema with custom SQL must be aware.
- **Rotation is two transactions on purpose.** Step 1 advances the table
  pointer; step 2 records a `txid` proving the switch is globally visible.
  Combining them defeats the safety mechanism and the queue grows without
  bound. Observed in the wild before the fix.
- **`force_tick` alone is not enough.** It only bumps the event sequence.
  `ticker()` must run after it to actually create the tick snapshot.
- **`maint` uses `pg_try_advisory_xact_lock`, not session-level.** Session
  locks persist in `pg_cron`'s connection pool after failures; transaction
  locks do not.
- **`LISTEN`/`NOTIFY` is a hint, not delivery.** The ticker emits
  `pg_notify` to wake consumers faster, but notifications are lossy. Always
  keep a polling fallback.
- **`INHERITS` not native partitioning.** PgQ uses inheritance for the
  three rotating tables. Native partitioning is unnecessary for a fixed
  three-child rotation and `TRUNCATE` of the oldest. Document this so users
  do not "modernize" the schema and break the rotation safety properties.

## 3. Writing rules

logres inherits the postgres-ai writing rules through `CLAUDE.md`. The ones
most likely to be violated when writing user-facing docs:

- **"Postgres" not "PostgreSQL"**, except when technical accuracy demands it
  (e.g. extension names, version banners).
- **Sentence-style capitalization** for headings — "Database migration
  testing", not "Database Migration Testing".
- **No emojis, no emotional language.** Objective, data-driven tone. No
  "amazing", "unfortunately", "awesome", "battle-tested" (the README uses
  "battle-tested" today; that is the boundary case — usable when it is
  factually descriptive, not as a marketing flourish).
- **Em dashes with spaces**: `word — word`.
- **Platform neutrality.** Never say "RDS does not support X" or "Supabase
  is limited to Y". Say "this setting is not configurable on managed
  Postgres" or "managed services typically do not allow X". This applies
  hardest to `docs/howto/install.md` and `docs/explanation/tradeoffs.md`.
- **Binary units** (KiB, MiB, GiB, TiB) for all sizes and rates in prose.
  Exception: PG config values use PG's native format (`shared_buffers =
  '2GB'`).
- **psql examples** must include `--no-psqlrc` and `PAGER=cat` so output is
  predictable for readers copy-pasting commands.
- **SQL examples** must use lowercase keywords, `snake_case` identifiers,
  and the formatting from `db-sql-style-guide.mdc` (root keywords on their
  own line, multi-arg calls with one arg per line). This already matches
  the style in `sql/logres.sql`.

It is worth adding a short writing-rules section to `CLAUDE.md` listing the
above bullets. The full URL is already there; an inline checklist makes
violations easier to catch in review.

## 4. README.md — what stays, what moves out

The current README is ~450 lines and tries to be landing page, quick
reference, and explainer simultaneously. The job of the README is to answer
*what is this, why should I care, and how do I try it* in under two minutes.
Everything else moves into `docs/`.

### Stays in README

- Title, tagline, badges.
- One-paragraph framing (PgQ heritage, what logres repackages).
- **Why logres** — five bullets, current content is good.
- **Latency trade-off** — short, currently good.
- **Comparison table** — keep. It is a useful decision aid in the landing
  page itself, and rewriting it as a how-to removes the at-a-glance value.
- **Installation** — slim to: `\i sql/logres.sql`, `select logres.start()`,
  one-line note that without `pg_cron` you call `ticker()` and `maint()`
  yourself. Detailed install guidance moves to `docs/howto/install.md`.
- **Project status** — keep, currently short.
- **Quick start** — keep. The current six-step flow is exactly the right
  length. The `02f649d` follow-up note about `force_tick` belongs here.
- **Client libraries** — keep brief mentions and links. The current Python
  and Go snippets are fine.
- **License + heritage** — keep.
- **Links to `docs/`** — new section near the top: one link per Diátaxis
  quadrant with a one-line description of each.

### Moves out of README

- **Usage examples section** → `docs/howto/` (one file per pattern).
- **Function reference table** → `docs/reference/api.md`. The README can
  keep a one-line "see reference" link and drop the table.
- **Benchmarks section** → user already noted performance gets its own
  document. Move to `docs/explanation/performance.md`. While moving:
  - normalize units across every row (every row reports per-second and
    where applicable per-second-bandwidth in MiB/s)
  - normalize the client/concurrency information across every row (the
    current table only mentions "16 clients" on the first row; either show
    that column for every row or drop it)
  - state vCPU count, RAM, PG version, and `synchronous_commit` setting
    once at the top of the table, not buried in a single row
  - link to the full methodology issue
- **Architecture bullet list** → trim README to one paragraph + link to
  `docs/explanation/architecture.md`. The bullets are useful but they
  belong in the explanation.

This trims the README to roughly 200–250 lines, with everything still
discoverable through the new `docs/` index near the top.

## 5. docs/ tree — the four Diátaxis quadrants

Proposed layout:

```
docs/
  tutorial.md
  howto/
    install.md
    exactly-once.md
    fan-out.md
    retry-and-dlq.md
    delayed-delivery.md
    batch-loading.md
    monitoring.md
    manual-maintenance.md
    tuning-latency.md
    uninstall.md
  reference/
    api.md
    configuration.md
    schema.md
    roles.md
    message-format.md
    maintenance.md
    status-output.md
  explanation/
    architecture.md
    exactly-once-semantics.md
    bloat-free-design.md
    pgq-heritage.md
    tradeoffs.md
    performance.md
```

### 5.1 Tutorial — `docs/tutorial.md`

A single guided walkthrough for a newcomer who has never used logres. It
teaches the mental model by making the user *experience* it, not by
describing it.

Concrete scenario: an order-processing queue. Steps:

1. Install (`\i sql/logres.sql`).
2. `create_queue('orders')`, `subscribe('orders', 'worker')`.
3. `send` an order. Try `receive` immediately — get nothing. This is the
   teaching moment for the ticker.
4. `force_tick('orders')` then `ticker()`. `receive` again — get the batch.
   `ack`.
5. Send a "bad" order. `nack` it with a 5-second delay. Watch it reappear.
6. Drive nack-loop to exhaustion. See it land in the DLQ.
7. `logres.status()` and `logres.get_consumer_info()`.
8. `logres.start()` to switch to `pg_cron`-driven operation.
9. Tear down.

Every step shows expected output. No detours. No reference tables.

### 5.2 How-to guides — `docs/howto/`

Each file solves one task. No conceptual material — that lives in the
explanations. Each file is short (target ~100 lines), starts with the goal,
ends with the result.

| File | Question |
|---|---|
| `install.md` | How do I install on managed Postgres (with and without `pg_cron`)? |
| `exactly-once.md` | How do I process events exactly once? |
| `fan-out.md` | How do I deliver the same events to several independent consumers? |
| `retry-and-dlq.md` | How do I configure retries and inspect/replay the DLQ? |
| `delayed-delivery.md` | How do I schedule events for future delivery? |
| `batch-loading.md` | How do I load events efficiently in bulk? |
| `monitoring.md` | How do I set up alerts on ticker lag, consumer lag, DLQ depth? |
| `manual-maintenance.md` | How do I run logres without `pg_cron`? |
| `tuning-latency.md` | How do I reduce batch latency? |
| `uninstall.md` | How do I remove logres cleanly? |

### 5.3 Reference — `docs/reference/`

Dry, complete, alphabetical or grouped where that helps lookup. No
narrative. Someone should find any function/parameter/column in under ten
seconds.

| File | Content |
|---|---|
| `api.md` | Every public function: signature, parameters, return type, one-line example. Two sections: modern API (`send`, `receive`, `ack`, `nack`, `subscribe`, `unsubscribe`, `start`, `stop`, `status`); PgQ primitives (`insert_event`, `next_batch`, `next_batch_info`, `next_batch_custom`, `get_batch_events`, `finish_batch`, `event_retry`). |
| `configuration.md` | Every `set_queue_config` key: name, type, default, valid range, what it controls. Plus the `logres.config` singleton. |
| `schema.md` | Every table and column: `queue`, `consumer`, `subscription`, `tick`, event template, `retry_queue`, `dead_letter`, delayed events, `config`. |
| `roles.md` | Role hierarchy, exact grants per role, how to assign. |
| `message-format.md` | The `logres.message` composite type, every field, when each is NULL, how `retry_count` evolves. |
| `maintenance.md` | What ticker, `maint`, `rotate_step_1`, `rotate_step_2` do; cadences; statement timeouts; `pg_cron` job names. |
| `status-output.md` | Column-by-column meaning of `logres.status()`, `get_queue_info()`, `get_consumer_info()`, `get_batch_info()`. |

### 5.4 Explanation — `docs/explanation/`

The "why" behind the design. No instructions, no tables of parameters.
These are essays.

| File | Topic |
|---|---|
| `architecture.md` | Snapshot-based batching vs row claiming. Three-table rotation. Why `TRUNCATE` not `DELETE`. Why `INHERITS` not native partitioning. Tick/snapshot/batch relationship. How `batch_event_sql` uses transaction visibility. Why latency is seconds. |
| `exactly-once-semantics.md` | Why wrapping `receive` + business writes + `ack` in one transaction yields exactly-once. The at-least-once default outside of that wrapping. Why external brokers cannot offer this property. |
| `bloat-free-design.md` | The dead-tuple problem in `SKIP LOCKED` queues (with the Brandur Leach analysis already linked from the README). How rotation solves it. Why a stopped consumer blocks rotation and causes growth. |
| `pgq-heritage.md` | PgQ's history (Skype, ~2007, Marko Kreen). What logres changes (schema rename, PG14+ APIs, `xid8`, `pg_cron`, `LISTEN`/`NOTIFY`, DLQ, roles). What it preserves unchanged (the entire batch/tick/rotation/consumer engine). The PL/pgSQL-only path that made logres possible. |
| `tradeoffs.md` | When to choose logres vs `pgmq` vs `SKIP LOCKED` vs Redis vs Kafka. Honest about limitations: second-level latency, no priorities, no worker lifecycle. Where logres wins: durability, zero bloat, fan-out, exactly-once, managed-Postgres compatibility. Platform-neutral framing throughout. |
| `performance.md` | The full benchmark methodology, results table, interpretation. Owns this material so the README can stay short. |

## 6. Cross-cutting principles

- **Do not mix quadrants.** Tutorials are not reference. How-tos are not
  explanation. Reference is not narrative. Each kind serves a different
  reader in a different mode.
- **Every SQL example follows the SQL style guide** in `CLAUDE.md` and the
  postgres-ai rules. This already matches `sql/logres.sql`.
- **Every shell example uses** `set -Eeuo pipefail` and double-quoted
  expansions.
- **Every psql example uses** `--no-psqlrc` and `PAGER=cat`.
- **Reinforce the ticker** in tutorial, install, manual-maintenance,
  monitoring, and troubleshooting. It is the single most common failure
  mode and costs nothing to mention again.
- **Mark experimental clearly.** Anything in `sql/experimental/` (delayed
  delivery, observability, DLQ tooling) gets an "Experimental" callout in
  the relevant doc and is not promoted in the tutorial. `PHASES.md` is the
  source of truth for what is in the default install in v0.1.
- **Cross-link generously between quadrants** but keep the direction of
  flow sensible: tutorial → how-tos for next-step tasks; how-tos →
  reference for parameter detail; reference → explanation for "why this
  default"; explanation → reference for "what to call".

## 7. Effects on `CLAUDE.md`

Two small additions worth making in a separate small PR before the docs
work starts:

1. A short "Writing rules" section in `CLAUDE.md` listing the bullets
   from section 3 of this doc. The full URL stays.
2. A "Documentation layout" pointer naming the four Diátaxis directories
   under `docs/` and saying which kind of content goes where. This catches
   drift early — without it, future contributors will reach for the README
   to add anything user-facing.

## 8. Suggested order of work

Prioritized so each PR is reviewable on its own and unblocks the next one.

1. **CLAUDE.md updates** — writing rules checklist + docs layout pointer.
   Tiny PR. Lands first so all subsequent docs PRs can be reviewed against
   the rules.
2. **README slim-down** — move benchmarks, function reference, usage
   examples, and architecture detail into placeholder files under `docs/`.
   Add the `docs/` index section near the top of the README. The `docs/`
   files at this point can be stubs that contain the moved material as-is;
   subsequent PRs polish each file.
3. **Tutorial** (`docs/tutorial.md`). The single highest-leverage doc for
   new users.
4. **Reference fill-in.** `api.md`, `configuration.md`, `message-format.md`,
   `maintenance.md`, `status-output.md`. These are mechanical writes from
   the source SQL, so they can be batched.
5. **Explanation: `architecture.md` + `bloat-free-design.md` + `pgq-heritage.md`.**
   These ground the rest of the docs and link in well from the README.
6. **How-to guides** in priority order: `install.md`,
   `manual-maintenance.md`, `exactly-once.md`, `monitoring.md`,
   `retry-and-dlq.md`, `tuning-latency.md`, `fan-out.md`,
   `batch-loading.md`, `delayed-delivery.md`, `uninstall.md`.
7. **Remaining explanations:** `exactly-once-semantics.md`, `tradeoffs.md`,
   `performance.md`.
8. **Reference: `schema.md`, `roles.md`** — last because they are the
   driest and least-frequently-read.

## 9. Out of scope for this design doc

- Client library docs (`logres-py`, `logres-go`, CLI). Each gets its own
  `docs/` tree under its own subdirectory or repo when those projects
  mature. The logres core docs link out, they do not host.
- Internal contributor docs. `SPECx.md`, `PHASES.md`, and this file already
  cover that audience.
- Marketing site or hosted docs. This plan covers in-repo Markdown only.
  A docs site (mkdocs, docusaurus) can be layered on top later without
  changing the source structure.
- Translations.

## 10. Open questions

- Should the `docs/` index live as a section in the README, as a separate
  `docs/README.md`, or both? Recommendation: both. README links to four
  entry points (tutorial, how-tos index, reference index, explanation
  index). Each index page is a `README.md` in its directory.
- Where does the recurring-jobs-with-`pg_cron` example go? It is currently
  in the README under usage examples. Recommendation: short mention stays
  in the README quick-start area as a teaser, full pattern moves to
  `docs/howto/` as a small file or as a section in `manual-maintenance.md`.
- How much of `sql/experimental/` to document. Recommendation: document
  the surface that ships in `sql/logres.sql` first, add experimental
  surfaces only when they are promoted into the default install per
  `PHASES.md`.
