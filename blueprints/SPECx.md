# PgQue -- PgQ Universal Edition

- **Version:** 1.1
- **Date:** 2026-04-12
- **License:** Apache-2.0
- **Status:** Approved — implementation-ready
- **Companion:** SPEC.md (v0.7.0-draft) contains the deep architectural analysis of PgQ internals -- rotation mechanics, snapshot isolation, batch_event_sql algorithm, dual-filter optimization, INHERITS justification, rotation state machine, subtransaction caveats, tick cleanup invariants. This document references SPEC.md for those topics rather than duplicating them.

**Two-layer architecture:** pgque is explicitly structured as two layers:
- **pgque-core:** Productization of PgQ (rename, modernize, pg_cron, security hardening, single-file install, health/metrics views). Low risk -- mechanical transformation of proven code.
- **pgque-api:** Modern convenience layer (send/receive/ack/nack, DLQ, delayed delivery, client SDKs). Higher risk -- new semantic surface area that must reduce cleanly to PgQ primitives.

## Changelog

| Version | Date | Description |
|---------|------|-------------|
| 0.1.0-draft | 2026-04-12 | Initial draft: repackaging thesis, what changes from PgQ, modern API layer, observability, client libraries, advanced patterns, implementation phases, source file inventory. |
| 0.2.0-draft | 2026-04-12 | Landscape comparison (28 systems across PG-native, external brokers, workflow engines, Python task queues; architectural comparison table; positioning rationale). Team/staffing with week-by-week Gantt. Risk table (11 risks). Best practices. Sprint-level implementation plan with deliverables and test plans. |
| 0.3.0-draft | 2026-04-12 | First review round. Rename pgqx to pgque (PgQ Universal Edition). Explicit two-layer architecture (pgque-core vs pgque-api). Fix receive() batch ownership trap (rename i_batch_size to i_max_return, document that ack processes entire batch). Fix nack() to accept retry_count parameter (avoid re-querying batch). Fix send_batch() to resolve queue/table once. Fix OTel counter/gauge semantics. Fix queue_health() edge cases. Soften "Exactly-once capable" to "Exactly-once capable (transactional pattern)". Resolve priority contradiction. Add VACUUM for delayed_events and dead_letter to maint(). Defer full OTel export architecture and Node/Ruby SDKs to v2. Align Sprint 5 with risk mitigation (Python + Go only). Document receive() rotation-blocking behavior. |
| 0.4.0-draft | 2026-04-12 | Second review round (3 reviewers). Add preliminary benchmark results (section 2.9, from NikolayS/pgq#1 -- quick-and-dirty laptop benchmark, needs repetition on server hardware). Update throughput claim from ~10-20k to ~86k ev/s (PL/pgSQL measured). Add PgQ code import strategy: git submodule + build/transform.sh (section 8.0). Fix `event_dead()` to accept event fields from caller instead of re-querying batch. Remove dead `qstate` lookup from `send_batch()`, leave TODO. Read `max_retries` from queue config instead of hardcoding 5 in `nack()`. Drop `peek()` from v1 scope. Fix `delayed_events` index (remove broken partial-index predicate with `now()`). Rename `send_at()` return type documentation to clarify it returns a scheduled-entry ID, not a queue event ID. Fix Node.js/Ruby class names (PgqxClient -> PgqueClient, Pgqx:: -> Pgque::). Fix CLI env var (PGQX_DSN -> PGQUE_DSN). Align Gantt with v1 scope (remove Node.js/Ruby from weeks 7-8). Add `queue_max_retries` column to `pgque.queue` table. Fix `queue_health()` to handle queues with no ticks. |
| 0.5.0-draft | 2026-04-12 | Third review round (3 approvals). Fix section 2.7 throughput claim (was still ~10-20k, now reflects benchmarks with `synchronous_commit=off` caveat). Add `sync_commit=off` caveat to section 2.5 comparison table. Combine `nack()` two lookups into single join. Add `queue_max_retries` column to schema definition (section 3.4.6.1). Document `ev_txid` NULL in DLQ (pgque.message does not carry txid). Correct RedPanda comparison units (MiB/s not Mbps). |
| 0.6.0-draft | 2026-04-12 | Add red/green TDD methodology (section 13.2). Add 10 user stories as acceptance tests (section 13.3): basic produce/consume, fan-out, retry+DLQ, delayed delivery, batch under load, rotation under lag, transactional exactly-once, managed PG install, observability, idempotent install. Tests serve both CI automation and manual/AI-agent verification. |
| 1.0.0 | 2026-04-12 | Spec approved. Three independent review rounds, all reviewers approved. |
| 1.1 | 2026-04-12 | Clarify US-3 retry cycle sequencing, unit vs integration test distinction in TDD section, US-10 idempotency implementation challenges. |

---

## 1. What pgque Is

pgque is a repackaging of [PgQ](https://github.com/pgq/pgq) (v3.5.1, ISC
license) into a modern, extension-free PostgreSQL queue system with a simplified
API and built-in observability.

**pgque is NOT a reimplementation.** PgQ already ships complete PL/pgSQL
replacements for all its C code in `lowlevel_pl/`. The Makefile has a
`make plonly` target that produces `pgq_pl_only.sql` -- a single concatenated
file. There is a test (`sql/switch_plonly.sql`) that hot-swaps C for PL/pgSQL
at runtime. The PL-only install (`structure/install_pl.sql`) is six lines:

```
\i structure/tables.sql
\i structure/func_internal.sql
\i lowlevel_pl/insert_event.sql
\i structure/func_public.sql
\i structure/triggers_pl.sql
\i structure/grants.sql
```

The substitution is exactly two components:

| C component | PL/pgSQL replacement | Lines |
|---|---|---|
| `lowlevel/pgq_lowlevel.sql` (C shared lib for event insertion) | `lowlevel_pl/insert_event.sql` | 60 |
| `structure/triggers.sql` (C trigger lib) | `lowlevel_pl/jsontriga.sql`, `logutriga.sql`, `sqltriga.sql` | 318 + 326 + 363 = 1,007 |

Everything else -- tables (225 lines), internal functions (32 include lines
expanding to ~1,300 lines of function bodies), public functions (70 include
lines expanding to ~1,600 lines), grants (13 lines) -- is IDENTICAL between
C and PL-only installs. Total PL-only source: ~4,028 lines across 40 files.

**pgque IS a productization.** It takes this proven, tested code and:

1. Repackages it as a single-file install (no `make`, no `CREATE EXTENSION`)
2. Renames to `pgque` schema (coexists with original PgQ)
3. Modernizes for PG14+ (`pg_snapshot` functions, `xid8` type)
4. Replaces `pgqd` daemon with `pg_cron` jobs
5. Adds a modern API layer (`send`/`receive`/`ack`/`nack`, DLQ, delayed delivery)
6. Adds observability (metrics views, OTel integration, health diagnostics)
7. Provides native client libraries for Python, Go, Node.js, Ruby

The positioning: "PgQ already works in pure SQL -- we packaged it so you can
use it on managed databases, and added modern conveniences."

Installation: `\i pgque.sql` followed by `SELECT pgque.start()`.

### License

PgQ is ISC-licensed (copyright Marko Kreen, Skype Technologies OU). ISC is
a permissive license functionally equivalent to MIT/BSD-2-Clause. pgque is
licensed under **Apache-2.0**. The ISC license requires preserving the
copyright notice and permission notice in all copies — pgque includes PgQ's
original copyright notice in its source headers.

---

## 2. Landscape Comparison

This section surveys the queue, job, and durable-execution ecosystem that pgque
enters. The goal is not to declare winners but to make the architectural
trade-offs visible so that a reviewer (or a prospective user) can judge where
pgque fits and where it does not.

### 2.1 PostgreSQL-native queue systems

These systems run entirely inside PostgreSQL (or require only a PG extension).
They are the most direct competitors to pgque.

| System | Stars | Language | Architecture | Key trait |
|---|---|---|---|---|
| **PgQ** (pgtools) | ~300 | PL/pgSQL + C | Snapshot-based batch isolation, TRUNCATE rotation, 3-table INHERITS | pgque's foundation — battle-tested at Skype scale for 15+ years |
| **PGMQ** (Tembo) | ~4.8k | Rust ext + SQL | SKIP LOCKED + DELETE, visibility timeout, per-message ack. Also has SQL-only install. | SQS-like API. Dead tuple bloat under sustained load. |
| **River** | ~5k | Go | SKIP LOCKED, transactional enqueue, LISTEN/NOTIFY, unique jobs, COPY FROM for bulk | Go-native, excellent DX, ~46k jobs/sec. Go-only. |
| **graphile-worker** | ~2.2k | Node.js | SKIP LOCKED, LISTEN/NOTIFY, sub-3ms latency, batch jobs, cron | Fastest PG queue (~184k jobs/sec with batching). Node.js only. |
| **pg-boss** | ~3.4k | Node.js | SKIP LOCKED, polling, priority, scheduling, throttling, DLQ, partitioned archival | Feature-rich all-in-one. Node.js only. |
| **Oban** | ~3.9k | Elixir | SKIP LOCKED, LISTEN/NOTIFY, unique jobs, priorities 0-9. Pro: DAG workflows, rate limiting. | Elixir ecosystem standard. Pro is commercial. |
| **Que** | ~2.3k | Ruby | Advisory locks (not SKIP LOCKED) — locks held in memory, no dead tuples from claiming | Avoids bloat via advisory locks. ~9.8k jobs/sec. Thin codebase (~1,200 lines). |
| **good_job** | ~3k | Ruby | Advisory locks via CTE, multi-threaded, cron, concurrency controls, dashboard | Most popular Ruby PG queue. CTE degrades above ~1M queued jobs. |
| **solid_queue** | ~2.4k | Ruby | FOR UPDATE SKIP LOCKED, separate hot/scheduled tables, supervisor architecture | Rails 8 default. Backed by 37signals production (HEY, Basecamp). ~20M jobs/day. |
| **Delayed::Job** | ~4.8k | Ruby | Polling (no SKIP LOCKED), YAML serialization, priority | Legacy (~376 jobs/sec). Still widely deployed. |
| **QueueClassic** | ~1.2k | Ruby | LISTEN/NOTIFY + SKIP LOCKED, JSON payloads | Simple, transactional enqueue. Lower adoption than alternatives. |
| **Procrastinate** | ~0.7k | Python | SKIP LOCKED + LISTEN/NOTIFY, pure PG, Django integration | Small but notable: pure-PG Python job queue. |

### 2.2 External broker systems (commonly compared)

Teams evaluating PostgreSQL-native queues often come from these systems, or are
deciding whether to add a separate broker. Understanding what they trade away
(and gain) is essential context.

| System | Stars | Backend | Architecture | Why teams consider PG queues instead |
|---|---|---|---|---|
| **Sidekiq** | ~13.5k | Redis | In-memory, multi-threaded Ruby workers, ~50k jobs/sec | No transactional enqueue with PG, Redis is another dependency |
| **Celery** | ~28k | Redis / RabbitMQ | Python distributed task queue, chains/groups/chords | Complex ops (broker + result backend), PG backend poorly maintained |
| **BullMQ** | ~8.7k | Redis | Node.js, Redis Streams, rate limiting, flows, multi-language clients | Redis dependency, no transactional enqueue |
| **Faktory** | ~6.1k | Redis | Language-agnostic Go server, any-language workers (by Sidekiq author) | Extra server + Redis, two dependencies |

The common thread: teams adopt PG-native queues to eliminate the Redis (or
RabbitMQ) dependency and to get **transactional enqueue** — the ability to
insert an event and write application state in the same COMMIT, with rollback
guaranteeing neither persists. No external broker can offer this.

### 2.3 Workflow / durable execution engines (different category)

These are not queues. They solve a fundamentally different problem — multi-step,
stateful, long-running processes with durable execution guarantees. They appear
in queue comparisons because teams sometimes conflate "I need a queue" with
"I need a workflow engine."

| System | Stars | Architecture | Why it is different | When to use pgque instead |
|---|---|---|---|---|
| **Temporal.io** | ~19.5k | Go server cluster, event sourcing, deterministic replay, SDKs in 6+ languages | Full workflow orchestration. Requires Temporal server + DB (Cassandra/MySQL/PG). Python SDK alone is ~170k lines. A team reported Temporal became a "barrier to adoption by Enterprise customers" due to operational burden (Nango migrated to a simple PG queue). | When you need a queue, not a workflow engine. pgque is one SQL file; Temporal is a distributed system. |
| **Restate.dev** | ~3.7k | Rust server, virtual objects, durable execution, custom replicated log | State machines with durable execution. Requires Restate server. More like "durable functions" than a queue. Lower ops burden than Temporal but still an external service. | When your workload is queue-shaped (produce event, consume event), not workflow-shaped (multi-step stateful processes). |
| **Inngest** | ~5.2k | Go server, event-driven step functions, serverless-compatible (Vercel, Netlify) | Event-triggered durable workflows. Can run serverless or self-hosted. Event-centric model (triggers, not just queues). | When you need simple event streaming, not step-function orchestration. |
| **Hatchet** | ~6.8k | Go server on PostgreSQL, DAG workflows, multi-tenant queue fairness, YC W24 | PG-native workflow engine with queue semantics. Bridges simple queues and full workflow engines. Dashboard, CLI, alerting built in. | When you need a queue primitive, not a workflow platform. Hatchet is closer to pgque's PG-native philosophy but adds significant complexity. |
| **Rivet.gg** | ~5.4k | Rust platform, actors + workflows, FoundationDB + V8 isolates | Platform for durable applications, actors, matchmaking. Gaming/real-time focus. Durable Objects model. | When you just need a PG queue, not an actor platform. |
| **Absurd** | ~1.6k | Pure PG (PL/pgSQL stored procedures), no extensions, pull-based scheduling | "Temporal but just Postgres." By Armin Ronacher (Flask, Rye). Single SQL file, tiny SDKs (~1.4k-1.9k LOC). Checkpointed step execution with resume on failure. Architecturally closest to pgque's PG-native philosophy in the workflow space. | If you need simple produce/consume, pgque. If you need step-by-step workflows with per-step checkpointing, consider Absurd. Both prove that PG-native, no-extension approaches work. |

### 2.4 Python-specific task systems

Python has a rich ecosystem of task/job systems, almost all Redis-backed.
Teams running PostgreSQL-heavy Python stacks often evaluate whether they can
eliminate Redis entirely.

| System | Stars | Backend | Notes |
|---|---|---|---|
| **Dramatiq** | ~5.2k | Redis / RabbitMQ | Celery alternative, simpler API, better defaults. LGPL licensed. |
| **Huey** | ~5.9k | Redis / SQLite / in-memory | Lightweight, multiple backends, good for smaller projects. No PG backend. |
| **RQ (Redis Queue)** | ~10.6k | Redis | Extremely simple. Fork-per-job (~10x slower than Dramatiq/Huey). |
| **ARQ** | ~2.9k | Redis | Async-native (asyncio), by Pydantic creator. |
| **SAQ** | ~0.5k | Redis | ARQ-inspired, Redis Streams, sub-5ms latency. |
| **TaskTiger** | ~1.3k | Redis | Unique tasks, scheduled tasks, reliable locking. |

pgque with `pgque-py` (section 6.2) gives Python teams a PG-native alternative
that requires no Redis, supports transactional enqueue, and avoids MVCC bloat.

### 2.5 Key architectural comparison

This table compares pgque against the most-adopted PostgreSQL-native queue
systems across the features that matter for production operations.

| Feature | pgque | PGMQ | River | graphile-worker | pg-boss | Oban | solid_queue |
|---|---|---|---|---|---|---|---|
| Claim mechanism | Snapshot isolation (lockless) | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | SKIP LOCKED | FOR UPDATE + SKIP LOCKED |
| Table bloat under sustained load | None (TRUNCATE rotation) | Yes (DELETE + VACUUM) | Yes (UPDATE/DELETE) | Yes (UPDATE/DELETE) | Mitigated (partitioned archival) | Yes (UPDATE/DELETE) | Yes (DELETE) |
| Delivery semantics | At-least-once (batch) | At-least-once (per-msg) | At-least-once | At-least-once | At-least-once | At-least-once | At-least-once |
| Exactly-once capable | Yes (transactional ack pattern -- see 7.1) | No | Yes (transactional) | No | No | No | No |
| Batch processing | Native (tick-bounded) | Manual | Manual | Manual batching | No | No | No |
| DLQ | Built-in (v1) | Via archival | No | No | Built-in | Via plugin | Via Solid Queue API |
| CDC triggers | jsontriga (v1) | No | No | No | No | No | No |
| Multiple consumers | Built-in | Manual | No | No | No | Via queues | Via queues |
| Requires C extension | No | Yes (Rust ext) | No (Go binary) | No | No | No | No |
| Language-agnostic | Yes (SQL API) | Yes (SQL API) | Go only | Node.js only | Node.js only | Elixir only | Ruby only |
| Managed PG compatible | Yes | Depends (needs ext) | Yes | Yes | Yes | Yes | Yes |
| Latency (typical) | 1-2s (tick interval) | Sub-100ms | Sub-100ms | Sub-3ms | ~1s | ~1s | ~1s |
| Throughput | ~86k ev/s single-TX, ~164k batched (PL/pgSQL, `sync_commit=off` per-session; see 2.9) | ~30k msg/sec read | ~46k jobs/sec | ~184k jobs/sec | ~10k jobs/sec | ~20k jobs/sec | Not published |
| Battle-tested | 15+ years (Skype/MS) | ~2 years | ~2 years | ~5 years | ~5 years | ~5 years | ~1 year |

**Reading the throughput column:** These numbers come from different benchmarks
on different hardware and are not directly comparable. They indicate order of
magnitude. pgque's lower raw throughput is offset by zero maintenance overhead
under sustained load — the systems that benchmark higher in bursts degrade as
dead tuples accumulate (see the Brandur/PlanetScale MVCC analysis in
SPEC.md section 10.2).

### 2.6 Why pgque

Given the landscape, pgque's value proposition rests on six pillars:

1. **Zero bloat under sustained load.** pgque is the only PostgreSQL queue
   system that uses TRUNCATE rotation instead of DELETE for event lifecycle.
   Every SKIP LOCKED queue eventually hits the Brandur/PlanetScale dead tuple
   wall — where a single long-running transaction pins the MVCC horizon and
   causes index scan degradation, job backlog growth, and a positive feedback
   loop that only manual VACUUM or downtime resolves. pgque is structurally
   immune. Events are never individually DELETEd. Entire tables are cleared
   via TRUNCATE (DDL, not DML — no dead tuples, no MVCC visibility checks).

2. **Language-agnostic.** pgque's API is pure SQL. Any language that can
   execute `SELECT pgque.send(...)` can produce events. Any language that can
   execute `SELECT * FROM pgque.receive(...)` can consume them. Client
   libraries (Python, Go, Node.js, Ruby) are convenience wrappers, not
   requirements. This is unique among modern PG queues — River is Go-only,
   graphile-worker is Node.js-only, Oban is Elixir-only, solid_queue is
   Ruby-only.

3. **No extra infrastructure.** One SQL file. No Redis, no separate server
   process, no C extension to compile, no `shared_preload_libraries`, no
   server restart. `\i pgque.sql` and you have a production queue.
   pg_cron (pre-installed on every major managed provider) handles the ticker.

4. **Battle-tested core.** PgQ has run at Skype/Microsoft scale for 15+ years,
   processing billions of events. pgque is not a prototype or a weekend project
   — it is a repackaging of proven code with modern conveniences added on top.

5. **Managed PG compatible.** Runs on RDS, Aurora, AlloyDB, Cloud SQL,
   Supabase, Neon, Crunchy Bridge without modifications. No extension
   installation required. No DBA needed.

6. **Batch-oriented.** Natural fit for ETL, CDC, analytics pipelines, and any
   workload where processing N events together is more efficient than
   processing them one at a time. Tick-bounded batches give consumers a
   consistent, snapshot-isolated set of events to work with.

### 2.7 When NOT to use pgque

pgque is not the right choice for every queue workload. Be honest about the
trade-offs:

- **Sub-10ms latency requirements.** pgque's tick-based architecture means
  typical latency is 1-2 seconds (reducible to ~100ms with LISTEN/NOTIFY
  wakeup). If you need sub-10ms job dispatch, graphile-worker or direct
  LISTEN/NOTIFY is a better fit.

- **100k+ jobs/sec sustained throughput.** Preliminary benchmarks
  (section 2.9) show ~86k ev/s with `synchronous_commit=off` (settable
  per-session or per-transaction — safe for queue workloads even when the
  global setting is `on`, since at worst the last few ms of committed events
  are lost on crash). With `synchronous_commit=on`, expect lower numbers.
  If you need 100k+ jobs/sec sustained, you likely need a dedicated broker
  (Kafka, RedPanda) or a C-level extension.

- **Complex multi-step workflows with branching.** If your workload is
  "step A then step B, but if B fails retry B three times, then escalate to
  step C" — that is a workflow engine problem, not a queue problem. Use
  Temporal, Restate, or Absurd.

- **You are already all-in on one ecosystem.** If your team is pure Go, River
  gives you better DX. Pure Elixir, Oban is the standard. Rails 8, solid_queue
  is built in. pgque's value is strongest when you have a polyglot stack or
  when you need the zero-bloat guarantee.

### 2.8 Architectural trade-off summary

Three fundamentally different approaches exist for PG-native job claiming:

**SKIP LOCKED systems** (PGMQ, River, graphile-worker, pg-boss, Oban,
solid_queue): Fast per-job claiming, but every completed job creates a dead
tuple via UPDATE or DELETE. Under sustained high throughput, VACUUM cannot keep
up, index scans degrade, and throughput enters a death spiral. This is the core
problem documented by Brandur Leach (2015) and validated by PlanetScale (2026)
— see SPEC.md section 10.2.

**Advisory lock systems** (Que, good_job): Avoid the dead-tuple problem — locks
are held in memory, no row UPDATE needed to claim a job. But advisory lock
tables have their own contention limits at extreme concurrency, and CTE-based
lock acquisition degrades above ~1M queued jobs.

**Snapshot-based batch isolation + TRUNCATE rotation** (PgQ, pgque): Zero bloat
by design. No per-job locking, no dead tuples, no VACUUM pressure on event
tables. The trade-off is batch-oriented consumption (not per-job) and
ticker-driven latency (1-2 seconds, not sub-3ms).

### 2.9 Preliminary benchmark results

A quick-and-dirty benchmark was run on a laptop (Apple Silicon, 10 cores,
24 GiB RAM, APFS SSD, PostgreSQL 18.3). **These numbers are preliminary and
will need to be repeated on proper server hardware with controlled
conditions.** Full details, methodology, and raw data:
[NikolayS/pgq#1](https://github.com/NikolayS/pgq/issues/1).

Key findings (PgQ v3.5.1, tuned config: `synchronous_commit=off` — can be
set per-session/per-TX for queue workloads only; `shared_buffers=4GB`,
`max_wal_size=8GB`, `wal_level=minimal`):

| Scenario | Throughput | Per core |
|---|---|---|
| C mode, single insert/TX, ~100 B, 16 clients | 117,924 ev/s | ~11.8k ev/s |
| **PL/pgSQL mode, single insert/TX, ~100 B, 16 clients** | **85,836 ev/s** | **~8.6k ev/s** |
| C mode, batched 1000/TX, ~100 B, 16 clients | 417,414 ev/s | ~41.7k ev/s |
| C mode, batched 1000/TX, ~2 KiB, 16 clients | 257,179 ev/s (479 MiB/s) | ~25.7k ev/s (~47.9 MiB/s) |
| C mode, batched 1000/TX, 30-min sustained, ~2 KiB (70 ckpts) | 163,940 ev/s (301 MiB/s avg) | ~16.4k ev/s (~30.1 MiB/s) |
| **PL/pgSQL mode, batched 100k/TX, ~100 B, 1 client** | **80,515 ev/s** | **~8.1k ev/s** |
| **PL/pgSQL mode, batched 100k/TX, ~2 KiB, 1 client** | **48,899 ev/s (~91.5 MiB/s)** | **~4.9k ev/s (~9.2 MiB/s)** |
| Consumer read rate, 100k batch, ~100 B | ~2.4M ev/s | ~240k ev/s |
| Consumer read rate, 100k batch, ~2 KiB | ~305k ev/s (568 MiB/s) | ~30.5k ev/s (~56.8 MiB/s) |

Per-core numbers assume all 10 cores are utilized (Apple Silicon, mixed
P/E cores). Actual per-core throughput on server hardware with uniform cores
may differ. These per-core figures enable direct comparison with systems
like RedPanda (~100 MiB/s per core claimed). PgQ sustained ~30.1 MiB/s per
core — roughly **1/3 of RedPanda's per-core claim**, but with full ACID
transactions, transactional batch isolation, and zero bloat under sustained
load. The ~3x gap is far from the "1-2 orders of magnitude" sometimes
claimed for PG-based queues vs. dedicated brokers. On server-grade NVMe
(where I/O was 57% of wait time on this laptop SSD), the gap would narrow
further.

The PL/pgSQL rows are the most relevant for pgque — they show the
throughput ceiling for the no-C-extension mode that pgque will use. At
~8.6k ev/s per core for single-insert-per-TX, PgQ's PL/pgSQL mode is
competitive with C-based alternatives, especially considering that it
produces zero dead tuples under sustained load.

Notable observations from the benchmark:

- **Tuning matters more than C vs. PL/pgSQL.** PL/pgSQL tuned (86k ev/s)
  beats C untuned (52k ev/s).
- **Batching matters most.** 1000 inserts/TX reaches 417k ev/s — 3.6x over
  single-insert-per-TX.
- **Consumer is never the bottleneck.** Reading events is 3-6x faster than
  writing them.
- **Checkpoints cause dips but not collapse.** Sustained throughput over 70
  checkpoints (30 min) averaged 301 MiB/s with no degradation over time.
- **Storage is the bottleneck.** pg_ash showed 57% of time spent on
  `IO:DataFileWrite` — on server-grade NVMe, throughput would scale higher.

**Caveat:** This is a quick-and-dirty benchmark on a laptop. The numbers are
indicative, not authoritative. A proper benchmark must be run on server
hardware with controlled conditions, multiple runs, and statistical analysis.
See Sprint 6 for the planned benchmark methodology.

---

## 3. Why

### 3.1 The managed-database problem

PgQ requires installing two custom C shared libraries (`pgq_lowlevel.so`,
`pgq_triggers.so`) and an external daemon (`pgqd`). This makes it unusable on:

- **Amazon RDS / Aurora** — no custom C extensions
- **Google Cloud SQL** — no custom C extensions
- **AlloyDB** — no custom C extensions
- **Azure Flexible Server** — limited extension allowlist, pg_cron permissions more constrained
- **Supabase, Neon, Crunchy Bridge** — curated extension catalogs
- **Any environment** where the DBA cannot (or will not) install C code

These are now the majority of PostgreSQL deployments. PgQ's architecture —
designed in the Skype era when everyone ran self-hosted Postgres — locks it out
of the modern ecosystem.

**pg_cron availability by provider:**

| Provider | pg_cron available | Notes |
|----------|------------------|-------|
| Amazon RDS / Aurora | Yes | Supported since RDS PG 12.5 |
| Google Cloud SQL | Yes | Supported, requires flag |
| AlloyDB | Yes | Supported |
| Azure Flexible Server | Yes | More constrained permissions model |
| Supabase | Yes | Pre-installed |
| Neon | Yes | Jobs only run when compute is active (no scale-to-zero) |
| Crunchy Bridge | Yes | Supported |
| Self-hosted | Yes | Must install separately |

Where pg_cron is unavailable or unsuitable (e.g., serverless scale-to-zero),
pgque works with any external scheduler calling `pgque.ticker()` and
`pgque.maint()` via `psql` or a database connection.

### 3.2 The daemon problem

PgQ requires `pgqd`, an external C daemon, to generate ticks and run
maintenance. This means:

- Another process to deploy, monitor, and restart
- Another failure mode (daemon dies → ticks stop → consumers stall)
- Container/Kubernetes complexity (sidecar? separate deployment?)
- No option on managed databases where you can't run custom daemons

pg_cron (supported on the major providers listed in section 3.1, with
provider-specific constraints) eliminates this entirely.
The ticker and maintenance run as scheduled SQL inside the database.

### 3.3 The proven path

PgQ already ships complete PL/pgSQL replacements for all C code in
`lowlevel_pl/`. The switch is toggled by `sql/switch_plonly.sql`:

```sql
-- switch_plonly.sql: switches from C to PL/pgSQL implementations
\i lowlevel_pl/insert_event.sql
\i lowlevel_pl/jsontriga.sql
\i lowlevel_pl/logutriga.sql
\i lowlevel_pl/sqltriga.sql
```

This means:
1. PgQ's authors already validated PL/pgSQL as a correct replacement for C
2. The PL/pgSQL code has been in the PgQ repo since v3.2 (2012)
3. pgque does not invent new queue logic — it repackages proven code

### 3.4 What changes from PgQ to pgque

#### 3.4.1 Rename `pgq` to `pgque`

All schema objects move from the `pgq` schema to `pgque`. This is a mechanical
search-and-replace across ~40 source files with no behavioral change.

#### 3.4.2 Replace `txid_*` with `pg_*` snapshot functions

PostgreSQL 13+ introduced `pg_snapshot` functions that replace the older
`txid_*` family:

| PgQ (deprecated) | pgque (modern) | Notes |
|---|---|---|
| `txid_current()` | `pg_current_xact_id()` | Returns `xid8` not `bigint` |
| `txid_current_snapshot()` | `pg_current_snapshot()` | Returns `pg_snapshot` |
| `txid_snapshot_xmax()` | `pg_snapshot_xmax()` | |
| `txid_snapshot_xmin()` | `pg_snapshot_xmin()` | |
| `txid_snapshot_xip()` | `pg_snapshot_xip()` | |
| `txid_visible_in_snapshot()` | `pg_visible_in_snapshot()` | |
| `txid_snapshot` type | `pg_snapshot` type | |

Concrete changes in the schema:

```sql
-- PgQ (tables.sql):
queue_switch_step1   bigint not null default txid_current(),
queue_switch_step2   bigint default txid_current(),
tick_snapshot         txid_snapshot not null default txid_current_snapshot(),
ev_txid              bigint not null default txid_current(),

-- pgque:
queue_switch_step1   xid8 not null default pg_current_xact_id(),
queue_switch_step2   xid8 default pg_current_xact_id(),
tick_snapshot         pg_snapshot not null default pg_current_snapshot(),
ev_txid              xid8 not null default pg_current_xact_id(),
```

The `xid8` type avoids the `bigint` -> `xid8` cast overhead on the hot insert
path. `pg_visible_in_snapshot()` accepts `xid8` natively.

Functions affected (each has `txid_*` calls that become `pg_*`):
- `pgque.ticker()` -- `txid_snapshot_xmax()`, `txid_current()`
- `pgque.batch_event_sql()` -- `txid_snapshot_xmax()`, `txid_visible_in_snapshot()`, `txid_snapshot_xip()`
- `pgque.maint_rotate_tables_step1()` -- `txid_current()`, `txid_snapshot_xmin()`
- `pgque.maint_rotate_tables_step2()` -- `txid_current()`
- `pgque.insert_event_raw()` -- implicit via `ev_txid` default

#### 3.4.3 Replace `pgqd` with `pg_cron`

PgQ requires `pgqd`, an external C daemon. pgque replaces it with two `pg_cron`
jobs created by `pgque.start()`:

```sql
-- Ticker: every 2 seconds (pg_cron >= 1.5 required for sub-minute scheduling)
SELECT cron.schedule_in_database(
    'pgque_ticker',
    '2 seconds',
    $$SET statement_timeout = '1500ms'; SELECT pgque.ticker()$$,
    current_database()
);

-- Maintenance: every 30 seconds
SELECT cron.schedule_in_database(
    'pgque_maint',
    '30 seconds',
    $$SET statement_timeout = '25s'; SELECT pgque.maint()$$,
    current_database()
);
```

See SPEC.md section 4.3 for the full pg_cron integration design including
idempotent start, worker starvation detection, and graceful degradation
without pg_cron.

#### 3.4.4 Remove `CREATE EXTENSION` dependency

PgQ installs via `CREATE EXTENSION pgq`. pgque installs via a single SQL file:

```
\i pgque.sql
SELECT pgque.start();  -- optional: creates pg_cron jobs
```

No `.control` file, no PGXS, no `pg_dump` extension handling.
The install script is idempotent -- safe to re-run.

Uninstall:

```
SELECT pgque.uninstall();  -- stops pg_cron jobs + DROP SCHEMA pgque CASCADE
```

#### 3.4.5 SECURITY DEFINER hardening

PgQ's functions use `SECURITY DEFINER` but lack `search_path` pinning.
pgque adds `SET search_path = pgque, pg_catalog` to every `SECURITY DEFINER`
function. See SPEC.md section 3.2.7 for the full hardening rules.

PgQ example (vulnerable):
```sql
create function pgq.insert_event(...) ... language plpgsql security definer;
```

pgque (hardened):
```sql
create function pgque.insert_event(...) ...
language plpgsql security definer set search_path = pgque, pg_catalog;
```

Every function in pgque must follow this pattern. No exceptions.

#### 3.4.6 Drop `queue_per_tx_limit`

PgQ's `queue_per_tx_limit` uses C-level per-transaction state tracking via
`GetTopTransactionId()`. This cannot be replicated cleanly in PL/pgSQL.
The feature is rarely used. pgque drops it.

The `queue_per_tx_limit` column is removed from `pgque.queue`.

#### 3.4.6.1 Add `queue_max_retries` column

pgque adds a `queue_max_retries` column to `pgque.queue`:

```sql
alter table pgque.queue add column queue_max_retries int4;
-- NULL means use default (5). Set via create_queue() JSONB options
-- or set_queue_config().
```

The `create_queue()` JSONB overload maps `"max_retries"` to this column.
`nack()` reads `queue_max_retries` to decide retry vs. DLQ routing
(see section 4.3).

#### 3.4.7 Drop `set default_with_oids = 'off'`

PgQ's `structure/tables.sql` sets `default_with_oids = 'off'` (removed in
PG12). pgque drops this line.

#### 3.4.8 Clean up maintenance operations

PgQ's `maint_operations()` includes hardcoded references to `pgq_node` and
`londiste` (checking for their procedures by name). pgque removes these --
Londiste and pgq_node are out of scope. The `queue_extra_maint` column is
preserved but CHECK-constrained to NULL in v1 (see SPEC.md section 4.4.2).

#### 3.4.9 Add `pgque.config` singleton table

pgque adds a config table for pg_cron job tracking:

```sql
CREATE TABLE pgque.config (
    singleton       bool PRIMARY KEY DEFAULT true CHECK (singleton),
    ticker_job_id   bigint,
    maint_job_id    bigint,
    installed_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO pgque.config (singleton) VALUES (true);
```

#### 3.4.10 Add lifecycle functions

Functions not present in PgQ:

| Function | Purpose |
|---|---|
| `pgque.start()` | Create pg_cron jobs, store job IDs |
| `pgque.stop()` | Remove pg_cron jobs |
| `pgque.uninstall()` | Stop + DROP SCHEMA pgque CASCADE |
| `pgque.status()` | Diagnostic dashboard (TABLE return type) |

#### 3.4.11 LISTEN/NOTIFY in ticker

PgQ's ticker does not emit notifications. pgque's ticker adds:

```sql
PERFORM pg_notify('pgque_' || queue_name, tick_id::text);
```

after each tick, enabling low-latency consumer wakeup.

#### 3.4.12 Summary of changes

| Area | PgQ v3.5.1 | pgque | Type of change |
|---|---|---|---|
| Schema | `pgq` | `pgque` | Rename |
| Snapshot functions | `txid_*` | `pg_*` | Rename |
| Transaction ID type | `bigint` | `xid8` | Type change |
| Snapshot type | `txid_snapshot` | `pg_snapshot` | Type change |
| Daemon | `pgqd` (external C) | `pg_cron` jobs | Architecture |
| Installation | `CREATE EXTENSION` | `\i pgque.sql` | Packaging |
| `search_path` pinning | Missing | On all SECURITY DEFINER functions | Security |
| `queue_per_tx_limit` | Supported (C) | Removed | Scope reduction |
| `default_with_oids` | Set to 'off' | Removed (PG12+) | Cleanup |
| `maint_operations` | pgq_node/Londiste hooks | Removed | Scope reduction |
| `config` table | Not present | Added | New |
| Lifecycle functions | Not present | `start/stop/uninstall/status` | New |
| LISTEN/NOTIFY | Not present | Ticker emits NOTIFY | New |
| PG minimum | PG 9.x+ | PG 14+ | Version bump |

---

## 4. The Modern API Layer

PgQ's native API is powerful but low-ceremony-hostile. A consumer must:
`register_consumer` -> `next_batch` -> `get_batch_events` -> process each event
-> `event_retry` for failures -> `finish_batch`. This is fine for infrastructure
engineers; it is not what a product engineer expects from a queue.

pgque adds a simplified API layer that wraps PgQ's internals. The PgQ API remains
available for advanced use cases. The modern API targets the 80% use case:
send a JSON message, receive it, ack or nack it.

### 4.1 Publishing: `pgque.send()`

```sql
-- Default path: untyped literal resolves to send(text, text) -- verbatim bytes
select pgque.send('orders', '{"order_id": 42, "total": 99.95}');

-- Opt-in validation: explicit ::jsonb cast resolves to send(text, jsonb)
select pgque.send('orders', '{"order_id": 42, "total": 99.95}'::jsonb);

-- Send with explicit type (both overloads available on the same rules)
select pgque.send('orders', 'order.created', '{"order_id": 42}');
select pgque.send('orders', 'order.created', '{"order_id": 42}'::jsonb);

-- Send batch (text[] default; use ::jsonb[] cast to opt into validation)
select pgque.send_batch('orders', 'order.created', array[
    '{"order_id": 42}',
    '{"order_id": 43}',
    '{"order_id": 44}'
]);

-- Textual non-JSON payloads (XML, CSV, base64/hex-encoded binary) go
-- through the text overload as-is. Raw binary with NUL bytes is rejected
-- by PG text -- encode first (e.g. encode(raw_proto, 'base64')).
-- Note PG bytea hex input is a single \x prefix followed by hex digits;
-- per-byte separators are not allowed (use \x082a1063, not \x08\x2a\x10\x63).
select pgque.send('orders', 'order.xml', '<order id="42"/>');
select pgque.send('orders', 'order.proto_b64', encode('\x082a1063'::bytea, 'base64'));

-- Delayed send (deliver after timestamp)
select pgque.send_at('orders', 'reminder.send',
    '{"user_id": 7}'::jsonb,
    now() + interval '24 hours');

-- Priority: use separate queues (recommended) or ev_extra1 for
-- client-side sorting within a batch (see section 7.6)
```

**Payload type choice.** Storage is always `ev_data TEXT`; the two overloads
differ only in the client-side validation/reserialization contract.

**Overload resolution.** PostgreSQL picks the overload that needs fewest
implicit casts. An untyped SQL string literal has type `unknown`, and
`unknown → text` is a direct match while `unknown → jsonb` needs an
implicit cast. So:

```sql
-- Untyped literal → resolves to send(text, text), bytes verbatim
select pgque.send('orders', '{"order_id": 42}');

-- Explicit ::jsonb → resolves to send(text, jsonb), validated + canonicalized
select pgque.send('orders', '{"order_id": 42}'::jsonb);
```

The `text` overload is therefore the natural default for plain SQL callers
and for drivers that pass parameters as text (most of them). The `jsonb`
overload is opt-in via an explicit `::jsonb` cast.

- `text` overload (default for untyped literals): fast path. Bytes flow
  straight through to `insert_event`. No parse, no reserialization, key
  order preserved. Required for non-JSON *textual* payloads (XML, CSV,
  base64/hex-encoded binary). Caller is responsible for validating the
  payload.
- `jsonb` overload (opt-in): PG rejects malformed JSON at parse time and
  the payload is stored in canonical form (keys sorted, whitespace
  normalized). Useful when you want PG to be the last line of defense
  against malformed JSON.

**NUL bytes.** `ev_data` is `text`, and PostgreSQL `text` does not accept
NUL (`\x00`). Raw binary wire formats (protobuf, msgpack, Avro, packed
bytea dumps) routinely contain NULs and will be rejected with `invalid
byte sequence` at insert time. Callers that want to ship binary payloads
must encode them first -- `encode(bytes, 'base64')`, `encode(bytes, 'hex')`,
or a custom escape -- and decode on the consumer side. A future `bytea`
overload could bypass this at the cost of changing `ev_data`'s storage
type; deferred pending demand.

Both overloads return `bigint` (event ID). `receive()` returns payload as
`text`, so the `text`-both-sides path is symmetric (no implicit canonicalization
anywhere). The `jsonb`-in / `text`-out path leaves client-side JSON parsing
to the consumer, which is where it has to live anyway (a PG composite type
cannot polymorphically carry both `text` and `jsonb`).

**Internal mapping:**

```sql
-- jsonb overloads (opt-in via ::jsonb cast; validation + canonicalization)
create function pgque.send(i_queue text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create function pgque.send(i_queue text, i_type text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- text overloads (default for untyped literals; fast path, opaque payload)
create function pgque.send(i_queue text, i_payload text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create function pgque.send(i_queue text, i_type text, i_payload text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create function pgque.send_batch(
    i_queue text, i_type text, i_payloads jsonb[])
returns bigint[] as $$
declare
    ids bigint[] := '{}';
    p jsonb;
begin
    -- TODO: optimize to resolve queue/table once and bypass insert_event_raw
    -- with a single multi-VALUES insert. Currently each insert_event() call
    -- resolves the queue independently. Deferred to implementation.
    foreach p in array i_payloads loop
        ids := array_append(ids,
            pgque.insert_event(i_queue, i_type, p::text));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create function pgque.send_batch(
    i_queue text, i_type text, i_payloads text[])
returns bigint[] as $$
declare
    ids bigint[] := '{}';
    p text;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            pgque.insert_event(i_queue, i_type, p));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

### 4.2 Consuming: `pgque.receive()`

`receive()` wraps `next_batch` + `get_batch_events` into a single call that
returns messages directly.

```sql
-- Receive messages (returns up to 100 from the current batch)
select * from pgque.receive('orders', 'order_processor', 100);
-- Returns: msg_id, batch_id, type, payload, retry_count, created_at
```

**Return type:**

```sql
CREATE TYPE pgque.message AS (
    msg_id      bigint,       -- ev_id
    batch_id    bigint,       -- batch containing this message
    type        text,         -- ev_type
    payload     text,         -- ev_data (caller casts to jsonb if needed)
    retry_count int4,         -- ev_retry (NULL for first delivery)
    created_at  timestamptz,  -- ev_time
    extra1      text,         -- ev_extra1
    extra2      text,         -- ev_extra2
    extra3      text,         -- ev_extra3
    extra4      text          -- ev_extra4
);
```

**Implementation:**

```sql
CREATE FUNCTION pgque.receive(
    i_queue text, i_consumer text, i_max_return int DEFAULT 100)
RETURNS SETOF pgque.message AS $$
DECLARE
    v_batch_id bigint;
    ev record;
    cnt int := 0;
BEGIN
    -- Get next batch (may return NULL if no events)
    v_batch_id := pgque.next_batch(i_queue, i_consumer);
    IF v_batch_id IS NULL THEN
        RETURN;
    END IF;

    -- Yield messages from the batch
    FOR ev IN
        SELECT ev_id, ev_type, ev_data, ev_retry, ev_time,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
        FROM pgque.get_batch_events(v_batch_id)
    LOOP
        RETURN NEXT ROW(
            ev.ev_id, v_batch_id, ev.ev_type, ev.ev_data,
            ev.ev_retry, ev.ev_time,
            ev.ev_extra1, ev.ev_extra2, ev.ev_extra3, ev.ev_extra4
        )::pgque.message;
        cnt := cnt + 1;
        EXIT WHEN cnt >= i_max_return;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

**Batch ownership semantics (critical — read carefully):**

`receive()` opens a PgQ batch via `next_batch()`. A batch contains ALL events
committed between two ticks. The `i_max_return` parameter limits how many
messages are *returned to the caller*, but the batch contains all events
regardless.

**When `ack(batch_id)` is called, the ENTIRE batch is finished** — including
events that were not returned due to `i_max_return`. This is not a bug; it
matches PgQ's design where `finish_batch()` advances the consumer past the
entire tick range.

**Users coming from per-message queues (SQS, PGMQ, Redis):** this is the
single biggest conceptual difference. pgque is batch-oriented. The recommended
consumer pattern is:

1. Call `receive()` — get a batch of messages
2. Process ALL returned messages
3. For individual failures, call `nack(batch_id, msg_id, ...)` to retry that event
4. Call `ack(batch_id)` — finishes the batch and advances the consumer position
5. Nacked events reappear in a future batch via the retry queue

**Is `ack()` legal after some messages were nacked?** Yes. `nack()` copies the
failed event into the retry queue with a delay. `ack()` then finishes the
batch normally. The nacked event will reappear in a future batch when its
retry delay expires.

**What about mixed outcomes?** Process all events in the batch. Call `nack()`
for each failure. Call `ack()` to finish. This is the standard pattern and
is how PgQ has always worked.

**Rotation blocking:** An open batch (from `receive()` with no subsequent
`ack()`) blocks table rotation the same way a slow consumer does in raw PgQ.
If the caller takes minutes to process, rotation cannot TRUNCATE the tables
the batch reads from. Client libraries should enforce a maximum batch
processing timeout and ack/nack on timeout.

### 4.3 Acknowledging: `pgque.ack()` and `pgque.nack()`

```sql
-- Ack: finish the batch, advance consumer position
select pgque.ack(batch_id);

-- Nack a single message: retry after 60 seconds
-- Pass the full message record (avoids re-querying the batch)
select pgque.nack(batch_id, msg, '60 seconds'::interval);

-- Nack with reason (goes to DLQ after max retries, read from queue config)
select pgque.nack(batch_id, msg, '60 seconds'::interval, 'upstream timeout');
```

**Internal mapping:**

```sql
create function pgque.ack(i_batch_id bigint)
returns integer as $$
begin
    return pgque.finish_batch(i_batch_id);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create function pgque.nack(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval default '60 seconds',
    i_reason text default null)
returns integer as $$
declare
    v_max_retries int4;
begin
    -- Single lookup: subscription -> queue config
    select coalesce(q.queue_max_retries, 5) into v_max_retries
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    where s.sub_batch = i_batch_id;

    if coalesce(i_msg.retry_count, 0) >= v_max_retries then
        -- Move to dead letter queue (pass event fields, no re-query)
        perform pgque.event_dead(i_batch_id, i_msg.msg_id,
            coalesce(i_reason, 'max retries exceeded'),
            i_msg.created_at, null::xid8, i_msg.retry_count,
            i_msg.type, i_msg.payload,
            i_msg.extra1, i_msg.extra2, i_msg.extra3, i_msg.extra4);
    else
        -- Retry after delay
        perform pgque.event_retry(i_batch_id, i_msg.msg_id,
            extract(epoch from i_retry_after)::integer);
    end if;
    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

### 4.4 Subscriptions: fan-out

PgQ already supports multiple consumers per queue. pgque wraps this:

```sql
-- Subscribe a consumer (starts receiving from current position)
SELECT pgque.subscribe('orders', 'analytics_pipeline');
SELECT pgque.subscribe('orders', 'notification_sender');
SELECT pgque.subscribe('orders', 'audit_logger');

-- Each consumer receives independently
SELECT * FROM pgque.receive('orders', 'analytics_pipeline', 100);
SELECT * FROM pgque.receive('orders', 'notification_sender', 100);

-- Unsubscribe
SELECT pgque.unsubscribe('orders', 'analytics_pipeline');
```

**Internal mapping:**

```sql
CREATE FUNCTION pgque.subscribe(i_queue text, i_consumer text)
RETURNS integer AS $$
BEGIN
    RETURN pgque.register_consumer(i_queue, i_consumer);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

### 4.5 Dead Letter Queue

PgQ has a retry queue but no dead letter queue. pgque adds one.

**Table:**

```sql
CREATE TABLE pgque.dead_letter (
    dl_id           bigserial PRIMARY KEY,
    dl_queue_id     int4 NOT NULL REFERENCES pgque.queue(queue_id),
    dl_consumer_id  int4 NOT NULL REFERENCES pgque.consumer(co_id),
    dl_time         timestamptz NOT NULL DEFAULT now(),
    dl_reason       text,

    -- Original event fields (copied from event at time of death)
    ev_id           bigint NOT NULL,
    ev_time         timestamptz NOT NULL,
    ev_txid         xid8,       -- NULL: pgque.message does not carry txid
                                -- (internal detail, meaningless after batch closes)
    ev_retry        int4,
    ev_type         text,
    ev_data         text,
    ev_extra1       text,
    ev_extra2       text,
    ev_extra3       text,
    ev_extra4       text
);

CREATE INDEX dl_queue_time_idx ON pgque.dead_letter (dl_queue_id, dl_time);
```

**Functions:**

```sql
-- Move event to DLQ (called by nack() when max retries exceeded)
pgque.event_dead(batch_id bigint, event_id bigint, reason text)
    RETURNS integer

-- Inspect DLQ
pgque.dlq_inspect(queue_name text, limit_count int DEFAULT 100)
    RETURNS SETOF pgque.dead_letter

-- Replay a dead letter event back into the queue
pgque.dlq_replay(dead_letter_id bigint)
    RETURNS bigint  -- new event ID

-- Replay all DLQ events for a queue
pgque.dlq_replay_all(queue_name text)
    RETURNS integer  -- count of replayed events

-- Purge old DLQ entries
pgque.dlq_purge(queue_name text, older_than interval DEFAULT '30 days')
    RETURNS integer  -- count of purged entries
```

**`event_dead()` implementation:**

`nack()` already has the full message from `receive()`. Rather than
re-querying the batch via `get_batch_events()` (which runs the full
snapshot-based dual-filter query), `nack()` performs the DLQ insert
directly. See the `nack()` implementation below — it calls
`event_dead()` with the event fields passed through from the caller.

```sql
create function pgque.event_dead(
    i_batch_id bigint,
    i_event_id bigint,
    i_reason text,
    i_ev_time timestamptz,
    i_ev_txid xid8,
    i_ev_retry int4,
    i_ev_type text,
    i_ev_data text,
    i_ev_extra1 text default null,
    i_ev_extra2 text default null,
    i_ev_extra3 text default null,
    i_ev_extra4 text default null)
returns integer as $$
declare
    v_sub record;
begin
    -- Look up subscription from batch
    select sub_queue, sub_consumer into v_sub
    from pgque.subscription where sub_batch = i_batch_id;
    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    -- Insert into dead letter table (no re-query of batch events)
    insert into pgque.dead_letter (
        dl_queue_id, dl_consumer_id, dl_reason,
        ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    values (
        v_sub.sub_queue, v_sub.sub_consumer, i_reason,
        i_event_id, i_ev_time, i_ev_txid, i_ev_retry, i_ev_type, i_ev_data,
        i_ev_extra1, i_ev_extra2, i_ev_extra3, i_ev_extra4);

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

### 4.6 Delayed / Scheduled Delivery

Events with a future delivery time go into a holding table. A maintenance step
moves them to the main event table when their time arrives.

**Table:**

```sql
CREATE TABLE pgque.delayed_events (
    de_id           bigserial PRIMARY KEY,
    de_queue_name   text NOT NULL,
    de_deliver_at   timestamptz NOT NULL,
    de_type         text,
    de_data         text,
    de_extra1       text,
    de_extra2       text,
    de_extra3       text,
    de_extra4       text
);

CREATE INDEX de_deliver_idx ON pgque.delayed_events (de_deliver_at);
```

**`send_at()` implementation:**

**Return value semantics:** When delivery is immediate (`i_deliver_at <= now()`),
returns the queue event ID (from `insert_event()`). When delivery is delayed,
returns the **scheduled-entry ID** from `delayed_events.de_id` — this is NOT
a queue event ID. The actual queue event ID is assigned later when
`maint_deliver_delayed()` moves the event into the queue. Client libraries
should document this distinction clearly.

```sql
create function pgque.send_at(
    i_queue text, i_type text, i_payload jsonb, i_deliver_at timestamptz)
returns bigint as $$
begin
    if i_deliver_at <= now() then
        -- Deliver immediately; returns queue event ID
        return pgque.insert_event(i_queue, i_type, i_payload::text);
    end if;

    insert into pgque.delayed_events
        (de_queue_name, de_deliver_at, de_type, de_data)
    values (i_queue, i_deliver_at, i_type, i_payload::text);

    -- Returns scheduled-entry ID (NOT a queue event ID)
    return currval('pgque.delayed_events_de_id_seq');
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

**Maintenance integration:** `pgque.maint()` calls
`pgque.maint_deliver_delayed()` which moves due events:

```sql
CREATE FUNCTION pgque.maint_deliver_delayed()
RETURNS integer AS $$
DECLARE
    ev record;
    cnt integer := 0;
BEGIN
    FOR ev IN
        DELETE FROM pgque.delayed_events
        WHERE de_deliver_at <= now()
        RETURNING *
    LOOP
        PERFORM pgque.insert_event(ev.de_queue_name, ev.de_type, ev.de_data,
            ev.de_extra1, ev.de_extra2, ev.de_extra3, ev.de_extra4);
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

### 4.7 Queue Management

```sql
-- Create a queue with options
SELECT pgque.create_queue('orders', '{
    "rotation_period": "4 hours",
    "ticker_max_count": 1000,
    "ticker_max_lag": "5 seconds",
    "max_retries": 10
}'::jsonb);

-- Pause/resume
SELECT pgque.pause_queue('orders');
SELECT pgque.resume_queue('orders');

-- Simplified wrappers around set_queue_config
CREATE FUNCTION pgque.pause_queue(i_queue text)
RETURNS void AS $$
BEGIN
    PERFORM pgque.set_queue_config(i_queue, 'ticker_paused', 'true');
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

### 4.8 API Summary: Modern vs. PgQ Primitives

| Modern API | PgQ primitive underneath | Notes |
|---|---|---|
| `pgque.send(queue, payload)` | `pgque.insert_event(queue, type, data)` | TEXT overload is default for untyped literals (fast path, opaque bytes); JSONB overload is opt-in via `::jsonb` cast (validation + canonicalization) |
| `pgque.send_batch(queue, type, payloads[])` | Loop of `insert_event()` calls | Single TX; `text[]` default, `jsonb[]` opt-in via `::jsonb[]` cast |
| `pgque.send_at(queue, type, payload, time)` | `delayed_events` table + `maint_deliver_delayed()` | New |
| `pgque.receive(queue, consumer, n)` | `next_batch()` + `get_batch_events()` | Combined |
| `pgque.ack(batch_id)` | `finish_batch(batch_id)` | Rename |
| `pgque.nack(batch_id, msg, delay)` | `event_retry(batch_id, msg_id, seconds)` | + DLQ logic, reads max_retries from queue config |
| `pgque.subscribe(queue, consumer)` | `register_consumer(queue, consumer)` | Rename |
| `pgque.unsubscribe(queue, consumer)` | `unregister_consumer(queue, consumer)` | Rename |
| `pgque.event_dead(batch, event_id, reason, ...)` | `dead_letter` table insert | New, accepts event fields from caller |
| `pgque.dlq_replay(dl_id)` | `insert_event()` from dead_letter row | New |
| `pgque.pause_queue(queue)` | `set_queue_config(queue, 'ticker_paused', 'true')` | Convenience |

The PgQ-style API (`insert_event`, `next_batch`, `get_batch_events`,
`finish_batch`, `event_retry`) remains fully available for users who need
fine-grained control.

---

## 5. Observability

### 5.1 SQL Metrics Views

**`pgque.queue_stats()`** -- real-time queue health:

```sql
CREATE FUNCTION pgque.queue_stats()
RETURNS TABLE (
    queue_name          text,
    queue_id            int4,
    depth               bigint,         -- events pending across all consumers
    oldest_msg_age      interval,       -- age of oldest unconsumed event
    consumers           int4,           -- number of registered consumers
    events_per_sec      numeric,        -- throughput estimate (ticks-based)
    cur_table           int4,           -- current rotation table index
    rotation_age        interval,       -- time since last rotation
    rotation_period     interval,       -- configured rotation period
    ticker_paused       boolean,
    last_tick_time      timestamptz,
    last_tick_id        bigint,
    dlq_count           bigint          -- dead letter queue entries
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        q.queue_name,
        q.queue_id,
        coalesce(
            (SELECT max(t_cur.tick_event_seq) - min(t_sub.tick_event_seq)
             FROM pgque.subscription s
             JOIN pgque.tick t_sub ON t_sub.tick_queue = q.queue_id
                 AND t_sub.tick_id = s.sub_last_tick
             CROSS JOIN LATERAL (
                 SELECT tick_event_seq FROM pgque.tick
                 WHERE tick_queue = q.queue_id
                 ORDER BY tick_id DESC LIMIT 1
             ) t_cur
             WHERE s.sub_queue = q.queue_id
            ), 0)::bigint AS depth,
        (SELECT now() - min(t.tick_time)
         FROM pgque.subscription s
         JOIN pgque.tick t ON t.tick_queue = q.queue_id
             AND t.tick_id = s.sub_last_tick
         WHERE s.sub_queue = q.queue_id
        ) AS oldest_msg_age,
        (SELECT count(*)::int4 FROM pgque.subscription
         WHERE sub_queue = q.queue_id) AS consumers,
        (SELECT CASE
            WHEN t2.tick_time = t1.tick_time THEN 0
            ELSE (t2.tick_event_seq - t1.tick_event_seq)::numeric
                / extract(epoch from t2.tick_time - t1.tick_time)
         END
         FROM pgque.tick t1, pgque.tick t2
         WHERE t1.tick_queue = q.queue_id AND t2.tick_queue = q.queue_id
           AND t2.tick_id = (SELECT max(tick_id) FROM pgque.tick
                             WHERE tick_queue = q.queue_id)
           AND t1.tick_id = t2.tick_id - 1
        ) AS events_per_sec,
        q.queue_cur_table,
        now() - q.queue_switch_time AS rotation_age,
        q.queue_rotation_period,
        q.queue_ticker_paused,
        (SELECT max(tick_time) FROM pgque.tick
         WHERE tick_queue = q.queue_id) AS last_tick_time,
        (SELECT max(tick_id) FROM pgque.tick
         WHERE tick_queue = q.queue_id) AS last_tick_id,
        (SELECT count(*) FROM pgque.dead_letter
         WHERE dl_queue_id = q.queue_id)::bigint AS dlq_count
    FROM pgque.queue q
    ORDER BY q.queue_name;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

**`pgque.consumer_stats()`** -- per-consumer metrics:

```sql
CREATE FUNCTION pgque.consumer_stats()
RETURNS TABLE (
    queue_name      text,
    consumer_name   text,
    lag             interval,       -- time behind latest tick
    pending_events  bigint,         -- estimated events to process
    last_seen       timestamptz,    -- sub_active timestamp
    batch_active    boolean,        -- has an open batch
    batch_id        bigint          -- current batch ID (NULL if none)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        q.queue_name,
        c.co_name,
        now() - t.tick_time AS lag,
        coalesce(
            (SELECT max(t2.tick_event_seq) FROM pgque.tick t2
             WHERE t2.tick_queue = q.queue_id) - t.tick_event_seq,
            0)::bigint AS pending_events,
        s.sub_active,
        s.sub_batch IS NOT NULL,
        s.sub_batch
    FROM pgque.subscription s
    JOIN pgque.queue q ON q.queue_id = s.sub_queue
    JOIN pgque.consumer c ON c.co_id = s.sub_consumer
    LEFT JOIN pgque.tick t ON t.tick_queue = s.sub_queue
        AND t.tick_id = s.sub_last_tick
    ORDER BY q.queue_name, c.co_name;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

**`pgque.queue_health()`** -- operational diagnostics:

```sql
CREATE FUNCTION pgque.queue_health()
RETURNS TABLE (
    queue_name  text,
    check_name  text,
    status      text,    -- 'ok', 'warning', 'critical'
    detail      text
) AS $$
BEGIN
    -- Check: ticker is running (handle queues with no ticks yet)
    RETURN QUERY
    SELECT q.queue_name, 'ticker_running'::text,
        CASE WHEN max(t.tick_time) IS NULL THEN 'critical'
             WHEN now() - max(t.tick_time) > interval '10 seconds'
             THEN 'critical' ELSE 'ok' END,
        'Last tick: ' || coalesce(max(t.tick_time)::text, 'never')
    FROM pgque.queue q
    LEFT JOIN pgque.tick t ON t.tick_queue = q.queue_id
    WHERE NOT q.queue_ticker_paused
    GROUP BY q.queue_name;

    -- Check: consumer lag
    RETURN QUERY
    SELECT q.queue_name,
        ('consumer_lag:' || c.co_name)::text,
        CASE
            WHEN now() - t.tick_time > q.queue_rotation_period THEN 'critical'
            WHEN now() - t.tick_time > q.queue_rotation_period / 2 THEN 'warning'
            ELSE 'ok'
        END,
        c.co_name || ' lag: ' || (now() - t.tick_time)::text
    FROM pgque.subscription s
    JOIN pgque.queue q ON q.queue_id = s.sub_queue
    JOIN pgque.consumer c ON c.co_id = s.sub_consumer
    JOIN pgque.tick t ON t.tick_queue = s.sub_queue AND t.tick_id = s.sub_last_tick;

    -- Check: rotation overdue
    RETURN QUERY
    SELECT q.queue_name, 'rotation_health'::text,
        CASE
            WHEN q.queue_switch_step2 IS NULL THEN 'warning'
            WHEN now() - q.queue_switch_time > q.queue_rotation_period * 2
                THEN 'warning'
            ELSE 'ok'
        END,
        CASE
            WHEN q.queue_switch_step2 IS NULL THEN 'mid-rotation (step2 pending)'
            ELSE 'last rotation: ' || q.queue_switch_time::text
        END
    FROM pgque.queue q;

    -- Check: DLQ growing
    RETURN QUERY
    SELECT q.queue_name, 'dlq_health'::text,
        CASE
            WHEN count(dl.*) > 1000 THEN 'warning'
            WHEN count(dl.*) > 0 THEN 'ok'
            ELSE 'ok'
        END,
        count(dl.*)::text || ' dead letter events'
    FROM pgque.queue q
    LEFT JOIN pgque.dead_letter dl ON dl.dl_queue_id = q.queue_id
    GROUP BY q.queue_name;

    -- Check: pg_cron jobs
    RETURN QUERY
    SELECT 'system'::text, 'pg_cron_ticker'::text,
        CASE WHEN cfg.ticker_job_id IS NULL THEN 'critical'
             ELSE 'ok' END,
        CASE WHEN cfg.ticker_job_id IS NULL
             THEN 'ticker job not scheduled'
             ELSE 'job_id=' || cfg.ticker_job_id::text END
    FROM pgque.config cfg;

    RETURN QUERY
    SELECT 'system'::text, 'pg_cron_maint'::text,
        CASE WHEN cfg.maint_job_id IS NULL THEN 'critical'
             ELSE 'ok' END,
        CASE WHEN cfg.maint_job_id IS NULL
             THEN 'maint job not scheduled'
             ELSE 'job_id=' || cfg.maint_job_id::text END
    FROM pgque.config cfg;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

**Historical metrics:**

```sql
-- Throughput over time (bucketed)
pgque.throughput(queue_name text, period interval, bucket_size interval)
    RETURNS TABLE (bucket_start timestamptz, events bigint, events_per_sec numeric)
    -- Uses tick history to compute per-bucket throughput

-- Latency percentiles (estimated from tick-to-consume gap)
pgque.latency_percentiles(queue_name text, consumer_name text, period interval)
    RETURNS TABLE (p50 interval, p95 interval, p99 interval)

-- Error rate (retries + DLQ per time period)
pgque.error_rate(queue_name text, period interval, bucket_size interval)
    RETURNS TABLE (bucket_start timestamptz, retries bigint, dead_letters bigint)
```

**Operational views:**

```sql
-- Messages currently being processed
pgque.in_flight(queue_name text)
    RETURNS TABLE (consumer_name text, batch_id bigint,
                   batch_age interval, estimated_events bigint)

-- Consumers that haven't processed in a long time
pgque.stuck_consumers(threshold interval DEFAULT '1 hour')
    RETURNS TABLE (queue_name text, consumer_name text,
                   lag interval, last_active timestamptz)
```

### 5.2 OpenTelemetry Integration

pgque provides OTel-compatible metrics via SQL functions that can be polled by
an OTel Collector sidecar or a pg_cron job that pushes to a collector.

**OTel Metric Mapping:**

| OTel Metric Name | Type | SQL Source |
|---|---|---|
| `pgque.queue.depth` | Gauge | `queue_stats().depth` |
| `pgque.queue.oldest_message_age_seconds` | Gauge | `queue_stats().oldest_msg_age` |
| `pgque.message.sent_total` | Counter | Tick event_seq delta |
| `pgque.message.acked_total` | Counter | Derived from `finish_batch` calls |
| `pgque.message.nacked_total` | Counter | `retry_queue` insertions |
| `pgque.message.dead_lettered` | Gauge | `dead_letter` current count (resets on `dlq_purge`) |
| `pgque.consumer.lag_seconds` | Gauge | `consumer_stats().lag` |
| `pgque.consumer.pending_events` | Gauge | `consumer_stats().pending_events` |
| `pgque.processing_latency_seconds` | Histogram | Consumer-side (client library) |
| `pgque.batch.size` | Histogram | Consumer-side (client library) |
| `pgque.ticker.duration_seconds` | Gauge | pg_cron `run_details` |

**OTel metrics export function** (called by pg_cron or external poller):

```sql
CREATE FUNCTION pgque.otel_metrics()
RETURNS TABLE (
    metric_name text,
    metric_type text,       -- 'gauge', 'counter'
    value       numeric,
    labels      jsonb       -- {"queue": "orders", "consumer": "processor"}
) AS $$
BEGIN
    -- Queue depth gauges
    RETURN QUERY
    SELECT 'pgque.queue.depth'::text, 'gauge'::text,
           qs.depth::numeric,
           jsonb_build_object('queue', qs.queue_name)
    FROM pgque.queue_stats() qs;

    -- Consumer lag gauges
    RETURN QUERY
    SELECT 'pgque.consumer.lag_seconds'::text, 'gauge'::text,
           extract(epoch from cs.lag)::numeric,
           jsonb_build_object('queue', cs.queue_name,
                              'consumer', cs.consumer_name)
    FROM pgque.consumer_stats() cs;

    -- DLQ counters
    RETURN QUERY
    SELECT 'pgque.message.dead_lettered'::text, 'gauge'::text,
           qs.dlq_count::numeric,
           jsonb_build_object('queue', qs.queue_name)
    FROM pgque.queue_stats() qs;

    -- Events per sec gauges
    RETURN QUERY
    SELECT 'pgque.queue.throughput'::text, 'gauge'::text,
           coalesce(qs.events_per_sec, 0),
           jsonb_build_object('queue', qs.queue_name)
    FROM pgque.queue_stats() qs;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = pgque, pg_catalog;
```

**OTel Traces:**

Trace propagation uses `ev_extra1` (or a dedicated field) to carry trace
context. The pattern:

1. **Producer:** Client library serializes W3C traceparent into `ev_extra1`
   before calling `pgque.send()`
2. **Queue wait:** Duration between `ev_time` and consumer `receive()` is the
   queue span
3. **Consumer:** Client library extracts traceparent from `ev_extra1`, creates
   a child span for processing

This requires no SQL-side changes -- trace propagation is entirely in the
client libraries (section 6).

**OTel Logs:**

Structured log events for key operations:

```sql
CREATE FUNCTION pgque.log_event(
    i_level text, i_component text, i_message text, i_attrs jsonb DEFAULT '{}')
RETURNS void AS $$
BEGIN
    RAISE LOG 'pgque.%.% %', i_component, i_level, i_message
        USING DETAIL = i_attrs::text;
END;
$$ LANGUAGE plpgsql;
```

Operations that emit log events: queue creation/deletion, consumer
registration/unregistration, DLQ insertions, rotation steps, ticker anomalies
(negative event counts, stale snapshots).

### 5.3 Export Architecture

```
┌──────────────────────────────────────────────────────┐
│ PostgreSQL                                            │
│                                                       │
│  pgque.otel_metrics()  ──> JSON rows                  │
│  pgque.queue_health()  ──> health check rows          │
│                                                       │
│  pg_cron (every 15s):                                │
│    SELECT * FROM pgque.otel_metrics()                 │
│    -> write to pgque.metrics_buffer (optional)        │
│                                                       │
└────────────────────┬─────────────────────────────────┘
                     │
                     │  SQL poll (OTLP push from sidecar)
                     │  or pg_cron -> http_post (pg_net)
                     │
              ┌──────▼──────┐
              │  OTel        │
              │  Collector   │
              └──────┬───────┘
                     │
         ┌───────────┼───────────┐
         ▼           ▼           ▼
    Prometheus   Grafana     Datadog
    / Thanos     Tempo       / etc.
```

Two export paths:

1. **Sidecar poller:** An OTel Collector with a SQL receiver polls
   `pgque.otel_metrics()` every 15 seconds and converts rows to OTLP.
2. **pg_cron + pg_net:** For environments where a sidecar is not feasible,
   pg_cron calls a function that formats OTLP JSON and pushes via `pg_net`
   (HTTP from inside PostgreSQL). This is the fully self-contained option.

---

## 6. Client Libraries

Each library wraps pgque's SQL API into idiomatic patterns. The SQL layer
handles all queue semantics; the client library handles connection lifecycle,
error recovery, and developer experience.

### 6.1 Common Architecture

All client libraries share the same structure:

```
┌─────────────────────────────────────────┐
│ Application Code                         │
│   producer.send("orders", payload)       │
│   consumer.on("order.created", handler)  │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ pgque Client Library                      │
│   - Connection pool awareness            │
│   - Graceful shutdown (drain + ack)      │
│   - Auto-reconnect with backoff          │
│   - OTel trace propagation               │
│   - Typed event dispatch                 │
│   - Batch transaction management         │
│   - LISTEN/NOTIFY wakeup + poll fallback │
└──────────────┬──────────────────────────┘
               │ SQL
┌──────────────▼──────────────────────────┐
│ PostgreSQL                               │
│   pgque.send() / pgque.receive()          │
│   pgque.ack() / pgque.nack()             │
└──────────────────────────────────────────┘
```

**Common features across all libraries:**

| Feature | Description |
|---|---|
| Connection pooling | Works with connection poolers (PgBouncer, Supavisor). Advisory locks and LISTEN require session-mode pooling; document this clearly. |
| Graceful shutdown | On SIGTERM/SIGINT: stop accepting new batches, finish current batch, ack, exit. No orphaned batches. |
| Auto-reconnect | Exponential backoff (100ms, 200ms, 400ms, ..., 30s cap). On reconnect, any in-progress batch is automatically redelivered by pgque. |
| OTel propagation | Inject W3C traceparent into `ev_extra1` on send. Extract and create child span on receive. |
| Batch transactions | The library wraps `receive -> process -> ack` in a database transaction. Crash = rollback = batch redelivered. |
| Health check | `library.health()` calls `pgque.queue_health()` and returns structured result. |

### 6.2 Python -- `pgque-py`

Built on `psycopg` (v3). Feels like a Python library, not a SQL wrapper.

**Producer:**

```python
import pgque

conn = pgque.connect("postgresql://localhost/mydb")

# Simple send
conn.send("orders", {"order_id": 42, "total": 99.95})

# Typed send
conn.send("orders", type="order.created",
          payload={"order_id": 42})

# Batch send (single transaction)
with conn.batch("orders") as batch:
    for order in orders:
        batch.send("order.created", order.to_dict())

# Delayed send
conn.send_at("orders", "reminder.send",
             {"user_id": 7}, delay=timedelta(hours=24))
```

**Consumer:**

```python
import pgque

consumer = pgque.Consumer(
    "postgresql://localhost/mydb",
    queue="orders",
    name="order_processor",
    poll_interval=30,       # seconds (fallback if NOTIFY missed)
    max_retries=5,
)

@consumer.on("order.created")
def handle_order(msg: pgque.Message):
    process_order(msg.payload)
    # Auto-acked if no exception raised

@consumer.on("order.created")
def handle_order_explicit(msg: pgque.Message):
    try:
        process_order(msg.payload)
    except TransientError:
        msg.nack(retry_after=60)  # retry in 60s
    except PermanentError:
        msg.dead_letter("invalid payload")

consumer.start()  # Blocks, processes until SIGTERM
```

**Internals:**
- `consumer.start()` runs: `LISTEN pgque_orders` + poll loop
- Each iteration: `SELECT * FROM pgque.receive('orders', 'order_processor', 100)`
- For each message: dispatch to registered handler based on `type`
- After batch: `SELECT pgque.ack(batch_id)` (in same TX as processing)
- On handler exception: `SELECT pgque.nack(batch_id, msg_id, 60)`

### 6.3 Go -- `pgque-go`

Built on `pgx/v5`. Follows Go conventions (context, interfaces, struct tags).

```go
package main

import (
    "context"
    "github.com/pgque/pgque-go"
)

func main() {
    client, _ := pgque.Connect(ctx, "postgresql://localhost/mydb")

    // Producer
    client.Send(ctx, "orders", pgque.Event{
        Type:    "order.created",
        Payload: Order{ID: 42, Total: 99.95},
    })

    // Consumer
    consumer := client.NewConsumer("orders", "order_processor",
        pgque.WithPollInterval(30 * time.Second),
        pgque.WithMaxRetries(5),
    )

    consumer.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
        var order Order
        msg.Decode(&order)
        return processOrder(ctx, order)
        // return nil = ack, return error = nack with retry
    })

    consumer.Start(ctx)  // blocks until context cancelled
}
```

### 6.4 Node.js -- `pgque-js`

Built on `pg` (node-postgres). TypeScript-first.

```typescript
import { PgqueClient } from 'pgque-js';

const client = new PgqueClient('postgresql://localhost/mydb');

// Producer
await client.send('orders', {
  type: 'order.created',
  payload: { orderId: 42, total: 99.95 },
});

// Consumer
const consumer = client.consumer('orders', 'order_processor', {
  pollInterval: 30_000,
  maxRetries: 5,
});

consumer.on('order.created', async (msg) => {
  await processOrder(msg.payload);
  // auto-acked on success, nacked on thrown error
});

await consumer.start();
```

### 6.5 Ruby -- `pgque-rb`

Built on `pg` gem. Rails-friendly.

```ruby
require 'pgque'

client = Pgque::Client.new("postgresql://localhost/mydb")

# Producer
client.send("orders", type: "order.created",
            payload: { order_id: 42, total: 99.95 })

# Consumer (standalone)
consumer = Pgque::Consumer.new(
  client, queue: "orders", name: "order_processor",
  poll_interval: 30, max_retries: 5
)

consumer.on("order.created") do |msg|
  OrderService.process(msg.payload)
end

consumer.start  # blocks

# Rails integration (ActiveJob adapter)
class OrderJob < ApplicationJob
  queue_as :orders

  def perform(order_id)
    Order.find(order_id).process!
  end
end
```

---

## 7. Advanced Patterns

### 7.1 Transactional Receive (Exactly-Once Processing)

The default `receive` -> `ack` pattern provides at-least-once delivery. For
exactly-once processing within a database transaction:

```sql
BEGIN;
    -- Receive messages (inside the transaction)
    SELECT * FROM pgque.receive('orders', 'processor', 100)
    INTO TEMP msgs;

    -- Process: write results to application tables (same TX)
    INSERT INTO processed_orders (order_id, status)
    SELECT (payload::jsonb->>'order_id')::int, 'done'
    FROM msgs;

    -- Ack the batch (same TX)
    SELECT pgque.ack(
        (SELECT DISTINCT batch_id FROM msgs LIMIT 1)
    );
COMMIT;
-- If anything fails, entire TX rolls back:
-- application writes AND ack are both undone.
-- Next receive() returns the same batch.
```

Client library support:

```python
@consumer.on("order.created", transactional=True)
def handle_order(msg, conn):
    # conn is the same connection/TX as the receive + ack
    conn.execute(
        "INSERT INTO processed_orders (order_id) VALUES (%s)",
        [msg.payload["order_id"]]
    )
    # auto-committed with ack if no exception
```

### 7.2 FIFO with Ordering Keys

PgQ processes events in `ev_id` order within a batch (the `ORDER BY 1` in
`batch_event_sql`). For strict per-entity ordering:

```python
# Producer: use ev_extra1 as ordering key
conn.send("orders", type="order.updated",
          payload=order_data,
          extra1=f"customer:{customer_id}")

# Consumer: group by ordering key, process serially within group
@consumer.on("order.updated", ordered_by="extra1")
def handle_order(msg):
    # Framework ensures messages with same extra1 value
    # are processed sequentially, even across batches
    update_customer_state(msg.payload)
```

The ordering guarantee within a single batch is inherent (events are ordered
by `ev_id`). Cross-batch ordering for a single key requires the client library
to track key -> last_processed_batch_id and hold back messages if a previous
batch for the same key is still in flight.

### 7.3 Rate Limiting

Client-side rate limiting using `next_batch_custom`:

```python
consumer = pgque.Consumer(
    dsn, queue="notifications", name="email_sender",
    # Process at most 1 batch per 5 seconds
    min_interval=timedelta(seconds=5),
    # Wait until batch has at least 50 events
    min_count=50,
    # Only process events older than 10 seconds
    min_lag=timedelta(seconds=10),
)
```

These map directly to `pgque.next_batch_custom(queue, consumer,
min_lag, min_count, min_interval)`.

For hard rate limiting (e.g., max 100 emails/minute), implement a token
bucket in the client:

```python
@consumer.on("notification.send",
             rate_limit=pgque.RateLimit(max_per_minute=100))
def send_notification(msg):
    send_email(msg.payload)
    # Framework automatically throttles by sleeping between events
    # Unprocessed events stay in the batch for next iteration
```

### 7.4 Cron / Scheduled Jobs

pgque uses `send_at()` for future delivery, and recommends `pg_cron` for
recurring schedules:

```sql
-- One-time scheduled delivery
SELECT pgque.send_at('reminders', 'reminder.send',
    '{"user_id": 7}'::jsonb,
    now() + interval '24 hours');

-- Recurring jobs: use pg_cron to insert events
SELECT cron.schedule('daily_report',
    '0 9 * * *',
    $$SELECT pgque.send('jobs', 'report.generate',
        '{"type": "daily"}'::jsonb)$$);
```

### 7.5 Message Expiry / TTL

Queue-level TTL causes messages older than the threshold to be silently
skipped during batch retrieval:

```sql
-- Configure TTL on a queue
SELECT pgque.set_queue_config('notifications', 'event_ttl', '7 days');
```

Implementation: `get_batch_events()` adds a filter:

```sql
AND ev_time > now() - queue_event_ttl
```

Messages that expire are not retried and not dead-lettered -- they simply
vanish. This is appropriate for notification-style queues where stale messages
are worthless.

### 7.6 Priority Queues

pgque does not natively support per-message priority within a batch (all events
in a batch are processed in `ev_id` order). For priority processing, use
separate queues:

```sql
SELECT pgque.create_queue('orders_high');
SELECT pgque.create_queue('orders_normal');
SELECT pgque.create_queue('orders_low');
```

The client library's multi-queue consumer processes higher-priority queues
first:

```python
consumer = pgque.MultiQueueConsumer(dsn, [
    ("orders_high", "processor", priority=0),
    ("orders_normal", "processor", priority=1),
    ("orders_low", "processor", priority=2),
])
# Drains orders_high before touching orders_normal
```

Alternatively, store a priority value in `ev_extra1` via `insert_event()`.
The client library sorts messages within a batch by this field before
dispatching. This is client-side sorting, not queue-level priority —
all events within a batch are equal at the PgQ level.

---

## 8. Implementation Plan

Most code already exists in PgQ's PL/pgSQL source. The plan is organized
by what is packaging work vs. new development, broken into sprints with
concrete deliverables and test plans.

### 8.0 PgQ Code Import Strategy

**pgque must not modify PgQ's core source code in-place.** The PgQ engine
(snapshot isolation, batch processing, table rotation, consumer tracking) is
proven code with 15+ years of production validation. We import it as a
dependency and apply transformations mechanically during the build step.

**Approach: git submodule.**

```
pgque/
  pgq/                 -- git submodule pointing to github.com/pgq/pgq
  sql/
    pgque.sql   -- built from pgq/ sources + pgque additions
  build/
    transform.sh        -- mechanical rename + modernization script
```

The `pgq/` submodule pins to a specific PgQ release tag (v3.5.1).
The build script (`transform.sh`) reads PgQ's PL-only source files, applies
the mechanical transformations (schema rename, `txid_*` -> `pg_*`, `xid8`,
`search_path` pinning, cleanup), and concatenates the result with pgque's
new code (modern API, DLQ, delayed events, observability) into
`pgque.sql`.

**Why git submodule:**

- PgQ upstream changes are visible via submodule diff
- Clear separation: pgq code is never edited, only transformed
- Updating to a new PgQ release is a submodule pointer update + re-test
- Build is reproducible: submodule pin + transform script = deterministic output
- License compliance: PgQ's ISC-licensed source is preserved unmodified

**Why not fork/copy:** Copying PgQ files into pgque and editing them
in-place creates a maintenance burden — any upstream fix requires manual
cherry-picking across renamed files. The submodule + transform approach
keeps the upstream relationship clean.

### Sprint 1: Repackaging (2 weeks)

**Nature:** Mechanical transformation. No new logic.

**Deliverables:**
1. Set up PgQ as a git submodule (`pgq/`, pinned to v3.5.1)
2. Create `build/transform.sh` that reads PL-only source files and applies
   all mechanical transformations (items 3-9 below)
3. Global rename: `pgq.` -> `pgque.` (schema prefix in ~40 files)
4. Replace `txid_*` with `pg_*` functions (8 distinct replacements)
5. Replace `bigint` with `xid8` for txid columns (schema + functions)
6. Replace `txid_snapshot` type with `pg_snapshot`
7. Add `SET search_path = pgque, pg_catalog` to all `SECURITY DEFINER` functions
8. Remove `queue_per_tx_limit` column and references
9. Remove `set default_with_oids = 'off'`
10. Remove `maint_operations` pgq_node/Londiste hooks
11. Add `pgque.config` table
12. Build concatenated `pgque.sql` and `pgque-unpgque.sql`
13. Create roles: `pgque_reader`, `pgque_writer`, `pgque_admin`
14. Regression tests: run PgQ's existing test suite against pgque

**Tests for Sprint 1:**
- Every PgQ test case (from `sql/` + `expected/`) passes against pgque after rename
- `\i pgque.sql` is idempotent (run twice, no errors)
- `pgque-unpgque.sql` cleanly removes all objects
- `pgque_reader` can call `get_queue_info()` but not `insert_event()`
- `pgque_writer` can call `insert_event()` but not `drop_queue()`
- `pgque_admin` can call all functions including `drop_queue()`
- All `SECURITY DEFINER` functions have `search_path` pinned (automated grep check)

**Verification:** The test from `sql/switch_plonly.sql` proves the PL/pgSQL
code is correct. After renaming, every PgQ test case should pass identically.

**Line count estimate:** ~4,028 lines to transform. Zero new logic.

### Sprint 2: pg_cron Lifecycle (1 week)

**Nature:** New code, but simple (pg_cron API calls).

**Deliverables:**
1. `pgque.start()` -- creates pg_cron jobs, stores IDs in `pgque.config`
2. `pgque.stop()` -- removes pg_cron jobs
3. `pgque.uninstall()` -- stop + DROP SCHEMA
4. `pgque.status()` -- diagnostic dashboard
5. Graceful degradation when pg_cron not installed
6. LISTEN/NOTIFY in ticker: add `pg_notify()` call

**Tests for Sprint 2:**
- `pgque.start()` creates exactly 2 pg_cron jobs with correct schedules
- `pgque.start()` is idempotent (calling twice does not create duplicate jobs)
- `pgque.stop()` removes both jobs; `pgque.status()` reports no active jobs
- `pgque.status()` returns correct TABLE rows (ticker status, maint status, pg version)
- Without pg_cron installed: `pgque.start()` raises informative error; manual `pgque.ticker()` and `pgque.maint()` still work
- `LISTEN pgque_<queue>` receives notifications after `pgque.ticker()` runs
- `pg_notify` channel name matches `'pgque_' || queue_name` exactly

**Line count estimate:** ~200-300 new lines.

### Sprint 3: Modern API Layer (2 weeks)

**Nature:** New code wrapping existing primitives.

**Deliverables:**
1. `pgque.message` type definition
2. `pgque.send()` / `send_batch()` / `send_at()`
3. `pgque.receive()`
4. `pgque.ack()` / `pgque.nack()`
5. `pgque.subscribe()` / `unsubscribe()`
6. `pgque.dead_letter` table + `event_dead()` + `dlq_inspect/replay/purge`
7. `pgque.delayed_events` table + `maint_deliver_delayed()`
8. `pgque.pause_queue()` / `resume_queue()`
9. `pgque.create_queue()` overload with JSONB options

**Tests for Sprint 3:**
- `pgque.send()` returns a valid event ID; event appears in `get_batch_events()` after tick
- `pgque.send_batch()` inserts all payloads atomically (rollback leaves zero events)
- `pgque.receive()` returns messages with correct fields; returns empty set when no events
- `pgque.ack()` advances consumer position; subsequent `receive()` gets next batch
- `pgque.nack()` with retry count < max schedules retry; event reappears after delay
- `pgque.nack()` with retry count >= max moves event to `dead_letter` table
- `pgque.dlq_replay()` re-inserts event into queue; `dlq_purge()` removes old entries
- `pgque.send_at()` with future timestamp inserts into `delayed_events`; `maint_deliver_delayed()` moves it to queue when due
- `pgque.send_at()` with past timestamp inserts directly into queue
- `pgque.pause_queue()` stops ticker from generating ticks for that queue; `resume_queue()` restarts
- `pgque.subscribe()` / `unsubscribe()` correctly manage consumer registration
- `peek()` is deferred to v2 (semantics need rigorous definition — "peek without claiming" vs "read existing batch" are different operations)

**Line count estimate:** ~500-700 new lines.

### Sprint 4: Observability (1 week)

**Nature:** New code (views and functions over existing tables).

**Deliverables:**
1. `pgque.queue_stats()` function
2. `pgque.consumer_stats()` function
3. `pgque.queue_health()` diagnostic function
4. `pgque.otel_metrics()` export function
5. `pgque.throughput()`, `latency_percentiles()`, `error_rate()` historical functions
6. `pgque.in_flight()`, `pgque.stuck_consumers()` operational functions
7. Log event integration in key operations

**Tests for Sprint 4:**
- `pgque.queue_stats()` returns correct depth, consumer count, and DLQ count for a queue with known state
- `pgque.consumer_stats()` shows correct lag and pending event count
- `pgque.queue_health()` returns 'critical' for a queue with no recent ticks
- `pgque.queue_health()` returns 'warning' for consumer lag > rotation_period / 2
- `pgque.queue_health()` returns 'ok' for healthy queue
- `pgque.otel_metrics()` returns rows with correct metric names, types, and label structure
- `pgque.stuck_consumers()` identifies consumers with lag exceeding threshold
- `pgque.in_flight()` shows open batches with correct age

**Line count estimate:** ~400-600 new lines.

### Sprint 5: Client Libraries — v1: Python + Go only (3-4 weeks)

**Nature:** New code. Python and Go cover the two largest PostgreSQL user bases.
Node.js and Ruby SDKs are deferred to v2 — shipping two solid libraries is
better than four incomplete ones (per reviewer feedback and risk table).

**v1 libraries:**

| Library | Language | DB Driver | Estimated Size |
|---|---|---|---|
| `pgque-py` | Python 3.10+ | psycopg 3 | ~800-1200 lines |
| `pgque-go` | Go 1.21+ | pgx/v5 | ~1000-1500 lines |

**v2 libraries (deferred):**

| Library | Language | DB Driver | Estimated Size |
|---|---|---|---|
| `pgque-js` | TypeScript/Node 18+ | pg / node-postgres | ~800-1200 lines |
| `pgque-rb` | Ruby 3.1+ | pg gem | ~600-1000 lines |

Each library includes:
- Producer class (send, send_batch, send_at)
- Consumer class (receive, ack, nack, event dispatch)
- Connection management (pool, reconnect, graceful shutdown)
- LISTEN/NOTIFY integration
- OTel trace propagation
- Tests

**Tests for Sprint 5 (per library):**
- Producer: send event, verify it arrives in batch after tick
- Consumer: receive + ack cycle completes; unacked batch is redelivered on reconnect
- Graceful shutdown: SIGTERM during batch processing finishes current batch and acks before exit
- LISTEN/NOTIFY: consumer wakes within 100ms of tick (not waiting for full poll interval)
- OTel: traceparent propagated from producer send to consumer receive
- Connection failure: auto-reconnect with backoff; no orphaned batches
- Batch transaction: crash mid-processing rolls back both application writes and ack

### Sprint 6: Testing, Benchmarks, Docs (2 weeks)

**Deliverables:**
1. Full regression suite (SQL-based, PG 14-18 matrix)
2. Performance benchmarks (see SPEC.md section 10 for methodology)
3. CI pipeline (GitHub Actions, multi-PG-version)
4. README, migration guides, API reference
5. Example applications in each language

**Tests for Sprint 6:**
- All SQL tests pass on PG 14, 15, 16, 17, 18
- Benchmark: insert throughput >= 10k events/sec on reference hardware
- Benchmark: sustained load (1 hour) shows zero dead tuple growth in event tables
- Benchmark: consumer latency p50 < 3s, p99 < 5s with default tick interval
- CI: GitHub Actions workflow runs full test suite on push; matrix covers all PG versions
- Documentation: every public function has a docstring; README covers install, quickstart, API reference

### Total Timeline

| Sprint | Duration | Dependencies |
|---|---|---|
| Sprint 1: Repackaging | 2 weeks | None |
| Sprint 2: pg_cron | 1 week | Sprint 1 |
| Sprint 3: Modern API | 2 weeks | Sprint 1 |
| Sprint 4: Observability | 1 week | Sprint 1 |
| Sprint 5: Client libraries | 4-6 weeks | Sprint 3 |
| Sprint 6: Testing + docs | 2 weeks | All |

**Critical path:** Sprint 1 -> Sprint 3 -> Sprint 5 -> Sprint 6 = ~11 weeks.

Sprints 2, 3, and 4 can run in parallel after Sprint 1 completes.
Sprint 5 (client libraries) can start after Sprint 3 defines the API,
and the four languages can be developed simultaneously.

With 2-3 engineers: **~8-10 weeks** to a complete release.

---

## 9. Team / Staffing

pgque is a repackaging + extensions project, not a ground-up build. The core
queue engine (snapshot isolation, batch processing, table rotation) already
exists and has 15+ years of production validation. This means the team is
smaller than a typical queue system build.

### 9.1 Minimum viable team: 2 people

**E1: Senior PostgreSQL engineer.** Owns Sprint 1 (repackaging), Sprint 2
(pg_cron lifecycle), and Sprint 4 (observability). Must understand PgQ
internals, `pg_snapshot` functions, rotation mechanics, and SECURITY DEFINER
hardening. This person writes the SQL that pgque is built on.

**E2: Application developer.** Owns Sprint 3 (modern API) and Sprint 5
(client libraries). Must be comfortable with PL/pgSQL and at least 2 of
Python/Go/Node.js/Ruby. This person builds the developer experience layer.

Sprint 6 (testing, benchmarks, docs) is shared between both engineers.

### 9.2 Week-by-week Gantt (2-person team)

```
Week  E1 (PG internals)               E2 (API + libraries)
────  ───────────────────────────────  ──────────────────────────────
1-2   Sprint 1: Repackaging            Sprint 1: assist with test
      (rename, modernize, build)       porting, CI setup

3     Sprint 2: pg_cron lifecycle      Sprint 3: message type,
      (start/stop/status)              send/receive/ack/nack

4     Sprint 4: queue_stats,           Sprint 3: DLQ, delayed
      consumer_stats, health           events, pause/resume

5     Sprint 4: otel_metrics,          Sprint 5: pgque-py
      historical metrics               (Python library)

6     Sprint 6: SQL regression         Sprint 5: pgque-go
      tests, benchmarks                (Go library)

7     Sprint 6: benchmark run,         Sprint 5: pgque-py + pgque-go
      stress tests                     integration tests, examples

8     Sprint 6: docs, README,          Sprint 6: SDK docs, example
      release packaging                apps, migration guides
```

### 9.3 Three-person variant

Adding a third engineer (E3) compresses the timeline to ~6 weeks:

- **E3: Test / DevOps engineer.** Takes over Sprint 6 duties (CI pipeline,
  multi-PG matrix testing, benchmark harness) starting in week 3. This frees
  E1 and E2 to focus on core development. E3 also writes example applications
  and migration guides.

```
Week  E1 (PG internals)       E2 (API + libraries)     E3 (Test + DevOps)
────  ─────────────────────   ─────────────────────     ────────────────────
1-2   Sprint 1: Repackaging   Sprint 1: assist          CI pipeline setup,
                                                        PG matrix, fixtures

3     Sprint 2: pg_cron       Sprint 3: modern API      Sprint 1 regression
                                                        test validation

4     Sprint 4: observability Sprint 3: DLQ, delayed    Sprint 3 test suite,
                                                        API integration tests

5     Sprint 4: otel metrics  Sprint 5: pgque-py         Benchmark harness,
                                                        sustained load test

6     Docs, release review    Sprint 5: pgque-go          Sprint 5 library
                                                        tests, examples
```

### 9.4 Hiring considerations

The hardest role to fill is E1. The required combination of PgQ internals
knowledge, snapshot function expertise, and PL/pgSQL fluency is rare. If E1
is not available, the project timeline extends because Sprint 1 (the critical
path foundation) cannot begin.

E2 is easier to source — any experienced application developer with PL/pgSQL
exposure and polyglot language skills can ramp up on the modern API layer in
a few days, since it is thin wrappers over well-documented PgQ primitives.

---

## 10. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| PgQ PL/pgSQL code has undiscovered bugs in edge cases | Low | High | PgQ has 15+ years of production use. Run full PgQ test suite against pgque in Sprint 1. Focus testing on snapshot boundary conditions and cross-rotation batches. |
| Schema rename introduces subtle breakage | Medium | Medium | Automated rename with `sed` + comprehensive regression tests. Each function tested individually. Grep for any remaining `pgq.` references in output. |
| `pg_snapshot` function behavior differs from `txid_*` | Very Low | High | Functions are documented aliases since PG13. Test with PG14+ specifically. Compare `pg_current_snapshot()` output with `txid_current_snapshot()` on PG13 (where both exist) to validate equivalence. |
| Modern API adds unexpected overhead vs raw PgQ API | Low | Low | Modern API is thin wrappers (1-2 SQL calls each). Benchmark both `pgque.send()` and `pgque.insert_event()` paths. Overhead should be <5% per call. |
| Client libraries fragment maintenance effort | Medium | Medium | Start with Python + Go only. Add Node.js + Ruby based on demand. Each library is <1500 lines and shares the same architecture, so maintenance burden is proportional and predictable. |
| Tick-based latency (1-2s) deters real-time use cases | Medium | Low | Document clearly in section 2.7 (When NOT to use pgque). LISTEN/NOTIFY reduces to ~100ms. For sub-10ms, recommend graphile-worker or direct LISTEN/NOTIFY. |
| Long-lived transactions degrade rotation and cleanup | Medium | Medium | `pgque.queue_health()` alerts when consumer lag exceeds rotation_period / 2. Documentation includes max lag guidance. Operational runbook defines escalation procedures. Consider `queue_max_consumer_lag` config parameter that triggers warnings. |
| pg_cron not available on a target provider | Low | High | Ticker and maint work from any scheduler. Document cron, systemd timer, `\watch`, and application-loop alternatives. Test all paths. |
| pg_cron 1-second minimum granularity insufficient | Low | Low | PgQ's `pgqd` default was 1-2 seconds anyway. Sub-second ticking has diminishing returns. Document this. |
| Old-style `INHERITS` deprecated in future PG | Very Low | High | INHERITS has been stable for 20+ years. Native partitioning is available but has different semantics for the rotation pattern (see SPEC.md 3.2.5). Monitor PG development. Migration path exists if needed. |
| `retry_queue` bloats under high-retry workloads with MVCC horizon pinning | Low | Low | Add `VACUUM pgque.retry_queue` to the `maint()` cycle. Monitor in benchmarks. Retry queue is small relative to event tables. |

---

## 11. Best Practices

### 11.1 Producers

- **Prefer `pgque.send()` for simplicity, `pgque.insert_event()` for control.**
  `send()` is the right choice for 80% of use cases (JSON payload, auto type).
  Use `insert_event()` when you need to set `ev_extra1..4`, use non-JSON
  payloads, or need maximum insert performance (one fewer function call).

- **Batch sends in transactions for throughput.** Group many `send()` or
  `insert_event()` calls in a single transaction (e.g., 100-1000 per COMMIT).
  This amortizes transaction overhead and is expected to achieve high
  throughput.

- **Use `ev_type` for routing, keep `ev_data` compact.** Consumers dispatch on
  `ev_type`. A well-chosen type (e.g., `order.created`, `user.updated`) lets a
  single queue serve multiple event kinds. Store references (IDs, keys) in
  `ev_data` rather than full payloads when possible.

- **Don't insert events in long-running transactions.** Events become visible
  only after COMMIT. A transaction that runs for 5 minutes and inserts events
  means those events are invisible for 5 minutes — and the ticker cannot
  advance past them.

- **Prefer direct API over triggers for high-throughput queues.**
  `pgque.insert_event()` is faster than CDC triggers because it skips column
  introspection, serialization, and dynamic SQL. Use triggers for CDC (capture
  all changes); use the API when the application knows exactly what event to
  produce.

### 11.2 Consumers

- **Always ack batches.** An unfinished batch blocks the consumer's position.
  Call `pgque.ack(batch_id)` after processing. If you crash before acking, the
  same batch is redelivered on next `pgque.receive()` — this is at-least-once
  delivery. Exactly-once processing requires idempotent consumers.

- **Make handlers idempotent.** If a consumer crashes after processing some
  events but before `ack()`, the entire batch is redelivered. Design handlers
  so that reprocessing an event is harmless (e.g., use upserts, check
  idempotency keys).

- **Use `pgque.nack()` for transient failures, let DLQ handle permanent ones.**
  Don't fail the entire batch because one event has a temporary problem. Nack
  that event (it retries with delay) and ack the batch. After max retries,
  `nack()` automatically moves the event to the dead letter queue.

- **Use LISTEN for low-latency wakeup with polling fallback.** Instead of
  polling `receive()` in a tight sleep loop, `LISTEN pgque_<queue>` and wake
  on notification. Always combine with a polling fallback (e.g., poll every
  30s in case a notification was missed). LISTEN requires session-mode
  connection pooling.

- **Monitor consumer lag via `pgque.queue_health()`.** A consumer falling
  behind blocks table rotation. Set up alerts for lag exceeding half the
  rotation period.

- **Use `next_batch_custom()` for batching control.** `min_count` and
  `min_interval` let you batch small trickles into larger units of work,
  reducing per-batch overhead.

### 11.3 Queue Design

- **One queue per event stream, not one queue per event type.** Put related
  events (`order.created`, `order.updated`, `order.cancelled`) in the same
  queue. Use `ev_type` to distinguish. This gives consumers a consistent,
  ordered view of the stream.

- **Multiple consumers for fan-out.** Each registered consumer gets its own
  independent position. Use this for fan-out: one queue, multiple consumers
  (audit logger, notification sender, analytics pipeline), each processing
  at their own pace.

- **Don't create too many queues.** Each queue has 3 event tables, 2
  sequences, and tick history. The ticker iterates all queues every 1-2
  seconds. 100+ queues is fine; 10,000 queues will slow the ticker.

### 11.4 Operations

- **Let pg_cron handle the ticker.** Don't run external ticker scripts
  alongside pg_cron — they will contend over tick generation and produce
  duplicate ticks.

- **Monitor `pgque.queue_health()`.** It catches ticker stalls, consumer lag,
  rotation issues, and DLQ growth in one call. Wire it into your alerting
  system.

- **Set rotation period > max consumer lag.** If your slowest consumer is 1
  hour behind, rotation period must be > 1 hour. Otherwise rotation is blocked
  indefinitely and you lose pgque's zero-bloat advantage.

- **Event tables are transient.** Don't rely on pgque for long-term event
  storage. Consume events and write to a permanent destination (data
  warehouse, object storage, etc.). TRUNCATE rotation means events are gone
  after the rotation period.

- **TRUNCATE is instant.** Don't worry about event table size during a
  rotation period. 10 million events in a table? TRUNCATE takes <1ms
  regardless of table size.

---

## 12. Relationship to SPEC.md

SPEC.md (v0.7.0-draft, 1616 lines) was written for the "pgque
reimplementation" approach. It contains deep technical analysis that remains
fully relevant to pgque:

| SPEC.md Section | Topic | Relevance to pgque |
|---|---|---|
| 3.2.5 | INHERITS justification for rotation | Directly applicable -- pgque uses same mechanism |
| 3.2.6 | Snapshot-based batch isolation, dual-filter algorithm | Core of pgque, unchanged |
| 3.2.6 | Subtransaction caveats | Same caveats apply |
| 3.2.7 | SECURITY DEFINER hardening rules | Applied in pgque Sprint 1 |
| 3.3 | C-to-PL/pgSQL replacement analysis | Documents exactly what pgque inherits |
| 3.4 | Performance expectations | Same numbers apply |
| 4.3 | pg_cron integration design | pgque uses this design |
| 4.4 | Rotation state machine with recovery rules | Core of pgque maintenance |
| 4.4.1 | Tick cleanup invariant | Applied unchanged |
| 7 | Risk table | Applicable risks carried forward |
| 8 | Migration paths (PGMQ, River, pg-boss, etc.) | Directly applicable with schema `pgque` |
| 9 | Best practices | Applicable with schema rename |
| 10 | Benchmark methodology (incl. Brandur/PlanetScale MVCC analysis) | pgque benchmark plan |

**What changes:**
- SPEC.md section 1 describes a "reimplementation" -- pgque is a repackaging
- SPEC.md section 5 (implementation plan) is replaced by this document's Sprint 1-6
- SPEC.md section 6 (staffing) needs revision (less work = smaller team)
- SPEC.md section 11 (future work) -- several items are now in pgque v1 scope
  (DLQ, delayed events, metrics views)

SPEC.md should be preserved alongside SPECx.md as the reference for PgQ's
internal architecture.

---

## 13. Testing and Admin CLI

### 13.1 SQL Test Suite

pgque ships a comprehensive SQL regression test suite modeled on PgQ's existing
`sql/` + `expected/` structure.

**Test categories:**

| Category | Tests |
|---|---|
| Core lifecycle | `create_queue`, `drop_queue`, `set_queue_config` |
| Event insertion | `insert_event`, `insert_event_raw`, `send`, `send_batch` |
| Ticker | Adaptive frequency, multi-queue, paused queues |
| Batch processing | `next_batch`, `get_batch_events`, `finish_batch`, cursor-based |
| Snapshot correctness | In-flight TX visibility, dual filter, cross-rotation batches |
| Rotation | step1/step2, blocked by slow consumer, concurrent inserts |
| Retry | `event_retry`, `maint_retry_events`, retry count tracking |
| DLQ | `nack` with max retries, `dlq_inspect`, `dlq_replay`, `dlq_purge` |
| Modern API | `send/receive/ack/nack`, delayed delivery, priority |
| Permissions | `pgque_reader`, `pgque_writer`, `pgque_admin` role enforcement |
| pg_cron integration | `start/stop/status`, idempotent start |
| Observability | `queue_stats`, `consumer_stats`, `queue_health`, `otel_metrics` |
| Triggers | `jsontriga` (INSERT/UPDATE/DELETE, pkey detection, ignore, backup) |
| Multi-PG version | PG 14, 15, 16, 17, 18 |

**Testing utilities** (available in pgque for user test suites):

```sql
-- Insert a test event and verify it arrives
pgque.test_send(queue text, payload jsonb)
    RETURNS bigint  -- event_id

-- Consume one event and verify content
pgque.test_consume(queue text, consumer text, expected_type text)
    RETURNS pgque.message

-- Assert queue is empty (no pending events for any consumer)
pgque.assert_empty(queue text)
    RETURNS boolean  -- raises exception if not empty

-- Assert DLQ is empty for a queue
pgque.assert_dlq_empty(queue text)
    RETURNS boolean  -- raises exception if not empty
```

### 13.2 Test Methodology: Red/Green TDD

All new pgque code (pgque-api layer, observability, client libraries) must
be developed using **red/green TDD** where it makes sense:

1. **Red:** Write a failing test that defines the expected behavior
2. **Green:** Write the minimum code to make the test pass
3. **Refactor:** Clean up without changing behavior; tests stay green

**Where TDD applies:**

- All modern API functions (`send`, `receive`, `ack`, `nack`, DLQ, delayed)
- Observability functions (`queue_stats`, `consumer_stats`, `queue_health`)
- Client library producer/consumer classes
- CLI commands
- The `build/transform.sh` pipeline (test that output SQL is valid)

**Where TDD does not apply:**

- pgque-core repackaging (Sprint 1) — PgQ already has tests; we run them
  after transformation and verify they pass. The tests exist before the code.
- Exploratory benchmarks — these inform design, not verify correctness.

**Test-first discipline in SQL:**

```sql
-- Red: test that nack() moves event to DLQ after max retries
-- (write this BEFORE implementing nack)
do $$
declare
    v_msg pgque.message;
    v_dlq_count bigint;
begin
    -- Setup: queue with max_retries=2
    perform pgque.create_queue('test_dlq');
    perform pgque.set_queue_config('test_dlq', 'queue_max_retries', '2');
    perform pgque.subscribe('test_dlq', 'c1');
    perform pgque.send('test_dlq', '{"x":1}'::jsonb);
    perform pgque.ticker();

    -- Simulate 2 prior retries (retry_count=2 >= max_retries=2)
    select * into v_msg from pgque.receive('test_dlq', 'c1', 1);
    -- Forge retry_count to simulate prior retries
    v_msg.retry_count := 2;
    perform pgque.nack(v_msg.batch_id, v_msg, '1 second', 'test failure');
    perform pgque.ack(v_msg.batch_id);

    -- Assert: event is in DLQ
    select count(*) into v_dlq_count from pgque.dead_letter
    where dl_queue_id = (select queue_id from pgque.queue
                         where queue_name = 'test_dlq');
    assert v_dlq_count = 1, 'expected 1 DLQ entry, got ' || v_dlq_count;

    -- Cleanup
    perform pgque.unsubscribe('test_dlq', 'c1');
    perform pgque.drop_queue('test_dlq');
end;
$$;
```

**Unit tests vs. integration tests:** The example above is a **unit test**
— it forges `retry_count` to isolate `nack()`'s DLQ routing logic. This
proves the conditional works but does not test that `ev_retry` actually
increments through the real retry flow (`event_retry_raw` → `maint_retry_events`
→ next batch delivery). Both are needed:

- **Unit test:** forge state, test one function's logic (fast, isolated)
- **Integration test:** full retry cycle (nack → ack → maint → ticker →
  receive), verify `retry_count` increments naturally (slower, end-to-end)

Write the unit test first (TDD red/green). Then write the integration test
as an acceptance test (section 13.3, US-3). Both must pass.

### 13.3 User Stories and Acceptance Tests

These are end-to-end scenarios that verify pgque works as a complete system.
They serve as both **CI acceptance tests** (automated) and **manual
verification paths** for humans or AI agents testing a fresh deployment.

Each story follows a consistent structure: setup, action, verify, teardown.

#### US-1: Basic produce/consume cycle

**As a** developer, **I want to** send a JSON message and receive it,
**so that** I can use pgque as a simple queue.

```
Setup:   install pgque, create queue "orders", subscribe consumer "app"
Action:  send('orders', '{"id":1}'), ticker(), receive('orders','app',10)
Verify:  exactly 1 message returned, payload = '{"id":1}', type = 'default'
         ack(batch_id) succeeds
         subsequent receive() returns empty set
Teardown: drop queue, uninstall
```

#### US-2: Multiple consumers (fan-out)

**As a** platform team, **I want** multiple independent consumers on one
queue, **so that** analytics, notifications, and audit each process the
same events at their own pace.

```
Setup:   create queue "events", subscribe "analytics", "notifier", "audit"
Action:  send 5 events, ticker()
Verify:  each consumer receives all 5 events independently
         acking one consumer does not affect the others
         consumer_stats() shows correct per-consumer lag
```

#### US-3: Retry and DLQ flow

**As a** developer, **I want** failed messages to retry automatically and
land in a dead letter queue after max retries, **so that** transient
failures are handled without manual intervention.

```
Setup:   create queue "jobs" with max_retries=2, subscribe "worker"
Action:  send event, ticker(), receive (retry_count=NULL, coalesced to 0)
         -- Retry cycle (each nack requires: nack → ack → maint → ticker → receive)
         Cycle 1: nack(msg) → ack(batch) → maint() → ticker() → receive
                  (retry_count=1, event_retry_raw incremented it)
         Cycle 2: nack(msg) → ack(batch) → maint() → ticker() → receive
                  (retry_count=2, now >= max_retries)
         Cycle 3: nack(msg) → retry_count=2 >= max_retries=2 → DLQ
                  ack(batch)
Verify:  event is in dead_letter table (not retried again)
         dlq_inspect() shows the event with reason
         dlq_replay() re-inserts it into the queue
         ticker(), receive() gets the replayed event (retry_count reset)
```

#### US-4: Delayed delivery

**As a** developer, **I want to** schedule a message for future delivery,
**so that** I can implement reminders and scheduled tasks.

```
Setup:   create queue "reminders", subscribe "sender"
Action:  send_at('reminders', 'remind', payload, now() + '5 seconds')
Verify:  receive() returns empty immediately
         wait 5+ seconds, call maint() (which runs maint_deliver_delayed)
         ticker()
         receive() now returns the event
```

#### US-5: Batch processing under load

**As an** ETL pipeline, **I want to** process thousands of events per
batch efficiently, **so that** I can keep up with high-throughput
producers.

```
Setup:   create queue "ingest", subscribe "etl"
Action:  insert 10,000 events in a single transaction, ticker()
         receive('ingest', 'etl', 10000)
Verify:  all 10,000 events returned in one batch
         ack completes successfully
         queue_stats() shows depth=0
         no dead tuples in event tables (check pg_stat_user_tables)
```

#### US-6: Graceful rotation under consumer lag

**As an** operator, **I want** table rotation to work correctly even when
a slow consumer is lagging, **so that** the system doesn't lose events.

```
Setup:   create queue "stream" with rotation_period='10 seconds'
         subscribe "fast" and "slow"
Action:  send events, ticker() repeatedly over 30+ seconds
         "fast" consumer: receive+ack every 2 seconds
         "slow" consumer: do not consume at all
Verify:  queue_health() shows 'warning' or 'critical' for "slow"
         rotation is blocked (cannot TRUNCATE tables "slow" reads from)
         "fast" consumer continues to receive normally
         once "slow" catches up and acks, rotation resumes
```

#### US-7: Transactional exactly-once processing

**As a** developer, **I want** my application writes and ack to be in the
same transaction, **so that** a crash leaves the system consistent.

```
Setup:   create queue "payments", subscribe "processor"
         create table processed_payments (id int primary key)
Action:  send event with payment_id=42, ticker()
         BEGIN; receive(); INSERT INTO processed_payments; ack(); COMMIT;
Verify:  processed_payments contains payment_id=42
         receive() returns empty (batch finished)
         -- Crash simulation:
         send event with payment_id=43, ticker()
         BEGIN; receive(); INSERT INTO processed_payments; -- NO COMMIT
         disconnect (simulates crash)
         reconnect, receive() returns payment_id=43 again (redelivered)
```

#### US-8: Install on managed PostgreSQL

**As a** developer on RDS/Cloud SQL/Supabase, **I want to** install pgque
with a single SQL file and start it with pg_cron, **so that** I don't
need DBA access or custom extensions.

```
Setup:   fresh managed PG database with pg_cron enabled
Action:  \i pgque.sql
         select pgque.start()
Verify:  pgque.status() shows ticker and maint running
         create queue, send, ticker, receive, ack all work
         pgque.queue_health() returns 'ok' for all checks
         pgque.stop() removes pg_cron jobs
         pgque.uninstall() removes all objects cleanly
```

#### US-9: Observability and health monitoring

**As an** operator, **I want** to quickly diagnose queue health and
consumer lag, **so that** I can set up alerting and respond to issues.

```
Setup:   create 3 queues with varying load patterns
         one queue healthy, one with lagging consumer, one with DLQ entries
Action:  query queue_stats(), consumer_stats(), queue_health()
Verify:  queue_stats() shows correct depth, throughput, DLQ count
         consumer_stats() shows correct lag per consumer
         queue_health() returns 'ok', 'warning', 'critical' appropriately
         otel_metrics() returns rows with correct metric names and types
         stuck_consumers() identifies the lagging consumer
```

#### US-10: Idempotent install and upgrade

**As an** operator, **I want to** safely re-run the install script,
**so that** upgrades and accidental re-runs don't break anything.

```
Setup:   install pgque, create queues, insert events, subscribe consumers
         note current queue depth and consumer positions
Action:  run \i pgque.sql again
Verify:  no errors
         existing queues and events are preserved (check depth matches)
         consumer positions are preserved (sub_last_tick unchanged)
         all functions work correctly after re-install
         send + ticker + receive + ack cycle works
```

**Implementation note:** This is the hardest test to get right. PgQ's
original source uses plain `CREATE TABLE` / `CREATE FUNCTION` without
idempotency guards. `build/transform.sh` must ensure the install script
uses `CREATE TABLE IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION`,
`CREATE TYPE ... IF NOT EXISTS` (or `DO $$ BEGIN ... EXCEPTION WHEN
duplicate_object ...`), and `CREATE SEQUENCE IF NOT EXISTS`. The test
must verify not just "no errors" but "data survives" — specifically that
queue contents, consumer positions, tick history, config, and DLQ entries
are all preserved across re-install. This is a Sprint 1 deliverable that
deserves dedicated test coverage beyond this user story.

#### Running acceptance tests

**Automated (CI):** Each user story maps to a SQL test file in
`tests/acceptance/`. CI runs them on PG 14-18 matrix. Tests are
self-contained: setup, action, verify, teardown in a single file.

**Manual / AI agent verification:** The stories above are written so that
a human or AI agent can execute them step-by-step against a live database
using `psql`. The setup/action/verify/teardown structure makes each story
independently executable and verifiable without special tooling.

### 13.4 Admin CLI

The `pgque` CLI is a thin wrapper around SQL calls, designed for operators
and CI/CD pipelines. Written in Go (single binary, no dependencies).

```
pgque - PgQ Extended administration tool

Usage:
  pgque <command> [flags]

Connection:
  --dsn, -d     PostgreSQL connection string (or PGQUE_DSN env var)
  --database    Database name (or PGDATABASE)

Commands:
  install       Install pgque schema into database
  upgrade       Upgrade pgque to latest version
  uninstall     Remove pgque schema (requires --force)

  start         Start pg_cron ticker and maintenance jobs
  stop          Stop pg_cron jobs
  status        Show system health, queue stats, consumer lag

  queues        List all queues with depth and health
  consumers     List all consumers with lag and status
  depth         Show queue depth over time (sparkline)

  drain         Wait until queue is empty or timeout
  replay-dlq    Replay dead letter events back into queue
  purge-dlq     Delete old dead letter events

  create-queue  Create a new queue
  drop-queue    Drop a queue (requires --force if consumers exist)
  pause         Pause a queue's ticker
  resume        Resume a queue's ticker

Examples:
  pgque install -d "postgresql://localhost/mydb"
  pgque start
  pgque status
  pgque queues
  pgque depth orders --watch 5s
  pgque consumers orders
  pgque replay-dlq orders --limit 100
  pgque drain orders --timeout 60s
```

**`pgque status` output example:**

```
pgque v1.0.0 | PostgreSQL 16.2 | pg_cron 1.6

System:
  Ticker:  running (job 42, every 2s, last run 1.2s ago)
  Maint:   running (job 43, every 30s, last run 12s ago)

Queues:
  NAME          DEPTH  RATE     ROTATION   CONSUMERS  DLQ   HEALTH
  orders           42  150/s    1h23m ago  3          0     ok
  notifications   831  45/s     0h47m ago  1          12    warning
  audit              0  0/s     1h59m ago  2          0     ok

Consumers:
  QUEUE          CONSUMER           LAG      PENDING  STATUS
  orders         order_processor    2.1s     42       ok
  orders         analytics          15.3s    412      ok
  orders         notifications      1m12s    1831     warning
  notifications  email_sender       3m45s    831      warning
  audit          compliance_log     0.5s     0        ok
```

---

## 14. Migration Paths

SPEC.md section 8 contains detailed migration tables for each alternative
queue system. The mappings apply to pgque with these adjustments:

| SPEC.md reference | pgque equivalent |
|---|---|
| `pgque.insert_event(queue, type, data)` | `pgque.send(queue, type, payload)` or `pgque.insert_event(queue, type, data)` |
| `pgque.next_batch()` + `get_batch_events()` | `pgque.receive(queue, consumer, batch_size)` |
| `pgque.finish_batch()` | `pgque.ack(batch_id)` |
| `pgque.event_retry()` | `pgque.nack(batch_id, msg_id, delay)` |
| Retry queue with max-retry logic in consumer | `pgque.nack()` handles DLQ automatically |
| Schema `pgque` | Schema `pgque` |

### Quick migration reference

**From PgQ:** Schema rename (`pgq` -> `pgque`), remove pgqd, call
`pgque.start()`. Consumer code structure is identical. See SPEC.md 8.1.

**From PGMQ:** Switch from per-message (`pgmq.read` + `pgmq.delete`) to
batch model (`pgque.receive` + `pgque.ack`). The modern API makes this close
to 1:1. See SPEC.md 8.2.

**From River / pg-boss / graphile-worker / Oban:** Switch from
callback-per-job to batch processing. pgque client libraries provide typed
dispatch that feels similar (`consumer.on("type", handler)`). The main
conceptual shift is batch-oriented ack. See SPEC.md 8.3-8.6.

**From DIY SKIP LOCKED:** The strongest migration case. pgque eliminates
the MVCC dead tuple failure mode entirely (see SPEC.md 10.2 for the
Brandur/PlanetScale analysis). See SPEC.md 8.7.

---

## Appendix A: PostgreSQL Version Support

| Version | Status | Notes |
|---------|--------|-------|
| PG 14 | Supported | Minimum. `pg_snapshot` functions available since PG13. |
| PG 15 | Supported | |
| PG 16 | Supported | |
| PG 17 | Supported | |
| PG 18 | Supported | |

## Appendix B: pg_cron Availability

| Provider | pg_cron | Notes |
|----------|---------|-------|
| Amazon RDS / Aurora | Yes | Since PG 12.5 |
| Google Cloud SQL | Yes | Requires flag |
| AlloyDB | Yes | Supported |
| Azure Flexible Server | Yes | Constrained permissions |
| Supabase | Yes | Pre-installed |
| Neon | Yes | Jobs only run when compute active |
| Crunchy Bridge | Yes | Supported |
| Self-hosted | Yes | Install separately |

Without pg_cron, pgque is fully functional -- the ticker and maintenance must
be called from an external scheduler. See SPEC.md 4.3.

## Appendix C: Source File Inventory

PgQ PL-only source files that pgque repackages:

| File | Lines | Purpose |
|---|---|---|
| `structure/tables.sql` | 225 | Schema (all tables, sequences, constraints) |
| `lowlevel_pl/insert_event.sql` | 60 | PL/pgSQL event insertion (replaces C) |
| `lowlevel_pl/jsontriga.sql` | 318 | JSON CDC trigger (replaces C) |
| `lowlevel_pl/logutriga.sql` | 326 | URL-encoded CDC trigger (replaces C) |
| `lowlevel_pl/sqltriga.sql` | 363 | SQL fragment CDC trigger (replaces C) |
| `functions/pgq.ticker.sql` | 165 | Ticker with adaptive frequency |
| `functions/pgq.batch_event_sql.sql` | 133 | Snapshot-based batch query builder |
| `functions/pgq.batch_event_tables.sql` | 67 | Determines tables for a batch |
| `functions/pgq.create_queue.sql` | 81 | Queue creation |
| `functions/pgq.drop_queue.sql` | 82 | Queue deletion |
| `functions/pgq.register_consumer.sql` | 129 | Consumer registration |
| `functions/pgq.unregister_consumer.sql` | 78 | Consumer unregistration |
| `functions/pgq.next_batch.sql` | 219 | Batch acquisition (3 variants) |
| `functions/pgq.get_batch_events.sql` | 39 | Batch event retrieval |
| `functions/pgq.get_batch_cursor.sql` | 117 | Cursor-based batch retrieval |
| `functions/pgq.finish_batch.sql` | 36 | Batch completion |
| `functions/pgq.event_retry.sql` | 78 | Event retry |
| `functions/pgq.event_retry_raw.sql` | 67 | Low-level retry insertion |
| `functions/pgq.batch_retry.sql` | 53 | Batch-level retry |
| `functions/pgq.maint_rotate_tables.sql` | 119 | Two-phase table rotation |
| `functions/pgq.maint_retry_events.sql` | 45 | Retry event re-insertion |
| `functions/pgq.maint_operations.sql` | 129 | Maintenance orchestration |
| `functions/pgq.maint_tables_to_vacuum.sql` | 57 | Vacuum scheduling |
| `functions/pgq.set_queue_config.sql` | 59 | Runtime config changes |
| `functions/pgq.get_queue_info.sql` | 141 | Queue info |
| `functions/pgq.get_consumer_info.sql` | 135 | Consumer info |
| `functions/pgq.get_batch_info.sql` | 53 | Batch info |
| `functions/pgq.grant_perms.sql` | 99 | Permission management |
| `functions/pgq.tune_storage.sql` | 48 | Storage parameter tuning |
| `functions/pgq.force_tick.sql` | 49 | Force immediate tick |
| `functions/pgq.find_tick_helper.sql` | 78 | Custom tick finding |
| `functions/pgq.seq_funcs.sql` | 65 | Sequence utilities |
| `functions/pgq.quote_fqname.sql` | 36 | Name quoting |
| `functions/pgq.current_event_table.sql` | 44 | Current table lookup |
| `functions/pgq.upgrade_schema.sql` | 49 | Schema migration |
| `functions/pgq.version.sql` | 16 | Version string |
| `structure/grants.sql` | 13 | Default grants |
| **Total** | **4,028** | |

New code added by pgque (estimated):

| Component | Estimated Lines |
|---|---|
| Lifecycle (start/stop/status/uninstall) | 200-300 |
| Modern API (send/receive/ack/nack/subscribe) | 300-400 |
| DLQ (table + functions) | 150-200 |
| Delayed events (table + functions) | 100-150 |
| Observability (stats/health/otel) | 400-600 |
| Testing utilities | 100-150 |
| **Total new PL/pgSQL** | **~1,250-1,800** |
