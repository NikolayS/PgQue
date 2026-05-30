# Durable Execution on PgQue — Feasibility & Adoption Study

- **Status:** Brainstorm / decision input (not approved scope)
- **Date:** 2026-05-30
- **Question:** Should PgQue extend beyond a queue into a **durable-workflow /
  durable-execution engine on Postgres**, the way DBOS and absurd have? What are
  the realistic chances of adoption success if we go that route?
- **Companion reading:** `blueprints/SPECx.md` §2.3 (workflow engines are
  deliberately treated as a *different category* today), `CLAUDE.md` Key Design
  Rules #2 (the PgQ engine is sacred) and #3 (modern API must reduce cleanly to
  PgQ primitives).

This study was produced after a deep review of the Hacker News thread
["Building durable workflows on Postgres"](https://news.ycombinator.com/item?id=48313530)
and a parallel investigation of the six systems that thread orbits around:
**DBOS, absurd, Temporal, Restate, Rivet, and Gadget's Silo**.

---

## 1. Verdict up front

**Do not pivot PgQue into a Temporal/DBOS-style durable-execution platform.**
Going head-to-head as a general "durable workflows on Postgres" engine is a
**late, undifferentiated, SDK-heavy bet** in a category that already has a
well-distributed Postgres-native incumbent (absurd) and a celebrity-founder
incumbent (DBOS). PgQue's signature advantage — zero-bloat snapshot+TRUNCATE
rotation — **does not transfer** to the workflow layer, which needs per-run
exclusive claiming, not batch rotation.

**Do pursue the adjacent, well-leveraged slice:** *transactional durable
queues + checkpointed steps*, shipped as an **optional, experimental
`pgque-api` layer** that reduces to PgQ primitives plus one new
claim/lease table. This stays inside PgQue's identity ("the best zero-bloat
Postgres queue, now with durable steps"), exploits the one capability DBOS
under-documents (atomic enqueue inside the caller's own transaction), and
defers the expensive part (multi-language deterministic-replay runtimes) until
demand is proven.

Net: **moderate-to-low feasibility for a standalone workflow-engine play;
moderate-to-high feasibility for a focused "durable steps" extension of the
queue.** The recommendation is the latter, phased and explicitly experimental.

---

## 2. Is the category real? (Yes.)

Durable execution is a funded, growing category, and the "just Postgres"
variant specifically is where the energy is:

| System | Model | License | Stars (May 2026) | Funding / backing |
|---|---|---|---|---|
| **Temporal** | Separate Go cluster, event-sourced replay, Cassandra/MySQL/PG | MIT | ~20.6k | ~$350M total, $1.72B valuation |
| **DBOS** | Embedded library, Postgres system-DB, checkpoint+replay | MIT (Transact) / proprietary (Conductor) | ~3.5k across 4 SDKs | $8.5M seed; Stonebraker + Zaharia |
| **absurd** | Single SQL file + thin SDK, SKIP-LOCKED claim/lease, checkpoint-replay | Apache-2.0 | ~1.95k | Armin Ronacher / Earendil |
| **Restate** | Self-contained Rust binary, RocksDB+log, journaling-replay | BSL 1.1 → Apache | ~3.9k | $7M seed (Redpoint); ex-Flink team |
| **Rivet** | Actor platform, Postgres/RocksDB/FoundationDB | Apache-2.0 | ~5.6k | YC W23 |
| **Silo (Gadget)** | Rust broker on SlateDB/object storage | MIT | ~31 (prototype) | Internal Gadget project |

Signals worth internalizing:

- **The market keeps asking "why not just Postgres?"** Restate (own RocksDB
  store, BSL) took repeated HN criticism on exactly this. Rivet hedged back
  toward Postgres as a self-host backend. That recurring question *is* PgQue's
  thesis — but absurd already answered it first in the workflow space.
- **Licensing trust is a real axis.** Restate's BSL drew loud "open source is
  misleading" criticism; Rivet's Apache-2.0 drew none. PgQue (Apache-2.0,
  literally "your own Postgres") inherits maximum trust by default.
- **The wedge against Temporal is operational, not technical.** Every
  competitor's pitch is the same: *don't run a second distributed system; reuse
  the database you already operate.* The complaints about Temporal are
  determinism learning curve, immutable shard-count decisions, and the
  Cassandra/Elasticsearch operational floor — not correctness.
- **AI agents are the current demand driver.** DBOS, absurd, Restate, and
  Temporal are all repositioning durable execution as the substrate for
  long-running LLM agent loops. absurd's canonical example is an agent loop;
  Temporal's Series C narrative is "durable AI workloads."

So the category is real and PgQue's "Postgres-native, OSI-licensed, no new
infra" framing is genuinely well-aligned with where the market is pulling. The
problem is not the thesis — it is **who already occupies the exact niche**.

---

## 3. The technical crux: a concurrency-model mismatch

This is the single most important finding of the study.

Every durable-execution engine examined (DBOS, absurd, Silo, and Temporal's
matching service) claims work units with **`SELECT … FOR UPDATE SKIP LOCKED` +
a time-limited lease**: one run is claimed exclusively by one worker, the lease
auto-extends while the worker checkpoints, and if the worker dies the lease
expires and another worker steals the run. Sleep/await is modelled as
`available_at` re-scheduling on a per-run row.

**PgQue does not work this way and must not be made to.** PgQ's engine is
*snapshot + TRUNCATE batch rotation*: consumers read all events committed
between two ticks using `pg_snapshot` visibility, and old event tables are
rotated and TRUNCATEd wholesale. There is:

- no per-item exclusive claim (consumption is cooperative, batch-oriented,
  at-least-once);
- no lease / steal-on-crash for an individual message;
- no per-row `available_at` rescheduling;
- and — critically — **a hard conflict with long-lived state**: a workflow that
  `sleep`s for a week or `awaitEvent`s indefinitely must persist its run and
  checkpoints far longer than any rotating event window survives. An open batch
  already blocks rotation; a long-running workflow on the rotation tables would
  be pathological.

`CLAUDE.md` Rule #2 ("the PgQ engine is sacred") forecloses retrofitting
rotation to behave like a claim/lease queue. So a durable layer on PgQue would
require a **new SKIP-LOCKED claim/lease table living *beside* the PgQ engine**,
not on top of it. That means **PgQue would carry two concurrency models at
once** — the rotation engine for streaming/CDC/fan-out, and a claim/lease engine
for durable runs.

This is the crux of the whole decision:

> PgQue's marketing identity is "the one Postgres queue that never bloats
> because it uses rotation instead of SKIP LOCKED + DELETE." A durable-execution
> layer is, by necessity, **a SKIP-LOCKED + UPDATE engine** — exactly the
> mechanism PgQue defines itself against. The zero-bloat differentiator does not
> apply to the workflow tables; they will accumulate dead tuples and need VACUUM
> like everyone else's (mitigated by partition-detach, as absurd does).

The latency objection, by contrast, **is no longer real**: PgQue 0.2.0 ticks at
a 100 ms cadence (`ticker_loop()` runs `pgque.ticker()` every
`tick_period_ms`, default 100 ms, committing between iterations), so end-to-end
delivery is sub-second — squarely in the same range as absurd's and DBOS's
worker polling loops. Latency is not what would hold a durable layer back.

---

## 4. What is reusable vs. net-new

**Reusable / aligned (low cost):**

- **Single-file, anti-extension, managed-PG install.** absurd's `absurd.sql`
  philosophy is identical to PgQue's `\i pgque.sql`. Validated approach.
- **pg_cron for maintenance.** absurd uses pg_cron for partition
  provisioning/cleanup/detach; PgQue already uses pg_cron for the ticker and
  `maint()`. Same muscle.
- **The data model** (tasks / runs / steps / checkpoints / events / waits) is
  clean and orthogonal to how raw events are queued underneath. PgQue could
  adopt absurd's schema shape almost verbatim as a reference.
- **The no-determinism, checkpoint-replay, task-level-retry design.** This is
  the most reusable idea and the one that *does* reduce cleanly to primitives
  (Rule #3): a `pgque.step()` checkpoint table + replay-on-retry needs no change
  to the PgQ engine. It also sidesteps Temporal's biggest adoption tax (the
  determinism paradigm and "deploy correct code, break all running workflows").
- **SQL-native observability comes free.** Workflows-as-rows means `psql`
  inspection, `list_workflows`-style queries, and forking — all trivial in a
  system that *is* SQL. DBOS markets this heavily; PgQue gets it for free.

**Net-new (the real cost):**

1. **The deterministic/checkpoint replay *runtime*, per language.** This is
   SDK-side logic — step memoization, resume-from-checkpoint, crash recovery
   coordination across executors. The DBOS study's headline finding:
   **~80% of DBOS's engineering lives in the multi-language SDK + replay runtime,
   not in SQL.** PgQ's PL/pgSQL gives you the substrate, not this.
2. **Claim / lease / watchdog machinery.** Claim expiry, lease extension on
   checkpoint write, watchdog termination of broken workers — net-new, and
   exactly the part absurd had to harden over five months in production.
3. **Long-lived-state lifecycle.** Non-rotated run/checkpoint tables with TTL /
   partition-detach cleanup, decoupled from event rotation.
4. **Event caching / first-write-wins** races (await/emit), needing a
   uniquely-indexed events table carefully ordered against wait registrations.
5. **N synchronized SDKs.** DBOS maintains four (Python, TS mature; Go, Java
   still 0.x). Keeping each in lockstep with engine semantics is the dominant
   ongoing cost — well beyond the "~1,500 lines" budgeted for pgque-api in
   SPECx. Durable workflows are a different order of magnitude.

---

## 5. Adoption analysis — can PgQue win here?

### Where PgQue *could* win

- **"It's literally just your Postgres."** No new datastore (vs Restate's
  RocksDB, Silo's SlateDB, Temporal's Cassandra), Apache-2.0 (vs Restate's BSL),
  managed-PG compatible. The strongest anti-lock-in story in the field.
- **True transactional exactly-once for in-DB effects.** Because PgQue *is*
  SQL, enqueue is just an `INSERT` in the caller's transaction — so "commit your
  business write and the workflow enqueue atomically" is a first-class,
  demonstrable feature. **DBOS markets a version of this but the study could not
  find it explicitly documented** — there is an opening to own it cleanly.
- **Proven-engine credibility.** Silo openly calls itself a prototype; absurd is
  "an experiment in durability"; Restate has no rigorous benchmarks. PgQue's
  PgQ lineage (15+ years, Skype/Microsoft scale) is the inverse story.
- **Concurrency/rate-limiting as native primitives.** Silo's standout feature
  (per-key concurrency queues, floating limits) is something Postgres does
  atomically and well — a credible differentiator if pursued.

### Where PgQue would *struggle*

- **absurd already owns the exact niche.** "Single SQL file, Postgres-only,
  self-hostable, thin SDK, no determinism" is *precisely* the slot a PgQue
  durable layer would target — and absurd has ~1.95k stars, Apache-2.0,
  production hardening, a Rust port (TensorZero), and **Armin Ronacher's
  distribution**. PgQue would arrive second with a near-identical pitch.
- **The differentiator doesn't transfer.** Zero-bloat rotation — the entire
  reason to choose PgQue over pgmq — is irrelevant to the claim/lease workflow
  tables. On the workflow layer PgQue is just another SKIP-LOCKED engine.
- **Two concurrency models = a muddier story.** "We never use SKIP LOCKED" and
  "our workflow engine uses SKIP LOCKED" coexisting invites the exact
  "isn't this just a complicated queue with state?" critique DBOS already takes
  on HN.
- **SDK breadth is years of work.** Competing on the workflow programming model
  means carrying replay runtimes in multiple languages — the opposite of
  PgQue's "language-agnostic, the SQL API *is* the product" advantage.
- **Single-Postgres ceiling.** DBOS guides ~a few thousand state
  transitions/sec on one Postgres; honest, but it concedes hyperscale to
  Temporal. Fine for a queue; a constraint to state loudly for workflows.

### Honest read on success probability

A **standalone "PgQue Workflows" engine** competing with DBOS/absurd head-on:
**low chance of breakout adoption.** Late entry, transferable differentiator
absent, high and ongoing SDK cost, against a founder-distribution incumbent
(absurd) and a credential incumbent (DBOS).

A **thin "durable steps + transactional enqueue" extension** of the existing
queue: **moderate-to-good chance of being genuinely useful and adopted by
PgQue's own users** — because it deepens the queue they already chose rather
than asking them to evaluate PgQue as a workflow platform. It rides existing
adoption instead of opening a new, contested front.

---

## 6. Strategic options

**Tier 0 — Stay the course (no change).** Keep SPECx §2.3's stance: PgQue is a
queue; if you need step-by-step workflows, use Temporal/Restate/absurd. Lowest
risk. Forgoes the category's momentum.

**Tier 1 — Own "transactional durable enqueue" (recommended, low risk).**
No workflow engine. Lean into what PgQue already does better than DBOS can
document: enqueue inside the caller's transaction, exactly-once consumption
patterns, idempotency helpers, and the supporting docs/examples. Pure
extension of the queue identity. Mostly documentation + small helpers +
TDD-tested examples. Reduces trivially to PgQ primitives.

**Tier 2 — Experimental absurd-style "durable steps" (optional, medium risk).**
Add a `sql/experimental/durable.sql` layer:
- a **new claim/lease run table** beside the PgQ engine (not on rotation),
- `tasks / runs / steps / checkpoints / events / waits` modelled on absurd,
- checkpoint-replay with **task-level retry, no determinism requirement**,
- pg_cron partition-detach cleanup for long-lived state,
- **exactly one** reference SDK (Python *or* TypeScript — whichever the user
  base skews to), explicitly experimental.
Gated behind the `blueprints/PHASES.md` promotion rule. Honest "experimental,
single-Postgres, up to a few thousand runs/sec" labelling. This is the only
tier that touches the workflow space, and it does so additively and reversibly.

**Tier 3 — Full multi-language deterministic-replay workflow platform
(not recommended).** Competing with Temporal/DBOS on their terms. Highest cost,
weakest differentiation, contradicts PgQue's language-agnostic identity.

---

## 7. Recommendation

1. **Adopt Tier 1 now.** It is low-cost, on-identity, and claims a feature DBOS
   leaves under-specified. It also makes the eventual Tier 2 story coherent.
2. **Prototype Tier 2 as an explicit experiment**, behind `sql/experimental/`,
   only if there is real pull from PgQue users (especially AI-agent use cases).
   Treat absurd's schema and the no-determinism checkpoint-replay model as the
   reference design. Build the claim/lease table as a *separate* mechanism and
   document plainly that this layer is a SKIP-LOCKED engine — do not pretend it
   inherits the zero-bloat property.
3. **Do not pursue Tier 3.** Concede hyperscale and full polyglot workflow
   orchestration to Temporal/DBOS; that honesty is itself credibility.
4. **Keep the queue the headline.** PgQue's winning, defensible story remains
   "the zero-bloat, managed-PG-compatible, language-agnostic Postgres queue."
   Durable steps are a *feature of that queue*, never a repositioning of it.

---

## 8. Open questions for the maintainers

- Is there demonstrated demand from PgQue's users for durable steps, or is this
  driven by category FOMO? (Tier 2 should wait for the former.)
- If Tier 2 proceeds, which single SDK first — Python (AI-agent gravity) or
  TypeScript (absurd's primary)?
- Are we comfortable carrying two concurrency models in one project, and
  messaging that without diluting the zero-bloat story?
- Does the transactional-enqueue feature (Tier 1) warrant a dedicated
  benchmark + example against DBOS's `DBOSClient` enqueue path?

---

## Appendix — per-system one-liners

- **DBOS** — embedded library, Postgres `dbos` system schema
  (`workflow_status`, `operation_outputs`, …), 1 write/step + 2/workflow,
  SKIP-LOCKED queue dequeue, ~40k workflows/sec on one Postgres, replay runtime
  is ~80% of the work, Conductor (ops) is the proprietary money-maker.
- **absurd** — single `absurd.sql`, per-queue `t_/r_/c_/e_/w_/i_` tables,
  SKIP-LOCKED claim-with-lease, task-level retry, no determinism, pg_cron
  partition detach, thin TS/Python SDKs, Rust port (TensorZero).
- **Temporal** — separate cluster (Frontend/History/Matching/Worker),
  event-sourced deterministic replay, Cassandra at scale, immutable
  `numHistoryShards`, 7 SDKs, determinism is the adoption tax.
- **Restate** — single Rust binary, log + RocksDB, virtual objects, durable
  promises, BSL license drew "not really open source" criticism.
- **Rivet** — Apache-2.0 actor platform (Durable-Objects-style), Postgres /
  RocksDB / FoundationDB, broader than workflows, no multi-actor atomic txns.
- **Silo** — Gadget's Rust broker on SlateDB/object storage, durable *job
  queue* (not workflow engine), single-shard-per-tenant (~4k jobs/sec cap),
  first-class concurrency + rate limiting, self-described prototype.
</content>
</invoke>
