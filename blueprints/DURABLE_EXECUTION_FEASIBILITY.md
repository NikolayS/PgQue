# Durable Execution on PgQue — Feasibility & Adoption Study

- **Status:** Brainstorm / decision input (not approved scope)
- **Date:** 2026-05-30
- **Question:** Should PgQue extend beyond a queue into a **durable-workflow /
  durable-execution engine on Postgres**, the way DBOS and absurd have? What are
  the realistic chances of adoption success if we go that route?
- **Companion reading:** `blueprints/SPECx.md` §2.3 (workflow engines treated as
  a separate category today), `blueprints/COOPERATIVE_CONSUMERS.md` (0.2,
  experimental), PR #237 (rotating zero-bloat `send_at`), `CLAUDE.md` Key Design
  Rules #2 (the PgQ engine is sacred) and #3 (modern API must reduce cleanly to
  PgQ primitives).

This study was produced after a deep review of the Hacker News thread
["Building durable workflows on Postgres"](https://news.ycombinator.com/item?id=48313530)
and a parallel investigation of the six systems that thread orbits around:
**DBOS, absurd, Temporal, Restate, Rivet, and Gadget's Silo**.

> **Revision note (2026-05-30):** An earlier draft of this study concluded that
> PgQue's zero-bloat differentiator "does not transfer" to a workflow layer,
> because durable engines need `SELECT … FOR UPDATE SKIP LOCKED` claim/lease
> semantics that conflict with PgQ's rotation model. **That conclusion was
> wrong.** It assumed the DBOS/absurd implementation strategy (a mutable
> `workflow_status` row updated per step). If instead workflow state transitions
> are modelled as **appended events over the rotating log** — i.e. durable
> execution as event sourcing — the rotation model is not an obstacle but an
> *advantage*. This revision rebuilds the analysis around that architecture.

---

## 1. Verdict up front

**Durable execution is feasible on PgQue's model, and for the workloads that
dominate today's demand it can win *because of* the model, not despite it.**

The key realisation: durable execution *is* event sourcing (this is literally
how Temporal's event-history-and-replay works), and PgQ is already an
append-only event log with snapshot-batched consumption and TRUNCATE rotation.
The mistake is to copy DBOS/absurd's storage strategy — a mutable
`workflow_status` row that gets `UPDATE`d on every step — because *that* is what
bloats, and it is exactly the pattern PgQue exists to avoid. The right strategy
is to model each workflow as a **stream of state-transition events**: process a
step, then **enqueue the next state as a new message** rather than mutating a
row. The workflow is always either (a) one in-flight message, (b) a *scheduled*
message awaiting a wake time, or (c) terminal. It never holds a batch open
across a wait, so it never blocks rotation, and every state transition is an
**append**, not an `UPDATE` — so the zero-bloat property carries straight
through to the workflow layer.

**What this means for strategy:** the well-leveraged bet is no longer "stay out
of the workflow category." It is to build an **event-sourced durable-execution
layer that is rotation-native**, shipped as an optional, experimental
`pgque-api` layer that reduces to PgQ primitives plus a small bounded
current-state projection. This stays inside PgQue's identity ("the zero-bloat
Postgres queue") and turns the engine into a genuine competitive moat for
high-throughput, fan-out-heavy, short-step durable workflows — precisely the
AI-agent-loop and event-processing workloads the whole category is currently
chasing.

**Honest scoping:** the part to defer is *not* the engine — it is the
"write-it-as-ordinary-linear-code" developer experience (Temporal/DBOS magic
checkpointing). PgQue's natural programming model is a message-driven state
machine (closer to AWS Step Functions / actors). That is a DX difference, not a
capability gap, and it is recoverable later with an SDK that compiles linear
code into re-enqueued continuations.

Net: **moderate-to-high feasibility**, with a real and defensible
differentiator — conditional on solving one genuinely hard piece
(`awaitEvent`/join semantics) and accepting a state-machine programming model
first, linear-code DX later.

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
  thesis.
- **Licensing trust is a real axis.** Restate's BSL drew loud "open source is
  misleading" criticism; Rivet's Apache-2.0 drew none. PgQue (Apache-2.0,
  literally "your own Postgres") inherits maximum trust by default.
- **The wedge against Temporal is operational, not technical.** Every
  competitor's pitch is the same: *don't run a second distributed system; reuse
  the database you already operate.* The complaints about Temporal are the
  determinism learning curve, immutable shard-count decisions, and the
  Cassandra/Elasticsearch operational floor — not correctness.
- **AI agents are the current demand driver**, and they are
  **high-volume, short-step, fan-out-heavy** — the exact workload shape where
  append+rotate beats update+vacuum (see §5).

So the category is real, PgQue's "Postgres-native, OSI-licensed, no new infra"
framing is well-aligned with where the market is pulling, *and* — once the
event-sourced architecture is adopted — PgQue's engine is a substrate advantage
rather than the liability the first draft assumed.

---

## 3. The architecture: durable execution as event sourcing

### 3.1 The core pattern — continuation-passing over the log

A workflow is a state machine. Each step is a short, independently-triggered
handler that does its work and **enqueues its successor state as a new event**:

```
msg {wf: 42, state: charge}  → charge_card();      enqueue {wf:42, state: ship};   ack
msg {wf: 42, state: ship}    → create_shipment();  enqueue {wf:42, state: notify}; ack
msg {wf: 42, state: notify}  → notify();           ack          -- terminal
```

There is no long-running function held in a worker process, so there is nothing
to "replay" (contrast §3.4). State lives in the event chain (and, for large
state, in a side row keyed by workflow id; for small state, in the payload
itself — continuation-passing). Every transition is an **append**. No
`UPDATE`, no per-step dead tuple, no VACUUM dependence on the hot path.

### 3.2 Exactly-once handoff between steps (PgQue is *stronger* here)

`pgque.insert_event()` (enqueue) and `pgque.finish_batch()` (ack) both run in
the consumer's own transaction, so a step's effect, its successor enqueue, and
the batch ack are **one atomic commit**:

```sql
begin;
  -- step's own DB side effects (idempotent or in-txn)
  perform pgque.insert_event(queue, next_state);  -- enqueue successor
  perform pgque.ack(batch_id);                     -- finish_batch
commit;
```

Commit → successor durably enqueued *and* batch finished atomically. Crash
before commit → txn aborts, no successor exists, the step redelivers cleanly via
PgQ's normal at-least-once redelivery. This is **exactly-once handoff** — the
capability DBOS markets as "piggyback the checkpoint in the transaction," except
here it is literally just SQL in the caller's transaction, and PgQue can
*demonstrate* it where DBOS documents it only indirectly.

### 3.3 The five durable-execution requirements, on this model

1. **Exclusive ownership — structural, not lease-based.** One logical consumer +
   cooperative subconsumers (`COOPERATIVE_CONSUMERS.md`, shipping experimental
   in 0.2). Invariant: **one live message per workflow** (each step enqueues
   exactly one successor). Each message goes to exactly one subconsumer, so only
   one worker touches a given workflow at any instant. absurd/DBOS need
   claim-with-lease + steal-on-crash; PgQue gets exclusivity for free from the
   single-live-continuation invariant, with the cooperative `dead_interval`
   takeover already designed for the worker-died-mid-batch case.

2. **Mutable run state — re-enqueue, don't update.** A transition appends a new
   event carrying the new state. For small state it rides in the payload and
   there is no long-lived table at all.

3. **Long-lived persistence — PR #237 is the foundation.** A step that sleeps a
   week acks immediately and enqueues a *scheduled* continuation:
   `sleep("7 days")` = `send_at(continuation, now()+7d)`. PR #237 makes
   `send_at` itself **TRUNCATE-rotated and zero-bloat**. A long sleep costs one
   row in a rotating delayed table — never an open batch, never a vacuum
   problem. (This dissolves the old "open batch blocks rotation" objection: the
   workflow does not hold a batch across the wait.)

4. **Per-row scheduling — half solved, half genuinely hard.** Timers/sleep:
   solved by PR #237's rotating `send_at`. Waking on an **external event**
   (`awaitEvent`) with a timeout is the real new design work — a small "waiting"
   registry keyed by `(workflow_id, event_name)`, an `emit` path that injects
   the continuation, and a maint sweep for timeouts. Low-volume (bounded by
   in-flight waiters, not throughput), tractable, but it must be designed
   carefully (first-write-wins event caching to avoid emit/await races — absurd's
   `e_`/`w_` table pair is a good reference shape).

5. **Checkpoint replay — not needed (see §3.4).**

### 3.4 Why "checkpoint replay" is unnecessary here

In Temporal/DBOS/absurd a workflow is one linear function run in one process; to
survive a crash, each step's result is saved, and on restart the function is
**re-run from the top** with completed steps short-circuited from the saved log
("replay"). That requires a long-lived run owned by a worker for its whole life.

The continuation-passing model **eliminates the concept**: there is no
long-running function to resume, so nothing to replay. Recovery is just
redelivery of the single in-flight step, and correctness comes from the
exactly-once handoff in §3.2 plus per-step idempotency keyed by
`(workflow_id, step_seq)` (a unique index that prevents double-advance). This is
strictly simpler than replay and native to a queue.

### 3.5 The one piece of mutable state — bounded by concurrency, not throughput

For observability, addressing, cancellation, and joins, keep a **current-state
projection**: one row per *live* workflow, replaced as it advances, deleted on
completion. This is the only mutable table, and it is bounded by **in-flight
concurrency, not total throughput** — a million finished runs leave zero rows.
VACUUM load scales with concurrency (fine), not with step volume.

**This split is the whole trick:** hot, high-churn step transitions → rotating
append-only log (zero bloat); cold, low-volume current-state index → tiny
mutable table (negligible bloat). DBOS/absurd put the high-churn part on the
mutable table and inherit the bloat wall; PgQue keeps the high-churn part on the
log.

---

## 4. What is reusable vs. net-new

**Reusable / already in flight (low cost):**

- **Single-file, anti-extension, managed-PG install** — identical to absurd's
  validated `absurd.sql` philosophy and PgQue's `\i pgque.sql`.
- **Cooperative consumers** (0.2) — gives parallel execution + structural
  per-workflow exclusivity.
- **Rotating `send_at`** (PR #237) — gives zero-bloat timers/sleep.
- **Transactional `insert_event` + `finish_batch`** — gives exactly-once handoff
  with no new primitive.
- **`jsontriga`** — CDC-triggered workflow starts, native to the engine.
- **SQL-native observability** — workflows-as-rows/events are `psql`-inspectable.

**Net-new (the real cost, in order of difficulty):**

1. **`awaitEvent` / wait registry + emit path** (§3.3.4) — the genuinely hard
   design: race-free event caching, timeout sweep, join/fan-in semantics.
2. **Fan-out / join primitive** — a step that spawns N children (distinct child
   workflow ids, each independently single-live) and a parent that awaits all N
   (a counter in the projection, or children emit completion events the parent
   awaits).
3. **Current-state projection + step idempotency index** (§3.5) — small, but
   needs careful transition logic so advance is exactly-once.
4. **A reference SDK** — *one* language first (Python for AI-agent gravity, or
   TypeScript for absurd-parity), exposing the state-machine API. Linear-code
   DX (compiling an `async` function with `await` points into re-enqueued
   continuations) is a *later* library project, not an engine requirement.

Critically, **none of these touch the PgQ engine** (Rule #2) and all reduce to
PgQ primitives + a couple of small side tables (Rule #3). The expensive
multi-language deterministic-replay runtime that dominates DBOS's effort
**does not exist in this model** — that cost simply isn't incurred.

---

## 5. Adoption analysis — where PgQue wins, and the tradeoffs

### Where PgQue wins *because of* the model

1. **Zero-bloat at high step-throughput — the differentiator now transfers.**
   DBOS/absurd do `UPDATE workflow_status` + `INSERT operation_outputs` per
   step: mutable-row churn → the exact bloat wall PgQue exists to defeat. In the
   event-sourced model every transition is an append to the rotating log; a
   million agent iterations leave zero dead tuples on the hot path. For the
   AI-agent-loop-at-scale workload everyone is chasing, **append+rotate
   structurally beats update+vacuum.** This is the headline.
2. **Native fan-out + batch step execution.** PgQ hands a *batch* of many
   workflows' step-events at once, snapshot-isolated — advance thousands of
   workflows in one transaction. DBOS is 1-write-per-step over per-row
   `SKIP LOCKED`. PgQue amortizes where they pay per item.
3. **Transactional exactly-once handoff** (§3.2) — stronger than at-least-once
   competitors, and just SQL.
4. **"It's literally just your Postgres"** — no new datastore (vs Restate's
   RocksDB, Silo's SlateDB, Temporal's Cassandra), Apache-2.0 (vs Restate's
   BSL), managed-PG compatible. Strongest anti-lock-in story in the field.
5. **Proven-engine credibility** — PgQ's 15+ years vs absurd's "an experiment in
   durability" and Silo's self-described prototype.

### The honest tradeoffs

- **State-machine programming model, not magic linear code.** Workflows are
  expressed as message-driven steps (Step-Functions/actor style), not Temporal's
  "write normal code, we checkpoint it invisibly." A *DX* difference, recoverable
  later via a continuation-compiling SDK.
- **`awaitEvent`/join is real new design** (§4.1–4.2), the main engineering risk.
- **Single-Postgres ceiling** — honest "up to a few thousand workflow
  transitions/sec per database" framing, as DBOS does; concede hyperscale to
  Temporal.
- **absurd already has distribution** in the "Postgres-only durable workflows"
  framing. PgQue's counter is not "also Postgres" but "**zero-bloat at
  throughput absurd's mutable-row design can't sustain**" — a concrete,
  benchmarkable claim, not a me-too.

### Success probability (revised)

- A **rotation-native, event-sourced durable-execution layer** marketed on
  *zero-bloat high-throughput durable workflows*: **moderate-to-high** — it is
  differentiated, reduces to existing primitives, and rides existing adoption.
- A **DBOS/absurd clone** (mutable status row + multi-language replay runtime):
  **low** — late, undifferentiated, and it would forfeit the one advantage.

The strategic point: don't enter the category the way the incumbents built it.
Enter it the way only PgQue *can* build it.

---

## 6. Strategic options

**Tier 0 — Stay a pure queue.** Lowest risk; forgoes a genuine, defensible
differentiator that the engine uniquely enables.

**Tier 1 — Own "transactional durable enqueue" now (low risk).** Document and
helper-ize the exactly-once handoff (§3.2) and idempotent-step patterns. Pure
extension of the queue identity; mostly docs + small helpers + TDD examples.
Also the foundation the durable layer builds on.

**Tier 2 — Event-sourced durable steps (recommended, medium risk).** A
`sql/experimental/durable.sql` layer:
- continuation-passing steps over the rotating log (no mutable status row on the
  hot path),
- transactional handoff (`insert_event` + `finish_batch` in one txn),
- rotating `send_at` (PR #237) for sleep/timers,
- a bounded current-state projection + `(workflow_id, step_seq)` idempotency
  index,
- the `awaitEvent`/emit registry + fan-out/join primitive (the hard part —
  design and TDD this first),
- exactly **one** reference SDK, explicitly experimental, gated behind the
  `PHASES.md` promotion rule.
Marketed on the zero-bloat-at-throughput advantage, not "also Postgres."

**Tier 3 — Multi-language deterministic-replay platform (not recommended).**
Competing with Temporal/DBOS on their terms and their costs, forfeiting the
model's advantage.

---

## 7. Recommendation

1. **Adopt Tier 1 now** — low-cost, on-identity, and the foundation for Tier 2.
2. **Prototype Tier 2 as an explicit experiment**, leading with the
   `awaitEvent`/join design (the one place the model has real new risk) and a
   throughput+bloat benchmark vs a mutable-status-row baseline (absurd/DBOS
   shape) — that benchmark *is* the marketing.
3. **Do not pursue Tier 3.**
4. **Keep the queue the headline; durable steps are a feature of the engine's
   zero-bloat append-and-rotate design**, which is exactly why PgQue can offer
   them where SKIP-LOCKED systems hit a wall.

---

## 8. Open questions for the maintainers

- Is there demonstrated user pull for durable steps (especially AI-agent
  use cases), or is this category FOMO? Tier 2 should follow real pull.
- Reference SDK first: Python (AI-agent gravity) or TypeScript (absurd-parity)?
- `awaitEvent` semantics: race-free emit/await caching, timeout handling, and
  fan-in/join — design and TDD before any code.
- Does the throughput+bloat benchmark vs a mutable-status-row baseline hold up
  on server hardware? (If yes, it is the whole pitch.)
- Are we comfortable shipping a state-machine programming model first, with
  linear-code DX as a later SDK project?

---

## Appendix — per-system one-liners

- **DBOS** — embedded library, Postgres `dbos` system schema
  (`workflow_status`, `operation_outputs`, …), **mutable status row updated per
  step**, SKIP-LOCKED queue dequeue, ~40k workflows/sec on one Postgres, replay
  runtime is ~80% of the work, Conductor (ops) is the proprietary money-maker.
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
