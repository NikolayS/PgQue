# PgQue Durable Workflows — SPEC v0.6

> Status: **experimental**, ships as optional `sql/experimental/durable.sql` gated by the project promotion rule. Workflow support ships first as **one thin-SQL-wrapper reference client (Python)**; the other PgQue clients (Go, TypeScript, + WIP) are a planned follow-up, not v0.1 (§7–§9, §12). Engine layer is sacred and untouched.

---

## 1. Goal & why it's needed

**Goal (user-outcome language).** Give developers durable, crash-proof workflows — multi-step processes and AI-agent loops that never lose progress, advance step-to-step with transactional handoff, and recover cleanly after crashes — using only the Postgres they already operate, with no separate system to run, and that **keep running fast under sustained high volume instead of degrading over time** (no gradual slowdown, no VACUUM wall, no throughput cliff, no tuning, no 3am pager).

**Positioning.** This is a **lighter, no-new-infra, stays-fast alternative to Temporal, DBOS, absurd, and pg_durable for Postgres-native workflows** — it competes on durable execution and delivers the core guarantees teams adopt those systems for (durable multi-step execution, transactional handoff, at-least-once steps, durable timers, fan-out/join), running entirely inside your existing managed Postgres and **not slowing down under load**. Eliminating per-step `workflow_status` `UPDATE` churn is the **headline benefit**, not a limitation. We compete on durability; we differ in *mechanism* (explained as the *how* below, never sold as the *what*). Throughput target is a **benchmark hypothesis**, not a release claim: high aggregate simple (await-light) transition throughput per database, flat under sustained load, with the headline being that it **does not degrade** where status-row systems hit the VACUUM wall; coordination-heavy (await/join) transitions cost more and are characterized honestly (§5.6). Beyond a single node, scale out by **sharding workflows across databases**.

**Why this exists.** Several Postgres-native durable-execution systems in the category (DBOS, absurd, and the long tail of `SELECT … FOR UPDATE SKIP LOCKED` + `DELETE` queues) model a workflow as a **mutable `workflow_status` row that is `UPDATE`d on every step**. At the throughput the category is actually chasing — AI agent loops doing many cheap iterations — that per-step `UPDATE` churns dead tuples until the workload hits a VACUUM wall, and throughput degrades. `pg_durable` attacks a neighboring problem with a background worker, Duroxide checkpoints, and a SQL graph DSL inside Postgres; that is serious prior art, but it chooses workflow control flow in the database and carries lifecycle/upgrade/security costs PgQue should avoid. PgQ already solved the hot-path bloat problem for *queues* with snapshot-batch isolation + wholesale `TRUNCATE` rotation: zero dead-tuple bloat under sustained load. This product tests whether that property can carry up to the workflow layer without importing a mutable status-row engine or an in-database workflow language.

This exists because no one else can credibly offer "durable workflows that stay fast for months under agent-loop load, on just your managed Postgres, with no separate datastore." That is the entire pitch.

**How it works / why it's possible (the mechanism — this is the *how*, not the headline).** Durable execution is event sourcing (this is how Temporal's event-history + replay works). PgQ is already an append-only event log. So instead of a mutable `workflow_status` row `UPDATE`d every step, we model each workflow as an **append-only stream of state-transition events over PgQ's snapshot + TRUNCATE rotation engine**: process a step, then **enqueue the next state as a new message** (continuation-passing) rather than mutating a row. A workflow is always either (a) one in-flight message, (b) a *scheduled* message awaiting a wake time/event, or (c) terminal; it never holds a batch open across a wait, so it never blocks rotation, and every transition is an **append**, not an `UPDATE`. That is precisely why the "stays fast under sustained load" outcome above is achievable.

**What it is NOT** (honored strictly throughout — see §12): it does **not** reproduce the Temporal/DBOS *durability mechanism* (deterministic replay of a linear function + a per-step-mutated status row) — we compete with them but eliminate that mechanism because it is the bloat source; not a per-language replay *runtime* (clients are thin SQL wrappers; **one reference client — Python — in v0.1, the rest a deferred follow-up**); not a separate server/daemon/datastore; not a `FOR UPDATE SKIP LOCKED` claim/lease model; cancellation/orphan-join propagation is deferred.

**pg_durable boundary.** `pg_durable` proves that serious durable execution can
live inside Postgres: pgrx extension, background worker, Duroxide checkpoint
runtime, SQL graph DSL, timers, signals, joins/races, HTTP, RLS, submitted-user
execution, and SSRF defenses. PgQue should absorb those lessons on primitives
and security, but reject the central product choice: **workflow control flow
inside SQL**. PgQue's boundary is **workflow durability in Postgres; workflow
code in application repositories**. No SQL graph DSL, no `shared_preload_libraries`,
no replay runtime until user demand justifies it.

---

## 2. Scope & resolved interview decisions

The interview answers were all delegated to the lead ("decide for me"). Resolved:

| Question | Decision (v0.1, carried through) |
|---|---|
| **Primary users** | Backend engineers running long-lived or high-iteration orchestration (AI agent loops, multi-step business processes, fan-out jobs) **on managed Postgres** who refuse a second datastore and refuse a VACUUM wall. |
| **Core job** | Advance a workflow from one step to the next with **exactly-once handoff** and **at-least-once step execution**, never losing or silently duplicating a workflow's progress — on a hot path that appends and rotates rather than updates. |
| **Durability / recovery guarantee** | At-least-once step execution + exactly-once handoff between steps; per-step idempotency keyed on `(workflow_id, step_seq)`. On crash, exactly the single in-flight step redelivers (PgQ's existing redelivery); there is no long function to replay. |
| **Success metric** | A throughput-and-bloat benchmark vs a mutable-status-row baseline (DBOS/absurd shape) and a pg_durable-style checkpointed graph baseline where feasible: **flat dead-tuple count + sustained throughput** on the append+rotate hot path where the baseline degrades. All throughput numbers are hypotheses until this benchmark exists. |
| **Out of scope for v0.1** | Cancellation / orphan-join propagation; linear-code (`async/await`-compiled) DX sugar; the per-language deterministic-replay *runtime* (we ship one thin SQL-wrapper reference client — Python — instead, §9); additional-language clients (Go/TS/WIP — a deferred follow-up, §11); imposing a determinism requirement on user code. **In scope:** the one Python reference client, the full durability/coordination engine, and the observability surface of §5.14. |

---

## 3. User stories

Each story is persona + action + outcome and is directly exercised as a manual acceptance test (§6.4).

1. **Agent-loop builder (stays fast at iteration scale).** *As* a backend engineer running an AI agent that loops thousands of times per run, *I* define each iteration as a step that processes and enqueues its successor, *so that* a million iterations complete with **no gradual slowdown and a flat dead-tuple count** on the hot tables — verifiable with `pg_stat_user_tables.n_dead_tup` staying flat through the run. (The flat-curve claim is scoped to await-light loops; await/join-heavy shapes are characterized honestly in §5.6.)

2. **Long-sleep orchestrator (durable timers).** *As* an engineer modeling a "wait 7 days, then send a reminder" process, *I* call `sleep('7 days')` inside a step, *so that* the workflow durably resumes after the wait **without holding any batch open** and **without a per-workflow polling row** — the sleep is one row in a TRUNCATE-rotated delayed-delivery table, and the woken continuation is **never** misclassified as a stale redelivery and DLQ'd (§5.4.1).

3. **Human-in-the-loop integrator (await external event).** *As* an engineer building an approval flow, *I* call `awaitEvent('approval', timeout => '24h')` and have an **authorized** part of my system call `emit(workflow_id, 'approval', payload, token)`, *so that* the workflow resumes **exactly once** on the event — robust against emit-before-await, await/emit interleave, and emit-racing-the-timeout — or resumes on the timeout branch if the deadline passes first. For approval-class waits the per-wait emit token is **mandatory** (§5.10.2), so the approval cannot be forged by an unauthorized caller (§5.10), nor by guessing or harvesting the `workflow_id` (§5.11), nor replayed without the wait token.

4. **Fan-out batch processor (spawn + join).** *As* an engineer processing a parent job that splits into N independent children (N capped, §5.12), *I* spawn N child workflows and `awaitAll`, *so that* the parent resumes **exactly once** when all N complete — neither zero times (lost-resume race closed, §5.8) nor twice — with a **per-child result array** (success/failure each) materialized in a join-result side table (§5.8) — not inlined in the resume payload — even under redelivery of any child's completion and under concurrent final completers.

5. **Exactly-once integrator (transactional handoff).** *As* an engineer whose step writes a row to *my own* business table and then advances the workflow, *I* run my side effect, the successor enqueue, and the batch ack in **one transaction**, *so that* a crash either commits all three or none — no successor without the side effect, no side effect without the ack, no duplicate handoff.

6. **On-call operator (monitor without a status row).** *As* the engineer on-call for a fleet of running workflows, *I* query the operational views of §5.14, *so that* I can see what is waiting/sleeping/overdue, list everything running right now, and read throughput/failure metrics — **without** the system paying a per-step status-row `UPDATE` to give me that visibility, and with exact per-step liveness available as a single opt-in knob (§5.14.4).

---

## 4. Architecture

<!-- architecture:begin -->

```text
application / SDK
  - user step handlers live in app code and Git
  - worker loop receives PgQue batches and dispatches by step name
  - handler returns one continuation: goto, sleep, awaitEvent, spawn, complete

sql/experimental/durable.sql
  - validates workflow_id / step_seq / payload caps
  - records dedup markers and coordination rows
  - enqueues the successor and finishes the current batch in one transaction
  - implements awaitEvent / emit / join / timeout sweep / observability views

PgQue engine (sacred boundary)
  - insert_event / next_batch / get_batch_events / finish_batch
  - cooperative consumers and dead_interval takeover
  - rotating event tables and rotating send_at delayed delivery
  - retry / DLQ / tick visibility contracts pinned by tests

Postgres
  - ordinary app tables hold large workflow state and idempotency effects
  - normal backup, HA, auth, and observability apply
```

<!-- architecture:end -->

### 4.1 Layering (the sacred boundary)

The durable layer **only calls** the PgQ primitives + `send_at`. It adds **no** modification to rotation/tick/batch logic and introduces **no** second concurrency model. Its dependencies on engine semantics (tick-visibility, durable per-event retry count, `send_at`, the `next_batch` max-events bound) are made explicit and pinned by engine-contract tests (§5.9).

### 4.2 Key abstractions

- **Workflow** — a logical state machine identified by `workflow_id`, which is a **128-bit unguessable capability** (§5.11), not a sequential id. At any instant it is in exactly one of three conditions: **(a)** one *in-flight* message (a step-event sitting in a PgQ batch being processed), **(b)** *scheduled* (a `send_at` continuation awaiting a wake time, or a registered wait awaiting an event), or **(c)** *terminal*. The **single-live-continuation invariant** — each processed step enqueues *exactly one* successor — is what makes exclusivity structural rather than lease-based.
- **`workflow_id` — addressing handle AND bearer capability.** It is used both to *address* a workflow (in payloads, user tables, and raw in hot queue rows for indexed lookup) and, combined with the role grants and per-wait tokens of §5.10, to *authorize* operations against it. Because it does double duty it must be treated as a secret; §5.11 specifies its confidentiality/leakage model (raw only in protected hot-path tables, hashed at rest in lower-trust audit/DLQ, never logged raw, mandatory token for approval waits).
- **Step-event** — the message on the PgQ queue. Payload carries: `workflow_id`, `step_seq` (monotonic progress anchor), `step_name`/state tag, `delivery_anchor` (the event's deliverable time, §5.4.1), small continuation state (continuation-passing), and — for retries — `retry_attempt`/`origin_step` (§5.2), subject to a **hard payload size cap** (§5.12). `workflow_id`/`step_seq`/`step_name` are also placed in `ev_extra1/2/3` for indexed observability (§5.14.2). Large state is the user's responsibility to hold in their own tables, addressed by `workflow_id`.
- **Transition** — process a step → emit successor as a *new append*. Never an `UPDATE` of a status row.
- **Coordination side tables** (the only mutable state; see §5.5) — `wf_registry`, `wf_wait`, `wf_event_cache`, `wf_join`, `wf_join_done`, `wf_dedup`, `wf_audit`, the consumer-wide `wf_dispatch_control` (one row per logical consumer, §5.2), and the **optional, opt-in** `wf_live` projection. Their churn is bounded by **concurrency and coordination-point count, not total step volume** — stated precisely (distinguishing live row-count from dead-tuple rate, and conceding the await/join-heavy case) in §5.6.

### 4.3 Concurrency / ownership model

One **logical consumer** with cooperative **subconsumers** splitting batches (PgQ 0.2 feature). Because exactly one live message exists per workflow, only one subconsumer ever touches a given workflow at a given instant — exclusivity is an emergent property of the invariant, requiring **no claim/lease/steal machinery**. Worker death mid-batch is covered by PgQ's existing cooperative `dead_interval` takeover: the unfinished batch is reassigned and the in-flight step redelivers (at-least-once), made safe by per-step idempotency (§5.4) whose dedup horizon is bounded ≥ max single-attempt redelivery latency (§5.4.1). **Because there are multiple concurrent subconsumers, any dispatcher control that must be uniform across the logical consumer — specifically the poison-pill `max_events` reduction (§5.2) — is held in the shared, consumer-wide `wf_dispatch_control` row, not in process-local dispatcher state.**

---

## 5. Implementation details

### 5.1 The hot path: one transition = append + ack, atomically

The foundational guarantee. `insert_event()` (enqueue successor) and `finish_batch()` (ack) run in the **consumer's own transaction**. **The atomic commit unit is the batch transaction** (§5.2); for the common case of a single-event batch it reduces exactly to one step's side effects + its successor enqueue + its ack committing together:

```
begin;
  -- 1. step's own DB side effects (idempotent or naturally in-txn)
  -- 2. record per-step dedup marker (workflow_id, step_seq)   [if first delivery]
  perform pgque.insert_event(queue, next_state);   -- enqueue exactly one successor
  perform pgque.finish_batch(batch_id);            -- ack this batch
commit;
```

- **Commit** ⇒ successor durably enqueued **AND** batch finished, atomically ⇒ exactly-once handoff.
- **Crash before commit** ⇒ txn aborts ⇒ no successor, no dedup marker, batch not finished ⇒ the step redelivers cleanly.

The dedup marker is keyed on *this attempt's* `(workflow_id, step_seq)`. A retry continuation is a **new transition with a fresh `step_seq`** (§5.2), so it carries its own marker and is therefore **not** absorbed as a dedup no-op — it re-executes. **No subtransactions are used on this path** (hard constraint; also §5.13).

### 5.1.1 The make-or-break rebuttal: "isn't per-workflow state a per-transition UPDATE, 1:1 with messages → same bloat?"

The single most important objection, answered head-on. **No** — and the reason is the same batching amortization that makes PgQ itself cheap:

- **PgQ's own mutable state is the `subscription` (consumer-position) row, updated per *batch*, not per *event*** — amortized N× by batching, and **zero** updates when there is nothing to consume. That is exactly why PgQ is low-bloat.
- **The workflow dispatcher IS a PgQ consumer**, so it inherits that property unchanged: one `subscription` `UPDATE` per tick/batch, amortized over all the (many different workflows') transitions in that batch, idle = zero. This row is **one-per-consumer**, ~tick-rate, HOT-updatable — it does **not** scale with workflow count or transition count.
- **Per-workflow state is carried in the in-flight message (continuation-passing), NOT in a per-workflow row.** Advancing workflow W from step n→n+1 is an **append** (enqueue successor with `step_seq+1`); the old message is consumed and rotates away. There is **no per-workflow position row `UPDATE`d per transition.** So "N workflows × M steps" = N×M **appends** to the rotating queue (zero bloat) + the *same* per-batch `subscription` update PgQ already does. It is **not** N×M row `UPDATE`s.
- **Dedup markers `(workflow_id, step_seq)` are INSERTs (appends) to a rotating short-horizon table, not UPDATEs.**
- The ONLY way to reintroduce per-transition `UPDATE` churn is a live "current step" projection updated every step — which is exactly why `wf_live` is **opt-in, default OFF**, and never on the correctness path (§5.5/§5.14.4).

### 5.2 Dispatch loop, transaction boundary, and retry

**The transaction boundary is the batch.** PgQ acks a *batch* wholesale via `finish_batch`; there is no per-event ack. The dispatcher processes every event in a batch within **one** transaction and commits once:

```
loop:
  K        := dispatch_control.current_max_events           -- shared, consumer-wide (§5.5)
  batch_id := pgque.next_batch(queue, consumer, max_events := K)   -- snapshot-bounded, ≤ K events
  if batch_id is null:
      run_timeout_sweep()                              -- §5.7.1 in-loop liveness
      sleep to next tick; continue
  events  := pgque.get_batch_events(batch_id)
  begin
    for each event in events:                          -- batch step execution
        if redelivery_age(event) > dedup_horizon:      -- §5.4.1 staleness GATE, BEFORE body
            route_to_dlq(event); continue              --   route-not-process: no user body runs
        advance_one(event)                             -- §5.3, appends successor(s)
    pgque.finish_batch(batch_id)
    run_timeout_sweep()                                -- opportunistic
  commit
  on abort:  note_batch_abort()                        -- ramps dispatch_control down (below)
  on clean commit at K=1:  note_clean_isolated_commit() -- ramps dispatch_control back up
```

**Batch size is bounded** (the `max_events := K` dispatch parameter, default small) so the blast radius of any rollback is bounded and the single-event reduction of §5.1 is the common shape. **`K` is read from the shared `wf_dispatch_control` row, not from process-local state** — see the poison-pill quarantine below.

**Per-event retry without subtransactions.** The v0.1 claim that a transiently failing step "calls `event_retry()` for that single event rather than aborting the whole batch" is **incorrect under the stated constraints** — PL/pgSQL cannot catch an error and continue the surrounding transaction without a savepoint, and **§5.1/§5.13 forbid subtransactions in hot paths** (the no-subtransaction rule lives in §5.1/§5.13). Two failure channels:

1. **Expected / transient failure → returned retry continuation (an append, not a throw).** A step that wants to retry re-enqueues a continuation of the *same logical step* via `send_at` with backoff and finishes normally. **The retry continuation is a NEW transition carrying a fresh `step_seq`** (with `retry_attempt` and `origin_step` recorded in the payload). Because its `step_seq` is new, the §5.4 dedup logic does **not** treat it as a committed no-op — the retried step **re-executes its body** on delivery. After `max_retries` the step returns a **DLQ transition** (an append to the DLQ queue) rather than throwing. This path is subtransaction-free and does **not** abort the batch.
2. **Unexpected exception (genuine bug, OOM, lost connection) → batch aborts and the whole batch redelivers.** Rare, correct, and safe: redelivery is idempotent (§5.4). **Poison-pill containment — consumer-wide coordinated `max_events` reduction (subconsumer-safe):** the durable layer **cannot** durably write a per-workflow exception counter from the aborting transaction (the abort discards it) and PgQ delivers batches as snapshot-bounded ranges, not per-workflow-selectable. Containment therefore rests on two mechanisms that need **no engine change and no sub-range partial ack**:
   - (a) **the engine's durable per-event retry counter**, which PgQ increments across redeliveries and uses to route an over-threshold event to the DLQ — pinned as engine contract #2 (§5.9); and
   - (b) **consumer-wide batch-size reduction on the existing `next_batch` max-events bound (engine contract #4, §5.9), coordinated through the shared `wf_dispatch_control` row.** On detecting an aborting batch, a subconsumer's `note_batch_abort()` **lowers `current_max_events` in `wf_dispatch_control` (down to 1) in its own short committed transaction** — this write survives the batch abort because it is a *separate* committed transaction, not the aborted one. Because the bound lives in a **single consumer-wide row read by every subconsumer at the top of its loop**, the reduction is uniform across all subconsumers, not process-local. Once `current_max_events = 1`, every subconsumer requests size-1 batches, so the unfinished poison event — **whichever subconsumer it is redelivered to** — arrives in its **own size-1 batch**, aborts **only a batch containing itself**, and crosses the engine retry threshold (contract #2) **in isolation**, landing in the DLQ. An innocent event that merely shared the original larger batch is re-processed (idempotently, §5.4) and **commits on its own size-1 redelivery** rather than being dragged to the DLQ.

   **Why this is subconsumer-safe.** A naive design that set `max_events` in process-local dispatcher state would let a subconsumer that had not itself aborted redeliver the poison event re-aggregated with K−1 innocents at `max_events = K`. Moving the bound into the shared `wf_dispatch_control` row fixes this: the first abort writes `current_max_events = 1` for the **whole logical consumer**, and every subconsumer reads it before its next `next_batch`, so no subconsumer re-aggregates the poison during the quarantine window. The row is **one-per-logical-consumer** (not per workflow, not per event), HOT-updatable, written only at abort/recovery transitions (≈abort-rate, rare) — it does **not** scale with workflow or transition count and is **not** a per-key coordination row of the kind §5.5 removed.

   **Quarantine recovery (the up-ramp — `max_events` restoration policy).** The down-ramp alone would permanently collapse throughput to one-event-per-batch after a single transient abort. Recovery is explicit: after `note_clean_isolated_commit()` observes **`quarantine_cooldown` consecutive clean size-1 commits across the consumer** (a configurable count, default small — long enough that the poison event has crossed the engine retry threshold and been DLQ'd in isolation), `wf_dispatch_control.current_max_events` is restored to the configured `K`. Restoration is gated on the cooldown count, **not** time, precisely so the poison is quarantined to the DLQ *before* batches re-aggregate; restoring too eagerly (before the threshold is crossed) is what the cooldown prevents. The cooldown counter is held in the same `wf_dispatch_control` row and advanced under a per-row lock so concurrent subconsumers count monotonically.

   **Honest bound on co-tenant impact.** Before the consumer-wide bound reaches 1, innocent co-tenants in an aborting batch do accrue a *bounded* number of retry-counter increments and idempotent re-processings (bounded by the configured starting `K` and the one-step drop to 1). Once isolated at size 1 they commit independently. We do **not** claim zero co-tenant disturbance — only that no innocent co-tenant is forced to the DLQ by the poison event, and that the isolation holds **across all subconsumers**, not just the one that first aborted.

A batch may still contain step-events for many distinct workflows advancing in one transaction (native fan-out); correctness no longer depends on per-event mid-transaction error recovery.

### 5.3 The five durable-execution requirements, mapped

1. **Exclusive ownership — structural.** Single-live-continuation invariant + cooperative `dead_interval` takeover. No lease.
2. **Mutable run state — re-enqueue, don't update.** Each transition appends a new event carrying new state; small state rides the payload; no long-lived per-run row on the hot path.
3. **Long-lived persistence — rotating `send_at`.** `sleep('7d')` = `send_at(continuation, now()+7d)`; the step acks immediately; the sleep is one row in a TRUNCATE-rotated delayed table — zero-bloat, never an open batch. The woken continuation's `delivery_anchor` is its wake time, so it is never confused with a stale redelivery (§5.4.1).
4. **Per-row scheduling.** Timers via rotating `send_at`. **`awaitEvent` with timeout** is the genuinely hard new piece (§5.7).
5. **Checkpoint replay — not needed.** No long-running function to resume. Recovery = PgQ's at-least-once redelivery of the single in-flight step. Correctness = exactly-once handoff (§5.1) + per-step idempotency (§5.4).

### 5.4 Per-step idempotency

Every step attempt is keyed `(workflow_id, step_seq)`. On (re)delivery a step first checks/inserts a dedup marker; the marker insert and the successor enqueue commit together (§5.1). A redelivered step **with the same `step_seq`** whose successor already committed is a no-op (marker present) and simply re-acks. A **retry continuation has a fresh `step_seq`** (§5.2) and therefore re-executes — it is a new attempt, not a redelivery of the prior one. The dedup store is append-based and short-horizon (rotating) so it does not itself become a bloat source (§5.6).

#### 5.4.1 The dedup-horizon bound and the delivery-anchor clock

Exactly-once handoff holds **iff the dedup horizon ≥ maximum single-attempt redelivery latency**. The clock the horizon is measured against is the **`delivery_anchor`**, carried in the payload, defined as the time the event *became deliverable* — its tick-visibility time, and for a `send_at` continuation its **scheduled wake time `now()+Δ`, NOT the time the continuation was created**. Redelivery age = `now − delivery_anchor`, evaluated **per-event/per-transition** and **reset at every transition and every timer fire**. Consequences:

- A freshly woken `sleep('7d')` continuation has `delivery_anchor` = its wake time, so its redelivery age on first delivery is ~0 — it is **never** confused with a 7-day-stale redelivery. The long wait lives in the gap between *creation* and *delivery anchor*, which the horizon does not see.
- Only repeated redelivery of the *same* deliverable event (takeover/retry of one attempt) advances age against the horizon.

Bound (note: single-attempt, **not** cumulative-over-retries, and **independent of max sleep**):
```
dedup_horizon  ≥  max_retry_backoff        (one attempt's backoff)
               +  dead_interval             (worst-case takeover delay)
               +  max_batch_duration
               +  safety_margin
```
It does **not** include `max_sleep` (re-anchored above) nor `max_retries × backoff` (each retry is a fresh transition with its own `step_seq` and `delivery_anchor`, §5.2, so it never ages against the prior attempt's marker). The horizon is configured and validated at install against `dead_interval`, `max_retry_backoff`, and `max_batch_duration`.

**Enforcement — and its ordering relative to the body.** The staleness check is a **pre-body dispatcher gate**: in the dispatch loop (§5.2) it runs **before `advance_one` executes any user step body**. Any deliverable event whose redelivery age (`now − delivery_anchor`) exceeds the horizon is **routed to the DLQ instead of processed** — a *route-not-process* decision that runs **no user body and therefore cannot abort**, so its DLQ routing commits cleanly with the surrounding batch transaction (or, if a co-tenant in the same batch later aborts, the event is simply re-gated and re-routed on redelivery — the route decision is idempotent and side-effect-free). Because the staleness gate runs first and never executes the body, a stale event is never handed to a body that would abort; the gate reliably quarantines genuinely-stale events. This makes a marker unable to rotate out underneath a still-live redelivery and silently report "first delivery."

**Reconciling the two DLQ-routing mechanisms.** There are two distinct routes to the DLQ, operating at different points, so they do not conflict:
  - **§5.4.1 staleness gate** fires on `now − delivery_anchor > dedup_horizon`, **before** the body runs, commits cleanly (route-not-process). It quarantines an event whose marker may have rotated away — a *correctness* guard against silent double-handoff.
  - **§5.2 / contract #2 engine retry counter** fires when a *body that actually ran and aborted* crosses the engine's per-event retry threshold — a *poison-pill* guard for the aborting-batch channel.
  The "only durable counter available to the aborting-batch channel" claim (§5.2) is scoped exactly to that channel: **after a user body has run and aborted its transaction**, the durable layer has discarded any state it tried to write, so the *engine* retry counter is indeed the only durable signal left for the abort path. The §5.4.1 gate is **not** part of the aborting-batch channel — it commits before any body runs — so it does not contradict that claim. For a poison event redelivered repeatedly in size-1 batches, the staleness gate fires first **only if the event also goes stale**; if it is processed promptly each redelivery (typical), the staleness clock stays under the horizon and contract #2's retry threshold is the route that fires — exactly as intended. Property tests (§6.2) assert **both** directions: no double-handoff at horizon-boundary redelivery age, AND a `sleep` longer than the horizon resumes normally and is **not** DLQ'd; and §6.3 asserts the gate-runs-before-body ordering.

### 5.5 Coordination side tables

| Table | Role | Churn driver | Lifecycle |
|---|---|---|---|
| `wf_registry` *(mandatory)* | minimal authoritative live-workflow set (id + status); source of truth for emit-liveness (§5.10.2) and unknown-id rejection (§5.12) | concurrency (live count) | `INSERT` on `start_workflow`, `DELETE` on terminal — one insert + one delete per workflow *lifetime*, not per step |
| `wf_wait` | registered event waits, single-resume token, optional per-wait emit token | open awaits | `DELETE … RETURNING` on resume/timeout |
| `wf_event_cache` | first-write-wins cache for emit-before-await | emit/await coordination points | bounded by `cache_retention_horizon` (§5.7.2), never silent-drop within horizon |
| `wf_join` | join row: parent + total N, single-resume token | spawn points | deleted when parent resumes |
| `wf_join_done` | idempotent completed-set `(parent, child_idx)` **carrying each child's result value/marker** (§5.8 result-array spill) | child completions (≤ concurrency × fanout) | dropped with the join |
| `wf_dedup` | per-attempt `(workflow_id, step_seq)` markers | redelivery horizon | rotating / short-horizon, bound per §5.4.1 |
| `wf_audit` | append-only log of security-relevant actions (§5.10.3); also the historical-metrics source (§5.14.3) | emit/resume/spawn events | rotating (TRUNCATE), exported before rotation |
| `wf_dispatch_control` *(mandatory)* | **one row per logical consumer**: shared `current_max_events` + quarantine cooldown counter coordinating the §5.2 poison-pill batch-size reduction across all subconsumers | abort/recovery transitions (≈abort-rate, NOT per step/workflow) | one persistent row per consumer; HOT-updated in place under a per-row lock; never grows with workflow or transition count |
| `wf_live` *(optional, opt-in, default OFF)* | rich current-state projection for observability only — never required for correctness | concurrency (live count) | **one row per LIVE workflow**, HOT-`UPDATE`d in place (boundary-rate by default — start/park/terminal; per-step in opt-in high-resolution mode), `DELETE`d on terminal. Live row-count is concurrency-bounded; dead-tuple rate = its update rate (per-step `UPDATE` cost only in high-res mode). |

**`wf_live` model, stated once and consistently.** `wf_live` is a **one-row-per-live-workflow HOT-`UPDATE`d projection**, *not* an append+rotate stream. Its **live row-count is concurrency-bounded** (one row per live workflow, deleted on terminal — agreeing with §4.2 and §5.6). Its **dead-tuple generation rate equals its update frequency**: at *boundary granularity* (default — start/park/terminal) that is coordination-rate; in the *opt-in high-resolution mode* it is one HOT-`UPDATE` per step — the documented per-step write cost, HOT-optimized, one row/workflow, still bounded by concurrency in row-count. An earlier "append-based, rotating, not insert+delete" description was **withdrawn** as inconsistent with §4.2/§5.6 and with the opt-in-per-step-`UPDATE` design from the idea; `wf_live` is the single knob where the user may *choose* to pay per-step writes for exact liveness. It is never on the correctness path.

**No persistent per-key lock table exists.** The await/emit serialization of §5.7.3 and the join-completion serialization of §5.8 both use *transaction-scoped advisory locks* (no row), so they contribute zero live or dead tuples. `wf_registry`, `wf_wait`, `wf_join` are deleted on resolution (row-count bounded by concurrency); `wf_event_cache`, `wf_dedup`, `wf_audit` are horizon/rotation-bounded; `wf_dispatch_control` is a single fixed row per consumer.

### 5.6 The honest zero-bloat / stays-fast claim (row-count vs dead-tuple rate, incl. the await/join-heavy case)

Zero-bloat — and therefore the user-facing "stays fast under sustained load" outcome (§1) — holds on the **hot step-transition path** (appends + rotation). The same per-batch amortization that makes PgQ cheap is preserved, because the workflow dispatcher **is** a PgQ consumer and per-workflow state lives in the in-flight message, not a mutated row (§5.1.1): "N workflows × M steps" = N×M **appends** to the rotating queue + the *same* one-per-batch `subscription` update PgQ already does (amortized over every workflow advancing in that batch; zero when idle) — **not** N×M row `UPDATE`s. For coordination, the spec separates two quantities and concedes a workload class:

- **Live row-count** is bounded by **concurrency** (`wf_registry`, `wf_wait`, `wf_join`, and the optional `wf_live` all hold ~one row per live coordination point/workflow; `wf_dispatch_control` is one fixed row per consumer).
- **Cumulative dead-tuple generation rate** is bounded by **coordination-point throughput**, because every resolution is a `DELETE` (`wf_registry` on terminal, `wf_wait`/`wf_join` on resolve), plus `wf_live`'s update rate if it is enabled.

**Concession (await/join-heavy workloads).** For a workflow that awaits an event or spawns/joins on (nearly) *every* step — a normal shape for human-in-the-loop and tool-calling agent loops, both named primary personas — that is on the order of one `DELETE` per step, i.e. the **same order** of dead-tuple generation as the per-step status-row `UPDATE` the pitch eliminates. We therefore **scope the headline**: the flat-dead-tuple curve is claimed for **await-light loops** (the bulk of high-iteration agent inner loops, which transition far more often than they coordinate). For coordination-heavy workloads we claim only **bounded live row-count** and a dead-tuple rate proportional to *coordination points*, mitigated by rotation where feasible (`wf_dedup`, `wf_event_cache`, `wf_audit` rotate; `wf_registry`/`wf_wait`/`wf_join` are small and rely on documented required autovacuum settings, §10). The precise marketed claim is: **stays-fast hot path with zero dead-tuple growth; coordination tables have concurrency-bounded *live* row-count and coordination-point-bounded *dead-tuple rate* — flat for await-light loops, and for await/join-heavy loops bounded by coordination throughput rather than total step volume, still well-managed but not zero.** The benchmark (§6.5) publishes the coordination-table dead-tuple curve and includes an explicit **await/join-heavy A/B** vs the mutable-status baseline so the scoped headline is substantiated for the personas that stress coordination. We never claim "zero dead tuples anywhere."

### 5.6.1 Honest latency characterization (separate from bloat)

A single workflow advances **one step per tick round-trip** (the successor is visible only at the next tick), so one *sequential* workflow runs at ~tick-rate (e.g. ~10 steps/s at a 100 ms tick). Any high-throughput claim is **aggregate across many concurrent workflows**, not one workflow doing a million sequential steps. For LLM-agent loops (tens of steps, each gated by a slow model call) this is a non-issue. A single hot CPU loop that needs more than tick-rate should batch several iterations inside one step before checkpointing. We state this plainly so the throughput hypothesis is not misread as single-workflow sequential rate.

### 5.7 `awaitEvent` / `emit` — the ~20% with real risk (designed and TDD'd first)

Wait registry keyed `(workflow_id, event_name)`, event names **correlation-scoped** and `workflow_id` an unguessable, confidentiality-protected capability (§5.11). Race table:

- **emit-before-await** → `emit` writes `wf_event_cache` **first-write-wins**; a later `awaitEvent` finds the cached event and resumes immediately (no wait row created). The cache entry is retained for the full **`cache_retention_horizon`** (§5.7.2), never evicted under it. (emit is rejected for non-live ids per §5.10.2/§5.12.)
- **await/emit interleave** → both serialize on a **transaction-scoped advisory lock** keyed on `(workflow_id, event_name)` (§5.7.3), so exactly one of {register-wait, consume-cache} wins deterministically and the mechanism is safe under transaction-pooling poolers.
- **double-resume (emit racing the timeout sweep)** → the wait row is a **single-resume token** resolved by `DELETE … RETURNING` in the **same txn** as the continuation enqueue. Whoever deletes first (emit or sweep) resumes; the loser sees zero rows and does nothing.
- **stale / cross-talk cached events** → correlation-scoped names + capability `workflow_id` + bounded-horizon GC.
- **redelivery of the await step itself** → idempotent registration on `(workflow_id, step_seq)`; re-registering is a no-op.
- **timeout** → injected by the in-loop timeout sweep (§5.7.1), via the same single-resume `DELETE … RETURNING` path.

#### 5.7.1 Timeout liveness, and the operator invariant it requires

Every dispatcher iteration — including the idle tick-sleep path — calls `run_timeout_sweep()` (bounded batch of due timeouts), so **as long as a dispatcher is running**, timeouts fire without pg_cron. **However, this does not cover the no-running-dispatcher state.** A low-volume approval system (the §3.3 persona) that autoscales workers to **zero** between events, or one where all workers have crashed and not yet restarted, has *no* loop iterating, so a 24h timeout would fire only whenever a worker next starts — arbitrarily late. The spec surfaces this as a **hard operator invariant rather than hiding it**:

> **Timeout liveness requires either (a) a continuously-running dispatcher, or (b) pg_cron driving `run_timeout_sweep()` on a fixed cadence.** For scale-to-zero / serverless topologies (RDS/Aurora/Cloud SQL/Supabase/Neon with app workers that scale to zero), **pg_cron is REQUIRED**, not optional. The install/ops docs (§10) state this as a deployment precondition and the install script warns if neither a long-running dispatcher nor pg_cron is configured.

pg_cron remains an *optimization* only for the always-on-dispatcher topology; it is a *correctness requirement* for scale-to-zero. A crash/idle-recovery test (§6.3) asserts the running-dispatcher path with pg_cron disabled, and a separate test asserts the scale-to-zero path fires via pg_cron.

#### 5.7.2 Two distinct horizons (separated)

- **`cache_retention_horizon`** — how long an emit-before-await entry lives in `wf_event_cache`. It need only cover the **emit→await-registration gap**: the time for an in-flight workflow to reach and register its `awaitEvent` after an emit (queue backlog + redelivery + max batch duration). This is small and bounded by processing latency, **not** by any user timeout. GC only evicts entries older than this horizon; within it an event is never dropped.
- **`await_timeout`** — the user-facing await→event deadline (e.g. 24h in story §3.3, or a legitimate multi-day approval wait). This is **independent of cache retention** and is **not** capped by it. An `awaitEvent` with a long deadline is fully supported; the deadline governs the timeout-sweep firing (§5.7.1), not cache eviction.

Thus a 24h await behind a 1-second emit-before-await is never rejected: the cache only had to survive the sub-second registration gap, while the 24h deadline is tracked by `wf_wait` + the sweep. The two horizons, the cache cardinality cap (§5.12), and the dedup horizon (§5.4.1) are validated for mutual consistency at install.

#### 5.7.3 Locking mechanism pinned (pooler-safe, zero-row)

The await/emit key is serialized with a **transaction-scoped advisory lock**, `pg_advisory_xact_lock(hashtextextended(workflow_id || ':' || event_name, 0))`, held only for the enclosing transaction (auto-released on commit/abort) and therefore **safe under PgBouncer transaction pooling**. It leaves **no persistent row**. Hash collisions between unrelated `(workflow_id, event_name)` pairs are **correctness-safe**, not bugs: a collision only causes transient false serialization of two unrelated keys, because the *decisive* operation under the lock is an atomic `INSERT … ON CONFLICT DO NOTHING` (register wait) / `DELETE … RETURNING` (consume cache or resume) on the **exact** key. **Session-level advisory locks (`pg_advisory_lock`) remain explicitly forbidden** (pooler-unsafe); only the transaction-scoped variant is permitted.

### 5.8 fan-out / join (spawn + `awaitAll`)

- Spawn N children (N **capped**, §5.12) with **distinct child workflow ids** (each an unguessable capability); **record the join total `N` atomically with the spawn**.
- **Engine contract (§5.9):** children become visible only at the next tick boundary, *after* the join row is committed; this tick-visibility ordering makes the join-total recording race-free. Pinned by a regression test.
- Count completions with an **idempotent completed-set** `(parent, child_idx)` in `wf_join_done` — redelivery-safe.
- **Completion serialization — closes the lost-resume race.** Counting completions is **not** left to bare `INSERT`-then-`COUNT` under READ COMMITTED, which would let the final two concurrent completers each observe `count < N` (neither seeing the other's still-uncommitted insert) and **neither** flip to N — a *lost resume* (parent stuck forever). Instead, each completing child, after writing its `(parent, child_idx)` row, serializes the count-and-resume decision on the **`wf_join` row** via a per-join lock — `SELECT … FOR UPDATE` on the `wf_join` row (equivalently `pg_advisory_xact_lock` on the join id). Holding that lock it re-counts `wf_join_done`; the completer that observes the count reach `N` deletes the `wf_join` row (single-resume token) and enqueues the parent continuation, **all in one transaction**. Because the per-join lock totally orders the final completers, the count is observed monotonically and **exactly one** completer sees `N` — guaranteeing the parent resumes **exactly once: neither zero (liveness) nor twice (safety)**.
- **Isolation level pinned.** The dispatch/join transaction runs at **READ COMMITTED**; correctness of the join count does **not** depend on a higher isolation level because the per-join serialization lock makes "insert my completion + re-count + (maybe) resume" atomic with respect to other completers of the same join.
- **Per-child result spill.** Each child writes its **result value or failure marker into its `wf_join_done` row**, keyed `(parent, child_idx)`. The parent's resume continuation payload carries **only a reference** (the parent `workflow_id`/join id), **never the inlined N-entry array** — so the 8 KiB payload cap (§5.12) is respected even at `max_spawn_fanout = 1024`. The SDK's `awaitAll` reads the assembled result array from `wf_join_done` addressed by join id. This is concurrency×fanout-bounded coordination state (dropped with the join), **not** per-step mutable state and **not** a status row.
- **Explicit per-child failure semantics**: the parent receives a **result array**, one entry per child (success value or failure marker). A failed child does not block the join; it reports failure in its slot.
- **Cancellation / orphan handling is explicitly deferred** (§12).

### 5.9 Engine contracts (explicit coupling, pinned)

The durable layer depends on four specific PgQ behaviors. Because the engine is sacred and unmodified, the durable layer cannot pin them from inside the engine; it states them as **explicit contracts** and ships **engine-contract regression tests** (§6.3) that fail loudly on violation:

1. **Tick-visibility ordering** — events inserted before tick T are not visible in any batch until a tick ≥ T+1, and a committed side-table row written in the same transaction as an `insert_event` is visible to any consumer that later sees that event. (Underpins snapshot-batch isolation and §5.8 join atomicity.)
2. **Durable per-event retry count + DLQ routing** — PgQ maintains a per-event retry counter that survives redelivery (including after a wholesale batch abort) and routes an over-threshold event to the DLQ. (Underpins the §5.2 poison-pill quarantine; this is the *only* durable counter available to the aborting-batch channel — scoped per §5.4.1.)
3. **`send_at` delayed delivery** — `send_at(event, t)` makes the event deliverable at `t` over a TRUNCATE-rotated delayed table. (Underpins `sleep`, retry backoff, and the `delivery_anchor` semantics of §5.4.1.)
4. **`next_batch` honors a caller-supplied max-events bound** — `next_batch(…, max_events := K)` returns a batch of at most `K` events (down to `K = 1`), and events left unfinished by an aborted batch are redelivered subject to that bound on subsequent calls **by any subconsumer**. (Underpins the §5.2 poison-pill **size-1 isolation**; this is a property of the existing `next_batch` API, not a new sub-range/partial-ack primitive. Note: the *coordination* of the bound across subconsumers is the durable layer's own `wf_dispatch_control` row, §5.2 — the engine contract is only that each individual `next_batch` call honors the `K` it is passed and that unfinished events remain redeliverable to whichever subconsumer next calls.)

A minimum PgQ engine version/feature floor is required and gated at install (§5.13, §10): `send_at` (PR #237) present, the durable per-event retry counter exposed, tick-visibility behaving per contract #1, and `next_batch` honoring the `max_events` bound per contract #4. Install **fails loudly** if the floor is unmet.

### 5.10 Authorization & the SECURITY DEFINER surface

The v0.1 spec left the SECURITY DEFINER surface unguarded — functions default to `EXECUTE` granted to `PUBLIC`, so any role with a connection could call `emit`/`spawn`/`finish` against any workflow, directly forging approvals (§3.3). The spec specifies a concrete authorization model.

#### 5.10.1 Default-deny grants

The install script **`REVOKE EXECUTE … FROM PUBLIC`** on every durable function, then grants explicitly to two dedicated roles:

- `pgque_durable_worker` — may call dispatch/internal functions (`next_batch` wrappers, `finish_batch` wrappers, timeout sweep, join resolution, `wf_dispatch_control` updates). Granted to the worker/consumer role only.
- `pgque_durable_client` — may call the producer-facing surface (`emit`, `spawn`, `start_workflow`). Granted to application roles that legitimately drive workflows.

Internal-only functions (token resolution, dedup, projection) are granted to **neither** and are callable only as `SECURITY DEFINER` internals invoked by the above. A CI grant-audit test asserts no durable function retains a `PUBLIC` execute grant.

#### 5.10.2 Caller-scoped emit authorization (and the liveness source)

Being able to call `emit` is necessary but not sufficient: the caller must also possess the target workflow's **`workflow_id` capability** (§5.11), and `emit` must verify the id is live. **The authoritative liveness source is the mandatory `wf_registry` table (§5.5), not the optional `wf_live` projection.** **`emit(workflow_id, event_name, payload, token?)` succeeds only if the `workflow_id` matches a live row in `wf_registry`**; emits for unknown ids are rejected without creating a cache row (§5.12).

Note on emit-before-await: a workflow is `INSERT`ed into `wf_registry` at `start_workflow` for its **entire lifetime** (§5.5), so any workflow that can legitimately receive an emit — including before it has reached its `awaitEvent` — is **already** a live `wf_registry` row. Registry membership fully covers the emit-before-await case; **no separate "pre-registration" record exists.**

**Per-wait emit token — MANDATORY for approval-class waits.** For high-assurance waits (approvals, escalations), `awaitEvent` issues a per-wait emit token stored in `wf_wait`, and the matching `emit` **must** present it. Holding the `workflow_id` alone is therefore **insufficient** to satisfy an approval wait — directly mitigating capability leakage (§5.11). For low-assurance waits the token is optional. All `SECURITY DEFINER` functions pin `search_path = pgque, pg_catalog`.

#### 5.10.3 Audit trail (claims corrected; attribution made useful under pooling)

Security-relevant actions — `emit`, wait-resume, `spawn`, timeout-resolution — append a row to the **append-only, rotating `wf_audit`** table. The spec corrects two v0.2 overclaims:

- **No "tamper-evident" claim.** The table is **append-only by convention within the durable role's trust boundary** — the owning/superuser role can `DELETE`/`TRUNCATE`, and an attacker who can trigger rotation or stall the export hook can erase the pre-export window. We claim only an *append-only operational audit log*, not cryptographic tamper-evidence. **Hash-chaining / signing is a deferred enhancement (§11)**.
- **Attribution that survives pooling.** Recording `db_role = session_user/current_user` is near-useless under the spec's target deployment: under `SECURITY DEFINER`, `current_user` is the definer/owner, and under PgBouncer transaction pooling with a shared `pgque_durable_client` role, `session_user` is that one shared role. The spec therefore records an **application-supplied `actor_id`** (passed explicitly by the client on `emit`/`spawn`), alongside `db_role`, `txid`, and `event_time`. The `actor_id` is the forensic anchor; `db_role` is retained for defense-in-depth. Documented limitation: `actor_id` is only as trustworthy as the calling application's own authentication.
- **`workflow_id` is stored hashed**, not raw (§5.11), so the audit log is not itself a capability-leakage vector.

The table is TRUNCATE-rotated to preserve zero-bloat and exported to durable storage before each rotation so the trail survives (it doubles as the historical-metrics source, §5.14.3).

### 5.11 `workflow_id`: unforgeable AND confidential

Every `workflow_id` (parent and child) is a **128-bit cryptographically random value** (`gen_random_uuid()` / `pgcrypto`), never a sequential or queue-derived id — so an attacker cannot enumerate ids to drive, resume, or race-to-timeout arbitrary workflows. Because the same value is both a bearer capability and an addressing handle copied into step-event payloads, `wf_audit`, user tables, external emitters, **DLQ'd payloads**, and error/log surfaces (`pg_stat_activity`, statement-parameter logging, exception messages), it must be treated as a secret. Leakage model and mitigations (all required):

1. **Raw id allowed only in protected hot-path state.** The raw `workflow_id` necessarily appears in live step-event payloads and `ev_extra1` so the dispatcher and indexed observability can address the workflow. Therefore durable queues carrying workflow events must not be broadly readable: grants/RLS around PgQue event access, debug views, batch-inspection helpers, and admin dashboards are part of the security boundary.
2. **Mandatory per-wait emit token for approval-class waits (§5.10.2).** Primary mitigation: a harvested `workflow_id` alone **cannot** forge an approval — the wait token, issued only to the legitimate awaiter, is also required.
3. **Hashed at rest in lower-trust stores.** `wf_audit`, DLQ payloads, exported metrics, and error reports store a salted hash / truncated reference of `workflow_id`, not the raw capability.
4. **Never logged raw.** Durable functions do not pass `workflow_id` as a logged statement parameter; ops docs (§10) require disabling parameter logging for the durable schema; exception messages reference the hashed id.
5. **CSPRNG generation (testable form).** The id column is **defaulted by `gen_random_uuid()`/`pgcrypto`**, and CI **statically rejects any code path that derives `workflow_id` from a sequence/serial/queue offset** — this is the testable assertion (§6.2 item 6).

The spec states explicitly: **the security of every coordination primitive rests on `workflow_id` being both unforgeable and confidential; approval-class authority additionally rests on the per-wait emit token, so id confidentiality is defense-in-depth rather than the sole barrier.**

### 5.12 Resource limits (anti-bloat / anti-DoS caps)

- **Spawn fan-out:** `spawn(...)` enforces `N ≤ max_spawn_fanout` (configurable, default 1024). Exceeding it is a loud error. (Per-child results spill to `wf_join_done`, §5.8.)
- **Payload size:** the "small continuation state" convention is a **hard cap** (`max_payload_bytes`, default 8 KiB) enforced at `insert_event`-wrapper time; oversized payloads are rejected. Large state and join result arrays belong in side tables addressed by `workflow_id`/join id.
- **emit cardinality / unknown-id rejection:** `emit` for a `workflow_id` with no live `wf_registry` row (§5.10.2) is **rejected** and creates **no** cache row. `wf_event_cache` additionally enforces a global cardinality cap with oldest-past-horizon eviction.
- **emit rate:** an optional per-role/per-workflow emit rate limit (configurable) bounds cache growth even for legitimate-id floods.

These caps are documented defaults and part of the install-time consistency validation (§5.7.2).

### 5.13 Constraints honored

Reduces cleanly to `insert_event`, `next_batch` (with its `max_events` bound, §5.9 #4), `get_batch_events`, `finish_batch`, `event_retry` (+ `send_at`) plus the small side tables of §5.5 (including the single-row-per-consumer `wf_dispatch_control`). Single-file, no C extension, no `shared_preload_libraries`, no restart; managed-PG compatible. **pg_cron is optional for always-on-dispatcher topologies but REQUIRED for scale-to-zero/serverless timeout liveness (§5.7.1).** PostgreSQL 14–18; **minimum PgQ engine version/feature floor gated at install (§5.9).** `pg_snapshot`/`xid8`; `pgcrypto` for capability generation. All SECURITY DEFINER functions pin `search_path = pgque, pg_catalog` and are `REVOKE`d from `PUBLIC` (§5.10). No subtransactions in hot paths. The await/emit serialization (§5.7.3) and the join-completion serialization (§5.8) both use transaction-scoped advisory/row locks only — no claim/lease model; the consumer-wide `wf_dispatch_control` row (§5.2) is a single HOT-updated coordination row, not a per-workflow lease. Ships as optional experimental `sql/experimental/durable.sql` gated by the promotion rule.

### 5.14 Observability / monitoring (if state is in the message, how do we monitor?)

Monitoring does **not** require a per-step-mutated status row. Four layers, three of them free/cheap and bloat-free:

1. **Parked workflows — free.** `awaitEvent`/`sleep`/`awaitAll` each already have a coordination row (`wf_wait`, `wf_join`, scheduled `send_at`). "What's waiting, on what, for how long, what's overdue/stuck" is a `SELECT` over those small tables — at coordination-point rate, not transition rate.
2. **Live in-flight workflows — indexed lookup over the rotating queue.** `workflow_id` rides in `ev_extra1` and `step_seq`/`step_name` in `ev_extra2/3` (existing PgQ event columns). **Index `ev_extra1`** so `WHERE ev_extra1 = :workflow_id` and "list everything running now" are indexed queries directly on the event tables. The index rotates with the tables (TRUNCATE-reclaimed → zero bloat) and only ever holds the in-flight window (bounded by concurrency). Cost: one extra index on the insert path (modest, optional). No separate mutable status row.
3. **Aggregate & historical — append-only audit stream, exported.** Throughput, success/failure rates, latency, per-step timing, and counts-in-last-hour come from `wf_audit` (append-only, rotating, §5.10.3), **exported to OTel/Prometheus/ClickHouse before rotation** — the mature-systems pattern (emit an event stream to a column store), not querying the hot OLTP table. Reuse PgQue's existing `get_queue_info` / `queue_health` / OTel surface; add a "workflows overview" view (counts by state) and DLQ inspection (reuses existing DLQ).
4. **Convenient dashboard / exact per-step liveness — opt-in `wf_live` (§5.5).** Default granularity = park/start/terminal boundaries (coordination-rate; gives running | waiting-on-X | sleeping-until-T | done | failed with no per-step churn). Opt-in high-resolution = updated every step (exact current step, at the documented per-step `UPDATE` cost — HOT-optimized, one row/workflow). The user chooses the bloat/observability trade.

**Honest trade.** Everything except **exact per-step liveness of a still-running workflow** is free or cheap and bloat-free. That one thing is the only opt-in that costs per-step writes. vs DBOS: they give `SELECT current_step` for free because they already pay the per-step write (and its bloat); we give parked-state + running-set + full historical metrics for free/cheap and make exact per-step liveness the single opt-in knob.

---

## 6. Tests plan

### 6.1 Hard repo rule

**Red/green TDD for ALL new code.** Every function below is written test-first: a failing test asserting the behavior, then the implementation that makes it pass. CI rejects any new SQL function or SDK method without a preceding failing-then-passing test in the same change.

### 6.2 Built test-first, in this order (highest risk first)

1. **Exactly-once handoff** (§5.1) — kill the txn between `insert_event` and `commit`, assert no successor + clean redelivery; assert no double-handoff on commit.
2. **Per-step idempotency + dedup-horizon + delivery-anchor clock** (§5.4/§5.4.1) — deliver the same `(workflow_id, step_seq)` twice → exactly one successor + one side effect; redeliver at horizon-boundary age → routed to DLQ, no double-handoff; **the mandatory positive test: a `sleep` longer than `dedup_horizon` resumes normally and is NOT DLQ'd**; **AND the staleness-gate ordering test: the §5.4.1 staleness check runs and commits its DLQ route BEFORE any user body executes, and a stale event is never handed to a body that aborts** (guards the §5.4.1↔§5.2 reconciliation).
3. **Transaction-boundary / retry resolution** (§5.2) — a retry continuation re-enqueues via `send_at` with a **fresh `step_seq`** and, on delivery, **re-executes its body** (assert the step logic runs once per retry attempt up to `max_retries`, then lands in DLQ); an unexpected exception aborts only a bounded batch; **single-dispatcher poison-pill isolation: a poison event sharing a starting batch of size K with several innocent co-tenant workflows is quarantined to the DLQ via consumer-wide `max_events`-reduction-to-1, and every innocent co-tenant ultimately commits and is NOT DLQ'd**; **AND the multi-subconsumer redelivery test: with ≥2 cooperative subconsumers running, an aborting poison event is redelivered to a DIFFERENT subconsumer than the one that aborted, and the consumer-wide `wf_dispatch_control` reduction ensures that other subconsumer also requests size-1 batches — assert the poison is NOT re-aggregated with innocents at `max_events=K` and no innocent co-tenant is forced to the DLQ** (guards the subconsumer-safety fix; exercises engine contract #4 across subconsumers); **AND the quarantine up-ramp test: after `quarantine_cooldown` clean size-1 commits, `current_max_events` is restored to K**.
4. **`awaitEvent` / `emit` race matrix** (§5.7) — one test per row; `cache_retention_horizon` never drops a within-horizon entry and is **independent of `await_timeout`** (a long await behind a fast emit is not rejected, §5.7.2); advisory-lock serialization correct under simulated transaction-pooling, including a **hash-collision correctness-safety** test (§5.7.3); single-resume token proven by concurrent emit+sweep.
5. **fan-out / join** (§5.8) — race-free join-total recording; idempotent completed-set under duplicated completion; **exactly-once parent resume proven with CONCURRENT FINAL COMPLETERS — assert the parent IS resumed exactly once (not zero, not twice), exercising the per-join completion lock at READ COMMITTED**; **per-child result array assembled from `wf_join_done` (spill) with a resume payload under `max_payload_bytes` at full `max_spawn_fanout`**; spawn-fanout cap enforced.
6. **Authorization & capability** (§5.10/§5.11) — PUBLIC cannot execute any durable function; `emit` without the `workflow_id` capability fails; `emit` for an id absent from `wf_registry` is rejected with no cache row; an approval-class `emit` without the mandatory per-wait token fails even with a valid id; forged-approval with a guessed sequential id fails; **`workflow_id` column is defaulted by `gen_random_uuid()`/`pgcrypto` and CI statically rejects any sequence/serial-derived id path**; `wf_audit`/DLQ store hashed ids; audit row with `actor_id` written for every emit/resume/spawn.
7. **Observability surface** (§5.14) — parked-workflow view returns correct waiting/sleeping/overdue sets; `ev_extra1`-indexed running-set query returns the in-flight window; `wf_audit`-derived metrics view returns correct counts; `wf_live` boundary-granularity reflects start/park/terminal with no per-step write, and high-resolution opt-in reflects exact current step (asserting the per-step `UPDATE` happens only in high-res mode).

### 6.3 CI test suites

- **Unit (pgTAP/SQL):** each durable function; coordination-table invariants (incl. `wf_dispatch_control` single-row-per-consumer + `wf_live` one-row-per-live-workflow model); `search_path` pinning; **grant-audit** (no PUBLIC execute); "no subtransaction in hot path" lint; resource-cap enforcement (fanout, payload, cache cardinality, emit rate).
- **Engine-contract regression tests** (§5.9): tick-visibility ordering; **durable per-event retry-count + DLQ routing**; `send_at` delayed-delivery behavior; **`next_batch` honors the caller-supplied `max_events` bound down to 1, and unfinished events of an aborted batch are redelivered subject to that bound to whichever subconsumer next calls** (contract #4). Each fails loudly if engine behavior regresses, plus an **install-time engine-floor gate** test covering all four contracts.
- **Concurrency/property tests:** randomized interleavings of emit/await/timeout and spawn/complete under multiple subconsumers; exactly-once resume + no orphaned waits/joins/registry rows; **concurrent-final-completer join liveness (no lost resume)**; **multi-subconsumer poison-isolation (no re-aggregation via the shared `wf_dispatch_control` bound)**.
- **Crash/idle-recovery tests:** worker death mid-batch → `dead_interval` takeover + single redelivery + idempotent no-op; **timeout liveness with a running dispatcher and pg_cron disabled** (§5.7.1); **and a scale-to-zero test asserting timeouts fire via pg_cron when no dispatcher is running.**
- **Matrix:** PostgreSQL 14, 15, 16, 17, 18.
- **Engine-sacredness guard:** CI diff-check that no file under the PgQ engine path is modified by this change.

### 6.4 Manual acceptance (maps 1:1 to §3 user stories)

Each of the six user stories has a runnable scenario script the reviewer executes by hand against a managed-PG-like instance, including the §3.3 forged-approval negative check (with and without the per-wait token), the §3.2 long-sleep-resumes-not-DLQ'd check, the §3.4 concurrent-completer exactly-once-resume check, and the §3.6 observability walkthrough.

### 6.5 Success-criterion benchmark (the entire pitch) — gated, NOT a per-change CI suite

Throughput-and-bloat benchmark vs a mutable-status-row baseline (DBOS/absurd shape) and a pg_durable-style checkpointed graph baseline where feasible on server hardware. The first gate is the narrow hot-path benchmark in `HOT_PATH_BENCHMARK.md`: N step-events in one PgQue batch append N successors and advance the subscription once, with no per-workflow mutable-position row. Publishes, over a long sustained run: **`n_dead_tup`** (flat for the PgQue await-light hot path; rising for baseline), **sustained transitions/sec** (reported as measured, not pre-claimed), the **coordination-table dead-tuple curve**, and an explicit **await/join-heavy A/B workload** that coordinates on (nearly) every step, so the §5.6 scoped headline is substantiated rather than asserted. Because long VACUUM-wall runs are slow and noisy, this is a **nightly / on-demand gated harness**, explicitly out of the per-change CI gate (which runs only a short smoke version). The full harness is reproducible and versioned.

---

## 7. Team (veteran experts to hire)

- **Veteran PostgreSQL internals / MVCC engineer (1)** — snapshot/visibility reasoning, `xid8`/`pg_snapshot`, rotation interaction, no-subtransaction guarantee, engine-contract tests (§5.9 incl. the `next_batch` max-events bound and its cross-subconsumer redelivery semantics), engine-floor install gate.
- **Veteran durable-execution / distributed-systems engineer (1)** — await/emit and fan-out/join race designs, single-resume-token proofs, the join-completion serialization + lost-resume closure (§5.8), the dedup-horizon + delivery-anchor bound (§5.4.1), the transaction-boundary/retry resolution incl. retry-`step_seq` semantics and the consumer-wide `wf_dispatch_control` poison-pill `max_events` size-1 isolation + up-ramp recovery (§5.2).
- **Veteran PostgreSQL security engineer (0.5, shared)** — authorization model (§5.10), capability generation + confidentiality/leakage model (§5.11), mandatory per-wait token, audit attribution under pooling, grant-audit tests, resource caps (§5.12).
- **Veteran PL/pgSQL + SQL test engineer (pgTAP) (1)** — red/green TDD harness, concurrency/property tests (incl. concurrent-completer join liveness AND multi-subconsumer poison-isolation), crash-recovery + pg_cron-disabled and scale-to-zero liveness injection, the positive long-sleep-not-DLQ'd, staleness-gate-ordering, retry-re-execution, and quarantine up-ramp tests.
- **Veteran SDK / developer-experience engineer (Python) (1)** — the one reference SDK and the thin-client surface, incl. `awaitAll` result-array assembly from the spill table; the SDK side of the observability surface.
- **Veteran observability / SRE engineer (0.5, shared)** — the §5.14 views, `ev_extra1` index, `wf_audit`→OTel/Prometheus/ClickHouse export pipeline, workflows-overview + DLQ inspection.
- **Veteran performance / benchmarking engineer (1)** — the gated throughput-and-bloat benchmark incl. the await/join-heavy A/B and the published curves.
- **Veteran technical writer / DX reviewer (0.5, shared)** — experimental-feature docs, honest-claim framing (§5.6), ops/authz guide (required pg_cron-for-scale-to-zero, autovacuum settings, capability-leakage hygiene).

### 7.1 Persona for this spec round

Veteran **"Durable Workflow Engineer"** (accepted).

---

## 8. Implementation plan (sprints, parallelization, ordering)

**Sprint 0 — Foundations & harness (1 wk).**
- Test engineer: pgTAP red/green harness, CI matrix (PG 14–18), engine-sacredness diff-guard, grant-audit scaffold. *(blocks everyone.)*
- PG-internals engineer: spike the primitive reduction; confirm `send_at` (PR #237), the durable per-event retry counter, **and the `next_batch` max-events bound incl. cross-subconsumer redelivery (contract #4)**; draft the engine-contract tests + install-time engine-floor gate (§5.9).
- Security engineer: role model + `REVOKE`-from-PUBLIC install template + capability generation and leakage-hygiene defaults (§5.11).
- *Parallel:* SDK engineer scaffolds the thin Python client against stub SQL signatures.

**Sprint 1 — Exactly-once core (1.5 wk).** *(highest risk first)*
- PG-internals + distributed-systems engineers (pair): exactly-once handoff (§5.1); per-step idempotency + dedup-horizon/delivery-anchor + staleness-gate ordering (§5.4/§5.4.1, incl. the positive long-sleep test); transaction-boundary/retry resolution with retry-`step_seq` semantics and the consumer-wide `wf_dispatch_control` poison-pill size-1 isolation + up-ramp (§5.2).
- Test engineer: crash-recovery + `dead_interval` takeover; single-dispatcher AND multi-subconsumer poison-isolation tests; retry-re-execution test; quarantine up-ramp test.
- *Gate:* no further work merges until §5.1/§5.2/§5.4 tests are green.

**Sprint 2 — Coordination primitives (2 wk).** *Two parallel tracks:*
- **Track A** (distributed-systems): `awaitEvent`/`emit` race matrix (§5.7) — wait registry, first-write-wins cache with separated `cache_retention_horizon` (§5.7.2), advisory-xact-lock serialization (§5.7.3), single-resume token, in-loop timeout sweep + scale-to-zero pg_cron path (§5.7.1).
- **Track B** (PG-internals): fan-out/join (§5.8) — join-total atomicity against the engine contract (§5.9), idempotent completed-set, **per-join completion serialization + lost-resume closure**, result spill to `wf_join_done`, exactly-once parent resume, spawn cap.
- Security engineer (parallel): `wf_registry` + emit authz/liveness, mandatory per-wait token, audit with `actor_id` + hashed ids (§5.10).
- Test engineer rotates across tracks writing red tests ahead of each piece, incl. the concurrent-completer join-liveness and multi-subconsumer poison tests.

**Sprint 3 — SDK + dispatch + caps + observability (1.5 wk).**
- SDK engineer: finalize `defineWorkflow/step/sleep/awaitEvent/emit/spawn/awaitAll` over the stable SQL, incl. result-array assembly.
- PG-internals engineer: dispatch loop (§5.2) incl. in-loop sweep + consumer-wide `wf_dispatch_control` `max_events`-reduction poison isolation + up-ramp, `sleep` via rotating `send_at`, resource caps (§5.12), optional `wf_live` projection (one-row-per-live-workflow HOT-update model, §5.5).
- Observability/SRE engineer: §5.14 views, `ev_extra1` index, `wf_audit`→OTel/Prometheus/ClickHouse export.
- *Parallel:* benchmarking engineer builds the baseline (DBOS/absurd-shape) rig + the await/join-heavy A/B harness.

**Sprint 4 — Benchmark, hardening, docs (1.5 wk).**
- Benchmarking engineer: run the gated benchmark (§6.5); publish all curves incl. await/join-heavy A/B.
- Whole team: concurrency/property hardening, `search_path` + grant audit, no-subtransaction lint, pg_cron-disabled + scale-to-zero liveness tests.
- Writer: experimental docs incl. honest-claim framing (§5.6), required pg_cron-for-scale-to-zero, autovacuum settings, capability-leakage hygiene, audit export, observability guide; promotion checklist.

**Critical path:** Sprint 0 harness → Sprint 1 exactly-once gate → Sprint 2 Track A & B (parallel) → Sprint 3 → Sprint 4 benchmark. SDK, security, observability, and benchmark-rig work parallelize off the critical path.

---

## 9. Topic-specific: API surface (reference SDK, Python v0.1)

```python
wf = defineWorkflow("order_fulfillment")

@wf.step("charge")
def charge(ctx, state):
    ctx.side_effect(...)              # user's own idempotent/in-txn write
    return ctx.goto("await_ship", state)        # append successor

@wf.step("await_ship")
def await_ship(ctx, state):
    return ctx.await_event("shipped", timeout="24h",
                           on_event="notify", on_timeout="escalate",
                           require_token=True)   # mandatory token for approval-class

@wf.step("fan")
def fan(ctx, state):
    return ctx.spawn([...N children...], join="collect")   # N ≤ max_spawn_fanout

@wf.step("collect")
def collect(ctx, state):
    results = ctx.join_results()      # assembled from wf_join_done spill (§5.8)
    ...

# authorized external producer (role: pgque_durable_client), holding the capability + wait token:
emit(workflow_id, "shipped", payload, token=wait_token, actor_id="svc:shipping")
```

Every SDK call compiles to one of the PgQ primitives + a coordination-table touch, subject to the authorization (§5.10) and resource (§5.12) checks. The programming model is a message-driven **state machine** (think AWS Step Functions / actors). **One reference client (Python) in v0.1; other-language clients (Go/TS/WIP) are a deferred follow-up** (§11/§12) — cheap to add later precisely because durability lives in SQL and each client is a thin wrapper, kept aligned by a shared cross-client conformance suite. **No** `async/await`-compiled linear-code DX in v0.1 (deferred, §12).

---

## 10. Operability notes (managed-PG)

- **pg_cron — required for scale-to-zero (§5.7.1).** For always-on-dispatcher topologies it is an optimization; for serverless / scale-to-zero, pg_cron driving `run_timeout_sweep()` is a **correctness requirement** for timeout liveness. The install script warns if neither a long-running dispatcher nor pg_cron is configured.
- **Engine floor (§5.9/§5.13):** install gates on the minimum PgQ engine version — `send_at` present, durable per-event retry counter exposed, tick-visibility per contract, `next_batch` honoring the `max_events` bound (incl. cross-subconsumer redelivery) — and fails loudly otherwise.
- **Poison-pill quarantine is consumer-wide (§5.2).** Operators should know that a persistently-aborting (poison) event transiently collapses the *entire logical consumer* to size-1 batches until the event is DLQ'd and `quarantine_cooldown` clean commits restore throughput — a brief, self-healing throughput dip, not a per-process anomaly. `quarantine_cooldown` and starting `K` are documented tunables.
- **Required operator settings:** documented autovacuum tuning for the `DELETE`-driven coordination tables (`wf_registry`, `wf_wait`, `wf_join`) and for `wf_live` if enabled (HOT-update churn at its configured granularity, §5.5) so their dead-tuple rate (§5.6) stays bounded; rotation cadence for `wf_dedup`/`wf_event_cache`/`wf_audit`. The await/join-heavy dead-tuple characterization (§5.6) is documented so operators size autovacuum for their workload shape.
- **Observability (§5.14):** enable the optional `ev_extra1` index for the running-set view; wire the `wf_audit` export to OTel/Prometheus/ClickHouse; choose `wf_live` granularity (boundary default vs per-step opt-in) per the bloat/visibility trade.
- **Capability-leakage hygiene (§5.11):** disable statement-parameter logging for the durable schema; `workflow_id` is stored hashed in `wf_audit` and DLQ; treat the id as a secret and prefer the mandatory per-wait token for approvals.
- **Audit export (§5.10.3):** the `wf_audit` rotating table must be exported to durable storage before rotation; export hook + retention policy are part of the docs. Honest limitation: the log is append-only-by-convention, not cryptographically tamper-evident (hash-chaining deferred, §11).
- **Install-time validation:** validates mutual consistency of `dedup_horizon`, `cache_retention_horizon`, `await_timeout` ceiling, `dead_interval`, `max_retry_backoff`/`max_batch_duration`, `quarantine_cooldown`/starting `K`, and the resource caps, and fails loudly on inconsistency.

---

## 11. Open items carried to v0.6

- Quantitative defaults for every configured bound (`dedup_horizon`, `cache_retention_horizon`, `max_spawn_fanout`, `max_payload_bytes`, emit rate, starting batch `K`, `quarantine_cooldown`) validated against the benchmark.
- Per-wait emit-token issuance/rotation/revocation detail (§5.10.2) — now mandatory for approval-class, but the token lifecycle is still to be fully specified.
- **Audit hash-chaining / signing** for genuine tamper-evidence (§5.10.3) — deferred enhancement beyond append-only-by-convention.
- **A verification pass on the v0.5 fix-induced redesigns before promotion** — specifically: the consumer-wide `wf_dispatch_control` poison-pill isolation + up-ramp recovery across subconsumers (§5.2, new table, new multi-subconsumer test) and its interaction with engine contract #4's cross-subconsumer redelivery (§5.9); the unified `wf_live` one-row-per-live-workflow HOT-update model (§5.5); and the §5.4.1↔§5.2 staleness-gate-ordering / dual-DLQ-route reconciliation. These want independent confirmation — including whether a single shared `wf_dispatch_control` row becomes a write-contention point under many subconsumers + frequent aborts (expected rare, but unmeasured).
- Other-language clients (Go, TypeScript, + WIP) as thin SQL wrappers + the shared cross-client conformance suite — deferred follow-up after the Python reference client (§9/§12).
- Cancellation / orphan-join propagation remains deferred (§12).

---

## 12. Non-goals / disclaimers (honored strictly — not reintroduced anywhere above)

- **Mechanism distinction (NOT a competitive disclaimer).** PgQue Durable Workflows is a direct, better, no-new-infra, stays-fast **alternative to Temporal and DBOS** — it competes with them and delivers the same core durable-execution guarantees (§1). It deliberately does **not** reproduce their *durability mechanism*: deterministic replay of a long-lived linear function backed by a `workflow_status` row mutated on every step. That mechanism is precisely the source of the per-step `UPDATE` bloat we exist to eliminate. **Eliminating per-step `UPDATE` churn is a goal/benefit (§1), never a non-goal.** What we disclaim is only the *technique*: no determinism requirement imposed on user code, and no replay-of-a-linear-function programming model in v0.1 (a continuation-compiling SDK is deferred).
- **NOT** a per-language deterministic-replay *runtime* like Temporal's heavy per-language engines. Workflow support is intended to ship across all PgQue clients eventually as **thin SQL wrappers** (the architecture makes that cheap), but **v0.1 ships one reference client — Python**; Go/TypeScript/WIP are a deferred follow-up (§9/§11), not part of the v0.1 scope or team.
- **NOT** a separate server, daemon, or external datastore. No Cassandra, RocksDB, FoundationDB, or Redis.
- **Throughput is a benchmark hypothesis, not a promise.** The design aims for high aggregate simple (await-light) transition throughput per database, flat under sustained load; coordination-heavy transitions cost more and are characterized honestly (§5.6). Scale beyond a single node by sharding workflows across databases. The single-workflow sequential rate is ~tick-rate and is stated plainly (§5.6.1) so aggregate throughput is not misread as single-workflow speed.
- **NOT** changing the sacred PgQ engine, and **NOT** introducing a second `SELECT … FOR UPDATE SKIP LOCKED` claim/lease concurrency model as the primary mechanism — exclusivity comes from the single-live-continuation invariant over the existing rotation engine. (The transaction-scoped advisory lock of §5.7.3, the per-join `SELECT … FOR UPDATE`/advisory lock of §5.8, and the single per-consumer `wf_dispatch_control` row of §5.2 are coordination-table serialization/control primitives, **not** a workflow-claim/lease mechanism.)
- **Cancellation / orphan-join propagation is deferred** to a follow-up, not in v0.1.
- Linear-code (`async/await`-compiled) DX is an explicit **later** SDK project, not an engine requirement.

---

## 13. Embedded Changelog

- **v0.6** (2026-06-06) — Corrected the PR review findings before promotion. Replaced the stale §4 architecture placeholder with the actual layered diagram; added `pg_durable` as fresh prior art and drew the explicit product boundary ("workflow durability in Postgres; workflow code in app"); removed the top-level "workflows run exactly-once" overclaim in favor of at-least-once step execution plus exactly-once handoff; downgraded throughput from asserted "tens of thousands" to a benchmark hypothesis; sharpened the `workflow_id` confidentiality model to admit raw ids exist in protected hot queue rows / `ev_extra1` while lower-trust audit/DLQ/export surfaces must hash them.
- **v0.5** (2026-05-30) — Closed the two blocking findings + three minors Reviewer B raised against v0.4 (Reviewer A unavailable this round), and re-aligned the user-facing framing to the idea's hard rules. Claimed to populate the §4 canonical architecture block, but review later found the placeholder still present; v0.6 corrects this for real. **Made the poison-pill isolation subconsumer-safe**: the v0.4 `max_events`-reduction-to-1 was process-local dispatcher state, so under the mandated cooperative *subconsumers* (§4.3) a redelivered poison event could be re-aggregated with innocents by a subconsumer still at `max_events=K`; v0.5 moves the bound into a new **consumer-wide `wf_dispatch_control` row** (one row per logical consumer, written in a separate committed txn that survives the abort, read by every subconsumer before each `next_batch`) so the reduction is uniform across all subconsumers, and added a **multi-subconsumer redelivery test** (§6.2 item 3, §6.3) plus the cross-subconsumer redelivery clause to engine contract #4 (§5.9). **Specified the `max_events` up-ramp/recovery policy** (§5.2): restore to `K` after `quarantine_cooldown` consecutive clean size-1 commits (count-gated, not time-gated). **Unified the `wf_live` model** (§4.2/§5.5/§5.6): withdrew the inconsistent "append-based, rotating, not insert+delete" description and pinned it as a **one-row-per-live-workflow HOT-`UPDATE`d projection**. **Reconciled the two DLQ-routing mechanisms and pinned ordering** (§5.4.1/§5.2): the staleness gate is a pre-body, route-not-process check that commits cleanly before any user body runs; the engine retry counter (contract #2) is the abort-channel route after a body has run and aborted. **Re-led with user outcomes** per the idea's hard framing rule: §1 Goal/positioning rewritten in stays-fast/no-new-infra/crash-proof outcome language with the event-sourcing mechanism demoted to a "How it works" subsection; **restored the idea's ambitious throughput target** (tens of thousands of await-light transitions/sec per database, scale-out by sharding) and **removed the "a few thousand / conceded to Temporal" framing** the idea explicitly forbids; **added the make-or-break per-transition-UPDATE rebuttal as a dedicated subsection** (§5.1.1); **added the honest single-workflow latency characterization** (§5.6.1); **added the mandated Observability section** (§5.14) with a sixth on-call user story (§3.6), observability tests (§6.2 item 7), and an observability/SRE half-hire (§7). Updated team/plan/tests/open-items/ops accordingly. All five Reviewer B findings accepted.
- **v0.4** (2026-05-30) — Closed all seven findings Reviewer B raised against v0.3 (Reviewer A unavailable this round). **NOTE (corrected in v0.6): this entry originally claimed the §4 architecture diagram was "actually populated" — that claim was false; the literal "(architecture not yet specified)" placeholder in fact remained, repeating the identical false claim the v0.3 entry made. The block was genuinely filled only in v0.6.** Corrected throughput positioning to the (later-reverted) "a few thousand transitions/sec" framing; **v0.5 reverts this back to the idea's ambitious target.** Redesigned poison-pill containment to use **only the existing `next_batch` `max_events` bound (reduce to size 1)** for isolation, withdrawing the v0.3 "re-process the same snapshot range with the batch split" framing; added the `next_batch` max-events bound as **explicit engine contract #4** (§5.9). **(Defect found in v0.5 review: the v0.4 reduction was process-local and not subconsumer-safe — fixed in v0.5 via `wf_dispatch_control`.)** Closed the fan-out/join **lost-resume race**: completion counting now serializes on the `wf_join` row (`SELECT … FOR UPDATE` / per-join advisory lock) at **READ COMMITTED**, and added a concurrent-final-completer liveness test (§5.8/§6.2/§6.3). Removed the undefined "within-horizon pre-registration" emit-authz clause as redundant. Reduced the client-scope claim to the **one Python reference client** actually staffed. Updated team, plan, tests, open items accordingly. All seven Reviewer B findings accepted.
- **v0.3** (2026-05-30) — Closed the fix-induced contradictions both reviewers raised against v0.2. Redefined dedup-horizon enforcement around a per-transition `delivery_anchor` so long `send_at` sleeps are never misclassified as stale redeliveries and DLQ'd, and recomputed the bound as single-attempt (§5.4.1). Pinned retry continuations to a fresh `step_seq` so they re-execute instead of being swallowed as a dedup no-op (§5.2/§5.4). Redesigned poison-pill containment onto the engine's durable per-event retry counter (§5.2), pinned as an explicit engine contract (§5.9). Replaced the unbounded per-key lock row with a transaction-scoped advisory lock (§5.7.3). Separated `cache_retention_horizon` from `await_timeout` (§5.7.2). Spilled per-child join results to `wf_join_done` (§5.8). Made timeout liveness an explicit operator invariant — pg_cron REQUIRED for scale-to-zero (§5.7.1, §10). Introduced a mandatory `wf_registry` as the authoritative emit-liveness source (§5.5/§5.10.2/§5.12). Added a `workflow_id` confidentiality/leakage model (§5.11). Corrected the audit overclaim and added `actor_id` attribution (§5.10.3). Scoped the flat-dead-tuple headline to await-light loops (§5.6/§6.5). Stated a minimum PgQ engine floor (§5.9/§5.13). Attempted to fill the empty §4 architecture block (placeholder in fact remained — corrected in v0.6). All findings from both reviewers accepted.
- **v0.2** (2026-05-30) — Hardening round against Reviewer A (security/ops). Added authorization model (§5.10) and `workflow_id`-as-unforgeable-capability (§5.11). Stated the dedup-horizon bound and its DLQ enforcement (§5.4.1). Resolved the batch-transaction vs per-event-retry contradiction (§5.2). Made timeout liveness a non-optional property of the dispatch loop (§5.7.1). Pinned the await/emit lock to a pooler-safe transaction-scoped lock (§5.7.3). Bounded `wf_event_cache` retention (§5.7.2). Demoted `wf_live` to optional/opt-in (§5.5). Refined the zero-bloat claim (§5.6). Added resource caps (§5.12). Stated the engine tick-visibility coupling as a regression-tested contract (§5.9). Scoped the benchmark out of per-change CI (§6.5). Added security engineer, operability section (§10), open-items (§11). Reviewer B unavailable this round.
- **v0.1** (2026-05-30) — Initial spec scaffold fleshed into full structure. Resolved all five delegated interview questions. Added Goal-&-why framing, user stories, layered architecture with the sacred-engine boundary, hot-path/coordination detail incl. the honest zero-bloat correction, await/emit + fan-out/join race designs, red/green TDD-first ordering, team roster, 5-sprint plan, SDK surface, and strict non-goals. No reviewer findings yet (first authoring round).
