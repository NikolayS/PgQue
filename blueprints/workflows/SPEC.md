# PgQue Durable Workflows — SPEC v0.3

> Status: **experimental**, ships as optional `sql/experimental/durable.sql` gated by the project promotion rule. One reference SDK. Engine layer is sacred and untouched.

---

## 1. Goal & why it's needed

**Goal.** Provide a durable-execution / durable-workflow layer for PgQue that models each workflow as an **append-only stream of state-transition events** running over PgQ's existing snapshot + TRUNCATE rotation engine, so durable workflows inherit PgQ's zero-bloat property instead of fighting it.

**Positioning.** This is a **bloat-free alternative to Temporal and DBOS** — it competes with them head-on on durable execution and delivers the same core guarantees teams adopt those systems for (durable multi-step execution, exactly-once handoff, at-least-once steps, durable timers, fan-out/join), on just your managed Postgres with a **flat dead-tuple curve at agent-loop throughput**. Eliminating per-step `workflow_status` `UPDATE` churn is the **headline benefit**, not a limitation. We compete on durability; we differ only in *mechanism* — event-sourced append + rotate instead of replay of a linear function backed by a mutated status row.

**Why this exists.** Every Postgres-native durable-execution engine in the category (DBOS, absurd, and the long tail of `SELECT … FOR UPDATE SKIP LOCKED` + `DELETE` queues) shares one structural liability: they model a workflow as a **mutable `workflow_status` row that is `UPDATE`d on every step**. At the throughput the category is actually chasing — AI agent loops doing millions of cheap iterations — that per-step `UPDATE` churns dead tuples until the workload hits a VACUUM wall, and throughput degrades. PgQ already solved exactly this problem for *queues* with snapshot-batch isolation + wholesale `TRUNCATE` rotation: zero dead-tuple bloat under sustained load. The insight this spec operationalizes is that **durable execution is event sourcing**, PgQ is **already an append-only event log**, and therefore a workflow can be modeled as a stream of appended transitions (continuation-passing) rather than a mutated row. The zero-bloat property then carries through, *for free*, from the queue layer to the workflow layer.

This exists because no one else can credibly claim "durable workflows with a flat dead-tuple curve at agent-loop throughput, on just your managed Postgres, no separate datastore." That is the entire pitch, and it is only reachable by building **on top of** the rotation engine rather than re-introducing a mutable-status model beside it.

**What it is NOT** (honored strictly throughout — see §12): it does **not** reproduce the Temporal/DBOS *durability mechanism* (deterministic replay of a linear function + a per-step-mutated status row) — we compete with them but eliminate that mechanism because it is the bloat source; not a multi-language replay runtime; not a separate server/daemon/datastore; not a hyperscale engine; not a `FOR UPDATE SKIP LOCKED` claim/lease model; cancellation/orphan-join propagation is deferred.

---

## 2. Scope & resolved interview decisions

The interview answers were all delegated to the lead ("decide for me"). Resolved:

| Question | Decision (v0.1, carried unchanged through v0.3) |
|---|---|
| **Primary users** | Backend engineers running long-lived or high-iteration orchestration (AI agent loops, multi-step business processes, fan-out jobs) **on managed Postgres** who refuse a second datastore and refuse a VACUUM wall. |
| **Core job** | Advance a workflow from one step to the next with **exactly-once handoff** and **at-least-once step execution**, never losing or silently duplicating a workflow's progress — on a hot path that appends and rotates rather than updates. |
| **Durability / recovery guarantee** | At-least-once step execution + exactly-once handoff between steps; per-step idempotency keyed on `(workflow_id, step_seq)`. On crash, exactly the single in-flight step redelivers (PgQ's existing redelivery); there is no long function to replay. |
| **Success metric** | A throughput-and-bloat benchmark vs a mutable-status-row baseline (DBOS/absurd shape) on server hardware: **flat dead-tuple count + sustained throughput** on the append+rotate hot path where the baseline degrades. |
| **Out of scope for v0.1** | Cancellation / orphan-join propagation; linear-code (`async/await`-compiled) DX; N synchronized SDKs; hyperscale (>~ a few thousand transitions/sec/db); deterministic replay. |

---

## 3. User stories

Each story is persona + action + outcome and is directly exercised as a manual acceptance test (§6.4).

1. **Agent-loop builder (zero-bloat at iteration scale).** *As* a backend engineer running an AI agent that loops thousands of times per run, *I* define each iteration as a step that processes and enqueues its successor, *so that* a million iterations complete with a **flat dead-tuple count** on the hot tables and no VACUUM-driven throughput cliff — verifiable with `pg_stat_user_tables.n_dead_tup` staying flat through the run. (The flat-curve claim is scoped to await-light loops; await/join-heavy shapes are characterized honestly in §5.6.)

2. **Long-sleep orchestrator (durable timers).** *As* an engineer modeling a "wait 7 days, then send a reminder" process, *I* call `sleep('7 days')` inside a step, *so that* the workflow durably resumes after the wait **without holding any batch open** and **without a per-workflow polling row** — the sleep is one row in a TRUNCATE-rotated delayed-delivery table, and the woken continuation is **never** misclassified as a stale redelivery and DLQ'd (§5.4.1).

3. **Human-in-the-loop integrator (await external event).** *As* an engineer building an approval flow, *I* call `awaitEvent('approval', timeout => '24h')` and have an **authorized** part of my system call `emit(workflow_id, 'approval', payload, token)`, *so that* the workflow resumes **exactly once** on the event — robust against emit-before-await, await/emit interleave, and emit-racing-the-timeout — or resumes on the timeout branch if the deadline passes first. For approval-class waits the per-wait emit token is **mandatory** (§5.10.2), so the approval cannot be forged by an unauthorized caller (§5.10), nor by guessing or harvesting the `workflow_id` (§5.11), nor replayed without the wait token.

4. **Fan-out batch processor (spawn + join).** *As* an engineer processing a parent job that splits into N independent children (N capped, §5.12), *I* spawn N child workflows and `awaitAll`, *so that* the parent resumes **exactly once** when all N complete, with a **per-child result array** (success/failure each) materialized in a join-result side table (§5.8) — not inlined in the resume payload — even under redelivery of any child's completion.

5. **Exactly-once integrator (transactional handoff).** *As* an engineer whose step writes a row to *my own* business table and then advances the workflow, *I* run my side effect, the successor enqueue, and the batch ack in **one transaction**, *so that* a crash either commits all three or none — no successor without the side effect, no side effect without the ack, no duplicate handoff.

---

## 4. Architecture

<!-- architecture:begin -->

```text
(architecture not yet specified)
```

<!-- architecture:end -->

### 4.1 Layering (the sacred boundary)

The durable layer **only calls** the PgQ primitives + `send_at`. It adds **no** modification to rotation/tick/batch logic and introduces **no** second concurrency model. Its dependencies on engine semantics (tick-visibility, durable per-event retry count, `send_at`) are made explicit and pinned by engine-contract tests (§5.9).

### 4.2 Key abstractions

- **Workflow** — a logical state machine identified by `workflow_id`, which is a **128-bit unguessable capability** (§5.11), not a sequential id. At any instant it is in exactly one of three conditions: **(a)** one *in-flight* message (a step-event sitting in a PgQ batch being processed), **(b)** *scheduled* (a `send_at` continuation awaiting a wake time, or a registered wait awaiting an event), or **(c)** *terminal*. The **single-live-continuation invariant** — each processed step enqueues *exactly one* successor — is what makes exclusivity structural rather than lease-based.
- **`workflow_id` — addressing handle AND bearer capability.** It is used both to *address* a workflow (in payloads, user tables) and, combined with the role grants and per-wait tokens of §5.10, to *authorize* operations against it. Because it does double duty it must be treated as a secret; §5.11 specifies its confidentiality/leakage model (hashed at rest in audit/DLQ, never logged raw, mandatory token for approval waits).
- **Step-event** — the message on the PgQ queue. Payload carries: `workflow_id`, `step_seq` (monotonic progress anchor), `step_name`/state tag, `delivery_anchor` (the event's deliverable time, §5.4.1), small continuation state (continuation-passing), and — for retries — `retry_attempt`/`origin_step` (§5.2), subject to a **hard payload size cap** (§5.12). Large state is the user's responsibility to hold in their own tables, addressed by `workflow_id`.
- **Transition** — process a step → emit successor as a *new append*. Never an `UPDATE` of a status row.
- **Coordination side tables** (the only mutable state; see §5.5) — `wf_registry`, `wf_wait`, `wf_event_cache`, `wf_join`, `wf_join_done`, `wf_dedup`, `wf_audit`, and the **optional, opt-in** `wf_live` projection. Their churn is bounded by **concurrency and coordination-point count, not total step volume** — stated precisely (distinguishing live row-count from dead-tuple rate, and conceding the await/join-heavy case) in §5.6.

### 4.3 Concurrency / ownership model

One **logical consumer** with cooperative **subconsumers** splitting batches (PgQ 0.2 feature). Because exactly one live message exists per workflow, only one subconsumer ever touches a given workflow at a given instant — exclusivity is an emergent property of the invariant, requiring **no claim/lease/steal machinery**. Worker death mid-batch is covered by PgQ's existing cooperative `dead_interval` takeover: the unfinished batch is reassigned and the in-flight step redelivers (at-least-once), made safe by per-step idempotency (§5.4) whose dedup horizon is bounded ≥ max single-attempt redelivery latency (§5.4.1).

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

### 5.2 Dispatch loop, transaction boundary, and retry (contradiction resolved)

**The transaction boundary is the batch.** PgQ acks a *batch* wholesale via `finish_batch`; there is no per-event ack. The dispatcher processes every event in a batch within **one** transaction and commits once:

```
loop:
  batch_id := pgque.next_batch(queue, consumer)        -- snapshot-bounded
  if batch_id is null:
      run_timeout_sweep()                              -- §5.7.1 in-loop liveness
      sleep to next tick; continue
  events  := pgque.get_batch_events(batch_id)
  begin
    for each event in events:                          -- batch step execution
        advance_one(event)                             -- §5.3, appends successor(s)
    pgque.finish_batch(batch_id)
    run_timeout_sweep()                                -- opportunistic
  commit
```

**Batch size is bounded** (a configured dispatch parameter, default small) so the blast radius of any rollback is bounded and the single-event reduction of §5.1 is the common shape.

**Per-event retry without subtransactions (the resolved contradiction).** The v0.1 claim that a transiently failing step "calls `event_retry()` for that single event rather than aborting the whole batch" is **incorrect under the stated constraints** — PL/pgSQL cannot catch an error and continue the surrounding transaction without a savepoint, and **§5.1/§5.13 forbid subtransactions in hot paths** (corrected cross-reference; the no-subtransaction rule lives in §5.1/§5.13, not §5.10). v0.2's two failure channels are retained and made precise:

1. **Expected / transient failure → returned retry continuation (an append, not a throw).** A step that wants to retry re-enqueues a continuation of the *same logical step* via `send_at` with backoff and finishes normally. **The retry continuation is a NEW transition carrying a fresh `step_seq`** (with `retry_attempt` and `origin_step` recorded in the payload). Because its `step_seq` is new, the §5.4 dedup logic does **not** treat it as a committed no-op — the retried step **re-executes its body** on delivery. This pins the step_seq identity the dedup model requires (resolving the retry-vs-dedup collision). After `max_retries` the step returns a **DLQ transition** (an append to the DLQ queue) rather than throwing. This path is subtransaction-free and does **not** abort the batch.
2. **Unexpected exception (genuine bug, OOM, lost connection) → batch aborts and the whole batch redelivers.** Rare, correct, and safe: redelivery is idempotent (§5.4). **Poison-pill containment, redesigned to be implementable:** the durable layer **cannot** durably write a per-workflow exception counter from the aborting transaction (the abort discards it) and PgQ delivers batches as snapshot-bounded ranges, not per-workflow-selectable. Containment therefore rests on two mechanisms that need no engine change: (a) **the engine's durable per-event retry counter**, which PgQ increments across redeliveries and uses to route an over-threshold event to the DLQ — pinned as an explicit engine contract in §5.9 rather than assumed silently; and (b) a **dispatcher fault-isolation re-dispatch**: on detecting an aborting batch, the dispatcher re-processes the *same snapshot range* with the batch split (halving down to size 1), isolating the offending event so it crosses the engine retry threshold **by itself** and is DLQ'd without dragging innocent co-tenant events with it. Blast radius per abort is bounded by the small batch size; permanent quarantine is bounded by the engine retry threshold.

A batch may still contain step-events for many distinct workflows advancing in one transaction (native fan-out); correctness no longer depends on per-event mid-transaction error recovery.

### 5.3 The five durable-execution requirements, mapped

1. **Exclusive ownership — structural.** Single-live-continuation invariant + cooperative `dead_interval` takeover. No lease.
2. **Mutable run state — re-enqueue, don't update.** Each transition appends a new event carrying new state; small state rides the payload; no long-lived per-run row on the hot path.
3. **Long-lived persistence — rotating `send_at`.** `sleep('7d')` = `send_at(continuation, now()+7d)`; the step acks immediately; the sleep is one row in a TRUNCATE-rotated delayed table — zero-bloat, never an open batch. The woken continuation's `delivery_anchor` is its wake time, so it is never confused with a stale redelivery (§5.4.1).
4. **Per-row scheduling.** Timers via rotating `send_at`. **`awaitEvent` with timeout** is the genuinely hard new piece (§5.7).
5. **Checkpoint replay — not needed.** No long-running function to resume. Recovery = PgQ's at-least-once redelivery of the single in-flight step. Correctness = exactly-once handoff (§5.1) + per-step idempotency (§5.4).

### 5.4 Per-step idempotency

Every step attempt is keyed `(workflow_id, step_seq)`. On (re)delivery a step first checks/inserts a dedup marker; the marker insert and the successor enqueue commit together (§5.1). A redelivered step **with the same `step_seq`** whose successor already committed is a no-op (marker present) and simply re-acks. A **retry continuation has a fresh `step_seq`** (§5.2) and therefore re-executes — it is a new attempt, not a redelivery of the prior one. The dedup store is append-based and short-horizon (rotating) so it does not itself become a bloat source (§5.6).

#### 5.4.1 The dedup-horizon bound and the delivery-anchor clock (made explicit)

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

**Enforcement.** Any deliverable event whose redelivery age (`now − delivery_anchor`) exceeds the horizon is routed to the DLQ instead of processed, so a marker can never rotate out underneath a still-live redelivery and silently report "first delivery." Because the clock is the *deliverable* time, a legitimate long sleep is processed normally; only a genuinely stale redelivery is DLQ'd. Property tests (§6.2) assert **both** directions: no double-handoff at horizon-boundary redelivery age, AND a `sleep` longer than the horizon resumes normally and is **not** DLQ'd.

### 5.5 Coordination side tables

| Table | Role | Churn driver | Lifecycle |
|---|---|---|---|
| `wf_registry` *(mandatory)* | minimal authoritative live-workflow set (id + status); the source of truth for emit-liveness (§5.10.2) and unknown-id rejection (§5.12) | concurrency (live count) | `INSERT` on `start_workflow`, `DELETE` on terminal — one insert + one delete per workflow *lifetime*, not per step |
| `wf_wait` | registered event waits, single-resume token, optional per-wait emit token | open awaits | `DELETE … RETURNING` on resume/timeout |
| `wf_event_cache` | first-write-wins cache for emit-before-await | emit/await coordination points | bounded by `cache_retention_horizon` (§5.7.2), never silent-drop within horizon |
| `wf_join` | join row: parent + total N, single-resume token | spawn points | deleted when parent resumes |
| `wf_join_done` | idempotent completed-set `(parent, child_idx)` **carrying each child's result value/marker** (§5.8 result-array spill) | child completions (≤ concurrency × fanout) | dropped with the join |
| `wf_dedup` | per-attempt `(workflow_id, step_seq)` markers | redelivery horizon | rotating / short-horizon, bound per §5.4.1 |
| `wf_audit` | append-only log of security-relevant actions (§5.10.3) | emit/resume/spawn events | rotating (TRUNCATE), exported before rotation |
| `wf_live` *(optional, opt-in, default OFF)* | rich current-state projection for observability only — never required for correctness | concurrency (live count) | append-based projection, rotating; **not** insert+delete (§5.8) |

**No persistent lock table exists.** The await/emit serialization of §5.7.3 uses a *transaction-scoped advisory lock* (no row), so it contributes zero live or dead tuples. `wf_registry`, `wf_wait`, `wf_join` are deleted on resolution (row-count bounded by concurrency); `wf_event_cache`, `wf_dedup`, `wf_audit` are horizon/rotation-bounded.

### 5.6 The honest zero-bloat claim (stated precisely — row-count vs dead-tuple rate, incl. the await/join-heavy case)

Zero-bloat holds on the **hot step-transition path** (appends + rotation). For coordination, v0.3 separates two quantities and concedes a workload class:

- **Live row-count** is bounded by **concurrency** (`wf_registry`, `wf_wait`, `wf_join`, optional `wf_live` all hold ~one row per live coordination point/workflow).
- **Cumulative dead-tuple generation rate** is bounded by **coordination-point throughput**, because every resolution is a `DELETE` (`wf_registry` on terminal, `wf_wait`/`wf_join` on resolve).

**Concession (await/join-heavy workloads).** For a workflow that awaits an event or spawns/joins on (nearly) *every* step — a normal shape for human-in-the-loop and tool-calling agent loops, both named primary personas — that is on the order of one `DELETE` per step, i.e. the **same order** of dead-tuple generation as the per-step status-row `UPDATE` the pitch eliminates. We therefore **scope the headline**: the flat-dead-tuple curve is claimed for **await-light loops** (the bulk of high-iteration agent inner loops, which transition far more often than they coordinate). For coordination-heavy workloads we claim only **bounded live row-count** and a dead-tuple rate proportional to *coordination points*, mitigated by rotation where feasible (`wf_dedup`, `wf_event_cache`, `wf_audit` rotate; `wf_registry`/`wf_wait`/`wf_join` are small and rely on documented required autovacuum settings, §10). `wf_registry` adds only one insert + one delete per *workflow lifetime*, not per step. The precise marketed claim is: **zero-bloat hot path; coordination tables have concurrency-bounded *live* row-count and coordination-point-bounded *dead-tuple rate* — flat for await-light loops, and for await/join-heavy loops bounded by coordination throughput rather than total step volume, still well-managed but not zero.** The benchmark (§6.5) publishes the coordination-table dead-tuple curve and includes an explicit **await/join-heavy A/B** vs the mutable-status baseline so the scoped headline is substantiated for the personas that stress coordination. We never claim "zero dead tuples anywhere."

### 5.7 `awaitEvent` / `emit` — the ~20% with real risk (designed and TDD'd first)

Wait registry keyed `(workflow_id, event_name)`, event names **correlation-scoped** and `workflow_id` an unguessable, confidentiality-protected capability (§5.11). Race table:

- **emit-before-await** → `emit` writes `wf_event_cache` **first-write-wins**; a later `awaitEvent` finds the cached event and resumes immediately (no wait row created). The cache entry is retained for the full **`cache_retention_horizon`** (§5.7.2), never evicted under it. (emit is rejected for non-live, non-pre-registered ids per §5.10.2/§5.12.)
- **await/emit interleave** → both serialize on a **transaction-scoped advisory lock** keyed on `(workflow_id, event_name)` (§5.7.3), so exactly one of {register-wait, consume-cache} wins deterministically and the mechanism is safe under transaction-pooling poolers.
- **double-resume (emit racing the timeout sweep)** → the wait row is a **single-resume token** resolved by `DELETE … RETURNING` in the **same txn** as the continuation enqueue. Whoever deletes first (emit or sweep) resumes; the loser sees zero rows and does nothing.
- **stale / cross-talk cached events** → correlation-scoped names + capability `workflow_id` + bounded-horizon GC.
- **redelivery of the await step itself** → idempotent registration on `(workflow_id, step_seq)`; re-registering is a no-op.
- **timeout** → injected by the in-loop timeout sweep (§5.7.1), via the same single-resume `DELETE … RETURNING` path.

#### 5.7.1 Timeout liveness, and the operator invariant it requires

Every dispatcher iteration — including the idle tick-sleep path — calls `run_timeout_sweep()` (bounded batch of due timeouts), so **as long as a dispatcher is running**, timeouts fire without pg_cron. **However, this does not cover the no-running-dispatcher state.** A low-volume approval system (the §3.3 persona) that autoscales workers to **zero** between events, or one where all workers have crashed and not yet restarted, has *no* loop iterating, so a 24h timeout would fire only whenever a worker next starts — arbitrarily late. v0.3 surfaces this as a **hard operator invariant rather than hiding it**:

> **Timeout liveness requires either (a) a continuously-running dispatcher, or (b) pg_cron driving `run_timeout_sweep()` on a fixed cadence.** For scale-to-zero / serverless topologies (RDS/Aurora/Cloud SQL/Supabase/Neon with app workers that scale to zero), **pg_cron is REQUIRED**, not optional. The install/ops docs (§10) state this as a deployment precondition and the install script warns if neither a long-running dispatcher nor pg_cron is configured.

pg_cron remains an *optimization* only for the always-on-dispatcher topology; it is a *correctness requirement* for scale-to-zero. A crash/idle-recovery test (§6.3) asserts the running-dispatcher path with pg_cron disabled, and a separate test asserts the scale-to-zero path fires via pg_cron.

#### 5.7.2 Two distinct horizons (separated)

v0.2 conflated two unrelated windows. v0.3 separates them:

- **`cache_retention_horizon`** — how long an emit-before-await entry lives in `wf_event_cache`. It need only cover the **emit→await-registration gap**: the time for an in-flight workflow to reach and register its `awaitEvent` after an emit (queue backlog + redelivery + max batch duration). This is small and bounded by processing latency, **not** by any user timeout. GC only evicts entries older than this horizon; within it an event is never dropped.
- **`await_timeout`** — the user-facing await→event deadline (e.g. 24h in story §3.3, or a legitimate multi-day approval wait). This is **independent of cache retention** and is **not** capped by it. An `awaitEvent` with a long deadline is fully supported; the deadline governs the timeout-sweep firing (§5.7.1), not cache eviction.

Thus a 24h await behind a 1-second emit-before-await is never rejected: the cache only had to survive the sub-second registration gap, while the 24h deadline is tracked by `wf_wait` + the sweep. The two horizons, the cache cardinality cap (§5.12), and the dedup horizon (§5.4.1) are validated for mutual consistency at install.

#### 5.7.3 Locking mechanism pinned (pooler-safe, zero-row)

The await/emit key is serialized with a **transaction-scoped advisory lock**, `pg_advisory_xact_lock(hashtextextended(workflow_id || ':' || event_name, 0))`, held only for the enclosing transaction (auto-released on commit/abort) and therefore **safe under PgBouncer transaction pooling**. It leaves **no persistent row** — eliminating the unbounded per-key lock-row growth that an `INSERT … ON CONFLICT` lock table would have introduced (that row was never inventoried in §5.5 and is removed entirely). Hash collisions between unrelated `(workflow_id, event_name)` pairs are **correctness-safe**, not bugs: a collision only causes transient false serialization of two unrelated keys, because the *decisive* operation under the lock is an atomic `INSERT … ON CONFLICT DO NOTHING` (register wait) / `DELETE … RETURNING` (consume cache or resume) on the **exact** key. **Session-level advisory locks (`pg_advisory_lock`) remain explicitly forbidden** (pooler-unsafe); only the transaction-scoped variant is permitted.

### 5.8 fan-out / join (spawn + `awaitAll`)

- Spawn N children (N **capped**, §5.12) with **distinct child workflow ids** (each an unguessable capability); **record the join total `N` atomically with the spawn**.
- **Engine contract (§5.9):** children become visible only at the next tick boundary, *after* the join row is committed; this tick-visibility ordering makes the join-total recording race-free. Pinned by a regression test.
- Count completions with an **idempotent completed-set** `(parent, child_idx)` in `wf_join_done` — redelivery-safe.
- **Per-child result spill (resolves the payload-cap conflict).** Each child writes its **result value or failure marker into its `wf_join_done` row**, keyed `(parent, child_idx)`. The parent's resume continuation payload carries **only a reference** (the parent `workflow_id`/join id), **never the inlined N-entry array** — so the 8 KiB payload cap (§5.12) is respected even at `max_spawn_fanout = 1024` (which would otherwise leave ~8 bytes/child). The SDK's `awaitAll` reads the assembled result array from `wf_join_done` addressed by join id. This is concurrency×fanout-bounded coordination state (dropped with the join), **not** per-step mutable state and **not** a status row.
- Resume the parent **exactly once** via the `wf_join` row as a deletable single-resume token (the last child to flip the count to N deletes the join and enqueues the parent continuation, in one txn).
- **Explicit per-child failure semantics**: the parent receives a **result array**, one entry per child (success value or failure marker). A failed child does not block the join; it reports failure in its slot.
- **Cancellation / orphan handling is explicitly deferred** (§12).

### 5.9 Engine contracts (explicit coupling, pinned)

The durable layer depends on three specific PgQ behaviors. Because the engine is sacred and unmodified, the durable layer cannot pin them from inside the engine; it states them as **explicit contracts** and ships **engine-contract regression tests** (§6.3) that fail loudly on violation, converting silent correctness breaks into CI failures:

1. **Tick-visibility ordering** — events inserted before tick T are not visible in any batch until a tick ≥ T+1, and a committed side-table row written in the same transaction as an `insert_event` is visible to any consumer that later sees that event. (Underpins snapshot-batch isolation and §5.8 join atomicity.)
2. **Durable per-event retry count + DLQ routing** — PgQ maintains a per-event retry counter that survives redelivery (including after a wholesale batch abort) and routes an over-threshold event to the DLQ. (Underpins the §5.2 poison-pill quarantine; this is the *only* durable counter available to the aborting-batch channel.)
3. **`send_at` delayed delivery** — `send_at(event, t)` makes the event deliverable at `t` over a TRUNCATE-rotated delayed table. (Underpins `sleep`, retry backoff, and the `delivery_anchor` semantics of §5.4.1.)

A minimum PgQ engine version/feature floor is required and gated at install (§5.13, §10): `send_at` (PR #237) present, the durable per-event retry counter exposed, and tick-visibility behaving per contract #1. Install **fails loudly** if the floor is unmet rather than silently risking a correctness regression the §6.3 tests would only catch post-hoc.

### 5.10 Authorization & the SECURITY DEFINER surface

The v0.1 spec left the SECURITY DEFINER surface unguarded — functions default to `EXECUTE` granted to `PUBLIC`, so any role with a connection could call `emit`/`spawn`/`finish` against any workflow, directly forging approvals (§3.3). v0.3 specifies a concrete authorization model.

#### 5.10.1 Default-deny grants

The install script **`REVOKE EXECUTE … FROM PUBLIC`** on every durable function, then grants explicitly to two dedicated roles:

- `pgque_durable_worker` — may call dispatch/internal functions (`next_batch` wrappers, `finish_batch` wrappers, timeout sweep, join resolution). Granted to the worker/consumer role only.
- `pgque_durable_client` — may call the producer-facing surface (`emit`, `spawn`, `start_workflow`). Granted to application roles that legitimately drive workflows.

Internal-only functions (token resolution, dedup, projection) are granted to **neither** and are callable only as `SECURITY DEFINER` internals invoked by the above. A CI grant-audit test asserts no durable function retains a `PUBLIC` execute grant.

#### 5.10.2 Caller-scoped emit authorization (and the liveness source)

Being able to call `emit` is necessary but not sufficient: the caller must also possess the target workflow's **`workflow_id` capability** (§5.11), and `emit` must verify the id is live. **The authoritative liveness source is the mandatory `wf_registry` table (§5.5), not the optional `wf_live` projection** — this resolves the v0.2 tension where unknown-id rejection (§5.12) needed a registry that `wf_live`'s default-OFF status could not provide. `emit(workflow_id, event_name, payload, token?)` succeeds only if the `workflow_id` matches a row in `wf_registry` (live) **or** a within-horizon pre-registration; emits for unknown ids are rejected without creating a cache row (§5.12).

**Per-wait emit token — MANDATORY for approval-class waits.** For high-assurance waits (approvals, escalations), `awaitEvent` issues a per-wait emit token stored in `wf_wait`, and the matching `emit` **must** present it. Holding the `workflow_id` alone is therefore **insufficient** to satisfy an approval wait — directly mitigating capability leakage (§5.11). For low-assurance waits the token is optional. All `SECURITY DEFINER` functions pin `search_path = pgque, pg_catalog`.

#### 5.10.3 Audit trail (claims corrected; attribution made useful under pooling)

Security-relevant actions — `emit`, wait-resume, `spawn`, timeout-resolution — append a row to the **append-only, rotating `wf_audit`** table. v0.3 corrects two v0.2 overclaims:

- **No "tamper-evident" claim.** The table is **append-only by convention within the durable role's trust boundary** — the owning/superuser role can `DELETE`/`TRUNCATE`, and an attacker who can trigger rotation or stall the export hook can erase the pre-export window. We claim only an *append-only operational audit log*, not cryptographic tamper-evidence. **Hash-chaining / signing is a deferred enhancement (§11)**, called out rather than implied.
- **Attribution that survives pooling.** Recording `db_role = session_user/current_user` is near-useless under the spec's target deployment: under `SECURITY DEFINER`, `current_user` is the definer/owner, and under PgBouncer transaction pooling with a shared `pgque_durable_client` role, `session_user` is that one shared role — so every emit attributes to the same role. v0.3 therefore records an **application-supplied `actor_id`** (passed explicitly by the client on `emit`/`spawn`), alongside `db_role`, `txid`, and `event_time`. The `actor_id` is the forensic anchor ("which application principal drove this"); `db_role` is retained for defense-in-depth. The documented limitation: `actor_id` is only as trustworthy as the calling application's own authentication.
- **`workflow_id` is stored hashed**, not raw (§5.11), so the audit log is not itself a capability-leakage vector.

The table is TRUNCATE-rotated to preserve zero-bloat and exported to durable storage before each rotation so the trail survives.

### 5.11 `workflow_id`: unforgeable AND confidential

Every `workflow_id` (parent and child) is a **128-bit cryptographically random value** (`gen_random_uuid()` / `pgcrypto`), never a sequential or queue-derived id — so an attacker cannot enumerate ids to drive, resume, or race-to-timeout arbitrary workflows. v0.2 made the id *unguessable*; v0.3 adds the missing **confidentiality model**, because the same value is both a bearer capability and an addressing handle that v0.2 copied everywhere a secret must not go (step-event payloads, `wf_audit`, user tables, external emitters, **DLQ'd payloads**, and error/log surfaces such as `pg_stat_activity`, statement-parameter logging, exception messages). A long-lived bearer secret copied into logs and a DLQ readable by other roles is trivially harvestable. Leakage model and mitigations (all required):

1. **Mandatory per-wait emit token for approval-class waits (§5.10.2).** This is the primary mitigation: a harvested `workflow_id` alone **cannot** forge an approval — the wait token, issued only to the legitimate awaiter, is also required. This breaks the "leak id → forge emit" chain even if an id escapes.
2. **Hashed at rest in lower-trust stores.** `wf_audit` and DLQ payloads store a salted hash / truncated reference of `workflow_id`, not the raw capability, so audit/DLQ read access does not yield a usable capability.
3. **Never logged raw.** Durable functions do not pass `workflow_id` as a logged statement parameter; ops docs (§10) require disabling parameter logging for the durable schema or routing it through non-logged channels; exception messages reference the hashed id.
4. **CSPRNG generation (testable form).** The id column is **defaulted by `gen_random_uuid()`/`pgcrypto`**, and CI **statically rejects any code path that derives `workflow_id` from a sequence/serial/queue offset** — this is the testable assertion (§6.2 item 6), replacing the v0.2 "rejects predictable generation" check, which described an *undecidable* test (a generator's unpredictability cannot be asserted by inspecting emitted values).

The spec states explicitly: **the security of every coordination primitive rests on `workflow_id` being both unforgeable and confidential; approval-class authority additionally rests on the per-wait emit token, so id confidentiality is defense-in-depth rather than the sole barrier.**

### 5.12 Resource limits (anti-bloat / anti-DoS caps)

Externally and recursively driven surfaces are capped so they cannot defeat the zero-bloat pitch:

- **Spawn fan-out:** `spawn(...)` enforces `N ≤ max_spawn_fanout` (configurable, default 1024). Exceeding it is a loud error, not silent flooding. (Per-child results spill to `wf_join_done`, §5.8, so a full-fanout join never violates the payload cap.)
- **Payload size:** the "small continuation state" convention is a **hard cap** (`max_payload_bytes`, default 8 KiB) enforced at `insert_event`-wrapper time; oversized payloads are rejected. Large state and join result arrays belong in side tables addressed by `workflow_id`/join id.
- **emit cardinality / unknown-id rejection:** `emit` for a `workflow_id` with no `wf_registry` row and no within-horizon pre-registration (§5.10.2) is **rejected** and creates **no** cache row, so an attacker cannot flood `wf_event_cache` with arbitrary ids. `wf_event_cache` additionally enforces a global cardinality cap with oldest-past-horizon eviction.
- **emit rate:** an optional per-role/per-workflow emit rate limit (configurable) bounds cache growth even for legitimate-id floods.

These caps are documented defaults and part of the install-time consistency validation (§5.7.2).

### 5.13 Constraints honored

Reduces cleanly to `insert_event`, `next_batch`, `get_batch_events`, `finish_batch`, `event_retry` (+ `send_at`) plus the small side tables of §5.5. Single-file, no C extension, no `shared_preload_libraries`, no restart; managed-PG compatible. **pg_cron is optional for always-on-dispatcher topologies but REQUIRED for scale-to-zero/serverless timeout liveness (§5.7.1).** PostgreSQL 14–18; **minimum PgQ engine version/feature floor gated at install (§5.9): `send_at` present, durable per-event retry counter exposed, tick-visibility per contract.** `pg_snapshot`/`xid8`; `pgcrypto` for capability generation. All SECURITY DEFINER functions pin `search_path = pgque, pg_catalog` and are `REVOKE`d from `PUBLIC` (§5.10). No subtransactions in hot paths. The await/emit serialization uses a transaction-scoped advisory lock only (§5.7.3) — no claim/lease model. Ships as optional experimental `sql/experimental/durable.sql` gated by the promotion rule.

---

## 6. Tests plan

### 6.1 Hard repo rule

**Red/green TDD for ALL new code.** Every function below is written test-first: a failing test asserting the behavior, then the implementation that makes it pass. CI rejects any new SQL function or SDK method without a preceding failing-then-passing test in the same change.

### 6.2 Built test-first, in this order (highest risk first)

1. **Exactly-once handoff** (§5.1) — kill the txn between `insert_event` and `commit`, assert no successor + clean redelivery; assert no double-handoff on commit.
2. **Per-step idempotency + dedup-horizon + delivery-anchor clock** (§5.4/§5.4.1) — deliver the same `(workflow_id, step_seq)` twice → exactly one successor + one side effect; redeliver at horizon-boundary age → routed to DLQ, no double-handoff; **AND the mandatory positive test: a `sleep` longer than `dedup_horizon` resumes normally and is NOT DLQ'd** (asserts the `delivery_anchor` re-anchoring, guarding against the per-workflow-age misinterpretation).
3. **Transaction-boundary / retry resolution** (§5.2) — a retry continuation re-enqueues via `send_at` with a **fresh `step_seq`** and, on delivery, **re-executes its body** (assert the step logic runs once per retry attempt up to `max_retries`, then lands in DLQ — guards against the dedup-no-op-swallows-retry bug); an unexpected exception aborts only a bounded batch; the fault-isolation re-dispatch isolates a poison event to the DLQ without DLQ-ing innocent co-tenants.
4. **`awaitEvent` / `emit` race matrix** (§5.7) — one test per row; `cache_retention_horizon` never drops a within-horizon entry and is **independent of `await_timeout`** (a long await behind a fast emit is not rejected, §5.7.2); advisory-lock serialization correct under simulated transaction-pooling, including a **hash-collision correctness-safety** test (§5.7.3); single-resume token proven by concurrent emit+sweep.
5. **fan-out / join** (§5.8) — race-free join-total recording; idempotent completed-set under duplicated completion; exactly-once parent resume; **per-child result array assembled from `wf_join_done` (spill) with a resume payload under `max_payload_bytes` at full `max_spawn_fanout`** (guards the cap conflict); spawn-fanout cap enforced.
6. **Authorization & capability** (§5.10/§5.11) — PUBLIC cannot execute any durable function; `emit` without the `workflow_id` capability fails; `emit` for an id absent from `wf_registry` is rejected with no cache row; an approval-class `emit` without the mandatory per-wait token fails even with a valid id (capability-leakage mitigation); forged-approval with a guessed sequential id fails; **`workflow_id` column is defaulted by `gen_random_uuid()`/`pgcrypto` and CI statically rejects any sequence/serial-derived id path** (testable CSPRNG check); `wf_audit`/DLQ store hashed ids; audit row with `actor_id` written for every emit/resume/spawn.

### 6.3 CI test suites

- **Unit (pgTAP/SQL):** each durable function; coordination-table invariants; `search_path` pinning; **grant-audit** (no PUBLIC execute); "no subtransaction in hot path" lint; resource-cap enforcement (fanout, payload, cache cardinality, emit rate).
- **Engine-contract regression tests** (§5.9): tick-visibility ordering; **durable per-event retry-count + DLQ routing** (the poison-pill quarantine dependency); `send_at` delayed-delivery behavior. Each fails loudly if engine behavior regresses, plus an **install-time engine-floor gate** test.
- **Concurrency/property tests:** randomized interleavings of emit/await/timeout and spawn/complete under multiple subconsumers; exactly-once resume + no orphaned waits/joins/registry rows.
- **Crash/idle-recovery tests:** worker death mid-batch → `dead_interval` takeover + single redelivery + idempotent no-op; **timeout liveness with a running dispatcher and pg_cron disabled** (§5.7.1); **and a scale-to-zero test asserting timeouts fire via pg_cron when no dispatcher is running.**
- **Matrix:** PostgreSQL 14, 15, 16, 17, 18.
- **Engine-sacredness guard:** CI diff-check that no file under the PgQ engine path is modified by this change.

### 6.4 Manual acceptance (maps 1:1 to §3 user stories)

Each of the five user stories has a runnable scenario script the reviewer executes by hand against a managed-PG-like instance, including the §3.3 forged-approval negative check (with and without the per-wait token) and the §3.2 long-sleep-resumes-not-DLQ'd check.

### 6.5 Success-criterion benchmark (the entire pitch) — gated, NOT a per-change CI suite

Throughput-and-bloat benchmark vs a mutable-status-row baseline (DBOS/absurd shape) on server hardware. Publishes, over a long sustained run: **`n_dead_tup`** (flat for the PgQue await-light hot path; rising for baseline), **sustained transitions/sec**, the **coordination-table dead-tuple curve**, and — **new in v0.3** — an explicit **await/join-heavy A/B workload** that coordinates on (nearly) every step, so the §5.6 scoped headline (flat for await-light; coordination-bounded for await/join-heavy) is substantiated rather than asserted. Because long VACUUM-wall runs are slow and noisy, this is a **nightly / on-demand gated harness**, explicitly out of the per-change CI gate (which runs only a short smoke version). The full harness is reproducible and versioned.

---

## 7. Team (veteran experts to hire)

- **Veteran PostgreSQL internals / MVCC engineer (1)** — snapshot/visibility reasoning, `xid8`/`pg_snapshot`, rotation interaction, no-subtransaction guarantee, engine-contract tests (§5.9), engine-floor install gate.
- **Veteran durable-execution / distributed-systems engineer (1)** — await/emit and fan-out/join race designs, single-resume-token proofs, the dedup-horizon + delivery-anchor bound (§5.4.1), the transaction-boundary/retry resolution incl. retry-`step_seq` semantics and poison-pill fault-isolation (§5.2).
- **Veteran PostgreSQL security engineer (0.5, shared)** — authorization model (§5.10), capability generation + confidentiality/leakage model (§5.11), mandatory per-wait token, audit attribution under pooling, grant-audit tests, resource caps (§5.12).
- **Veteran PL/pgSQL + SQL test engineer (pgTAP) (1)** — red/green TDD harness, concurrency/property tests, crash-recovery + pg_cron-disabled and scale-to-zero liveness injection, the positive long-sleep-not-DLQ'd and retry-re-execution tests.
- **Veteran SDK / developer-experience engineer (Python) (1)** — the one reference SDK and the thin-client surface, incl. `awaitAll` result-array assembly from the spill table.
- **Veteran performance / benchmarking engineer (1)** — the gated throughput-and-bloat benchmark incl. the await/join-heavy A/B and the published curves.
- **Veteran technical writer / DX reviewer (0.5, shared)** — experimental-feature docs, honest-claim framing (§5.6), ops/authz guide (required pg_cron-for-scale-to-zero, autovacuum settings, capability-leakage hygiene).

### 7.1 Persona for this spec round

Veteran **"Durable Workflow Engineer"** (accepted).

---

## 8. Implementation plan (sprints, parallelization, ordering)

**Sprint 0 — Foundations & harness (1 wk).**
- Test engineer: pgTAP red/green harness, CI matrix (PG 14–18), engine-sacredness diff-guard, grant-audit scaffold. *(blocks everyone.)*
- PG-internals engineer: spike the primitive reduction; confirm `send_at` (PR #237) and the durable per-event retry counter; draft the engine-contract tests + install-time engine-floor gate (§5.9).
- Security engineer: role model + `REVOKE`-from-PUBLIC install template + capability generation and leakage-hygiene defaults (§5.11).
- *Parallel:* SDK engineer scaffolds the thin Python client against stub SQL signatures.

**Sprint 1 — Exactly-once core (1.5 wk).** *(highest risk first)*
- PG-internals + distributed-systems engineers (pair): exactly-once handoff (§5.1); per-step idempotency + dedup-horizon/delivery-anchor (§5.4/§5.4.1, incl. the positive long-sleep test); transaction-boundary/retry resolution with retry-`step_seq` semantics and poison-pill fault-isolation (§5.2).
- Test engineer: crash-recovery + `dead_interval` takeover; poison-pill fault-isolation test; retry-re-execution test.
- *Gate:* no further work merges until §5.1/§5.2/§5.4 tests are green.

**Sprint 2 — Coordination primitives (2 wk).** *Two parallel tracks:*
- **Track A** (distributed-systems): `awaitEvent`/`emit` race matrix (§5.7) — wait registry, first-write-wins cache with separated `cache_retention_horizon` (§5.7.2), advisory-xact-lock serialization (§5.7.3), single-resume token, in-loop timeout sweep + scale-to-zero pg_cron path (§5.7.1).
- **Track B** (PG-internals): fan-out/join (§5.8) — join-total atomicity against the engine contract (§5.9), idempotent completed-set, result spill to `wf_join_done`, exactly-once parent resume, spawn cap.
- Security engineer (parallel): `wf_registry` + emit authz/liveness, mandatory per-wait token, audit with `actor_id` + hashed ids (§5.10).
- Test engineer rotates across tracks writing red tests ahead of each piece.

**Sprint 3 — SDK + dispatch + caps (1.5 wk).**
- SDK engineer: finalize `defineWorkflow/step/sleep/awaitEvent/emit/spawn/awaitAll` over the stable SQL, incl. result-array assembly.
- PG-internals engineer: dispatch loop (§5.2) incl. in-loop sweep + fault-isolation re-dispatch, `sleep` via rotating `send_at`, resource caps (§5.12), optional `wf_live` projection.
- *Parallel:* benchmarking engineer builds the baseline (DBOS/absurd-shape) rig + the await/join-heavy A/B harness.

**Sprint 4 — Benchmark, hardening, docs (1.5 wk).**
- Benchmarking engineer: run the gated benchmark (§6.5); publish all curves incl. await/join-heavy A/B.
- Whole team: concurrency/property hardening, `search_path` + grant audit, no-subtransaction lint, pg_cron-disabled + scale-to-zero liveness tests.
- Writer: experimental docs incl. honest-claim framing (§5.6), required pg_cron-for-scale-to-zero, autovacuum settings, capability-leakage hygiene, audit export; promotion checklist.

**Critical path:** Sprint 0 harness → Sprint 1 exactly-once gate → Sprint 2 Track A & B (parallel) → Sprint 3 → Sprint 4 benchmark. SDK, security, and benchmark-rig work parallelize off the critical path.

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

Every SDK call compiles to one of the PgQ primitives + a coordination-table touch, subject to the authorization (§5.10) and resource (§5.12) checks. **No** `async/await`-compiled linear-code DX in v0.1 (deferred, §12).

---

## 10. Operability notes (managed-PG)

- **pg_cron — required for scale-to-zero (§5.7.1).** For always-on-dispatcher topologies it is an optimization; for serverless / scale-to-zero (workers idle to zero between events), pg_cron driving `run_timeout_sweep()` is a **correctness requirement** for timeout liveness. The install script warns if neither a long-running dispatcher nor pg_cron is configured.
- **Engine floor (§5.9/§5.13):** install gates on the minimum PgQ engine version — `send_at` present, durable per-event retry counter exposed, tick-visibility per contract — and fails loudly otherwise.
- **Required operator settings:** documented autovacuum tuning for the `DELETE`-driven coordination tables (`wf_registry`, `wf_wait`, `wf_join`) so their dead-tuple rate (§5.6) stays bounded; rotation cadence for `wf_dedup`/`wf_event_cache`/`wf_audit`. The await/join-heavy dead-tuple characterization (§5.6) is documented so operators size autovacuum for their workload shape.
- **Capability-leakage hygiene (§5.11):** disable statement-parameter logging for the durable schema (or route through non-logged channels); `workflow_id` is stored hashed in `wf_audit` and DLQ; treat the id as a secret and prefer the mandatory per-wait token for approvals.
- **Audit export (§5.10.3):** the `wf_audit` rotating table must be exported to durable storage before rotation; export hook + retention policy are part of the docs. Note the honest limitation: the log is append-only-by-convention, not cryptographically tamper-evident (hash-chaining deferred, §11).
- **Install-time validation:** validates mutual consistency of `dedup_horizon`, `cache_retention_horizon`, `await_timeout` ceiling, `dead_interval`, `max_retry_backoff`/`max_batch_duration`, and the resource caps, and fails loudly on inconsistency.

---

## 11. Open items carried to v0.4

- Quantitative defaults for every configured bound (`dedup_horizon`, `cache_retention_horizon`, `max_spawn_fanout`, `max_payload_bytes`, emit rate) validated against the benchmark.
- Per-wait emit-token issuance/rotation/revocation detail (§5.10.2) — now mandatory for approval-class, but the token lifecycle is still to be fully specified.
- **Audit hash-chaining / signing** for genuine tamper-evidence (§5.10.3) — deferred enhancement beyond append-only-by-convention.
- A verification pass on the v0.3 fix-induced redesigns (poison-pill fault-isolation, `wf_registry` liveness source, capability confidentiality model) before promotion, since these are new this round.
- Cancellation / orphan-join propagation remains deferred (§12).

---

## 12. Non-goals / disclaimers (honored strictly — not reintroduced anywhere above)

- **Mechanism distinction (NOT a competitive disclaimer).** PgQue Durable Workflows is a direct, better, **bloat-free alternative to Temporal and DBOS** — it competes with them head-on and delivers the same core durable-execution guarantees (§1 Positioning). It deliberately does **not** reproduce their *durability mechanism*: deterministic replay of a long-lived linear function backed by a `workflow_status` row mutated on every step. That mechanism is precisely the source of the per-step `UPDATE` bloat we exist to eliminate; we deliver the same guarantees via event-sourced append-and-rotate. **Eliminating per-step `UPDATE` churn is a goal/benefit (§1), never a non-goal.** What we do disclaim here is only the *technique*: no determinism requirement imposed on user code, and no replay-of-a-linear-function programming model in v0.1 (a continuation-compiling SDK is deferred).
- **NOT** a multi-language deterministic-replay runtime in v1. No N synchronized SDKs — one reference SDK.
- **NOT** a separate server, daemon, or external datastore. No Cassandra, RocksDB, FoundationDB, or Redis.
- **NOT** targeting hyperscale (>~ a few thousand workflow transitions/sec per database) — conceded to Temporal honestly.
- **NOT** changing the sacred PgQ engine, and **NOT** introducing a second `SELECT … FOR UPDATE SKIP LOCKED` claim/lease concurrency model as the primary mechanism — exclusivity comes from the single-live-continuation invariant over the existing rotation engine. (The transaction-scoped advisory lock of §5.7.3 is a coordination-table serialization primitive for the await/emit key only, **not** a workflow-claim/lease mechanism.)
- **Cancellation / orphan-join propagation is deferred** to a follow-up, not in v0.1.
- Linear-code (`async/await`-compiled) DX is an explicit **later** SDK project, not an engine requirement.

---

## 13. Embedded Changelog

- **v0.3** (2026-05-30) — Closed the fix-induced contradictions both reviewers raised against v0.2. Redefined dedup-horizon enforcement around a per-transition `delivery_anchor` (reset at each transition and timer fire) so long `send_at` sleeps are never misclassified as stale redeliveries and DLQ'd, and recomputed the bound as single-attempt (not cumulative, not max-sleep-dependent) (§5.4.1). Pinned retry continuations to a fresh `step_seq` so they re-execute instead of being swallowed as a dedup no-op (§5.2/§5.4). Redesigned poison-pill containment onto the engine's durable per-event retry counter + a dispatcher fault-isolation re-dispatch, removing the un-writable counter and the unspecified per-workflow isolation (§5.2), and pinned the retry-counter behavior as an explicit engine contract (§5.9). Replaced the unbounded per-key lock row with a transaction-scoped advisory lock (zero row, pooler-safe, collision-correctness-safe) (§5.7.3, §5.5). Separated `cache_retention_horizon` (emit→registration) from the user-facing `await_timeout` so long awaits are not capped by cache retention (§5.7.2). Spilled per-child join results to `wf_join_done`, keeping the parent resume payload under the 8 KiB cap at full fan-out (§5.8). Made timeout liveness an explicit operator invariant — pg_cron REQUIRED for scale-to-zero (§5.7.1, §10). Introduced a mandatory minimal `wf_registry` as the authoritative emit-liveness source, resolving the unknown-id-rejection vs `wf_live`-demotion contradiction (§5.5/§5.10.2/§5.12). Added a `workflow_id` confidentiality/leakage model (mandatory per-wait token for approvals, hashed at rest in audit/DLQ, never logged raw) and reframed the CSPRNG check as a testable static assertion (§5.11). Corrected the audit overclaim (no "tamper-evident"; hash-chaining deferred) and added pooling-robust `actor_id` attribution (§5.10.3). Scoped the flat-dead-tuple headline to await-light loops and conceded coordination-point-bounded dead-tuple rate for await/join-heavy workloads, adding an await/join-heavy A/B to the benchmark (§5.6/§6.5). Stated a minimum PgQ engine floor gated at install (§5.9/§5.13). Filled the empty §4 architecture block. Fixed the §5.2 cross-reference (§5.1/§5.13, not §5.10). Added positive long-sleep-not-DLQ'd and retry-re-execution tests (§6.2). All findings from both reviewers accepted.
- **v0.2** (2026-05-30) — Hardening round against Reviewer A (security/ops). Added authorization model (§5.10: REVOKE-from-PUBLIC + role grants + caller-scoped emit authz + audit trail) and `workflow_id`-as-unforgeable-capability (§5.11). Stated the dedup-horizon ≥ max-redelivery-latency bound and its DLQ enforcement (§5.4.1). Resolved the batch-transaction vs per-event-retry contradiction (§5.2). Made timeout liveness a non-optional property of the dispatch loop (§5.7.1). Pinned the await/emit lock to a pooler-safe transaction-scoped row lock (§5.7.3). Bounded `wf_event_cache` retention (§5.7.2). Demoted `wf_live` to optional/opt-in (§5.5, §5.8). Refined the zero-bloat claim (§5.6). Added resource caps (§5.12). Stated the engine tick-visibility coupling as a regression-tested contract (§5.9). Scoped the benchmark out of per-change CI (§6.5). Added security engineer, operability section (§10), open-items (§11). Reviewer B unavailable this round.
- **v0.1** (2026-05-30) — Initial spec scaffold fleshed into full structure. Resolved all five delegated interview questions. Added Goal-&-why framing, 5 user stories, layered architecture with the sacred-engine boundary, hot-path/coordination detail incl. the honest zero-bloat correction, await/emit + fan-out/join race designs, red/green TDD-first ordering, team roster, 5-sprint plan, SDK surface, and strict non-goals. No reviewer findings yet (first authoring round).
