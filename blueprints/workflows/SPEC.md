# PgQue Durable Workflows — SPEC v0.1

> Status: **experimental**, ships as optional `sql/experimental/durable.sql` gated by the project promotion rule. One reference SDK. Engine layer is sacred and untouched.

---

## 1. Goal & why it's needed

**Goal.** Provide a durable-execution / durable-workflow layer for PgQue that models each workflow as an **append-only stream of state-transition events** running over PgQ's existing snapshot + TRUNCATE rotation engine, so durable workflows inherit PgQ's zero-bloat property instead of fighting it.

**Why this exists.** Every Postgres-native durable-execution engine in the category (DBOS, absurd, and the long tail of `SELECT … FOR UPDATE SKIP LOCKED` + `DELETE` queues) shares one structural liability: they model a workflow as a **mutable `workflow_status` row that is `UPDATE`d on every step**. At the throughput the category is actually chasing — AI agent loops doing millions of cheap iterations — that per-step `UPDATE` churns dead tuples until the workload hits a VACUUM wall, and throughput degrades. PgQ already solved exactly this problem for *queues* with snapshot-batch isolation + wholesale `TRUNCATE` rotation: zero dead-tuple bloat under sustained load. The insight this spec operationalizes is that **durable execution is event sourcing**, PgQ is **already an append-only event log**, and therefore a workflow can be modeled as a stream of appended transitions (continuation-passing) rather than a mutated row. The zero-bloat property then carries through, *for free*, from the queue layer to the workflow layer.

This exists because no one else can credibly claim "durable workflows with a flat dead-tuple curve at agent-loop throughput, on just your managed Postgres, no separate datastore." That is the entire pitch, and it is only reachable by building **on top of** the rotation engine rather than re-introducing a mutable-status model beside it.

**What it is NOT** (honored strictly throughout — see §10): not a Temporal/DBOS deterministic-replay engine; not a multi-language replay runtime; not a separate server/daemon/datastore; not a hyperscale engine; not a `FOR UPDATE SKIP LOCKED` claim/lease model; cancellation/orphan-join propagation is deferred.

---

## 2. Scope & resolved interview decisions

The interview answers were all delegated to the lead ("decide for me"). Resolved:

| Question | Decision (v0.1) |
|---|---|
| **Primary users** | Backend engineers running long-lived or high-iteration orchestration (AI agent loops, multi-step business processes, fan-out jobs) **on managed Postgres** who refuse a second datastore and refuse a VACUUM wall. |
| **Core job** | Advance a workflow from one step to the next with **exactly-once handoff** and **at-least-once step execution**, never losing or silently duplicating a workflow's progress — on a hot path that appends and rotates rather than updates. |
| **Durability / recovery guarantee** | At-least-once step execution + exactly-once handoff between steps; per-step idempotency keyed on `(workflow_id, step_seq)`. On crash, exactly the single in-flight step redelivers (PgQ's existing redelivery); there is no long function to replay. |
| **Success metric** | A throughput-and-bloat benchmark vs a mutable-status-row baseline (DBOS/absurd shape) on server hardware: **flat dead-tuple count + sustained throughput** on the append+rotate hot path where the baseline degrades. |
| **Out of scope for v0.1** | Cancellation / orphan-join propagation; linear-code (`async/await`-compiled) DX; N synchronized SDKs; hyperscale (>~ a few thousand transitions/sec/db); deterministic replay. |

---

## 3. User stories

Each story is persona + action + outcome and is directly exercised as a manual acceptance test (§6.4).

1. **Agent-loop builder (zero-bloat at iteration scale).** *As* a backend engineer running an AI agent that loops thousands of times per run, *I* define each iteration as a step that processes and enqueues its successor, *so that* a million iterations complete with a **flat dead-tuple count** on the hot tables and no VACUUM-driven throughput cliff — verifiable with `pg_stat_user_tables.n_dead_tup` staying flat through the run.

2. **Long-sleep orchestrator (durable timers).** *As* an engineer modeling a "wait 7 days, then send a reminder" process, *I* call `sleep('7 days')` inside a step, *so that* the workflow durably resumes after the wait **without holding any batch open** and **without a per-workflow polling row** — the sleep is one row in a TRUNCATE-rotated delayed-delivery table.

3. **Human-in-the-loop integrator (await external event).** *As* an engineer building an approval flow, *I* call `awaitEvent('approval', timeout => '24h')` and have another part of my system call `emit(workflow_id, 'approval', payload)`, *so that* the workflow resumes **exactly once** on the event — robust against emit-before-await, await/emit interleave, and emit-racing-the-timeout — or resumes on the timeout branch if the deadline passes first.

4. **Fan-out batch processor (spawn + join).** *As* an engineer processing a parent job that splits into N independent children, *I* spawn N child workflows and `awaitAll`, *so that* the parent resumes **exactly once** when all N complete, with a **per-child result array** (success/failure each), even under redelivery of any child's completion.

5. **Exactly-once integrator (transactional handoff).** *As* an engineer whose step writes a row to *my own* business table and then advances the workflow, *I* run my side effect, the successor enqueue, and the batch ack in **one transaction**, *so that* a crash either commits all three or none — no successor without the side effect, no side effect without the ack, no duplicate handoff.

---

## 4. Architecture

<!-- architecture:begin -->

```text
(architecture not yet specified)
```

<!-- architecture:end -->

### 4.1 Layering (the sacred boundary)

```
┌──────────────────────────────────────────────────────────┐
│  Reference SDK (Python, v0.1)                            │  ← thin client
│  defineWorkflow / step / sleep / awaitEvent / emit /     │
│  spawn / awaitAll  →  plain SQL function calls           │
├──────────────────────────────────────────────────────────┤
│  Durable layer  (sql/experimental/durable.sql)          │  ← THIS SPEC
│  • dispatch loop over PgQ batches                        │
│  • coordination side tables (waits, joins, dedup, proj.) │
│  • SECURITY DEFINER fns, search_path=pgque,pg_catalog    │
├──────────────────────────────────────────────────────────┤
│  PgQ engine  (SACRED — UNMODIFIED)                       │  ← do not touch
│  insert_event · next_batch · get_batch_events ·          │
│  finish_batch · event_retry · send_at (PR #237) ·        │
│  ticks · snapshot rotation · TRUNCATE · cooperative      │
│  consumers (dead_interval takeover)                      │
└──────────────────────────────────────────────────────────┘
```

The durable layer **only calls** the five PgQ primitives + `send_at`. It adds **no** modification to rotation/tick/batch logic and introduces **no** second concurrency model.

### 4.2 Key abstractions

- **Workflow** — a logical state machine identified by `workflow_id`. At any instant it is in exactly one of three conditions: **(a)** one *in-flight* message (a step-event sitting in a PgQ batch being processed), **(b)** *scheduled* (a `send_at` continuation awaiting a wake time, or a registered wait awaiting an event), or **(c)** *terminal*. The **single-live-continuation invariant** — each processed step enqueues *exactly one* successor — is what makes exclusivity structural rather than lease-based.
- **Step-event** — the message on the PgQ queue. Payload carries: `workflow_id`, `step_seq` (monotonic progress anchor), `step_name`/state tag, and small continuation state (continuation-passing). Large state is the user's responsibility to hold in their own tables, addressed by `workflow_id`.
- **Transition** — process a step → emit successor as a *new append*. Never an `UPDATE` of a status row.
- **Coordination side tables** (the only mutable state; see §5.5) — `wf_wait`, `wf_event_cache`, `wf_join`, `wf_join_done`, `wf_dedup`, `wf_live` (current-state projection). Their churn is bounded by **concurrency and coordination-point count, not total step volume** (the honest correction, §5.6).

### 4.3 Concurrency / ownership model

One **logical consumer** with cooperative **subconsumers** splitting batches (PgQ 0.2 feature). Because exactly one live message exists per workflow, only one subconsumer ever touches a given workflow at a given instant — exclusivity is an emergent property of the invariant, requiring **no claim/lease/steal machinery**. Worker death mid-batch is covered by PgQ's existing cooperative `dead_interval` takeover: the unfinished batch is reassigned and the in-flight step redelivers (at-least-once), made safe by per-step idempotency (§5.4).

---

## 5. Implementation details

### 5.1 The hot path: one transition = append + ack, atomically

The foundational guarantee. `insert_event()` (enqueue successor) and `finish_batch()` (ack) run in the **consumer's own transaction**, so a step's side effects, its successor enqueue, and its batch ack are **one atomic commit**:

```
begin;
  -- 1. step's own DB side effects (idempotent or naturally in-txn)
  -- 2. record per-step dedup marker (workflow_id, step_seq)   [if first delivery]
  perform pgque.insert_event(queue, next_state);   -- enqueue exactly one successor
  perform pgque.finish_batch(batch_id);            -- ack this step
commit;
```

- **Commit** ⇒ successor durably enqueued **AND** batch finished, atomically ⇒ exactly-once handoff.
- **Crash before commit** ⇒ txn aborts ⇒ no successor, no dedup marker, batch not finished ⇒ the step redelivers cleanly.

No subtransactions are used on this path (hard constraint).

### 5.2 Dispatch loop

```
loop:
  batch_id := pgque.next_batch(queue, consumer)        -- snapshot-bounded
  if batch_id is null: sleep to next tick; continue
  events  := pgque.get_batch_events(batch_id)          -- many workflows at once
  begin
    for each event in events:                          -- batch step execution
        advance_one(event)                             -- §5.3, appends successor(s)
    pgque.finish_batch(batch_id)
  commit
```

A batch may contain step-events for **thousands of distinct workflows**; they advance in **one transaction** (native fan-out + batch step execution). A step that fails transiently calls `pgque.event_retry()` for that single event rather than aborting the whole batch where the engine allows per-event retry; a poisoned step lands in PgQ's existing DLQ after max retries.

### 5.3 The five durable-execution requirements, mapped

1. **Exclusive ownership — structural.** Single-live-continuation invariant + cooperative `dead_interval` takeover. No lease.
2. **Mutable run state — re-enqueue, don't update.** Each transition appends a new event carrying new state; small state rides the payload; no long-lived per-run row on the hot path.
3. **Long-lived persistence — rotating `send_at`.** `sleep('7d')` = `send_at(continuation, now()+7d)`; the step acks immediately; the sleep is one row in a TRUNCATE-rotated delayed table — zero-bloat, never an open batch.
4. **Per-row scheduling.** Timers via rotating `send_at`. **`awaitEvent` with timeout** is the genuinely hard new piece (§5.7).
5. **Checkpoint replay — not needed.** No long-running function to resume. Recovery = PgQ's at-least-once redelivery of the single in-flight step. Correctness = exactly-once handoff (§5.1) + per-step idempotency (§5.4).

### 5.4 Per-step idempotency

Every step is keyed `(workflow_id, step_seq)`. On (re)delivery a step first checks/inserts a dedup marker; the marker insert and the successor enqueue commit together (§5.1). A redelivered step whose successor already committed is a no-op (marker present) and simply re-acks. The dedup store is **append-based and short-horizon (rotating)** so it does not itself become a bloat source (§5.6).

### 5.5 Coordination side tables

| Table | Role | Churn driver | Lifecycle |
|---|---|---|---|
| `wf_live` | current-state projection: one row per **live** workflow (observability + addressing) | concurrency (live count) | inserted at start, deleted at terminal |
| `wf_wait` | registered event waits, single-resume token | open awaits | `DELETE … RETURNING` on resume/timeout |
| `wf_event_cache` | first-write-wins cache for emit-before-await | emit/await coordination points | TTL-swept |
| `wf_join` | join row: parent + total N, single-resume token | spawn points | deleted when parent resumes |
| `wf_join_done` | idempotent completed-set `(parent, child_idx)` | child completions | dropped with the join |
| `wf_dedup` | per-step `(workflow_id, step_seq)` markers | redelivery horizon | rotating / short-horizon |

All are small relative to total step volume. `wf_live`, `wf_wait`, `wf_join` are deleted on resolution (row-count bounded by concurrency); `wf_event_cache` and `wf_dedup` are TTL/rotation-bounded.

### 5.6 The honest zero-bloat claim (stated precisely, never overstated)

Zero-bloat holds on the **hot step-transition path** (appends + rotation). The coordination tables churn proportionally to **concurrency and coordination-point count, not to total step volume**. `wf_live` holds one row per *live* workflow (deleted on completion), so its row-count is bounded by concurrency; per-step dedup is append-based and short-horizon (rotating). The precise marketed claim is therefore: **zero-bloat hot path, concurrency-bounded coordination churn** — still dramatically better than per-step status-row churn. The benchmark (§6.5) measures and publishes both curves; we never claim "zero dead tuples anywhere."

### 5.7 `awaitEvent` / `emit` — the ~20% with real risk (designed and TDD'd first)

Wait registry keyed `(workflow_id, event_name)`, event names **correlation-scoped** to prevent cross-talk. Race table:

- **emit-before-await** → `emit` writes `wf_event_cache` **first-write-wins**; a later `awaitEvent` finds the cached event and resumes immediately (no wait row created).
- **await/emit interleave** → both serialize on a **per-key advisory/row lock** so exactly one of {register-wait, consume-cache} wins deterministically.
- **double-resume (emit racing the timeout sweep)** → the wait row is a **single-resume token** resolved by `DELETE … RETURNING` in the **same txn** as the continuation enqueue. Whoever deletes the row first (emit or sweep) resumes; the loser sees zero rows and does nothing.
- **stale / cross-talk cached events** → correlation-scoped names + cache TTL.
- **redelivery of the await step itself** → idempotent registration on `(workflow_id, step_seq)`, with the projection's `step_seq` as the progress anchor; re-registering is a no-op.
- **timeout** → a maintenance sweep (optional pg_cron) injects the timeout-branch continuation via the same single-resume `DELETE … RETURNING` path.

### 5.8 fan-out / join (spawn + `awaitAll`)

- Spawn N children with **distinct child workflow ids**; **record the join total `N` atomically with the spawn** — tick visibility makes this race-free for free (children become visible only at the next tick boundary, after the join row is committed).
- Count completions with an **idempotent completed-set** `(parent, child_idx)` — redelivery-safe.
- Resume the parent **exactly once** via the `wf_join` row as a deletable single-resume token (last child to flip the count to N deletes the join and enqueues the parent continuation, in one txn).
- **Explicit per-child failure semantics**: the parent receives a **result array**, one entry per child (success value or failure marker). A failed child does not block the join; it reports failure in its slot.
- **Cancellation / orphan handling is explicitly deferred** (§10).

### 5.9 Constraints honored

Reduces cleanly to `insert_event`, `next_batch`, `get_batch_events`, `finish_batch`, `event_retry` (+ `send_at`) plus the small side tables of §5.5. Single-file, no C extension, no `shared_preload_libraries`, no restart; managed-PG compatible; optional pg_cron for ticker + maint sweeps. PostgreSQL 14–18; `pg_snapshot`/`xid8`. All SECURITY DEFINER functions pin `search_path = pgque, pg_catalog`. No subtransactions in hot paths. Ships as optional experimental `sql/experimental/durable.sql` gated by the promotion rule.

---

## 6. Tests plan

### 6.1 Hard repo rule

**Red/green TDD for ALL new code.** Every function below is written test-first: a failing test asserting the behavior, then the implementation that makes it pass. CI rejects any new SQL function or SDK method without a preceding failing-then-passing test in the same change.

### 6.2 Built test-first, in this order (highest risk first)

These are the pieces explicitly required to be designed and TDD'd **first**, before any happy-path step plumbing:

1. **Exactly-once handoff** (§5.1) — red test: kill the txn between `insert_event` and `commit`, assert no successor + clean redelivery; assert no double-handoff on commit.
2. **Per-step idempotency** (§5.4) — red test: deliver the same `(workflow_id, step_seq)` twice, assert exactly one successor + one side effect.
3. **`awaitEvent` / `emit` race matrix** (§5.7) — one red test per row of the race table (emit-before-await, interleave, double-resume, stale cache, await redelivery, timeout). Single-resume token proven by concurrent emit+sweep test.
4. **fan-out / join** (§5.8) — red tests: race-free join-total recording; idempotent completed-set under duplicated completion; exactly-once parent resume; per-child failure surfaced in the result array.

### 6.3 CI test suites

- **Unit (pgTAP/SQL):** each durable function in isolation, all six coordination tables' invariants, `search_path` pinning assertion, "no subtransaction in hot path" lint.
- **Concurrency/property tests:** randomized interleavings of emit/await/timeout and spawn/complete under multiple subconsumers; assert exactly-once resume + no orphaned waits/joins.
- **Crash-recovery tests:** inject worker death mid-batch, assert `dead_interval` takeover + single redelivery + idempotent no-op.
- **Matrix:** PostgreSQL 14, 15, 16, 17, 18.
- **Engine-sacredness guard:** CI diff-check that no file under the PgQ engine path is modified by this change.

### 6.4 Manual acceptance (maps 1:1 to §3 user stories)

Each of the five user stories has a runnable scenario script the reviewer executes by hand against a managed-PG-like instance.

### 6.5 Success-criterion benchmark (the entire pitch)

Throughput-and-bloat benchmark vs a mutable-status-row baseline (DBOS/absurd shape) on server hardware. Publishes two curves over a long sustained run: **`n_dead_tup`** (flat for PgQue hot path; rising for baseline) and **sustained transitions/sec** (stable for PgQue; degrading for baseline at the VACUUM wall). Also publishes the **coordination-table churn** curve to substantiate the honest claim of §5.6. This benchmark is a CI-runnable harness, not a one-off.

---

## 7. Team (veteran experts to hire)

- **Veteran PostgreSQL internals / MVCC engineer (1)** — owns the snapshot/visibility reasoning, `xid8`/`pg_snapshot`, rotation interaction, no-subtransaction guarantee.
- **Veteran durable-execution / distributed-systems engineer (1)** — owns the await/emit and fan-out/join race designs and the single-resume-token correctness proofs.
- **Veteran PL/pgSQL + SQL test engineer (pgTAP) (1)** — owns the red/green TDD harness, concurrency/property tests, crash-recovery injection.
- **Veteran SDK / developer-experience engineer (Python) (1)** — owns the one reference SDK and the thin-client surface.
- **Veteran performance / benchmarking engineer (1)** — owns the throughput-and-bloat benchmark harness and the published curves.
- **Veteran technical writer / DX reviewer (0.5, shared)** — owns the experimental-feature docs and the honest-claim framing.

### 7.1 Persona for this spec round

Veteran **"Durable Workflow Engineer"** (accepted).

---

## 8. Implementation plan (sprints, parallelization, ordering)

**Sprint 0 — Foundations & harness (1 wk).**
- Test engineer: stand up pgTAP red/green harness, CI matrix (PG 14–18), engine-sacredness diff-guard. *(blocks everyone — must land first.)*
- PG-internals engineer: spike `next_batch`/`get_batch_events`/`finish_batch` reduction; confirm `send_at` (PR #237) shape.
- *Parallel:* SDK engineer scaffolds the thin Python client surface against stub SQL signatures.

**Sprint 1 — Exactly-once core (1.5 wk).** *(highest risk first, per constraint)*
- PG-internals + distributed-systems engineers (pair): exactly-once handoff (§5.1) and per-step idempotency (§5.4), test-first.
- Test engineer: crash-recovery injection + `dead_interval` takeover tests.
- *Gate:* no further work merges until §5.1/§5.4 tests are green.

**Sprint 2 — Coordination primitives (2 wk).** *Two independent tracks in parallel:*
- **Track A** (distributed-systems engineer): `awaitEvent`/`emit` race matrix (§5.7) — wait registry, first-write-wins cache, single-resume token, timeout sweep.
- **Track B** (PG-internals engineer): fan-out/join (§5.8) — join-total atomicity, idempotent completed-set, exactly-once parent resume, per-child result array.
- Test engineer rotates across both tracks writing the red tests ahead of each piece.

**Sprint 3 — SDK + projection + dispatch (1.5 wk).**
- SDK engineer: finalize `defineWorkflow/step/sleep/awaitEvent/emit/spawn/awaitAll` over the now-stable SQL.
- PG-internals engineer: `wf_live` projection + dispatch loop (§5.2), `sleep` via rotating `send_at`.
- *Parallel:* benchmarking engineer builds the baseline (DBOS/absurd-shape) rig.

**Sprint 4 — Benchmark, hardening, docs (1.5 wk).**
- Benchmarking engineer: run the success-criterion benchmark (§6.5), publish dead-tuple + throughput + coordination-churn curves.
- Whole team: concurrency/property-test hardening, `search_path` audit, no-subtransaction lint pass.
- Writer: experimental docs with the honest-claim framing (§5.6); promotion-rule checklist.

**Critical path:** Sprint 0 harness → Sprint 1 exactly-once gate → Sprint 2 Track A & Track B (parallel) → Sprint 3 → Sprint 4 benchmark. SDK and benchmark-rig work parallelize off the critical path from Sprint 0/3 respectively.

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
                           on_event="notify", on_timeout="escalate")

@wf.step("fan")
def fan(ctx, state):
    return ctx.spawn([...N children...], join="collect")   # awaitAll → result array

# external producer, anywhere:
emit(workflow_id, "shipped", payload)
```

Every SDK call compiles to one of the five PgQ primitives + a coordination-table touch. **No** `async/await`-compiled linear-code DX in v0.1 (deferred, §10).

---

## 10. Non-goals / disclaimers (honored strictly — not reintroduced anywhere above)

- **NOT** a Temporal/DBOS-style deterministic-replay engine. No workflow-determinism requirement, no replay of a long linear function, no per-step `workflow_status` UPDATE churn.
- **NOT** a multi-language deterministic-replay runtime in v1. No N synchronized SDKs — one reference SDK.
- **NOT** a separate server, daemon, or external datastore. No Cassandra, RocksDB, FoundationDB, or Redis.
- **NOT** targeting hyperscale (>~ a few thousand workflow transitions/sec per database) — conceded to Temporal honestly.
- **NOT** changing the sacred PgQ engine, and **NOT** introducing a second `SELECT … FOR UPDATE SKIP LOCKED` claim/lease concurrency model as the primary mechanism — exclusivity comes from the single-live-continuation invariant over the existing rotation engine.
- **Cancellation / orphan-join propagation is deferred** to a follow-up, not in v0.1.
- Linear-code (`async/await`-compiled) DX is an explicit **later** SDK project, not an engine requirement.

---

## 11. Embedded Changelog

- **v0.1** (2026-05-30) — Initial spec scaffold fleshed into full structure. Resolved all five delegated interview questions (primary users, core job, durability model, success metric, out-of-scope). Added Goal-&-why framing, 5 user stories, layered architecture with the sacred-engine boundary, hot-path/coordination implementation detail incl. the honest zero-bloat correction, await/emit + fan-out/join race designs, red/green TDD-first ordering, team roster, 5-sprint plan with parallel tracks, SDK surface, and strict non-goals. No reviewer findings yet (first authoring round).
