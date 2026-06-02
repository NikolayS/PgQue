# Implementing Durable Workflows on PgQue — Implementation Research

- **Status:** research / grounding (input to `sql/experimental/durable.sql`, not yet code)
- **Date:** 2026-05-30
- **Companion:** `blueprints/workflows/SPEC.md` (the conceptual spec, v0.5) and
  `blueprints/DURABLE_EXECUTION_FEASIBILITY.md` (why this route). This document
  grounds the spec's design in **pgque's actual primitives, tables, and verified
  transaction semantics** — read straight from `sql/pgque.sql` (7,044 lines).
- **Method:** every claim below is checked against the real `sql/pgque.sql`
  function bodies and table DDL (line numbers cited), not against the conceptual
  spec.

---

## 1. The keystone, verified against real code

The whole design rests on one assumption: **a step's side effects, the enqueue
of its successor, and the batch ack all commit in one transaction** (exactly-once
handoff). Verified:

- `pgque.insert_event(queue, type, data[, ev_extra1..4])` (`sql/pgque.sql:1654,
  1678`) is plain `plpgsql` — it calls `insert_event_raw`, no internal `COMMIT`.
- `pgque.finish_batch(batch_id)` (`:2478`) is literally **one statement**:
  `update pgque.subscription set sub_active=now(), sub_last_tick=sub_next_tick,
  sub_next_tick=null, sub_batch=null where sub_batch=x_batch_id`. No commit, no
  autonomous work.
- The only `COMMIT`s in the file are in the `ticker_loop` **procedure** (`:4142`)
  and `upgrade_schema`; the only `pg_notify` is in the **ticker** (`:688,793,
  5190`) — neither is on the consume/ack path.

So this composes atomically and is the exactly-once-handoff primitive, for free:

```sql
begin;
  -- 1. the step's own business writes (caller's tables)
  -- 2. dedup marker insert (workflow_id, step_seq)         [first delivery only]
  perform pgque.insert_event(q, step_name, payload,
                             workflow_id, (step_seq+1)::text, null, null);
  perform pgque.finish_batch(batch_id);
commit;
```

Crash before `commit` ⇒ nothing happened, the step redelivers (PgQ at-least-once)
⇒ retry. Commit ⇒ successor durably enqueued **and** batch finished. No
subtransactions on this path (hard rule, satisfied).

### 1.1 This also proves the amortization answer

`finish_batch` updates **one `subscription` row per batch**, not per event — and a
batch carries the step-events of *many* workflows. So advancing N workflows
through a batch is **N appends (`insert_event`) + 1 subscription UPDATE**. The
per-workflow state never becomes a per-transition row UPDATE. This is the exact
mechanism behind the "per-batch amortization is preserved" claim.

---

## 2. The pgque primitives we build on (real signatures)

| Primitive | Signature (`sql/pgque.sql`) | Role in the workflow layer |
|---|---|---|
| `insert_event` | `(queue, type, data, ev_extra1..4)` `:1678` | append a step-event / successor; `ev_extra1=workflow_id`, `ev_extra2=step_seq` |
| `register_consumer` / `subscribe` | `:1753` | the workflow dispatcher's logical consumer |
| `register_subconsumer` / `receive_coop` | `:5979,:6126` | parallel workers under one logical consumer; structural per-workflow exclusivity + `dead_interval` takeover |
| `next_batch` / `get_batch_events` | `:2011,:2178` | snapshot-bounded batch of step-events to advance |
| `finish_batch` (`ack`) | `:2478,:5385` | exactly-once handoff partner (1 row/batch) |
| `event_retry` (`nack`) | `:2347` → `retry_queue` | transient step retry (see §5 — bloat caveat) |
| `event_dead` / DLQ | `:4912,:4967..` | poisoned step after max retries (reuse as-is) |
| `send_at` (experimental, PR #237) | `sql/experimental/delayed.sql` | `sleep()` and `awaitEvent` timeout — **rotating, zero-bloat** |
| `jsontriga` | `:2917` | CDC-triggered workflow starts |

### 2.1 Tables that already exist (and how they behave)

- `event_template` (`:204`) — the rotating event row; **has `ev_extra1..4`**.
  Rotated/TRUNCATEd ⇒ **zero bloat**. This is where step-events live.
- `subscription` (`:169`) — consumer cursor; `finish_batch` UPDATEs it **per
  batch** (HOT-updatable; one row per logical consumer/subconsumer).
- `retry_queue` (`:231`) — `like event_template` + `ev_retry_after`, indexed on
  `ev_retry_after`. **INSERT on `event_retry`, DELETE on `maint_retry_events`** ⇒
  DELETE-based ⇒ **does accumulate dead tuples**. Constraint, see §5.
- `tick`, `queue`, `consumer`, `dead_letter`, `config` — unchanged.

---

## 3. Workflow conventions on the event row (no new event table)

A workflow step-event is an ordinary pgque event with a convention:

- `ev_extra1 = workflow_id` (uuid/text) — **add a btree index on `ev_extra1`** on
  the event tables so "find the in-flight event(s) for workflow X" and "list
  running workflows" are indexed lookups. The index rotates/TRUNCATEs with the
  tables ⇒ **zero bloat**, bounded to the in-flight window.
- `ev_extra2 = step_seq` (monotonic per workflow) — progress anchor + dedup key.
- `ev_extra3 = run/parent ids` (fan-out), `ev_extra4 = flags` (e.g. retry_attempt).
- `ev_type = step_name`; `ev_data = continuation state` (small) or a pointer to
  the caller's own large-state table keyed by `workflow_id`.

Indexing `ev_extra1` adds one index-maintenance cost on the hot insert path
(modest, optional, and it rotates). This is the single change to how events are
written; everything else is convention in the payload.

---

## 4. Primitive-by-primitive mapping

| Workflow op | Implementation on pgque |
|---|---|
| **start / spawn(wf, input)** | `insert_event(q, first_step, input, workflow_id, '0', …)`; insert `wf_live` row (start boundary, §6). Returns `workflow_id`. |
| **step transition** | process event → `begin; <effects>; insert_event(successor, …, step_seq+1); finish_batch(batch); commit;` (§1). |
| **sleep(Δ)** | `send_at(q, continuation, now()+Δ)` then `finish_batch` — **rotating delayed delivery, not `event_retry`** (§5). Step holds no open batch across the sleep. |
| **step retry (transient)** | re-enqueue a continuation of the *same logical step* with a **fresh `step_seq`** via `send_at(now()+backoff)`; after `max_retries` → DLQ transition. (Using PgQ's `event_retry` is the built-in alternative but reuses `ev_id`/bumps `ev_retry` and is DELETE-based — see §5.) |
| **awaitEvent(name, timeout)** | register `wf_wait(workflow_id, name, …)`; `send_at` a timeout-continuation; `finish_batch`. Resume = single-resume token (§7). |
| **emit(workflow_id, name, payload)** | under a per-key lock: if a `wf_wait` row exists, delete it (token) + `insert_event` the resume continuation; else first-write-wins into `wf_event_cache` (§7). |
| **spawn N children + awaitAll** | `insert_event` N child-start events (distinct child `workflow_id`) + create `wf_join(parent, total=N)` **in one txn** (tick-visibility makes total-before-children race-free); children report via idempotent `wf_join_done(parent, child_idx)`; last one resumes parent (§8). |
| **complete / fail (terminal)** | `finish_batch` + delete `wf_live` row + append `wf_audit`. Fail after retries → `event_dead`/DLQ (existing). |
| **dispatch / scale** | one logical consumer + cooperative subconsumers (`register_subconsumer` + `receive_coop`); `dead_interval` takeover for worker crash. Scale-out beyond one DB = independent hash-shard on `workflow_id` (separate PgQue installs), **not** pgq_node cascading. |

---

## 5. The retry/sleep bloat constraint (grounded finding)

`event_retry` (`:2347`) INSERTs into `retry_queue`, and `maint_retry_events`
(`:826`) later DELETEs as it moves events back — **DELETE-based, so `retry_queue`
accumulates dead tuples** proportional to retry/sleep volume. For a workflow
engine where `sleep` and long waits are common, leaning on `retry_queue` would
reintroduce exactly the bloat we exist to avoid.

**Resolution:** route `sleep()` and `awaitEvent` timeouts through the **rotating
`send_at`** (PR #237: TRUNCATE-rotation, no DELETE, no VACUUM dependence), not
`retry_queue`. Reserve `event_retry`/`retry_queue` for *transient step retries*
only (lower volume, short backoff) — or model even those as fresh-`step_seq`
`send_at` continuations to keep the whole hot path append+rotate. **Dependency:
`send_at` must be promoted from `sql/experimental/` to a supported primitive
(and land PR #237's rotation) before the durable layer can claim zero-bloat
sleeps.**

---

## 6. New schema the durable layer adds (`sql/experimental/durable.sql`)

All small, coordination-only; row-count bounded by **concurrency / coordination
points**, never by total step volume (§9 bloat audit).

```sql
-- one row per LIVE workflow; observability + addressing; OPT-IN, default off.
-- updated at park/start/terminal boundaries (NOT per step); deleted on terminal.
create table pgque.wf_live (
  workflow_id text primary key, queue text, state text,        -- running|waiting|sleeping
  step_seq int, step_name text, updated_at timestamptz default now());

-- registered event waits; the single-resume token (deleted on resume/timeout).
create table pgque.wf_wait (
  workflow_id text, event_name text, step_seq int,
  resume_step text, timeout_at timestamptz,
  primary key (workflow_id, event_name));

-- emit-before-await cache, first-write-wins, correlation-scoped, TTL-swept.
create table pgque.wf_event_cache (
  event_name text primary key, payload jsonb, emitted_at timestamptz default now());

-- fan-out join state + idempotent completed-set.
create table pgque.wf_join (
  parent_id text primary key, total int, resume_step text);
create table pgque.wf_join_done (
  parent_id text, child_idx int, result jsonb, ok bool,
  primary key (parent_id, child_idx));

-- per-attempt idempotency markers; APPEND-only, short-horizon, rotating.
create table pgque.wf_dedup (
  workflow_id text, step_seq int, created_at timestamptz default now(),
  primary key (workflow_id, step_seq));

-- append-only security/audit + history feed (exported before rotation).
create table pgque.wf_audit (
  ts timestamptz default now(), workflow_id text, action text, detail jsonb);
```

`wf_dedup` and `wf_event_cache` need a rotation/TTL story (mirror PR #237) so they
don't become DELETE-bloat; `wf_live`/`wf_wait`/`wf_join` are delete-on-resolution
(row-count bounded by concurrency).

---

## 7. `awaitEvent` / `emit` — concrete race handling

The hard part. Build and TDD this first (two-session tests, like the repo's
`tests/two_session_*.sh`).

- **Serialize await-register vs emit-deliver** on a transaction-scoped advisory
  lock keyed by `hashtext(workflow_id||':'||event_name)` — no lock *table*, zero
  tuples. (PgQ already serializes batch allocation on a row lock, so this is
  idiomatic.)
- **emit:** `pg_advisory_xact_lock(key)`; if `wf_wait` row exists →
  `delete … returning` (the token) + `insert_event` the resume continuation in
  the same txn; else `insert into wf_event_cache … on conflict do nothing`
  (first-write-wins).
- **awaitEvent:** `pg_advisory_xact_lock(key)`; check `wf_event_cache` (resume
  immediately if present, consume it); else insert `wf_wait` + `send_at` timeout
  continuation; `finish_batch`.
- **double-resume (emit racing timeout):** both resolve via
  `delete from wf_wait where workflow_id=… and event_name=… returning *` — whoever
  deletes first resumes; the loser gets zero rows and no-ops.
- **redelivery of the await step:** idempotent on `(workflow_id, step_seq)` via
  `wf_dedup`; a redelivered await whose wait was already consumed sees the
  workflow advanced and just re-acks.
- **cross-talk:** event names are correlation-scoped (include `workflow_id` or a
  nonce); `wf_event_cache` TTL-swept by `maint`.

---

## 8. Fan-out / join — concrete

- **Spawn:** in one txn, `insert_event` each child-start (distinct child
  `workflow_id`, `ev_extra3=parent_id|child_idx`) **and** `insert wf_join(parent,
  total=N)`. Tick visibility guarantees children aren't processed until after the
  join row commits ⇒ "total before any child completes" is race-free for free.
- **Child completion:** `insert into wf_join_done(parent, child_idx, result, ok)
  on conflict do nothing` (idempotent under redelivery); then
  `select count(*) from wf_join_done where parent_id=…`; if `= total`,
  `delete from wf_join … returning` (token) + `insert_event` the parent resume
  carrying the result array; all in the child's handoff txn.
- **Partial failure:** `ok=false` rows still count toward `total`; the parent
  resume gets a per-child result array and decides. **Cancellation / orphan-join
  deferred** (spec non-goal).

---

## 9. Bloat audit, grounded in the real mechanics

| Structure | Write pattern | Bloat |
|---|---|---|
| event tables (`event_template`-derived) | INSERT, TRUNCATE-rotate | **none** (rotation) |
| `ev_extra1` index | rides the event tables | **none** (rotates with table) |
| `subscription` (`finish_batch`) | 1 UPDATE per **batch** | negligible (HOT, per-consumer) |
| `wf_dedup`, `wf_event_cache` | INSERT, rotate/TTL | none if rotated (PR #237 pattern) |
| `wf_live`, `wf_wait`, `wf_join` | INSERT + DELETE on resolution | concurrency-bounded (live count), VACUUM-able |
| `retry_queue` (if used for sleeps) | INSERT + DELETE | **bloats** ⇒ use `send_at` instead (§5) |

Net: the **hot per-step path is append + rotate (zero bloat)**; coordination is
concurrency-bounded; the only landmine is `retry_queue`, avoided by routing
sleeps through rotating `send_at`.

---

## 10. Gaps — what pgque must add before/with the durable layer

1. **Promote `send_at` to a supported primitive with PR #237 rotation.** Hard
   dependency for zero-bloat sleeps and await-timeouts.
2. **Optional `ev_extra1` index** (per-queue opt-in) for workflow lookup.
3. **`sql/experimental/durable.sql`**: the tables in §6 + the functions
   (`wf_start/step/sleep/await_event/emit/spawn/await_all/complete`), all
   `SECURITY DEFINER … set search_path=pgque,pg_catalog`, no subtransactions.
4. **Maint hooks**: TTL/rotation sweeps for `wf_dedup`/`wf_event_cache`, timeout
   firing for `wf_wait` (via the existing `pgque.maint()` cadence / pg_cron).
5. **Thin clients** in all PgQue languages (Python, Go, TS, +WIP): a worker loop
   (`receive_coop` → dispatch by `ev_type` → handler → handoff) + the `ctx`
   surface. Durability is in SQL, so each client stays thin.

---

## 11. Build order (red/green TDD, highest risk first)

1. **Harness + engine-contract tests**: pin the §1 semantics (insert_event +
   finish_batch atomic; finish_batch = 1 subscription UPDATE/batch; tick
   visibility ordering) so the design can't silently regress on a pgque change.
2. **Exactly-once handoff + `(workflow_id, step_seq)` dedup** (§1, §6).
3. **`send_at`-based sleep** (depends on PR #237) + **`awaitEvent`/`emit` race
   matrix** (§7) — two-session race tests.
4. **fan-out / join** (§8).
5. **Dispatch loop on cooperative consumers** + `dead_interval` takeover.
6. **Observability** (`ev_extra1` index, `wf_live` opt-in, `wf_audit` export).
7. **One reference client**, then the rest.

---

## 12. Open questions

- Promote `send_at` now (its own PR) so the durable layer has a stable dep?
- `step_seq` as `ev_extra2 text` vs a real int column — indexing/typing choice.
- Retry policy: fresh-`step_seq` `send_at` continuations (pure append) vs PgQ's
  `event_retry` (built-in, but DELETE-based) — recommend the former for the hot
  path, the latter only for low-volume cases.
- `wf_event_cache`/`wf_dedup` rotation: reuse PR #237's two-table TRUNCATE scheme
  vs a simpler TTL `DELETE` (acceptable if volume is low).
- Single-DB throughput target validated by the §11.1 benchmark before publishing
  numbers.
</content>
