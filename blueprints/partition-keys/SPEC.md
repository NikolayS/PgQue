# PgQue Partition Keys — Spec

- **Version:** v0.2 (draft)
- **Status:** review round 1 applied (Reviewer A ops/security + Reviewer B
  QA/testability). Core mechanism re-grounded against the engine; see §15
  changelog and `decisions.md`.
- **Slug:** partition-keys
- **Scope:** consumer-side ordered, parallel consumption by partition key.
  Producer-side idempotency/dedup is a *separate* spec (deferred — see §12).

---

## 1. Goal

Add a **partition key** to PgQue so that, within one queue, events sharing a key
are consumed **in order by a single worker at a time**, while events with
different keys are consumed **in parallel**. This is the log-native ("Kafka
partition") model: order *within* a key, parallelism *across* keys.

## 2. The guarantee (precise, testable)

Stated as three independently-testable clauses (this replaces vague "in order"
prose; per Reviewer B):

- **G1 — per-key affinity + happy-path FIFO.** For a queue whose events carry a
  partition key, and a fixed slot count `N`, every event of key `K` maps to
  exactly one slot `slot(K) = hashtextextended(K, 0) % N`. Within that slot,
  successfully-processed, never-retried events of `K` are delivered in
  non-decreasing `ev_id` order, and to **no other slot**.
- **G2 — single in-flight processor per key.** At any instant, at most one worker
  holds an unacked event for `K`.
- **G3 — failure boundary.** Under **`pause`** policy, if `K#i` fails, no later
  event of `K` is delivered until `K#i` is acked or dead-lettered; other keys are
  unaffected. Under **`skip`** policy, later events of `K` MAY be delivered before
  `K#i` resolves — so after a failure only *at-least-once* holds, not order.
  **Note (engine fact):** a retried event re-enters under a new transaction/tick
  (its `ev_id` is preserved but its `ev_txid` is new, so it reappears in a *later*
  batch — `event_retry` → `maint_retry_events` → `insert_event_raw`). So G1's
  `ev_id` monotonicity holds only *between non-retried* events; across a retry the
  only guarantee is G3's pause boundary, never `ev_id` ordering.

## 3. Why it's needed

PgQue is an **ordered, immutable log**, not a job queue. Real workloads need
**per-entity ordering without global ordering**. Motivating case (a multi-tenant
storage service evaluating PgQue to replace pg-boss):

- Millions of file-lifecycle events (`FileCreated`, `FileDeleted`,
  `FileOverwritten`), which **must be ordered per tenant** but **need no ordering
  across tenants**.
- One in-order consumer can't keep up; naive multi-worker consumption breaks
  per-tenant order.

## 4. Scope and ICP

**In scope (v0.1 implementation):**
- Carry a partition key on a `send()`-sourced event.
- N independent **slot consumers**, each filtering the stream to its hash class
  (§6). Stable `hashtextextended(key, 0) % N` affinity.
- G1 + G2 always; **`skip` failure policy as the v0.1 default** (sound, simple).

**Deferred to v0.2 implementation (specified, not built first):**
- **`pause` failure policy** (G3 strict). It is fully specified here (§7 D2, §8)
  but carries the crash-recovery risk (R1) and ships after `skip`.

**Out of scope:**
- Producer idempotency / dedup windows (separate spec — §12).
- Dynamic rebalancing / elastic `N` (fixed `N`; §7 D3, R4).
- **Trigger-sourced queues** (`jsontriga`/`logutriga`/`sqltriga` already store the
  table name in `ev_extra1` — §7 D1, R5). Partitioned consumption is defined only
  for `send()`-sourced queues in v0.1.
- Cross-queue / cascaded (multi-node) partitioning.
- Automatic hot-partition mitigation (documented only).

**ICP:** multi-tenant SaaS on managed Postgres running a high-volume per-entity
event stream where entity = partition key (tenant, user, document, device).

## 5. End-to-end workflow

```
producer:  pgque.send('files', 'default', payload, partition_key => tenant_id)
                       │  key → ev_extra1 (send-sourced queues only, v0.1)
                       ▼
engine:    append-only event tables · global ev_id/ev_txid order   (UNCHANGED)
                       │  full stream
                       ▼
consumers: N slot consumers, each an INDEPENDENT subscription with its own cursor;
           slot k reads the whole stream and server-side-filters to its hash class
```

## 6. Architecture

The v0.1 mechanism is **N independent slot consumers**, *not* a modification of
cooperative consumers. (Review round 1, B1: coop hands each member a disjoint
tick window; it cannot fan one batch to N hash-filtered slots without dropping
events when the shared cursor advances. So we do not use coop distribution.)

<!-- architecture:begin -->
```
 producers │ send(queue, type, payload, partition_key => K)
           │   key → ev_extra1
           ▼
 ┌──────────────────────────────────────────────────────────┐
 │ ENGINE · sacred — UNCHANGED                                │
 │ append-only tables · global ev_id/ev_txid · rotation      │
 │ next_batch / get_batch_cursor(i_extra_where) / get_events │
 └───────┬───────────────┬───────────────┬──────────────────┘
         │ full stream    │ full stream    │ full stream
         ▼                ▼                ▼
   slot 0 (sub#0/N)  slot 1 (sub#1/N)  slot N-1 (sub#(N-1)/N)
   own cursor        own cursor        own cursor
   extra_where:      extra_where:      extra_where:
   hashext%N=0       hashext%N=1       hashext%N=N-1
         │                │                │
   one worker        one worker        one worker
   keys h%N==0       keys h%N==1       keys h%N==N-1
   in ev_id order    in ev_id order    in ev_id order
```
<!-- architecture:end -->

**How filtering happens without touching the engine:** each slot's receive call
reuses the existing `pgque.get_batch_cursor(..., i_extra_where)` hook
(`pgque.sql` ~line 2229), injecting the predicate
`and hashtextextended(ev_extra1, 0) % N = k`. The predicate is built from
integers `N`, `k` (no user input → injection-safe). `batch_event_sql`,
`next_batch`, and rotation are **not modified**.

**Each slot is its own subscription**, so it has its own cursor (`sub_last_tick`)
and its own `sub_id` → **no cross-slot data loss** (each slot independently
advances over the full stream) and **retry/DLQ rows are naturally slot-scoped**
(`ev_owner = that slot's sub_id`), which is what makes `pause` re-derivable
(§8).

**Known cost — read amplification.** Every event is examined by all `N` slot
cursors (each discards `(N-1)/N` after the hash filter). The `extra_where`
push-down keeps *returned* rows minimal, but the index scan over each tick window
is repeated `N` times. Acceptable for moderate `N`; documented, with a
single-reader/dispatch optimization noted as future work (R6).

## 7. Decisions

| ID | Decision | Choice (v0.2) | Rationale / change |
|----|----------|---------------|--------------------|
| D1 | Where the key lives | `ev_extra1`, **`send()`-sourced queues only** | Trigger producers already use `ev_extra1` for the table name (`pgque.sql:2943`). Restrict, don't collide. (R5) |
| D2 | Failure policy | `skip` default (v0.1); `pause` specified, ships v0.2 | `pause` needs durable-ish blocked-key tracking; deliver the sound `skip` first. Rationale corrected re: retry (§2 note). |
| D3 | Elasticity | Fixed `N`, **persisted per (queue, consumer)** | A worker registering with a mismatched `N` is **rejected**, so "fixed N" is an invariant, not a convention. (N2) |
| D4 | Assignment function | `hashtextextended(key, 0) % N` | `hashtext()` is internal/unstable across PG majors → affinity would break on upgrade. `hashtextextended` is the documented, stable hash. (N1) |
| D5 | State budget | **No new mutable table; happy path writes nothing.** `pause` derives blocked keys from the engine's existing `retry_queue`/`dead_letter`, read only on failure/slot-start | Resolves the D2-vs-"no state" contradiction honestly: failure handling reuses state the engine already keeps, scoped per slot by `sub_id`. (B3/B4) |
| D6 | Producer signature | `send(queue, type, payload, partition_key => text)` (new 4-arg overload) | A 3-arg `send(queue, key, payload)` collides with the existing `send(queue, type, payload)`. (B4/N4) |
| D7 | Slot identity & single-owner | slot = a named consumer `"<consumer>#k/N"`; single owner enforced by the existing per-consumer receive lock (the `sub_batch`/`FOR UPDATE` path) | Defines what a "slot" is and what makes G2 true and testable. (B5) |

## 8. Implementation details

- **Producer:** `pgque.send(queue, type, payload, partition_key text default null)`
  → `insert_event(queue, type, payload, ev_extra1 => partition_key, …)`. Pure
  reduction to the existing primitive (Key Design Rule 3). Explicit
  `revoke execute … from public` + `grant … to pgque_writer`, `SECURITY DEFINER`
  with `set search_path = pgque, pg_catalog`.
- **Slot registration:** `pgque.subscribe_slot(queue, consumer, k, n)` registers
  subscription `"<consumer>#k/n"` and persists `n` for the consumer; a later
  registration with a different `n` is rejected (D3).
- **Partitioned receive:** `pgque.receive_partitioned(queue, consumer, k, n, …)`
  → `next_batch` + `get_batch_cursor(..., i_extra_where =>
  format('and hashtextextended(ev_extra1,0) %% %s = %s', n, k))`. Server-side
  filter; engine untouched.
- **`pause` blocked-set (v0.2):** within a run, a worker holds back later events
  of a key whose head event is unacked/retrying. On (re)start, the blocked set is
  rebuilt by querying `retry_queue where ev_owner = <slot sub_id>` (existing
  state; read once at slot start, not per event). A key unblocks when its head
  event is acked **or** dead-lettered (`dead_letter`), so a poison event cannot
  wedge a tenant beyond `max_retries` (B5). `skip` mode needs none of this.
- **Grants:** producer overload → `pgque_writer`; `subscribe_slot` /
  `receive_partitioned` → `pgque_reader`. Deny-by-default re-applied.

## 9. Tests plan (red/green TDD)

Write the failing test first. CI matrix PG 14–18. Map to the guarantee:

- **T-G1a (affinity):** same key → same slot; assert the **literal integer**
  `hashtextextended(K,0) % N` (not "same within one version") on **every** CI PG
  version — guards D4 stability. *(red first)*
- **T-G1b (per-key FIFO, happy path):** interleave A,B,A,A,B; assert each key in
  `ev_id` order across batches, no key on two slots.
- **T-G2 (single owner):** two sessions, same slot → second blocks on the
  receive lock (mirror `tests/two_session_receive_lock.sh`).
- **T-no-drop:** keys spanning all slots in one tick window; run all N slots;
  assert union delivered = all events, **zero loss** (guards the cursor/filter
  interaction, §6).
- **T-G3-pause (order-after-retry):** A#2 nacked (`pause`); assert A#3 withheld
  until A#2 acked-or-DLQ'd; B unaffected.
- **T-G3-skip (reorder boundary):** with `skip`, assert the *exact* permitted
  reorder after A#2 fails (not just "A#3 proceeds").
- **T-DLQ-unblock:** A#2 exhausts retries → `dead_letter`; assert A#3 then
  proceeds (no permanent wedge).
- **T-slot-crash:** slot-k worker holds A#2 unacked and dies; another worker takes
  slot k; assert A#2 redelivered before A#3 and only to slot k.
- **T-empty-slot / T-hot-key:** an empty slot doesn't wedge others; a single hot
  key saturates one slot while others still drain (correctness, not perf).
- **T-no-bloat (happy path):** processing M events with all acks adds **zero**
  rows to `retry_queue`/`dead_letter` and issues no per-event UPDATE/DELETE. (The
  failure path legitimately writes `retry_queue` — out of this test's scope.)
- **T-engine-untouched:** `pg_get_functiondef` of `batch_event_sql` and
  `next_batch_custom` byte-identical to baseline (assert on the **definition**,
  not the generated SQL — N2).
- **T-idempotent-install:** re-running `pgque.sql` re-creates partition functions
  cleanly (mirror `tests/test_install_idempotency.sql`).

## 10. Risks and open questions

- **R1 — `pause` crash recovery.** Rebuilding the blocked set from `retry_queue`
  at slot start (D5) is the crux; needs the exact predicate and a test
  (T-slot-crash). This is why `pause` ships after `skip`.
- **R2 — read amplification.** N× scans (§6). Bench it; if it bites, R6.
- **R3 — hot partitions.** One hot key saturates its slot; v0.1 documents only.
- **R4 — changing N.** Now an invariant (D3): mismatched workers are rejected, so
  no silent reorder. True rebalancing is a future spec.
- **R5 — `ev_extra1` semantics.** Restricted to `send()`-sourced queues (D1);
  a dedicated partition-key column is possible future work.
- **R6 — single-reader/dispatch optimization** to remove read amplification
  (one reader hash-routes to per-slot staging). Future; adds a hop/state, so out
  of v0.1's no-state budget.

## 11. (reserved)

## 12. Relationship to producer idempotency (deferred sibling)

Producer-side dedup is a **TTL window** (SQS/NATS model), append-only, GC'd by
rotation — a separate spec. In a log, "processed" is a per-consumer fact the
producer cannot see, so dedup must be a producer-side time window, while
ordering/serialization is this consumer-side partition feature. Rationale and
prior art: `blueprints/idempotency/DESIGN.md`.

## 13. Team of veteran experts (review panel)

- **Lead:** drafts/revises (this document).
- **Reviewer A — ops/security:** failure modes, crash safety, scope. Round 1
  applied (B1–B5, N1–N5).
- **Reviewer B — QA/testability:** ordering precision, slot model, falsifiable
  tests. Round 1 applied (B1–B6, N1–N7, the G1/G2/G3 restatement in §2).

## 14. Sprint plan

1. **S1 — producer + key plumbing:** `send(…, partition_key =>)`, key on
   `ev_extra1` for send-sourced queues. Tests T-G1a, T-no-bloat(happy),
   T-idempotent-install.
2. **S2 — slot consumers (`skip` default):** `subscribe_slot`,
   `receive_partitioned` via `get_batch_cursor` `extra_where`; persisted N (D3);
   single-owner (D7). Tests T-G1b, T-G2, T-no-drop, T-G3-skip, T-engine-untouched.
3. **S3 — `pause` policy (v0.2):** blocked-set from `retry_queue`; DLQ-unblock.
   Tests T-G3-pause, T-DLQ-unblock, T-slot-crash.
4. **S4 — docs + benchmark:** throughput vs N; read-amplification cost (R2);
   per-tenant order under load.

## 15. Changelog

- **v0.2 (draft):** review round 1 (Reviewer A + B) applied. **Re-grounded the
  core mechanism**: dropped the (impossible) coop-distribution model for **N
  independent slot consumers** filtering via `get_batch_cursor` `extra_where`
  (§6). Restated the guarantee as testable **G1/G2/G3** (§2). **Corrected** the
  retry rationale (ev_id preserved, ev_txid changes). Resolved D2-vs-state with
  **D5** (derive `pause` from existing `retry_queue`; no new table). Made `skip`
  the v0.1 default, `pause` a specified v0.2 follow. Fixed: `send` signature
  collision (D6), `ev_extra1`/trigger collision (D1), unstable `hashtext` (D4),
  fixed-N as enforced invariant (D3), slot/owner definition (D7). Added missing
  tests (no-drop, order-after-retry, DLQ-unblock, slot-crash, empty/hot,
  cross-version affinity). Recorded accepted/rejected items in `decisions.md`.
- **v0.1 (draft):** initial single-pass SamoSpec-format draft.
