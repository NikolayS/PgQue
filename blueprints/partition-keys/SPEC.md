# PgQue Partition Keys — Spec

- **Version:** v0.6 (draft)
- **Status:** review rounds 1–3 applied. **Phase 1 (`skip`-default partition
  consumption) is converged / implementation-ready.** Phase 2 (`pause` strict
  ordering) is specified but has open design items (§11) and is a deliberate
  follow-up. See §16 changelog and `decisions.md`.
- **Slug:** partition-keys
- **Scope:** consumer-side ordered, parallel consumption by partition key.
  Producer-side idempotency/dedup is a separate spec (deferred — §12).

---

## 1. Goal

Within one queue, events sharing a partition key are consumed **in order by a
single worker at a time**; events with different keys are consumed **in
parallel** — the log-native ("Kafka partition") model: order *within* a key,
parallelism *across* keys.

## 2. The guarantee (precise, testable)

- **G1 — per-key affinity + FIFO.** For a queue whose events carry a partition
  key and a fixed slot count `N`, every event of key `K` maps to one slot
  `slot(K) = (hashtextextended(K, 0) % N + N) % N` (the `+N` normalizes the sign;
  `hashtextextended` returns `bigint`). Within that slot, non-retried events of
  `K` are delivered in non-decreasing `ev_id` order, to **no other slot**.
  Intra-batch order is the engine's `order by 1` (`pgque.sql:440`), preserved
  through `get_batch_cursor`'s filter re-wrap (`pgque.sql:2277`); cross-batch
  order follows from one subscription's monotonically-advancing cursor.
- **G2 — single in-flight processor per key.** At most one worker holds an
  unacked event for `K`. Enforced by the per-subscription receive lock
  (`next_batch_custom … for update of s`, `pgque.sql:5761` — the #97/#125 guard).
- **G3 — failure boundary (Phase 2 / `pause`).** Under `pause`, no later event of
  `K` is delivered until `K`'s failed head event is acked or dead-lettered, and
  after it resolves the deferred events deliver in `ev_id` order, exactly once.
  Under `skip` (Phase 1 default), later events of `K` MAY arrive before the
  failure resolves — only at-least-once holds.
  - **Engine fact:** a retried event keeps its `ev_id`, gets a new `ev_txid`
    (re-injected by `maint_retry_events` → `insert_event_raw`, `pgque.sql:859`),
    and re-routes to the **same slot** because `ev_extra1` is preserved
    (`pgque.sql:861`). So G1's `ev_id` monotonicity holds only between non-retried
    events; across a retry the only ordering guarantee is G3's pause boundary.

## 3. Why it's needed

PgQue is an ordered, immutable **log**, not a job queue — workloads need
per-entity ordering without global ordering. Motivating case (a multi-tenant
storage service evaluating PgQue vs pg-boss): millions of file-lifecycle events
that **must be ordered per tenant** but need **no ordering across tenants**.

## 4. Scope and phasing

**Phase 1 — converged, build now:**
- Partition key on a `send()`-sourced event (D1, D6).
- N independent **slot consumers**, each filtering the stream to its hash class
  via `get_batch_cursor` `extra_where` (§6). Stable affinity (D4).
- G1 + G2. **`skip` failure policy** (stateless, sound).
- Persisted/enforced `N` (D3); slot identity + single-owner (D7); SECURITY
  DEFINER ownership model (§6).
- **Client-side claim assignment** (§15): workers self-distribute over the fixed
  `N` slots via a per-slot advisory try-lock — no leader, no assignor.

**Phase 2 — specified, follow-up (NOT converged):**
- **`pause` failure policy** (G3 strict). Needs a *defer-without-retry-increment*
  primitive that does not exist today (§11 O1), a durable blocked-key marker
  (D5), and carries the hot-blocked-key cost (§11 O2). Build after Phase 1.

**Out of scope:** producer idempotency (§12); dynamic `N` / rebalancing (R4);
**trigger-sourced queues** (triggers use `ev_extra1` for the table name — D1, R5);
cascaded/multi-node; automatic hot-partition mitigation.

**ICP:** multi-tenant SaaS on managed Postgres with a high-volume per-entity
event stream (entity = partition key).

## 5. End-to-end workflow

```
producer:  pgque.send('files', 'default', payload, partition_key => tenant_id)
                       │  key → ev_extra1 (send-sourced queues only)
                       ▼
engine:    append-only tables · global ev_id/ev_txid order   (UNCHANGED)
                       │  full stream
                       ▼
consumers: N slot consumers, each an INDEPENDENT subscription with its own cursor;
           slot k reads the whole stream, server-side-filtered to its hash class
```

## 6. Architecture

The mechanism is **N independent slot consumers**, not a modification of
cooperative consumers (round 1 B1: coop hands disjoint tick windows; confirmed
`pgque.sql:6262`). Each slot is its own subscription → own cursor + `sub_id` → no
cross-slot data loss; retry/DLQ rows are slot-scoped (`ev_owner = sub_id`,
`pgque.sql:2374`).

<!-- architecture:begin -->
```
 producers │ send(queue, type, payload, partition_key => K) → ev_extra1
           ▼
 ┌──────────────────────────────────────────────────────────┐
 │ ENGINE · sacred — UNCHANGED                                │
 │ append-only tables · global ev_id/ev_txid · rotation      │
 │ next_batch / get_batch_cursor(i_extra_where) / order by 1 │
 └───────┬───────────────┬───────────────┬──────────────────┘
         ▼ full stream    ▼ full stream    ▼ full stream
   slot 0 (sub#0/N)  slot 1 (sub#1/N)  slot N-1
   own cursor        own cursor        own cursor
   filter h%N=0      filter h%N=1      filter h%N=N-1
```
<!-- architecture:end -->

**Filtering without touching the engine.** Each slot's receive reuses the
admin-only `pgque.get_batch_cursor(…, i_extra_where)` hook (`pgque.sql:2229`,
the 4-arg overload), injecting `and (hashtextextended(ev_extra1,0) % N + N) % N =
k`, assembled only from the validated integers `N`,`k` (§8). `batch_event_sql`,
`next_batch`, rotation are not modified; the filter re-wrap preserves G1.

**SECURITY DEFINER ownership (round 3 — corrected).** `get_batch_cursor`'s
`extra_where` is a trusted-SQL sink, revoked from `public/pgque_reader/
pgque_writer`, admin-only (`pgque.sql:2221`, `:4852`). `receive_partitioned` and
`subscribe_slot` reach it **because they are owned by the same role that owns
`get_batch_cursor` (the install owner) — a function owner may execute its own
functions regardless of grants.** This is *not* the `receive`/`nack` pattern
(those never call `get_batch_cursor`; they call reader-granted internals), and it
does *not* depend on the owner holding `pgque_admin`. **Invariant:** the
partition functions MUST be created by the same role that ran `\i pgque.sql`. On
managed Postgres the installer is a non-superuser admin role; co-ownership (not a
grant) is what makes the wrapper work — state and test this explicitly.

**Read amplification.** Every event is scanned by all `N` slot cursors (filter
applied after the engine materializes the window, `pgque.sql:2277` — reduces
returned rows, not scan work). ≈ N× steady; up to ~2N× during rotation overlap;
a stalled slot scans an ever-widening window. Documented; single-reader/dispatch
optimization is future (R6).

## 7. Decisions

| ID | Decision | Choice (v0.4) | Notes |
|----|----------|---------------|-------|
| D1 | Key location | `ev_extra1`, `send()`-sourced queues only | Triggers use `ev_extra1` for table name. |
| D2 | Failure policy | `skip` default (Phase 1); `pause` is Phase 2 (§11) | `pause` has open mechanics. |
| D3 | N | Fixed, persisted in `pgque.partition_consumer(queue, consumer, n)` (written inside SECURITY DEFINER `subscribe_slot`; table revoked from app roles); changed `n` rejected | Enforced invariant, not convention. |
| D4 | Assignment | `(hashtextextended(key,0) % N + N) % N` | Stable, sign-safe. |
| D5 | State budget | **Phase 1 / happy / `skip`: no state, no per-event writes.** **Phase 2 `pause`:** durable `pgque.partition_block(sub_id, partition_key, head_ev_id)` marker (FK `sub_id → subscription on delete cascade`; index `(sub_id, partition_key)`). Blocked keys additionally incur defer churn (§11 O1) — so "no per-event churn" is a Phase-1/non-blocked-key claim only. | Round 3 corrected the churn framing. |
| D6 | Producer signature | `send(queue, type, payload, partition_key => text)` | Avoids `send(queue,type,payload)` collision. |
| D7 | Slot & single-owner | slot = consumer `"<consumer>#k/N"`; G2 via per-subscription receive lock; functions SECURITY DEFINER co-owned with `get_batch_cursor` (§6) | Reader-callable, owner-reachable. |
| D8 | Worker→slot assignment | Client-side **claim** (§15): each worker takes a slot via non-blocking `pg_try_advisory_lock`; **no leader, no `PartitionAssignor`, no rebalance protocol.** G2's blocking receive lock is the correctness backstop; the advisory lock only distributes | Kafka needs an assignor because partitions are exclusive *by protocol*; here the DB arbitrates. |

## 8. Implementation details

- **Producer:** `send(queue, type, payload, partition_key text default null)` →
  `insert_event(…, ev_extra1 => partition_key, …)`. SECURITY DEFINER, pinned
  search_path; revoke public, grant `pgque_writer`.
- **Tables (created in Phase 1, `if not exists`):** `partition_consumer`
  (N persistence) and `partition_block` (Phase-2 marker, empty in Phase 1 so
  test assertions are well-formed). Both revoked from app roles (the
  `dead_letter` pattern, `dlq.sql:236`); written only inside SECURITY DEFINER
  functions.
- **`subscribe_slot(queue, consumer, k int, n int)`:** validate `n>=1 and
  0<=k<n`; upsert persisted `n`, reject a changed `n` (D3); register
  `"<consumer>#k/n"`. Idempotent for the same `(k,n)`.
- **`receive_partitioned(queue, consumer, k int, n int, …)`:** after casting
  `k,n` to int, `next_batch` + `get_batch_cursor(…, i_extra_where =>
  format('and (hashtextextended(ev_extra1,0) %% %s + %s) %% %s = %s', n,n,n,k))`.
  SECURITY DEFINER (§6); granted `pgque_reader`.
- **`pause` (Phase 2):** on nack of `K#i`, upsert `partition_block(sub_id, K,
  head_ev_id => ev_id)`. A later event of a blocked key (open marker with
  `head_ev_id < ev_id`) is **deferred** (see §11 O1 for the missing primitive),
  not server-side-dropped (dropping + cursor-advance would lose it). Clear the
  marker when `K#i` is acked, **or** when it is dead-lettered — DLQ-unblock
  predicate: a `dead_letter` row exists for `ev_id = K#i` and `dl_consumer_id`
  equal to this slot's `co_id`, where the slot's `co_id` is obtained by joining
  `subscription` (`partition_block.sub_id → subscription.sub_consumer =
  dead_letter.dl_consumer_id`) — `sub_id` and `co_id` are different ID spaces
  (`dlq.sql:24,75-85`, `pgque.sql:170-183`); do not compare them directly.
- **Teardown:** `unsubscribe_slot` removes the slot subscription (the
  `partition_block` FK cascades). Note `unregister_consumer` cascades
  `dead_letter` (`dlq.sql:24`), so dropping a slot drops its DLQ audit —
  documented.
- **Grants:** producer → `pgque_writer`; `subscribe_slot`/`unsubscribe_slot`/
  `receive_partitioned` → `pgque_reader`; `partition_consumer`/`partition_block`
  revoked from all app roles; `get_batch_cursor` stays admin-only. Deny-by-default
  re-applied.

## 9. Tests plan (red/green TDD), CI PG 14–18

**Phase 1 (must pass to ship):**
- **T-G1a:** literal `(hashtextextended(K,0)%N+N)%N` on every CI version, pinning
  one concrete `(K, expected)` pair. *(red first)*
- **T-G1b:** interleave A,B,A,A,B → each key in `ev_id` order across batches, no
  key on two slots. (No existing test guards intra-batch `ev_id` order.)
- **T-retry-affinity:** nack a keyed event; `maint_retry_events()` +
  `force_next_tick` + `ticker()`; assert redelivery to the **same** slot only.
- **T-G2-block / T-G2-parallel:** same slot → second worker blocks (mirror
  `two_session_receive_lock.sh`); different slots → neither blocks.
- **T-no-drop:** keys across all slots in one window; all N slots; union = all
  events, zero loss.
- **T-security:** run against an install whose owner is a **non-superuser,
  non-`pgque_admin` role** — a bare `pgque_reader` can call
  `receive_partitioned`/`subscribe_slot` end-to-end, and **cannot** call
  `get_batch_cursor` directly (`42501`, mirror `test_security_get_batch_cursor.sql`);
  non-integer/out-of-range `n`,`k` rejected.
- **T-N-invariant:** `subscribe_slot(…,k,n)` idempotent; `(…,k,n2≠n)` raises.
- **T-claim (assignment):** two sessions take per-slot `pg_try_advisory_lock` over
  `N=2`; assert they land on **distinct** slots (disjoint locks), a third session
  gets neither, and releasing one frees exactly that slot for the third. Liveness
  only — correctness is already covered by T-G2-block.
- **T-no-bloat (happy path):** all-ack of M events → zero `retry_queue`/
  `dead_letter`/`partition_block` rows (guard the `partition_block` clause with
  `to_regclass('pgque.partition_block') is not null`) and no per-event
  UPDATE/DELETE.
- **T-engine-untouched:** `pg_get_functiondef` of `batch_event_sql`,
  `next_batch_custom`, and `get_batch_cursor/4` (pin the 4-arg overload)
  byte-identical to baseline.
- **T-idempotent-install:** re-running `pgque.sql` re-creates functions + the two
  new tables (`create table/unique index if not exists`) cleanly.

**Phase 2 (`pause`) — write when O1/O2 (§11) resolve:**
- **T-G3-pause:** A#2 nacked; drive maint+tick; A#3 withheld until A#2
  acked-or-DLQ'd; **and after unblock A#2 then A#3 deliver in `ev_id` order,
  exactly once**; B unaffected.
- **T-DLQ-unblock:** A#2 exhausts retries → `dead_letter`; assert the
  `partition_block` row for A drops to 0 **via the DLQ branch (no ack ever
  occurred)** and A#3 then proceeds.
- **T-slot-crash:** worker holding A#2 dies; assert the `partition_block` row is
  present after the crash and before the new worker's first receive (durability);
  drive maint+tick; A#2 redelivered before A#3, only to slot k. Crash in the
  post-`maint` window (retry_queue row already gone).
- **T-hot-blocked-key:** hot `K` blocked under `pause` → other slots unaffected;
  defer cost is bounded by `K`'s backlog until DLQ, not total throughput.

## 10. Risks

- **R2 — read amplification:** N× steady, ~2N× rotation overlap, widening for a
  stalled slot. Benchmark the stalled case.
- **R3 — hot partitions:** documented only.
- **R4 — changing N / over-provisioning:** N is an enforced invariant (D3) **and**
  the read-amplification multiplier (§6 — every slot scans the full stream), so
  inflating N to dodge a future resize is **not free**: it linearly raises steady
  read cost. Online resize is genuinely hard — `(hash % N)` reshuffles keys across
  slots, so a key mid-flight in one slot would jump to another, breaking G1/G2
  during the transition. A correct resize needs either drain-and-cutover (a
  consumption pause) or a power-of-two split with per-slot draining; both are
  future. Phase 1 guidance: choose N to match the parallelism you need, bounded by
  the read-amp budget.
- **R5 — `ev_extra1`:** send-sourced queues only.
- **R6 — single-reader/dispatch** to remove read amplification; future.
- **R7 — rotation pinning (first-class risk, not a footnote):** rotation waits
  for `min(sub_last_tick)` over ALL subscriptions (`pgque.sql:910`), so N slots
  lower the rotation floor to the **slowest** slot. One stalled or lagging slot
  pins rotation for the whole queue, old event tables stop being dropped, and the
  log grows — i.e. **the bloat pgque exists to avoid comes back with nicer
  names.** This is the sharpest operational hazard of the N-slot model and gets
  worse as N grows (more slots → more chances one is behind). Mitigation is
  monitoring + bounding N, not code: a per-slot staleness/lag alert is mandatory
  for any Tier-B deployment, and N should be the minimum that meets the
  parallelism target (see R2/R4 — N is also the read-amp multiplier, so the same
  "keep N small" pressure applies from two directions). A `pause`-blocked slot
  does *not* pin rotation (deferred events go to retry, not the held cursor),
  **but** a hot blocked key keeps that slot perpetually lagging, so the alert
  cannot by itself distinguish "wedged" from "hot key under pause".

## 11. Open design items — Phase 2 `pause` (why it's a follow-up)

- **O1 — defer-without-retry-increment primitive (blocking for `pause`).**
  `finish_batch` acks the whole batch, so withholding `K#i+1` requires removing it
  from the batch. A **server-side filter that lets the cursor advance would lose
  it** (round-2 data-loss). `event_retry` preserves it but **increments
  `ev_retry`**, so a long-blocked key's deferred events would falsely march toward
  `max_retries`/DLQ. `pause` therefore needs a new "re-queue without counting as a
  retry" path (or a hold-cursor design that doesn't wedge rotation). Undecided.
- **O2 — hot-blocked-key cost.** Until O1 is settled, a hot blocked key makes its
  slot re-defer a growing backlog each poll (per-event churn for that key,
  bounded by the head's time-to-DLQ). Acceptable for rare failures (the migration
  ICP); needs documentation + T-hot-blocked-key before `pause` ships.

## 12. Relationship to producer idempotency (separate feature)

Producer dedup is a TTL window (SQS/NATS), append-only, GC'd by rotation — a
**separate, orthogonal send-layer feature**, not a tier of this spec. Feature
spec: `blueprints/idempotency/SPEC.md`; rationale: `…/DESIGN.md`. It ships as
**Phase 1B** (independent of partition-keys 1A).

**The migration use case is not a partition-keys use case.** It is a plain-queue
recipe = producer TTL dedup (1B) + consumer mutual exclusion (per-key advisory
lock + idempotent handler). It needs no slots, no fixed N, and no ordering — the
"partition key" there is a *lock key*, not a partition. Keeping migrations out of
this feature is deliberate: it stops the partition-keys design from drifting
toward a mutable job queue (pg-boss with extra steps).

## 13. Review panel

- **Lead:** drafts/revises.
- **Reviewer A — ops/security** · **Reviewer B — QA/testability.** Rounds 1–3
  applied. Round 3 verdict: Phase 1 converged; Phase 2 (`pause`) has open items
  O1/O2 → split out as follow-up.

## 14. Sprint plan

1. **S1 — producer + key plumbing** (+ the two tables, empty). T-G1a,
   T-no-bloat(happy), T-idempotent-install.
2. **S2 — slot consumers (`skip`), SECURITY DEFINER + co-ownership, persisted N.**
   T-G1b, T-retry-affinity, T-G2-block/parallel, T-no-drop, T-security,
   T-N-invariant, T-engine-untouched. **← Phase 1 ships here.**
3. **S3 — `pause` (Phase 2), gated on O1/O2.** T-G3-pause, T-DLQ-unblock,
   T-slot-crash, T-hot-blocked-key.
4. **S4 — docs + benchmark** (read-amp: steady/rotation/stalled; per-tenant order).

## 15. Worker → slot assignment

Slots are **claimed, not assigned.** There is no consumer-group leader, no
`PartitionAssignor`, and no stop-the-world rebalance protocol — Kafka needs all
three because a partition is exclusive *by protocol* (the broker hands it to one
member, with no per-message arbitration). Here the **database is the
coordinator**, so assignment is pull-based and self-balancing.

**Claim loop (client-side).** Each worker iterates candidate slots `0..N-1` and
attempts a non-blocking `pg_try_advisory_lock(slot_key(queue, consumer, k))`. The
first slot it locks, it owns: it calls `receive_partitioned(queue, consumer, k, N,
…)` for that slot and keeps it **sticky-until-idle** (re-poll the same slot while
it has work; release the advisory lock on drain or shutdown). On a failed try-lock
the worker moves to the next slot — it never blocks waiting on a busy slot.

- **Symmetric (N workers, N slots).** Each worker locks one distinct slot; the
  try-lock makes ownership disjoint with zero coordination. Sequential per-key
  consumption follows from G1 (routing) + the single owner.
- **Fan-in (N slots, M<N workers).** Each worker cycles its claim loop and holds
  ≈⌈N/M⌉ slots.
- **Scale-up (M → M′ workers).** New workers run the same loop and lock whatever
  slots are unclaimed or get released at a batch boundary. No revocation, no
  leader recompute — convergence to ≈N/M′ slots per worker within one claim cycle.
- **Scale-down / crash.** The dead session's advisory lock is released by Postgres,
  so its slots are immediately reclaimable — there is no `session.timeout.ms`
  equivalent and no rebalance to trigger.

**Two locks, different jobs.** The design separates correctness from distribution
so a buggy client can never corrupt ordering:

- **G2 receive lock** (`for update of s` on the subscription, *blocking*, §2) is
  the **correctness** backstop: even if two workers both target slot `k`, exactly
  one gets the batch; the other blocks. Ordering can never break.
- **Advisory slot lock** (non-blocking `try`) is the **distribution** layer: it
  steers workers onto distinct slots so they don't pile up blocked on the same
  one. It is pure liveness — a client that skips it cannot violate G1/G2, it only
  loses even spread.

**What claiming does not give you.** Even spread under *key skew*: a hot slot
stays hot no matter who owns it (R3). First-free claiming can be made fairer with
**lag-aware claiming** (prefer the free slot with the oldest unconsumed `ev_id`) —
a future refinement, not Phase 1.

**N bounds the fan-out.** Assignment distributes a *fixed* N (D3) over a variable
worker count: useful parallelism is capped at N (workers > N sit idle), and
because N is also the read-amplification multiplier (§6, R4), N is chosen to match
the parallelism you need — not inflated "to be safe." Resizing N is the
rebalancing problem (R4), deferred.

## 16. Changelog

- **v0.6 (draft):** review (Max / consumer Q&A). **Scoped this feature to ordered
  per-key streams only** — producer idempotency split into its own send-layer
  feature (`blueprints/idempotency/SPEC.md`, Phase 1B) and the migration use case
  reframed as a plain-queue recipe (dedup + mutual exclusion), explicitly *not* a
  partition-keys use case (§12). Promoted **rotation pinning** to a first-class
  risk (R7) — N slots lower the rotation floor to the slowest slot, the bloat
  pgque exists to avoid; per-slot lag alert mandatory, bound N. Roadmap reframed
  to 1A (slots) / 1B (producer dedup) / 2 (read-amp) / 3 (pause).
- **v0.5 (draft):** added the **worker→slot assignment** model (§15, D8):
  client-side **claim** via a non-blocking per-slot `pg_try_advisory_lock` — no
  leader, no `PartitionAssignor`, no rebalance protocol; G2's blocking receive
  lock is the correctness backstop, the advisory lock only distributes; crash
  recovery is just session-death lock release (no `session.timeout.ms`).
  Clarified that fixed N (D3) is also the read-amp multiplier, so over-provisioning
  N to avoid a resize is **not free** and online resize breaks G1 mid-flight (R4
  expanded). Added T-claim (assignment liveness).
- **v0.4 (draft):** review round 3. **Phase 1 declared converged /
  implementation-ready; `pause` split into Phase 2** with explicit open items
  (§11 O1 defer-without-retry-increment, O2 hot-blocked-key). Corrected the
  SECURITY DEFINER justification to the **co-ownership** invariant (not
  `pgque_admin`; not "like receive/nack") + non-superuser-owner security test
  (round 3 B1). Fixed the DLQ-unblock `sub_id`↔`co_id` join; added
  `partition_block` FK-cascade + index + revoked-from-roles; tables created empty
  in Phase 1; `T-no-bloat` guarded with `to_regclass`; `T-engine-untouched` pins
  the `/4` overload; `T-G3-pause` now asserts in-order-exactly-once after unblock;
  `T-DLQ-unblock` asserts marker-clear-via-DLQ; `T-slot-crash` asserts marker
  durability; added `T-hot-blocked-key`. Round-3 detail in `decisions.md`.
- **v0.3:** round 2 — confirmed G1 ordering + G2 lock real; SECURITY DEFINER
  wiring; durable `partition_block`; modulo sign fix; R7.
- **v0.2:** round 1 — N independent slot subscriptions; G1/G2/G3; `skip` default.
- **v0.1:** initial SamoSpec-format draft.
