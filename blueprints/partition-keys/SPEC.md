# PgQue Partition Keys — Spec

- **Version:** v0.8 (draft)
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
  `hashtextextended` returns `bigint`). A **null** partition key (e.g. from the
  keyless `send()` overloads) routes to slot 0, so no event is dropped by the hash
  filter. Within that slot, non-retried events of
  `K` are delivered in non-decreasing `ev_id` order, to **no other slot**.
  Intra-batch order is the engine's `order by 1` (`pgque.sql:440`), preserved
  through `get_batch_cursor`'s filter re-wrap (`pgque.sql:2277`); cross-batch
  order follows from one subscription's monotonically-advancing cursor.
- **G2 — single in-flight processor per key.** At most one worker at a time is
  *issued* events of `K`, and in steady state at most one *processes* them. Two
  mechanisms compose: (1) the per-subscription receive lock (`next_batch_custom …
  for update of s`, `pgque.sql:5761`, the #97/#125 guard) serializes batch
  **issuance** — one active batch per slot; concurrent `receive()` returns that
  same batch idempotently, never a second independent batch; (2) the
  **batch-granularity lease** (§15) on the slot — a `pgque.partition_slot` row
  carrying `(lease_owner, lease_until, epoch)` — held *logically* across the
  receive→process→ack loop and **renewed on every `receive_partitioned` /
  `ack_partitioned` call**, so the heartbeat rides the writes that already happen
  per batch (zero extra round-trips). The lease is **server-enforced on the
  pgque-api surface**: `receive_partitioned` and `ack_partitioned` refuse to act
  unless the caller's worker id holds the lease, and the plain `receive`/`ack`/
  `nack` wrappers reject slot consumers (`#`-names) outright — so a worker using
  the API cannot skip the claim and put two workers on the same open batch.
  Enforcement scope, stated honestly: the raw PgQ primitives (`next_batch`,
  `finish_batch`, …) stay reader-granted and unguarded — PgQ's inherited
  cooperative trust model, under which any reader could always drive any consumer
  by name; the lease narrows the *accidental*-misuse surface, not that inherited
  trust boundary. So the lease is **load-bearing for G2**, not
  distribution polish. If a leaseholder's worker dies mid-batch, the lease expires
  after its TTL; the successor takes over (epoch bump) and is re-issued the same
  still-open batch — at-least-once, with possible transient overlap with a zombie
  for at most the remaining TTL; the zombie's next `receive`/`ack` raises on the
  epoch/owner mismatch instead of silently double-acking (fenced). Handlers must
  tolerate redelivery.
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
- Persisted/enforced `N` (D3); slot identity + single-owner via the
  batch-granularity lease (D7); SECURITY DEFINER ownership model (§6).
- **Client-side claim assignment** (§15): workers self-distribute over the fixed
  `N` slots via a per-slot **lease** (`claim_slot`/`release_slot`, arbitrated by
  core SQL rows) — no leader, no assignor.

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
the 4-arg overload), injecting `case when ev_extra1 is null then 0 else
(pg_catalog.hashtextextended(ev_extra1, 0) % N + N) % N end = k` (a null key routes
to slot 0 — G1, so no event is dropped), assembled only from the validated integers
`N`,`k` (§8). `batch_event_sql`,
`next_batch`, rotation are not modified; the filter re-wrap preserves G1.

**SECURITY DEFINER ownership (round 3 — corrected).** `get_batch_cursor`'s
`extra_where` is a trusted-SQL sink, revoked from `public/pgque_reader/
pgque_writer`, admin-only (`pgque.sql:2221`, `:4852`). `receive_partitioned` and
`subscribe_partitioned`/`subscribe_slot` reach it **because they are owned by the same role that owns
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
| D3 | N | Fixed, persisted in `pgque.partition_consumer(queue, consumer, n)` (written inside SECURITY DEFINER `subscribe_partitioned`, with `subscribe_slot` retained for forward-only repair — the missed window is lost; table revoked from app roles); changed `n` rejected | Enforced invariant, not convention. |
| D4 | Assignment | `(hashtextextended(key,0) % N + N) % N` | Stable, sign-safe. |
| D5 | State budget | **Phase 1 / happy / `skip`: no state, no per-event writes.** **Phase 2 `pause`:** durable `pgque.partition_block(sub_id, partition_key, head_ev_id)` marker (FK `sub_id → subscription on delete cascade`; index `(sub_id, partition_key)`). Blocked keys additionally incur defer churn (§11 O1) — so "no per-event churn" is a Phase-1/non-blocked-key claim only. | Round 3 corrected the churn framing. |
| D6 | Producer signature | `send(queue, type, payload, partition_key => text)` | Avoids `send(queue,type,payload)` collision. |
| D7 | Slot, single-owner, key namespace | slot = consumer `"<consumer>#k/N"`; **G2 = receive lock (batch issuance) + batch-granularity lease held logically across process→ack (§15)**; **slot identity + lease helpers `claim_slot`/`release_slot` are core**, arbitrated by the `pgque.partition_slot` table (all clients agree via shared rows, not a shared lock namespace); `subscribe_partitioned` creates the whole slot set atomically; `partition_slot_status` exposes subscription completeness plus owner+lag. **`slot_lock_key` removed** (v0.8 — no advisory-lock namespace). | Reader-callable; clients cannot diverge because arbitration is a server-side row. |
| D8 | Worker→slot assignment | Client-side **claim loop** (§15) is still client *policy* (which slot to grab, poll cadence), but the arbitration is now **server-side rows**: `claim_slot` returns an epoch or NULL by inspecting/updating `pgque.partition_slot`; **no leader, no `PartitionAssignor`, no rebalance protocol.** Boundary follows the **mechanism/policy seam**: corruption-capable transitions → core SQL, guarded policy loops → client. | Kafka needs an assignor because partitions are exclusive *by protocol*; here the DB arbitrates per batch and per lease. |
| D9 | Online resize (grow N) | Epoch-gated **drain-then-cutover** state machine in **core SQL** (`begin_resize`/`resize_ready`/`complete_resize`/`abort_resize`); client drives the drain loop, core re-validates on cutover. Immutable N in Phase 1 (§15 → *Online resize*). | Grow-N reshuffles `hash%N` and breaks G1/G2 for in-flight keys (Fabrizio); the log-native analog of Kinesis parent-shard drain. |
| D10 | No per-interval heartbeat `UPDATE` churn | **Upheld for that specific cost, superseded as an argument against a lease table.** The v0.7 rejection targeted a *per-interval* heartbeat `UPDATE` — that churn is real and still rejected. **Batch-boundary lease renewal (D11) is new information:** the lease is renewed on the `receive`/`ack` writes that already happen per batch, so there is no per-interval churn to add. Observability still via the read-only `pgque.partition_slot_status` view (now reading lease columns, not `pg_locks`). | A *polling* heartbeat buys nothing; a lease renewed at batch boundaries costs no extra round-trips. |
| D11 | Slot ownership = batch-granularity lease | **Session-scoped advisory locks rejected; lease adopted.** Session advisory locks are incompatible with transaction-mode poolers (PgBouncer/Supavisor — the transaction-pooler ICP): the lock leaks onto the pooled backend and forces one session-mode connection per live worker, exhausting connections at high worker counts. The lease is plain short-transaction DML over `pgque.partition_slot` → works on one transaction-mode pool, no session state. Trade: crash-takeover latency = lease TTL remainder (a `session.timeout.ms`-equivalent knob returns), `clock_timestamp()`-based; worker ids must be unique per live worker. Fencing is automatic — takeover bumps `epoch`, a zombie's next `receive`/`ack` raises. | Fabrizio review: pooler incompatibility + connection floor. Worker→slot redistribution as workers come and go must be kept (static pinning alone was insufficient), so the lease must arbitrate live claims, not just pin — distinct from the out-of-scope dynamic-N rebalancing (§4). |

## 8. Implementation details

- **Producer:** `send(queue, type, payload, partition_key text default null)` →
  `insert_event(…, ev_extra1 => partition_key, …)`. SECURITY DEFINER, pinned
  search_path; revoke public, grant `pgque_writer`.
- **Tables (created in Phase 1, `if not exists`):** `partition_consumer`
  (N persistence) and `partition_slot` (lease rows, below). `partition_block` is
  **not** created in Phase 1 — it is created only when Phase 2 (`pause`) ships;
  Phase-1 test assertions that reference it therefore guard on
  `to_regclass('pgque.partition_block') is not null`. All revoked from app roles
  (the `dead_letter` pattern, `dlq.sql:236`); written only inside SECURITY DEFINER
  functions.
- **`partition_slot(queue_id, co_name, slot, lease_owner text, lease_until
  timestamptz, lease_ttl interval, epoch bigint)`:** one row per registered slot,
  created atomically by `subscribe_partitioned` or individually by the repair
  API `subscribe_slot`. Primary key `(queue_id, co_name, slot)`; FK
  `(queue_id, co_name) → partition_consumer on delete cascade` (a dropped consumer
  drops its lease rows). `lease_owner` null when unleased; `epoch` starts at some
  base and increments on every takeover of a free/expired lease (the fencing
  token). Revoked from app roles.
- **`subscribe_partitioned(queue, consumer, n int)`:** the default setup path.
  Validate `1<=n<=256`, persist N, read one starting tick, then register all generated
  consumers `"<consumer>#0/N"` through `"<consumer>#(N-1)/N"` at that shared tick
  and create all lease rows in one transaction. A failure rolls back the whole
  consumer. Repeating a complete setup with the same N is idempotent and does
  not reposition cursors. A pre-existing partial setup raises rather than
  implying that late-created slots can recover history that they already missed.
- **`subscribe_slot(queue, consumer, k int, n int)`:** alpha-compatible repair
  API. Validate `1<=n<=256 and 0<=k<n`; upsert persisted `n`, reject a changed `n`
  (D3); register
  `"<consumer>#k/n"`; create the `partition_slot` row for `k` (unleased).
  Idempotent for the same `(k,n)`. Repair ≠ recovery: it closes the gap going
  forward only — the repaired slot starts at the current tick, and events
  ticked while the slot was missing are **permanently lost** (recreate the
  consumer via `unsubscribe_partitioned` before producing to avoid the gap).
- **`claim_slot(queue, consumer, k int, worker text, ttl interval default
  '30 seconds')` → bigint (epoch):** validate `ttl` is finite and `>= '1 second'`
  (`'infinity'` rejected). Under a `for update skip locked` row lock on the
  `partition_slot` row: if the lease is live and owned by another worker → return
  NULL (steered away); if the row is momentarily locked by another transaction —
  including the SAME worker's own in-flight `receive`/`ack` lease renewal —
  → return NULL without blocking; if owned by `worker` → renew
  (`lease_until = clock_timestamp() + ttl`), return the **same** epoch; if free or
  **expired** (`lease_until <= clock_timestamp()` or `lease_owner is null`) → take
  over: set `lease_owner = worker`, `lease_until = clock_timestamp() + ttl`, **bump
  `epoch`**, return the new epoch. NULL therefore means "not claimable right
  now" (busy or foreign-owned); do NOT infer loss of an existing lease from
  NULL. Grace rule: an expired lease that no successor
  has taken may be renewed by its own worker (still its epoch — no heir existed, so
  it is safe).
- **`release_slot(queue, consumer, k int, worker text)` → boolean:** owner-only.
  If `worker` holds the lease and the slot has no open engine batch, clear it
  (`lease_owner = null`) and return true; raises if the owner's slot batch is
  still open — ack first (§15); a non-owner returns false, never raises.
- **`receive_partitioned(queue, consumer, k int, n int, worker text, …)`:** after
  casting `k,n` to int, **require `worker` to hold the lease on slot `k`** (a
  non-owner raises — server-enforced G2); an expired lease still owned by the same
  worker (no successor took over) is renewed under the grace rule; **renew the
  lease** (batch-boundary heartbeat); then `next_batch` + `get_batch_cursor(…,
  i_extra_where => format('(case when ev_extra1 is null then 0 else
  (pg_catalog.hashtextextended(ev_extra1, 0) %% %s + %s) %% %s end) = %s',
  n,n,n,k))`.
  SECURITY DEFINER (§6); granted `pgque_reader`. **`claim_slot` returns the current
  lease `epoch`** so a handler can stamp it into side effects as a user-space
  **fencing token** (a zombie worker on an old epoch is then detectable downstream) —
  this is the consumer for the `epoch` column (D11, §15).
- **`ack_partitioned(queue, consumer, k int, n int, worker text)`:** same lease
  precondition and renewal as `receive_partitioned` — a non-owner (e.g. a zombie
  after takeover) raises instead of acking, which is the fencing behavior.
- **`nack_partitioned(queue, consumer, k int, n int, worker text, msg
  pgque.message, retry_after interval, reason text)`:** the lease-fenced
  per-message retry/DLQ path for slots (plain `nack` rejects slot batches, which
  otherwise would have no retry path). Validates N, fences+renews the lease, then
  routes through the shared retry/DLQ core (`_nack_batch_event` — the #98
  canonical-event re-query and #104 idempotent-DLQ behaviors are preserved).
  A retried keyed event keeps `ev_extra1`, so redelivery stays on the same slot.
- **Raw-slot guards:** plain `receive`/`ack`/`nack` reject partition slot
  consumers — `receive` rejects `#`-carrying consumer names (the partitioned
  setup APIs reserve `#`), `ack`/`nack` resolve the batch's consumer and reject `#`-names —
  otherwise the plain path would hand back the whole unfiltered stream with no
  lease fence, and a fenced zombie could double-ack via `ack(batch_id)` (G2, §2).
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
- **Teardown:** `unsubscribe_partitioned(queue, consumer)` is the whole-consumer
  inverse of `subscribe_partitioned`: one transaction drops every slot
  subscription and the pinned-N row (lease rows cascade via FK). It succeeds on
  partial setups — it is the recreate path the incomplete-setup error names —
  and an absent consumer is a notice-level no-op. It drops only
  catalog-registered slots (`partition_slot` rows): a legacy ordinary consumer
  that merely shares the name shape survives, with its cursor history, and is
  removed with plain `unsubscribe`. `unsubscribe_slot` removes one
  slot (the `partition_slot` and `partition_block` FKs cascade); on a complete
  consumer it emits a WARNING, because it creates exactly the incomplete-setup
  state `subscribe_partitioned` rejects; its completeness/last-slot accounting
  is likewise catalog-driven, so a legacy name-shaped subscription never keeps
  the pinned-N row alive. Note `unregister_consumer` cascades
  `dead_letter` (`dlq.sql:24`), so dropping a slot drops its DLQ audit —
  documented.
- **Grants:** producer → `pgque_writer`; `subscribe_partitioned`/`subscribe_slot`/`unsubscribe_slot`/`unsubscribe_partitioned`/
  `receive_partitioned`/`ack_partitioned`/`nack_partitioned`/`claim_slot`/
  `release_slot` and `select` on `partition_slot_status` → `pgque_reader`;
  `partition_consumer`/`partition_slot`/`partition_block` revoked from all app
  roles; `get_batch_cursor` stays admin-only. Deny-by-default re-applied.

## 9. Tests plan (red/green TDD), CI PG 14–18

**Phase 1 (must pass to ship):**
- **T-G1a:** literal `(hashtextextended(K,0)%N+N)%N` on every CI version, pinning
  one concrete `(K, expected)` pair. *(red first)*
- **T-G1b:** interleave A,B,A,A,B → each key in `ev_id` order across batches, no
  key on two slots. (No existing test guards intra-batch `ev_id` order.)
- **T-retry-affinity:** nack a keyed event; `maint_retry_events()` +
  `force_next_tick` + `ticker()`; assert redelivery to the **same** slot only.
  Implemented in `tests/test_partition_keys.sql`.
- **T-G2-block / T-G2-parallel:** same slot → second worker blocks (mirror
  `two_session_receive_lock.sh`); different slots → neither blocks.
- **T-no-drop:** keys across all slots in one window; all N slots; union = all
  events, zero loss.
- **T-security:** run against an install whose owner is a **non-superuser,
  non-`pgque_admin` role** — a bare `pgque_reader` can call
  `receive_partitioned`/`subscribe_partitioned` end-to-end, and **cannot** call
  `get_batch_cursor` directly (`42501`, mirror `test_security_get_batch_cursor.sql`);
  non-integer/out-of-range `n`,`k` rejected.
- **T-setup-atomicity:** `subscribe_partitioned(…,n)` creates all N engine
  subscriptions at one tick in one transaction; events produced afterward are
  reachable across the union of all slots. Repeating setup does not reposition
  cursors. Existing partial setup raises and remains explicitly repairable.
- **T-N-invariant:** `subscribe_partitioned(…,n)` idempotent; a changed N raises.
  `subscribe_slot(…,k,n)` retains the same pinned-N invariant for repair.
- **T-lease (claim/renew/steer/release):** over `N=2`, worker `wk-a` claims slot 0
  (epoch returned); a second worker `wk-b` claiming slot 0 gets NULL (steered
  away); the owner re-claiming slot 0 renews with the **same** epoch; `wk-b` claims
  the free slot 1; `release_slot` by a non-owner returns false, by the owner
  returns true and frees the slot; a short-TTL lease, once expired, is taken over
  by another worker with an **epoch bump** (`clock_timestamp()`-based crash
  recovery). Correctness of single-in-flight is already covered by T-G2-block.
- **T-fencing:** a zombie opens a batch under a short lease and stalls past the
  TTL; the heir takes over (epoch bump) and is re-issued the **same** still-open
  batch idempotently (engine receive lock); the zombie's `ack_partitioned` then
  **raises** (owner/epoch mismatch) instead of silently double-acking; the heir
  acks normally. Also asserts `receive_partitioned`/`ack_partitioned` by a worker
  holding no lease raise (server-enforced G2). Implemented in
  `tests/test_partition_keys.sql`.
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
  read cost. Online resize breaks order for in-flight keys — `(hash % N)` reshuffles
  keys across slots (Fabrizio's point) — so you must **drain the old epoch first**;
  the sanctioned protocol is the epoch-gated drain-then-cutover in §15 (*Online
  resize*, D9), not a hand-wave. Phase 1 guidance: **immutable N**, chosen to match
  the parallelism you need, bounded by the read-amp budget.
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
  for any Tier-B deployment — canonical form `pending_events > X or not
  subscribed` on `partition_slot_status`, because a threshold-only alert skips
  the NULL-lag rows of unsubscribed slots and misses incomplete setup — and N
  should be the minimum that meets the
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
recipe = producer TTL dedup (1B) + consumer mutual exclusion (per-key
`pg_try_advisory_xact_lock` + idempotent handler — *xact*-scoped, so pooler-safe,
unlike the session locks D11 rejects). It needs no slots, no fixed N, and no
ordering — the
"partition key" there is a *lock key*, not a partition. Keeping migrations out of
this feature is deliberate: it stops the partition-keys design from drifting
toward a mutable job queue (pg-boss with extra steps).

**Why two plain workers on one consumer is unsafe (the caller's responsibility).**
The receive lock (`for update of s`) is released at the `next_batch` transaction
commit — **not** held across the process→ack gap. So the engine *does* coordinate
(a second `receive()` blocks, then idempotently returns the *same* active batch —
`two_session_receive_lock.sh`; there is no distinct-batch double-delivery), but if
worker A opens a batch in autocommit and goes off to process, worker B's
`receive()` can pick up that same still-open batch and reprocess the same events.
That is redundant reprocessing of **one** batch under PgQ's one-active-batch-per-
`(queue,consumer)` model — fixed not by an external coordinator but by giving each
parallel worker its own cursor (cooperative subconsumers, or a partition slot), or
the per-key advisory-lock recipe above.

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
calls `pgque.claim_slot(queue, consumer, k, worker, ttl)` — a **core** function
(D7, D11) that arbitrates over the shared `partition_slot` rows, so Go/Python/TS/CLI
cannot silently collide on a slot. A non-NULL return (the lease epoch) means the
worker owns the slot; NULL means the slot is not claimable right now — another
worker holds a live lease, or the row is momentarily locked by another
transaction (possibly this worker's own in-flight `receive`/`ack` renewal) —
and the worker moves to the next slot; it never blocks waiting on a busy slot,
and it must not infer loss of an existing lease from NULL. The first slot it
claims, it owns: it calls `receive_partitioned(queue, consumer, k, N, worker, …)`
for that slot and keeps it **sticky-until-idle** (re-poll the same slot while it
has work; each `receive`/`ack` renews the lease, so a working slot never expires).
It calls `release_slot` **only at a batch boundary** — after ack/finish — on drain
or shutdown, since releasing mid-batch hands the successor the same still-open
batch (§12); on shutdown, finish or explicitly abandon the open batch first. A
worker that simply dies never calls `release_slot`; the lease expires after its
TTL and the slot becomes claimable (see *Scale-down / crash* below).

- **Symmetric (N workers, N slots).** Each worker claims one distinct slot; the
  lease makes ownership disjoint with zero coordination. Sequential per-key
  consumption follows from G1 (routing) + the single owner.
- **Fan-in (N slots, M<N workers).** Each worker cycles its claim loop and holds
  ≈⌈N/M⌉ slots, renewing each held lease as it polls.
- **Scale-up (M → M′ workers).** New workers run the same loop and claim whatever
  slots are unleased or get released at a batch boundary. No revocation, no
  leader recompute — convergence to ≈N/M′ slots per worker within one claim cycle.
- **Scale-down / crash.** A dead worker never releases; its leases **expire after
  the TTL** (`clock_timestamp()`-based) and the slots then become claimable, with
  an `epoch` bump fencing the zombie. Unlike session-death advisory-lock release,
  takeover is not instant — the recovery latency is the remaining TTL, the
  `session.timeout.ms`-equivalent knob (D11). Still no rebalance to trigger.

**Two mechanisms, different jobs.**

- **G2 receive lock** (`for update of s` on the subscription, *blocking*, §2)
  protects the **log and cursor**: one active batch per slot; concurrent
  `receive()` returns the *same* batch idempotently (§12); the cursor advances
  only on ack. No client behavior can obtain two divergent batches, lose events,
  or reorder *delivery*.
- **Batch-granularity slot lease** (a `partition_slot` row, held logically across
  receive→process→ack, renewed on every `receive`/`ack`) protects **processing
  exclusivity** — it closes the process→ack gap the receive lock does not span, and
  is therefore *load-bearing for G2's "single in-flight processor," not pure
  liveness.* Unlike the v0.7 advisory claim, the lease is **server-enforced on
  the pgque-api surface**: `receive_partitioned`/`ack_partitioned` refuse to act
  for a worker that does not hold the lease, and plain `receive`/`ack`/`nack`
  reject slot consumers, so a worker using the API cannot skip it to put two
  workers on the same open batch — the second worker's calls raise. (The raw PgQ
  primitives remain reader-granted and unguarded — the inherited PgQ trust
  boundary; see G2, §2.) A zombie that stalls past its
  TTL is taken over (epoch bump) and fenced: its next `receive`/`ack` raises rather
  than double-acking. The residual overlap window is the TTL remainder, not
  unbounded. Even spread is the same lease's second job.

**What claiming does not give you.** Even spread under *key skew*: a hot slot
stays hot no matter who owns it (R3). First-free claiming can be made fairer with
**lag-aware claiming** (prefer the free slot with the oldest unconsumed `ev_id`) —
a future refinement, not Phase 1.

**N bounds the fan-out.** Assignment distributes a *fixed* N (D3) over a variable
worker count: useful parallelism is capped at N (workers > N sit idle), and
because N is also the read-amplification multiplier (§6, R4), N is chosen to match
the parallelism you need — not inflated "to be safe." Growing N online is the
*Online resize* protocol below.

**Every slot must be polled (hard requirement, not fairness polish).** With M<N
workers, sticky first-free claiming can leave a slot **unclaimed and unpolled**;
its cursor stalls, and by R7 a stalled slot pins rotation for the *whole queue* →
the append-only log grows unbounded (clean, but growth). So a Tier-B deployment
MUST guarantee every slot is polled within a bounded interval — either M ≥ N, or a
claim policy that cycles idle slots, or lag-aware claiming (below). This is a
correctness-adjacent operational invariant, monitored via `partition_slot_status`
+ the mandatory R7 lag alert.

### Where coordination lives — mechanism vs policy (Fabrizio review)

Cut the core/client boundary along the **mechanism/policy seam**, not the feature.
Everything a *wrong client could use to corrupt shared state* lives in **core SQL**,
written once and guarded; everything *idempotent-safe* (which slot to grab, poll
cadence, when to resize) lives in the **client** or **user space**, where a bug
can degrade one worker's spread, stall, or cause duplicate/interleaved processing
on the *one slot it targets* — but can never corrupt the log, the cursor, or
another slot (which is exactly why the *claim helpers* are core, D7). Same
two-lock discipline as above: the log/cursor ride a blocking lock the DB always
enforces; per-slot processing exclusivity + distribution ride the lease.

- **Core SQL (mechanism):** the receive lock (G2), the hash-routing filter, the
  enforced N (D3), the shared `claim_slot`/`release_slot` lease arbitration and its
  server-enforced check inside `receive_partitioned`/`ack_partitioned`, the
  read-only `partition_slot_status` view, and the resize state machine (below).
- **Client library (policy):** the claim loop, the receive→ack loop, the resize
  drain-driver. A buggy loop loses spread or stalls; it cannot corrupt order.
- **User space (policy):** choosing N, static-pin-vs-claim, the plain-queue
  mutual-exclusion recipe (§12), when/whether to resize, and the R7 lag alert.

**Batch-granularity lease, not a per-interval heartbeat table (D10, D11).** The
rejected design (v0.7 D10) was a *polling* lease/heartbeat table (Kafka group
coordinator, Kinesis KCL DynamoDB lease table) whose worker threads issue a
periodic `UPDATE` to prove liveness — that per-interval churn is the per-row bloat
pgque exists to avoid, and it stays rejected. The v0.8 `partition_slot` lease
carries the *same* ownership metadata but pays none of that cost: it is renewed
**only on the `receive`/`ack` writes that already happen at each batch boundary**
(D11), so a working slot's heartbeat rides existing round-trips — zero extra
traffic, no polling thread. It does the two jobs a coordinator table does:
(1) **crash detection** — an unrenewed lease expires after its TTL
(`clock_timestamp()`-based); the recovery latency is the TTL remainder, the one
knob (the `session.timeout.ms`-equivalent, back by design because session-death
release is unavailable under pooling — D11); (2) **exclusive ownership** —
server-enforced on `receive`/`ack`, with `epoch` fencing the zombie. Its
ownership+lag benefit — *who owns what, how far behind* — is delivered by
**`pgque.partition_slot_status`**, a read-only view over the `partition_slot`
lease columns (`lease_owner`/`lease_until`/`epoch`) + subscription cursors
(per-slot lag) + resize state. It has one row for every expected slot and an
explicit `subscribed` boolean. A missing engine subscription reports
`subscribed=false`, `last_tick=NULL`, and `pending_events=NULL`: zero would mean
known caught-up state, while a missing cursor makes lag unknowable. The
canonical alert is therefore `pending_events > X or not subscribed` — a
threshold-only `pending_events > X` alert silently skips the NULL rows, so
incomplete setup would never fire it. Classification is catalog-driven, the
same way the consume API classifies: a slot counts as `subscribed` only when
its `partition_slot` row AND its engine subscription both exist — a legacy
ordinary consumer that merely shares the name shape (`"C#k/N"`, creatable on
older installs) is never attributed to the slot. Reading lease
columns rather than `pg_locks` means
the view **works through poolers** (a backend pid behind PgBouncer was meaningless
anyway). Zombie caveat: lease-expiry ≠ process-death, so a partitioned worker can
still emit side effects until the TTL lapses while the successor is re-issued the
same open batch — bounded by the TTL and fenced by `epoch` (`claim_slot`
returns it so handlers can stamp it, §8).

**Works through a transaction-mode pooler (matters for the transaction-pooler ICP).** Every
lease operation — `claim_slot`, `release_slot`, and the renewals inside
`receive_partitioned`/`ack_partitioned` — is plain short-transaction DML over
`partition_slot`. It holds **no session state**, so it runs correctly over **one**
PgBouncer/Supavisor transaction-mode pool: no session-mode
connection, no second pool, no connection-per-worker floor, no per-backend lock
leak. This is the reason the lease replaced the v0.7 session advisory claim (D11):
that claim leaked onto the pooled backend under transaction pooling and forced one
session-mode connection per live worker, exhausting connections at high worker
counts. The honest trade is crash-takeover latency: the lease TTL remainder rather
than instant session-death release, and worker ids must be unique per live worker
(document). No `tcp_keepalives_*` tuning is needed — a silent partition is handled
by ordinary TTL expiry, not by connection-death detection.

### Online resize — grow N without breaking order (D9, Phase 3)

> **Status: draft protocol — must pass its own review round before Phase 3 build.**
> The v0.7 refinement review corrected an earlier `ev_id`-gated sketch (which had an
> abort-path data-loss hole and a watermark type conflation) to the **tick-window**
> gating below. Phase 1 ships **immutable N**; this is the sanctioned *future*
> path, not built code.

The leverage: pgque partitions are a *read-side filter over one append-only log*.
A resize moves **no data** and **never blocks producers** (the log has no N) — only
*which cursor reads which hash class* is re-derived. So grow-N is a clean
drain-then-cutover watermark (the log-native analog of Kinesis parent-shard drain).
State on `partition_consumer(…, epoch, resize_state, n_next, seal_tick)`,
`resize_state ∈ {stable, draining}`; all writes SECURITY DEFINER, table revoked
from app roles. Note: `partition_consumer.epoch` (resize generation, D9) and
`partition_slot.epoch` (per-slot lease fencing token, D11) are **distinct
counters** — the token handlers stamp is the lease epoch.

**The boundary is a tick, not an ev_id.** Old epoch = events in tick windows ≤
`seal_tick`; new epoch = windows > `seal_tick`. An in-flight producer txn holding
a low `ev_id` that commits post-seal lands in a *post-seal window* → new epoch,
consumed post-cutover in window order — identical to the engine's existing
cross-txn window semantics, so there is no straggler special case and no dual
watermark.

| Step | What happens | Lives in |
|------|--------------|----------|
| `begin_resize(q,c,N′)` | assert `stable`; require a live ticker; `force_tick`, record `seal_tick`; `n_next=N′`, `resize_state=draining`. Register the N′ new slots at `seal_tick` (`register_consumer_at`, `pgque.sql:1782`). | core; operator/CLI |
| drain | old slots consume normally but are **server-side gated to windows ≤ `seal_tick`** (`receive_partitioned` refuses to open a batch past the seal, and caps the batch window regardless of `min_count`/`min_interval`, which could otherwise overshoot). New slots return empty **without calling `next_batch`** — cursor parked at `seal_tick`, so the gate never advances them past the backlog. | core filter; client drives tick+poll |
| `resize_ready(q,c)→bool` | true iff every old slot has `sub_last_tick = seal_tick`, no open batch, **and zero `retry_queue` rows** owned by old-slot `sub_id`s (nor `partition_block` under pause). Retry-flush is mandatory: `unregister_consumer` deletes retry rows, so a naive flip drops nacked-pending events. | core guard |
| `complete_resize(q,c)` | takes `for update` on the old-slot subscription rows (the same lock `next_batch_custom` holds, `pgque.sql:5761`) so no batch can open between re-check and cutover; **re-checks `resize_ready`, refuses if false**; sets `n=N′`, `epoch+1`, `stable`, clears `seal_tick`; unsubscribes old slots **re-homing their `dead_letter` rows** (per-row re-hash `hash(ev_extra1)%N′` → new slot's `co_id`, or a queue archive consumer — a bare `unregister_consumer` cascades DLQ history away). | core |
| `abort_resize(q,c)` | drop the gate, unsubscribe the N′ slots, back to `stable`. **Loss-free by construction** — under tick-gating the old slots never advanced past `seal_tick`, so nothing new-epoch was skipped. | core |

The operator/CLI calls `begin_resize`; a **client drain-driver** loops
`tick()`+`resize_ready()` until true-or-timeout, then `complete_resize`/`abort_resize`.
Because cutover re-validates atomically under the subscription-row lock, a buggy
driver can only **stall** (state stays `draining`, old epoch keeps working) — never
cut over early. R7 during resize: both slot sets pin rotation while draining
(bounded, released at cutover); a wedged old slot keeps `resize_ready` false — it
stalls the resize and trips the lag alert, never corrupts. **Shrink N** is the
machine run backward, deferred. Cannot resize under `pause` until O1 (a blocked
key never drains).

## 16. Changelog

- **v0.8 (draft):** **Slot ownership moved from a session-scoped advisory lock to a
  batch-granularity lease in core SQL** (new D11; D7/D8/D10 updated, `slot_lock_key`
  removed). Credit: **Fabrizio review** — session advisory locks are incompatible
  with transaction-mode poolers (the lock leaks onto the pooled backend) and force
  one session-mode connection per live worker, exhausting connections at high
  worker counts. New `pgque.partition_slot(queue_id, co_name, slot, lease_owner,
  lease_until, lease_ttl, epoch)` table; `claim_slot`/`release_slot` arbitrate over
  its rows; `receive_partitioned`/`ack_partitioned` gain a `worker` arg, **require
  the live lease (server-enforced G2)**, and **renew it on every call** so the
  heartbeat rides existing per-batch writes — no per-interval `UPDATE` churn (this
  is why D10's lease-table rejection is superseded for the ownership mechanism,
  while the per-interval-churn rejection stands). Fencing is automatic: takeover
  bumps `epoch`, a zombie's next `receive`/`ack` raises. Everything is plain
  short-transaction DML → one transaction-mode pool suffices; the
  connection-pooler caveat became a positive statement. `partition_slot_status`
  now reads lease columns (`lease_owner`/`lease_until`/`epoch`) instead of
  `pg_locks`, so it works through poolers. Honest trade: crash-takeover latency is
  the lease TTL remainder (a `session.timeout.ms`-equivalent knob returns);
  `clock_timestamp()`-based; worker ids must be unique per live worker. Rebalancing
  is kept (static pinning alone was insufficient), so the lease arbitrates live
  claims. Tests: T-claim → **T-lease**, added **T-fencing**; T-retry-affinity's
  "not yet implemented" note cleared (now in `tests/test_partition_keys.sql`).
  *Review pass (adversarial SQL + consistency, applied in place):* plain
  `receive`/`ack`/`nack` now **reject slot consumers** (`#`-names) — without the
  guard a reader could drive the raw slot subscription lease-free and unfiltered;
  **`nack_partitioned` added** (the lease-fenced slot retry/DLQ path plain `nack`
  no longer serves; shared `_nack_batch_event` core keeps #98/#104);
  G2's "server-enforced" claim scoped honestly to the pgque-api surface (raw PgQ
  primitives keep the inherited cooperative trust model); epoch documented as
  returned by `claim_slot` (not `receive_partitioned`); `partition_block`
  correctly stated as Phase-2-only; the two `epoch` counters (D9 resize vs D11
  lease) disambiguated; null-key→slot-0 routing added to G1/§6/§8.
- **v0.7 (draft):** Fabrizio review (he tested the repro) + advisor + workflow.
  Corrected the receive-lock claim (core *does* coordinate; the real hazard is the
  process→ack gap, §12) — verified in `pgque.sql:5761/5798/5866/5905` +
  `two_session_receive_lock.sh`. Stated the **mechanism/policy seam** (D8) and made
  the **member/heartbeat table an explicit rejection** (D10) — redundant with
  session-death lock release + G2 — replaced by core `slot_lock_key` (D7) +
  read-only `partition_slot_status` view. Added the **connection-pooler caveat**
  (session advisory locks need session-mode/direct connections — the transaction-pooler
  ICP). Upgraded R4 from hand-wave to the sanctioned **epoch-gated drain-then-
  cutover online-resize** protocol (D9, §15) with retry-flush + DLQ-preservation
  guards, modeled on Kinesis parent-shard drain. Promoted "every slot must be
  polled" to a hard R7-adjacent requirement.
  - *Refinement pass (Fable review of v0.7):* propagated the receive-lock
    correction into G2/§15/brief — the **session claim is load-bearing for G2, not
    pure liveness** (skipping it → duplicate/interleaved processing of one batch);
    claim releases only at a batch boundary. Reworked online resize from `ev_id`-
    gating to **tick-window gating** (fixes the abort-path data-loss hole + the
    `ev_seal` type conflation; `abort_resize` now loss-free by construction).
    Sharpened D10/pooling: session-lock **leaks onto the pooled backend** under
    transaction pooling (wedge, not miss) — only the claim connection needs
    session-mode; a silent partition needs `tcp_keepalives_*`. Gave `epoch` a job
    (fencing token returned by `receive_partitioned`). Marked the resize protocol
    "draft — needs its own review round before Phase 3 build."
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

## 17. User stories

Canonical user-story layer for this feature — the shared language the spec, the
public brief, and the acceptance suite all speak. Single-session facets are
proven by `tests/acceptance/us12_partition_keys.sql`; the cross-session facets of
US-12.4 and US-12.5 (a concurrent worker blocking on the receive lock, lease
exclusivity across live workers, and lease-expiry crash recovery with epoch
fencing) need two sessions and are covered by `tests/two_session_slot_claim.sh`
(mirrors `tests/two_session_receive_lock.sh`).

- **US-12.1 — Keyed send.** As an event producer, I want `pgque.send(queue,
  type, payload, partition_key)` so each event carries its tenant key (stored in
  `ev_extra1`).
  - *Accept:* a keyed `send` round-trips the partition key to
    `pgque.message.extra1` on receive.
  - *Test:* `us12_partition_keys.sql` — US-12.1.
- **US-12.2 — Per-key order.** As a consumer, I want all events of one key
  delivered by exactly one slot, in `ev_id` order, so per-tenant processing is
  sequential.
  - *Accept:* under interleaved keys, every event of one key lands on its hash
    slot only (zero on any other slot), in non-decreasing `ev_id` order (G1).
  - *Test:* `us12_partition_keys.sql` — US-12.2.
- **US-12.3 — Cross-key parallelism.** As an operator, I want N slots consuming
  disjoint key subsets concurrently (union of slots = whole stream, pairwise
  disjoint).
  - *Accept:* over N=3, the union of slot reads equals the whole stream, no event
    appears on two slots, and every event sits on `(hashtextextended(key,0) % N +
    N) % N` (D4).
  - *Test:* `us12_partition_keys.sql` — US-12.3.
- **US-12.4 — Single processor per slot.** As an operator, a second worker on the
  same slot never obtains a divergent batch (engine receive lock returns the same
  active batch idempotently); the lease steers it away entirely.
  - *Accept:* re-`receive_partitioned` on an open slot returns the same batch
    (single-session facet); a second worker claiming the same slot gets NULL and is
    routed to a different slot by the lease (cross-worker).
  - *Test:* `us12_partition_keys.sql` — US-12.4 (single-session);
    `tests/two_session_slot_claim.sh` (cross-session).
- **US-12.5 — Claim/release + crash recovery.** As a worker, I claim a free slot
  via `pgque.claim_slot` (returning the lease epoch) and release at a batch
  boundary via `pgque.release_slot`; when a claiming worker dies its lease expires
  after the TTL and the slot is then taken over with an epoch bump that fences the
  zombie.
  - *Accept:* `claim_slot` takes a free slot (epoch returned), an owner re-claim
    renews with the same epoch, `release_slot` frees it owner-only and the slot is
    re-claimable (single-session); an expired lease is taken over by another worker
    with a bumped epoch (cross-session crash recovery).
  - *Test:* `us12_partition_keys.sql` — US-12.5 (single-session);
    `tests/two_session_slot_claim.sh` (crash recovery).
- **US-12.6 — Observability.** As an operator, `pgque.partition_slot_status` shows
  each expected slot, whether its engine subscription exists, its `lease_owner`
  (null if unleased), and cursor lag, so I can alert on incomplete setup or a
  stalled slot (rotation-pinning risk R7) — canonical alert `pending_events > X
  or not subscribed`, since a threshold-only alert skips the NULL-lag rows of
  missing subscriptions — and it works through a pooler because
  it reads lease columns, not `pg_locks`.
  - *Accept:* the view has one row per expected slot with the right `n`,
    `subscribed=true` for materialized subscriptions; missing subscriptions show
    `subscribed=false`, `last_tick=NULL`, and `pending_events=NULL`; otherwise
    `lease_owner` null when unleased and equal to the holding worker id when leased
    (clearing on release), and a non-negative `pending_events` lag.
  - *Test:* `us12_partition_keys.sql` — US-12.6.
- **US-12.7 — Enforced N.** As an operator, a worker calling with the wrong N is
  rejected with a clear error, never silently misrouted.
  - *Accept:* `subscribe_partitioned` atomically persists N and all N slots for
    `(queue, consumer)` at one start tick; re-calling with the same N is
    idempotent without cursor movement, a changed N raises, and existing partial
    setup is rejected as incomplete (D3).
  - *Test:* `us12_partition_keys.sql` — US-12.7.
