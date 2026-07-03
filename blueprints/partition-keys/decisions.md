# Partition Keys — decisions log

Accepted / rejected / deferred choices, tracked across review rounds.

## Review round 1 (Reviewer A ops/security · Reviewer B QA/testability)

### Accepted (changed the spec)

- **A1 — Drop the cooperative-consumer distribution model.** Both reviewers
  proved coop hands each member a *disjoint tick window*, not a hash-filtered
  shared batch; a filter overlay would drop other slots' events on cursor
  advance. → v0.2 uses **N independent slot subscriptions**, each filtering the
  full stream via `get_batch_cursor` `extra_where`. (SPEC §6)
- **A2 — Correct the retry rationale.** `event_retry` preserves `ev_id` and
  changes `ev_txid` (event reappears in a later window); the original "later
  ev_id" claim was wrong. → guarantee restated as G1/G2/G3 with an explicit
  engine note. (SPEC §2)
- **A3 — Resolve D2-vs-"no state".** `pause` derives its blocked-key set from the
  engine's existing `retry_queue`/`dead_letter`, scoped per slot by `sub_id`
  (each slot is its own subscription); no new mutable table. (SPEC §7 D5, §8)
- **A4 — DLQ must unblock.** A paused key releases when its head event is acked
  *or* dead-lettered, so a poison event cannot wedge a tenant past `max_retries`.
  (SPEC §8; test T-DLQ-unblock)
- **A5 — `send` signature.** Use a new 4-arg `send(queue, type, payload,
  partition_key =>)`; a 3-arg `send(queue, key, payload)` collides with the
  existing `send(queue, type, payload)`. (SPEC §7 D6)
- **A6 — `hashtextextended(key, 0)`** instead of `hashtext()` (unstable across PG
  majors → affinity would break on upgrade). (SPEC §7 D4)
- **A7 — `ev_extra1` restricted to `send()`-sourced queues** (triggers store the
  table name there). (SPEC §7 D1)
- **A8 — Fixed N as an enforced invariant**: persisted per (queue, consumer); a
  mismatched-N worker is rejected, not silently misrouting. (SPEC §7 D3)
- **A9 — Define "slot" and single-owner**: slot = named consumer `"<c>#k/N"`;
  G2 enforced by the existing per-consumer receive lock. (SPEC §7 D7)
- **A10 — Test corrections**: `T-engine-untouched` asserts `pg_get_functiondef`
  (not generated SQL); `T-no-bloat` scoped to the happy path; added T-no-drop,
  order-after-retry, DLQ-unblock, slot-crash, empty/hot-key, cross-version
  affinity. (SPEC §9)

### Deferred

- **`pause` policy implementation** ships after `skip` (sound + simple first);
  `skip` is the v0.1 default. `pause` is fully specified. (SPEC §4, §7 D2)
- **Read-amplification optimization** (single-reader/dispatch) — R6; adds a
  hop/state, out of v0.1's no-state budget.
- **Trigger-sourced queues**, **dynamic N / rebalancing**, **hot-partition
  mitigation** — out of scope, documented.

### Rejected

- **Lease table / advisory-lock-per-event** for serialization — reintroduces the
  per-event churn PgQue exists to avoid (carried over from the superseded
  `IDEMPOTENCY_AND_PARTITIONS.md`, now removed).
- **Modifying `batch_event_sql`** to push partitioning into the engine — violates
  the sacred-engine rule; the `extra_where` hook achieves filtering without it.

## Review round 2 (both personas, verified against the engine)

### Confirmed sound (no change needed)
- **G1 `ev_id` ordering is true.** `batch_event_sql` emits `order by 1`
  (`pgque.sql:440`); `get_batch_cursor` re-wraps the filtered stream with
  `order by 1` (`pgque.sql:2277`) → per-key order survives the filter, no
  consumer sort. (Reviewer B headline.)
- **G2 single-owner lock is real and tested** — `next_batch_custom … for update
  of s` (`pgque.sql:5761`), guarded by `two_session_receive_lock.sh`.
- **Retry affinity holds** — `ev_extra1` preserved through `event_retry` /
  `maint_retry_events`, so a retried event re-routes to the same slot.
- **Coop genuinely hands disjoint windows** (`for update skip locked`,
  `pgque.sql:6262`) — confirms round-1 A1.

### Accepted (changed the spec → v0.3)
- **B-R2-2 — `pause` blocked-set must be durable.** `retry_queue` is transient
  (`maint_retry_events` deletes the row on re-injection, `pgque.sql:863`),
  leaving a crash hole that violates G3. → durable
  `partition_block(sub_id, partition_key, head_ev_id)` marker, O(failing keys),
  cleared on ack-or-DLQ. Honestly reopens "no new table," scoped to `pause`.
  (D5, §8, R1)
- **B-R2-1 — security trust boundary.** `get_batch_cursor.extra_where` is an
  admin-only trusted-SQL sink (`pgque.sql:2221`, `:4852`). → `receive_partitioned`
  / `subscribe_slot` are `SECURITY DEFINER` installer-owned; integers `n,k`
  validated + cast; no caller string interpolated. Reframed the "injection-safe"
  prose. (§6, §8, D7; test T-security)
- **Negative modulo bug** — `hashtextextended` returns `bigint`; bare `% N` can
  be negative → `(h % N + N) % N`. (D4, §6, §8)
- **R7 rotation wedge** — N slots lower the rotation floor to the slowest slot; a
  wedged `pause` slot could pin rotation for the whole queue. → `pause` must not
  hold the batch open; cursor advances past non-blocked keys. (R7, §8)
- **N persistence + teardown** — N persisted in `partition_consumer`;
  `unsubscribe_slot` + DLQ-cascade caveat. (D3, §8)
- **Tests** — added T-retry-affinity, T-security, T-N-invariant; split T-G2 into
  block/parallel; added `get_batch_cursor` to T-engine-untouched; pinned a
  concrete hash pair. (§9)

### Round-1 closure scorecard (both reviewers)
- B1 (coop model) ✅ closed · B2 (retry rationale) ✅ closed · B3 (crash-derive
  blocked set) ♻️ reopened in v0.2, now ✅ closed via durable marker (D5) ·
  B4 (D2-vs-state / send sig) ✅ closed · B5 (DLQ-unblock / slot definition)
  ✅ closed; spawned B-R2-1 (now fixed) · B6 ✅ closed.

## Review round 3 (convergence; both personas verified against the engine)

### Verdict
**Phase 1 (`skip`-default partition consumption) CONVERGED / implementation-ready.
Phase 2 (`pause`) NOT converged — split out as a follow-up** with open items O1/O2.
Both reviewers agreed the round-2 engine-anchor and security *posture* are solid;
the remaining gaps are all in the new `pause`/DLQ surface.

### Accepted → v0.4
- **B1 (security ownership, affects Phase 1).** The "SECURITY DEFINER owned like
  receive/nack" justification was wrong: `receive`/`nack` never call
  `get_batch_cursor`. The real mechanism is **co-ownership** — a function owner
  may execute its own functions regardless of grants, so `receive_partitioned`
  reaches the admin-only `get_batch_cursor` only because the install owner owns
  both. Not `pgque_admin` membership. Invariant: partition functions created by
  the `\i pgque.sql` owner. Test under a non-superuser owner. (§6, D7, T-security)
- **The `pause` withhold mechanic is genuinely unsolved (O1).** Combining the two
  reviewers: a server-side filter that advances the cursor **loses** the withheld
  event (data loss); `event_retry` preserves it but **increments `ev_retry`**, so
  deferred events of a long-blocked key march toward false DLQ. `pause` needs a
  *defer-without-retry-increment* primitive that does not exist. → `pause` is
  Phase 2; O1 is its blocking open item. (§11 O1)
- **Hot-blocked-key cost (O2).** Until O1, a hot blocked key re-defers a growing
  backlog per poll (bounded by head's time-to-DLQ). Document + T-hot-blocked-key.
- **DLQ-unblock ID-space join.** Marker keyed on `sub_id`; `dead_letter.dl_consumer_id`
  is `co_id`. Must join `subscription` to map; do not compare directly. (§8)
- **`partition_block` hygiene:** FK `sub_id → subscription on delete cascade`
  (no orphans), index `(sub_id, partition_key)`, revoked from app roles, created
  empty in Phase 1 so `T-no-bloat` is well-formed (guard with `to_regclass`). (§8, D5)
- **Test tightening:** `T-engine-untouched` pins the `get_batch_cursor/4` overload;
  `T-G3-pause` asserts in-order-exactly-once after unblock; `T-DLQ-unblock`
  asserts marker-clear-via-DLQ-branch (no ack); `T-slot-crash` asserts marker
  durability; `T-security` runs under a non-superuser owner. (§9)
- **N persistence writer/grants:** `partition_consumer` written inside
  SECURITY DEFINER `subscribe_slot`; table revoked from app roles. (D3, §8)

### Round closure
B-R2-1 security: posture closed, ownership prose corrected (B1). B-R2-2 durable
marker: direction correct; hygiene (FK/index/grants) + the withhold mechanic (O1)
now specified/flagged. All round-1 + round-2 *Phase-1* items closed.

## Follow-up Q&A (v0.5) — worker→slot assignment

Two design questions surfaced after round 3 (fixed-N rebalancing; how partitions
map to workers). Resolved into the spec without reopening a formal review round:

- **D8 — assignment is a *claim*, not an *assignment*.** No consumer-group leader,
  no `PartitionAssignor`, no rebalance protocol (Kafka needs them because a
  partition is exclusive by protocol). Each worker claims a slot via a
  non-blocking `pg_try_advisory_lock`; scale-up/down is just lock acquire/release,
  with crash recovery via session-death lock release (no `session.timeout.ms`).
  (§15, D8)
- **Two-lock separation.** G2's blocking receive lock stays the *correctness*
  backstop; the advisory slot lock is a pure *distribution* layer, so a client that
  skips it loses even spread but can never break G1/G2. (§15)
- **Fixed N is the right call, and over-provisioning is not free.** N is also the
  read-amp multiplier (§6), so inflating N to dodge a resize linearly raises read
  cost; online resize breaks G1 mid-flight (`hash % N` reshuffles keys). Resize
  stays future (R4); choose N to match needed parallelism. (R4)
- **Added** `T-claim` (assignment liveness: disjoint advisory locks, freed-slot
  reclaim). (§9)

## Follow-up (v0.7) — Fabrizio review (tested the repro) + advisor + workflow

Two independent stronger-model passes (a Fable advisor and a 4-agent workflow)
converged on the same calls; recorded here with provenance.

- **Receive-lock correction.** Fabrizio: "core does no coordination — two workers
  double-process." Verified false on mechanism: `next_batch` holds `for update of
  s` and returns the *same* active batch idempotently (`pgque.sql:5761/5798`;
  `two_session_receive_lock.sh` asserts equal batch_ids + a blocked wait). The real
  hazard is narrower — the lock releases at the `next_batch` txn commit, not across
  process→ack, so a second worker can reprocess the *same* open batch. His caution
  is valid; the fix is per-worker cursors (coop/slots), not a coordinator. (§12)
- **Mechanism/policy seam (D8).** Core SQL owns only corruption-capable transitions;
  clients/users own guarded policy loops. Fabrizio's "keep coordination out of the
  library" is right about the *protocol* (there is none) but overshoots on the
  *substrate* (shared key namespace + resize guard must be core or clients diverge).
- **D10 — no member/heartbeat/lease table.** Rejected, not deferred: session-death
  advisory-lock release already does crash detection (no TTL to tune) and G2 does
  exclusivity; a lease buys neither and re-adds heartbeat `UPDATE` churn. Its only
  real benefit (owner+lag) → the writeless `partition_slot_status` view. Note: this
  reaffirms round-1's lease-table rejection against a prospect arguing the opposite.
- **Connection-pooler caveat.** Session advisory locks don't survive transaction-
  mode pooling (Supavisor/PgBouncer) → partition workers need session-mode/direct
  connections. Material for the Supabase ICP; documented Phase-1 constraint.
- **D9 — online resize.** Epoch-gated drain-then-cutover state machine in core
  (`begin/resize_ready/complete/abort`), client drives the drain loop, core
  re-validates on cutover. Handles the advisor's 3 holes: common seal tick
  (`register_consumer_at`), retry_queue flush before cutover, DLQ-row preservation
  on unsubscribe. Modeled on Kinesis parent-shard drain. Phase 3; immutable N in P1.
- **Every slot must be polled** (R7-adjacent hard requirement): an unclaimed slot
  stalls → pins rotation → unbounded (clean) growth. M≥N or cycle idle slots.
- **`slot_lock_key`/`claim_slot`/`release_slot` promoted to core** (D7) so all
  language clients share one advisory-lock namespace.

## Still open (Phase 2 `pause`, before it can be built)
- **O1** — choose the defer-without-retry-increment mechanism (new primitive vs
  hold-cursor-without-wedging-rotation).
- **O2** — bound + document the hot-blocked-key degradation.
- Read-amplification bench numbers (R2) to decide if R6 is needed.
