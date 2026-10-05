# decisions

- No review-loop decisions yet.

## Round 1 — 2026-05-30T09:43:28.231Z

- accepted missing-risk#1: Added §5.10 authorization model — REVOKE EXECUTE FROM PUBLIC on all durable functions, dedicated worker/client roles, and caller-scoped emit authorization — closing the privilege-escalation path on the approval flow.
- accepted missing-risk#2: Added §5.11 requiring workflow_id to be a 128-bit CSPRNG-generated unforgeable capability, with a test rejecting predictable id generation, so coordination primitives cannot be driven by id enumeration.
- accepted weak-implementation#1: Added §5.4.1 stating the explicit dedup_horizon ≥ max-redelivery-latency bound and enforcing it by routing over-horizon events to DLQ rather than reprocessing, preventing marker-rotation double-handoff.
- accepted weak-implementation#2: Resolved in §5.2: the batch is the single transaction unit, transient retries become returned send_at continuations (subtransaction-free appends), unexpected exceptions abort only a size-bounded batch, and repeat offenders are quarantined to DLQ to cap poison-pill amplification.
- accepted missing-risk#3: Added §5.7.1 making the timeout sweep a hard property of the dispatch loop itself (run every iteration including idle tick-sleeps), so timeout liveness no longer depends on optional pg_cron; asserted by a pg_cron-disabled test.
- accepted weak-implementation#3: Pinned §5.7.3 to a transaction-scoped row lock via INSERT…ON CONFLICT + SELECT…FOR UPDATE on the exact (workflow_id,event_name) key, pooler-safe under PgBouncer and collision-free, and explicitly forbade session-level advisory locks.
- accepted missing-risk#4: Added §5.7.2 retaining cache entries for a configured await horizon guaranteed ≥ worst-case await latency, GC evicting only past-horizon entries and rejecting awaits whose deadline exceeds the horizon, eliminating silent loss of a legitimate emit-before-await.
- accepted unnecessary-scope#1: Demoted wf_live to an optional, opt-in, default-OFF, append-based/rotating projection that is never required for correctness (addressing remains by workflow_id in user tables), removing the insert+delete per-workflow dead-tuple tax on the headline property.
- accepted weak-implementation#4: Rewrote §5.6 to separate concurrency-bounded live row-count from coordination-throughput-bounded cumulative dead-tuple rate, conceding DELETE-driven tables need autovacuum/rotation and publishing their dead-tuple curve in the benchmark.
- accepted missing-risk#5: Added §5.12 caps: max_spawn_fanout, hard max_payload_bytes, unknown-id emit rejection with no cache row, cache cardinality cap, and optional emit rate limit, closing the bloat/DoS vectors on the externally driven surfaces.
- accepted weak-implementation#5: Added §5.9 stating the tick-visibility dependency as an explicit engine contract with a regression test that fails if engine tick/visibility semantics change, converting a silent join-correctness break into a CI failure.
- accepted missing-risk#6: Added §5.10.3 wf_audit, an append-only rotating table recording role-attributed emit/resume/spawn/timeout actions exported before rotation, providing a tamper-evident trail for approval/escalation flows.
- accepted unnecessary-scope#2: Scoped the heavy sustained throughput-and-bloat benchmark out of the per-change CI gate to a nightly/on-demand gated harness (§6.5), keeping only a short smoke version in standard CI.

## Round 2 — 2026-05-30T09:55:00.614Z

- accepted rA-1: Resolved by defining a per-transition delivery_anchor (reset at timer fire) as the horizon clock and recomputing the bound to single-attempt, so a woken 7-day sleep has ~0 redelivery age and is never DLQ'd (§5.4.1).
- accepted rA-2: Rebuilt poison-pill containment on PgQ's durable per-event retry counter (pinned as engine contract §5.9) plus a dispatcher fault-isolation re-dispatch that halves the batch to isolate the offender — removing the un-writable side-table counter and the per-workflow-selection assumption (§5.2).
- accepted rA-3: Surfaced the no-running-dispatcher gap explicitly and made pg_cron a hard correctness requirement for scale-to-zero/serverless topologies, with an install warning and a dedicated test (§5.7.1, §10, §6.3).
- accepted rA-4: Eliminated the never-reclaimed lock row entirely by switching to a transaction-scoped advisory lock (no row, pooler-safe, hash-collision-correctness-safe), so the locking primitive contributes zero tuples (§5.7.3, §5.5).
- accepted rA-5: Spilled per-child results into wf_join_done and reduced the parent resume payload to a join reference, so a full 1024-child fan-out stays under the 8 KiB payload cap (§5.8).
- accepted rA-6: Dropped the 'tamper-evident' claim (deferred hash-chaining), and added an application-supplied actor_id as the forensic anchor since session_user/current_user are useless under pooling/SECURITY DEFINER (§5.10.3).
- accepted rA-7: Added a confidentiality/leakage model: mandatory per-wait emit token for approvals (so a leaked id alone cannot forge), hashed workflow_id at rest in audit/DLQ, and a no-raw-logging requirement (§5.11).
- accepted rA-8: Scoped the flat-dead-tuple headline to await-light loops, conceded coordination-point-bounded dead-tuple rate for await/join-heavy workloads, and added an await/join-heavy A/B to the benchmark (§5.6, §6.5).
- accepted rA-9: Added an explicit minimum PgQ engine floor (send_at + durable retry counter + tick-visibility) gated and failing loudly at install, alongside the PG 14–18 matrix (§5.9, §5.13, §10).
- accepted rB-1: Introduced a mandatory minimal wf_registry as the authoritative emit-liveness source (concurrency-bounded, one insert+delete per workflow lifetime), so unknown-id rejection no longer depends on the optional wf_live projection (§5.5, §5.10.2, §5.12).
- accepted rB-2: Pinned retry continuations to a fresh step_seq with retry_attempt/origin_step in the payload, so the dedup model re-executes the retry instead of treating it as a committed no-op (§5.1, §5.2, §5.4).
- accepted rB-3: Pinned the load-bearing definition: the age clock is the per-event delivery_anchor (deliverable time), not workflow origin, explicitly reset across sleeps — resolved jointly with rA-1 (§5.4.1).
- accepted rB-4: Separated cache_retention_horizon (emit→registration gap) from the user-facing await_timeout, so a long await is no longer capped by cache retention and is not rejected at registration (§5.7.2).
- accepted rB-5: Added the mandatory symmetric positive test asserting a sleep longer than dedup_horizon resumes normally and is NOT DLQ'd (§6.2 item 2).
- accepted rB-6: Specified the isolation mechanism (dispatcher fault-isolation re-dispatch over the same snapshot range) and pinned the durable per-event retry counter as an engine contract — resolved jointly with rA-2 (§5.2, §5.9).
- accepted rB-7: Restated timeout liveness as conditional on a continuously-running dispatcher, with pg_cron required for scale-to-zero — resolved jointly with rA-3 (§5.7.1).
- accepted rB-8: Reframed the CSPRNG check as a decidable static assertion: id column defaulted by gen_random_uuid()/pgcrypto and CI rejects any sequence/serial-derived id path (§5.11, §6.2 item 6).
- accepted rB-9: Corrected the cross-reference: the no-subtransaction constraint is cited to §5.1/§5.13, not §5.10 (§5.2).
- accepted rB-10: Claimed to populate the canonical architecture:begin/end block with the layered diagram, but later review found the stale '(architecture not yet specified)' placeholder still present; corrected for real in the 2026-06-06 manual pass (§4).
- accepted rB-11: Added an assertion that a transient-failure step re-executes its body once per retry attempt up to max_retries then lands in the DLQ, guarding the dedup-vs-retry path (§6.2 item 3).

## Round 3 — 2026-05-30T11:37:14.368Z

- accepted rB-1: Claimed to actually populate the canonical architecture:begin/end block with the layered SDK→durable-layer→sacred-engine diagram, but later review found the literal '(architecture not yet specified)' placeholder still present; corrected for real in the 2026-06-06 manual pass (§4).
- accepted rB-2: Removed the 'tens of thousands of transitions/sec' / 'not throughput-timid' framing from §1/§2 and aligned all of §1, §2, and §12 to the idea's honest concession of ~a few thousand transitions/sec per database with hyperscale conceded to Temporal.
- accepted rB-3: Redesigned poison-pill containment to use only the existing next_batch max_events bound (reduce to size 1), withdrawing the implied sub-range/partial-ack primitive, and pinned that bound as explicit engine contract #4 gated at install (§5.2/§5.9/§5.13).
- accepted rB-4: Pinned per-join completion serialization (SELECT … FOR UPDATE / advisory lock on the join id) at READ COMMITTED so the final concurrent completers are ordered and the parent resumes exactly once — closing the lost-resume (zero-resume) race (§5.8).
- accepted rB-5: Added an engine-contract regression test for the next_batch max_events bound (contract #4) plus a multi-tenant batch poison-isolation test asserting innocent co-tenants are not DLQ'd, and an install-floor gate covering all four contracts (§6.2/§6.3).
- accepted rB-6: Dropped the undefined 'within-horizon pre-registration' clause; live wf_registry membership (held for a workflow's whole lifetime) is the sole emit-liveness source and already covers emit-before-await (§5.10.2/§5.12).
- accepted rB-7: Reduced the 'all clients get workflows' claim to the single Python reference client actually staffed/scheduled, with Go/TS/WIP clients explicitly deferred, making §1/§2/§9/§11/§12 consistent with §7/§8 (§1/§2).

## Round 4 — 2026-05-30T11:56:01.783Z

- accepted contradiction#1: Pasted a real layered architecture diagram into the §4 architecture:begin/end block and corrected the false v0.4 changelog claim to admit the placeholder had remained, ending the twice-repeated conflict between the changelog and the actual block.
- accepted ambiguity#1: Moved the poison-pill max_events reduction from process-local dispatcher state into a new consumer-wide wf_dispatch_control row read by every subconsumer before each next_batch, so a redelivered poison event cannot be re-aggregated at K by another subconsumer, and added a multi-subconsumer redelivery test plus a cross-subconsumer clause to engine contract #4.
- accepted ambiguity#2: Specified the up-ramp: current_max_events is restored to K only after quarantine_cooldown consecutive clean size-1 commits (count-gated, not time-gated) so the poison is DLQ'd before batches re-aggregate, with a regression test for the restoration.
- accepted contradiction#2: Withdrew the inconsistent 'append-based, rotating, not insert+delete' description and pinned wf_live as a single one-row-per-live-workflow HOT-UPDATEd projection (concurrency-bounded row-count; dead-tuple rate = update rate) so §4.2, §5.5, and §5.6 agree.
- accepted ambiguity#3: Pinned the §5.4.1 staleness check as a pre-body, route-not-process gate that commits cleanly before any user body runs, scoped the contract-#2 'only durable counter' claim to the aborting-batch channel only, reconciling the two DLQ routes and adding a gate-ordering test.

## Manual correction — 2026-06-06T00:45:00Z

- accepted manual-1: Added `pg_durable` as fresh prior art and drew the
  boundary clearly: PgQue should learn from its primitives and security work,
  but keep workflow control flow in application code rather than a SQL graph
  DSL inside Postgres.
- accepted manual-2: Replaced the stale architecture placeholder for real in
  SPEC v0.6 and corrected the historical changelog notes that previously
  claimed v0.5 had done it.
- accepted manual-3: Replaced broad exactly-once workflow language with the
  honest contract: at-least-once step execution, exactly-once transactional
  handoff, and idempotent external effects.
- accepted manual-4: Downgraded unbenchmarked throughput claims to benchmark
  hypotheses and expanded the benchmark comparison to include a pg_durable-style
  checkpointed graph baseline where feasible.
- accepted manual-5: Tightened the `workflow_id` security model: raw ids may
  exist in protected hot queue rows / `ev_extra1`, while lower-trust audit, DLQ,
  metrics, errors, and exports must hash or truncate them.
