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
