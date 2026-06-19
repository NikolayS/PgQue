# PgQue Partition Keys — Spec

- **Version:** v0.1 (draft)
- **Status:** draft for review; single-pass lead draft in SamoSpec format
  (the live GPT+Claude review panel was not run in this environment)
- **Slug:** partition-keys
- **Scope:** consumer-side ordered, parallel consumption by partition key.
  Producer-side idempotency/dedup is a *separate* spec (deferred — see §11).

---

## 1. Goal

Add a **partition key** to PgQue so that, within one queue, events sharing a key
are consumed **in order by a single consumer at a time**, while events with
different keys are consumed **in parallel**. This is the log-native ("Kafka
partition") model: order *within* a key, parallelism *across* keys.

Concretely: `send(queue, key, payload)` tags an event with a partition key;
a partition-aware consumer guarantees that for any given key, its events are
delivered in `ev_id` order to exactly one worker at a time.

## 2. Why it's needed

PgQue is an **ordered, immutable log**, not a job queue. Real workloads need
**per-entity ordering without global ordering**. The motivating case (Supabase
Storage, evaluating PgQue to replace pg-boss):

- Millions of file-lifecycle events (`FileCreated`, `FileDeleted`,
  `FileOverwritten`). They **must be processed in order per tenant**, but
  **order across tenants does not matter**.
- A single in-order consumer can't keep up with millions of events; naive
  multi-worker consumption breaks per-tenant order.

Today PgQue offers no way to parallelize a queue while preserving per-key order.
Cooperative consumers exist but distribute events without key affinity, so they
do not preserve order for a key. This spec closes that gap.

Non-goal restatement: this is **not** "one job at a time per key via locks"
(that was a job-queue framing). Ordering here is achieved by **routing**, with
no per-event lock or mutable state — consistent with PgQue's no-bloat thesis.

## 3. Scope and ICP

**In scope (v0.1):**
- Carry a partition key on an event.
- Partition-aware assignment: stable `hash(key) → slot` mapping over a fixed set
  of N consumer slots.
- Per-key ordering guarantee across batches.
- A documented failure policy (§7, decision D2).

**Out of scope (v0.1):**
- Producer idempotency / dedup windows (separate spec).
- Dynamic rebalancing / elastic slot count (fixed N in v0.1; §10 D3).
- Cross-queue / cascaded (multi-node) partitioning.
- Hot-partition mitigation beyond documentation.

**ICP:** multi-tenant SaaS on managed Postgres (RDS/Aurora/Cloud SQL/AlloyDB/
Supabase/Neon) running a high-volume per-entity event stream where entity =
partition key (tenant, user, document, device).

## 4. End-to-end workflow

```
producer:  pgque.send('files', partition_key => tenant_id, payload => '{...}')
                       │  (key stored on the event; no new hot-path state)
                       ▼
engine:    append-only event tables, global ev_id order  (UNCHANGED, sacred)
                       ▼
consumer:  N partition-aware sub-consumers; slot = hash(key) % N
           - each slot processes its keys in ev_id order
           - a key never spans two slots → per-key order preserved
           - different keys → different slots → parallel
```

## 5. User stories

- **US-1 (per-tenant order):** As a consumer, when I read `files`, all events for
  `tenant=42` arrive in `ev_id` order, even under N parallel workers.
- **US-2 (cross-tenant parallelism):** As an operator, throughput scales with N
  workers because distinct tenants are processed concurrently.
- **US-3 (single processor per key):** As a consumer author, I never have two
  workers processing `tenant=42` events at the same instant, so I need no
  external lock on the tenant's resources.
- **US-4 (no new bloat):** As a DBA, enabling partitions adds **no per-event
  UPDATE/DELETE** and no vacuum-dependent side table.
- **US-5 (failure policy is explicit):** As a consumer author, I can choose
  whether a failing event **pauses its partition** (strict order) or is
  **skipped** (at-least-once, possible reorder). Default per D2.

## 6. Architecture

<!-- architecture:begin -->
```
            ┌─────────────────────────────────────────────┐
 producers  │  pgque.send(queue, partition_key, payload)  │
            └───────────────────────┬─────────────────────┘
                                    │ key on ev_extra1 (free today)
                                    ▼
            ┌─────────────────────────────────────────────┐
   ENGINE   │  append-only event tables · global ev_id     │   <-- UNCHANGED
  (sacred)  │  next_batch / get_batch_events / rotation    │       (no edits to
            └───────────────────────┬─────────────────────┘        batch_event_sql)
                                    │ batch of events
                                    ▼
            ┌─────────────────────────────────────────────┐
 PARTITION  │  assignment: slot = hash(key) % N            │   <-- NEW logic,
  LAYER     │  (rides on cooperative consumers)            │       distribution only
            └───┬───────────────┬───────────────┬─────────┘
                ▼               ▼               ▼
            slot 0          slot 1          slot N-1
          worker A        worker B        worker C
        keys h%N==0      keys h%N==1     keys h%N==N-1
        in ev_id order   in ev_id order  in ev_id order
```
<!-- architecture:end -->

**Key property:** the new code lives entirely in the *distribution* step of the
cooperative-consumer layer. The batch/tick/snapshot/rotation engine
(`batch_event_sql`, `next_batch`, rotation) is **not modified** (design rule:
the PgQ engine is sacred).

## 7. Decisions

| ID | Decision | Choice (v0.1) | Rationale |
|----|----------|---------------|-----------|
| D1 | Where the key lives | `ev_extra1` (no schema change) | Already carried through batching and exposed on `pgque.message`. Dedicated column can come later. |
| D2 | Failure policy (head-of-line) | **Pause the partition** by default; `skip` opt-in | Motivating workload "cares about per-tenant order". PgQ retry re-inserts with a later `ev_id`, which would reorder — so strict order must block the key until the failure resolves. |
| D3 | Elasticity | Fixed N in v0.1 | Rebalancing safely (without reordering across the change) is its own hard problem; defer. |
| D4 | Assignment function | `hashtext(key)` mod N, stable | Deterministic key→slot affinity; standard partition model. |
| D5 | No per-event state | Routing only; no lease table, no advisory lock per event | Preserves the append-only / no-vacuum thesis. |

## 8. Implementation details

- **Producer:** `pgque.send(queue, partition_key text, payload …)` wrapper →
  `insert_event(queue, type, payload, partition_key /*ev_extra1*/, …)`.
  Pure reduction to the existing primitive.
- **Assignment:** extend cooperative-consumer distribution so a sub-consumer N
  receives exactly the batch events where `hashtext(ev_extra1) % total = N`.
  Filtering happens in the distribution/consume layer, **not** in
  `batch_event_sql`.
- **Per-key order across batches:** a key always maps to the same slot, and a
  slot processes its events in `ev_id` order, so order holds across ticks.
- **Pause-on-failure (D2):** when an event for key K fails, the slot must not
  advance past K for that key until K succeeds or is dead-lettered. Built on the
  existing `event_retry` / DLQ primitives plus a per-slot "blocked keys" set held
  **in the consumer**, not in a table. (Exact mechanism is the main design risk —
  §10.)
- **Security/grants:** producer wrapper → `pgque_writer`; partition consumer →
  `pgque_reader`. `SECURITY DEFINER` functions pin `search_path = pgque,
  pg_catalog`.

## 9. Tests plan (red/green TDD)

Write the failing test first, then the implementation. CI matrix PG 14–18.

- **T1 (order):** interleave events for keys A,B,A,A,B; assert each key delivered
  in `ev_id` order under N≥2 slots. *(red first)*
- **T2 (parallelism):** distinct keys land on distinct slots per `hash%N`.
- **T3 (affinity/stability):** same key always → same slot across batches.
- **T4 (single processor):** two workers, same key in one batch → only one slot
  ever holds it concurrently.
- **T5 (pause-on-failure):** key A event #2 fails → A#3 is NOT delivered before
  #2 resolves; B continues unaffected.
- **T6 (skip mode):** with `skip` policy, A#3 proceeds after A#2 fails (reorder
  allowed). 
- **T7 (no bloat):** processing M events adds zero rows to any side table and
  issues no per-event UPDATE/DELETE (assert via `pg_stat`/row counts).
- **T8 (engine untouched):** `batch_event_sql` text/byte-identical to baseline.

## 10. Risks and open questions

- **R1 — pause-on-failure mechanism.** Keeping a key "blocked" without a mutable
  table, across crashes and re-delivery, is the hard part. Needs a concrete
  design that survives a worker restart (likely: re-derive blocked state from the
  presence of an unacked/retrying event for the key at slot start).
- **R2 — cooperative-consumer internals.** Must confirm where assignment hooks in
  pgq-coop without touching the engine, and whether coop guarantees a sub-consumer
  sees a key consistently. *(Next concrete investigation step.)*
- **R3 — hot partitions.** One very active key saturates its slot. v0.1: document;
  no automatic mitigation.
- **R4 — fixed N / rebalancing.** Changing N reshuffles affinity and can reorder
  in-flight keys. Out of scope; needs a future spec.

## 11. Relationship to producer idempotency (deferred sibling)

A separate spec covers producer-side dedup as a **TTL window** (SQS/NATS model),
append-only, GC'd by rotation. It is intentionally decoupled: in a log,
"processed" is a per-consumer fact the producer cannot see, so dedup must be a
producer-side time window, while ordering/serialization is this consumer-side
partition feature. Prior-art and rationale: `blueprints/IDEMPOTENCY_DESIGN.md`.

## 12. Team of veteran experts (review panel)

- **Lead (spec author):** drafts and revises.
- **Reviewer A — ops/security:** scope creep, the pause-on-failure crash story,
  grants, managed-PG constraints.
- **Reviewer B — QA/testability:** ordering under concurrency, the reorder edge
  in skip mode, "engine untouched" assertion.

*(Live multi-model review loop not run here; reviewer personas listed for when
this is iterated through the actual `samospec` CLI.)*

## 13. Sprint plan

1. **S1 — producer + key plumbing:** `send(queue, key, payload)`, key on
   `ev_extra1`, exposed on `pgque.message`. Tests T2–T3.
2. **S2 — partition-aware assignment** over cooperative consumers. Tests T1, T4,
   T7, T8. Resolves R2.
3. **S3 — pause-on-failure** (D2 default) + `skip` mode. Tests T5, T6. Resolves R1.
4. **S4 — docs + benchmark** (throughput vs N; per-tenant order under load).

## 14. Changelog

- **v0.1 (draft):** initial single-pass SamoSpec-format draft. Defines the
  partition-key consumer feature, the hash-assignment architecture, the
  pause-on-failure default (D2), and the no-per-event-state constraint (D5).
  Producer idempotency split out to a sibling spec.
