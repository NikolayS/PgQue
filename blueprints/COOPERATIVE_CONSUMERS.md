# Cooperative consumers

Blueprint version: `0.2-draft.7`

## Change log

| Version | Date | Notes |
|---|---|---|
| `0.2-draft.7` | 2026-05-05 | Final SQL-core cleanup: define upgrade defaults and `sub_role` state transitions; make coop auto-registration explicit; clarify touch semantics and deterministic lock order; remove immediate-retry wording. |
| `0.2-draft.6` | 2026-05-05 | Close stale-token correctness holes: takeover must allocate fresh `batch_id`; forced unregister invalidates old tokens; mixed normal/cooperative receive is forbidden; choose `sub_role`; reject dotted names; add heartbeat and client options. |
| `0.2-draft.5` | 2026-05-05 | Make `subscription.sub_cooperative` the recommended marker column and clarify cooperative-consumer workload fit vs high-frequency fan-out. |
| `0.2-draft.4` | 2026-05-05 | Tighten active subconsumer unregister semantics: force unregister must retry/DLQ active messages, never drop them; add lock-contention warning and mixed-version/downgrade notes. |
| `0.2-draft.3` | 2026-05-05 | Harden TypeScript release guidance: SQL compatibility checks, npm Trusted Publishing caveats, release runner requirements, package shape, and oldest-supported SQL integration tests. |
| `0.2-draft.2` | 2026-05-05 | Mark feature experimental everywhere; add release-note requirements; track blueprint version and change log. |
| `0.2-draft.1` | 2026-05-05 | Initial clean-room implementation plan for PgQue core, Go, Python, TypeScript, docs, tests, and parallel worktree split. |

## Goal

Add cooperative consumers to PgQue 0.2 so several workers can share one
logical consumer cursor and split work by batch.

Normal PgQue consumers are fan-out consumers: each registered consumer sees all
events in the queue through its own cursor. Cooperative consumers are different:
one logical consumer has multiple subconsumers, and each batch is assigned to at
most one active subconsumer.

Example:

```text
queue: orders
logical consumer: billing
subconsumers: worker-1, worker-2, worker-3

worker-1 receives batch 10
worker-2 receives batch 11
worker-3 receives batch 12
```

This feature is needed for parallel processing under one logical subscription.
It must not change existing fan-out behavior.

## Clean-room constraint

`pgq-coop` was studied as behavior reference only. It has no visible license
file or copyright header in the repository. PgQue must not copy SQL text,
comments, structure files, tests, or documentation from it.

The implementation should reuse PgQue's existing PgQ-derived, already licensed
core model and reimplement cooperative behavior from first principles.

## Experimental status

Cooperative consumers must ship as **experimental** everywhere in 0.2:

- SQL/API reference docs
- tutorials and examples
- client README files
- roadmap table, using `🔬 experimental` for experimental status instead of `✅`
- function comments where users are likely to discover the API

Recommended wording:

```text
Experimental in PgQue 0.2. Function names, edge-case behavior, and client API
shape may change before this feature is marked stable. Do not use this as the
only processing path for critical workloads without idempotent handlers and
stale-worker takeover tests.
```

The implementation should still be production-minded, but the public contract
must be explicitly unstable until PgQue has real-world feedback on concurrency,
stale takeover, and client ergonomics.

## Non-goals

- No client-side fake cooperative mode by manually concatenating names.
- No new event ownership table.
- No change to `pgque.message`.
- No change to `pgque.ack(batch_id)` or `pgque.nack(batch_id, msg, ...)` API.
- No replacement for normal fan-out consumers.

## Data model

Use existing tables:

- `pgque.consumer`
- `pgque.subscription`

The key invariant is:

```text
one logical consumer group = multiple subscription rows sharing one sub_id
```

Main consumer row:

```text
consumer name: billing
subscription.sub_id: 42
subscription.sub_last_tick: group cursor
subscription.sub_next_tick: null when idle
subscription.sub_batch: null when idle
```

Subconsumer row:

```text
consumer name: billing.worker-1
subscription.sub_id: 42
subscription.sub_last_tick: null when idle
subscription.sub_next_tick: null when idle
subscription.sub_batch: active batch id when this worker owns a batch
```

This keeps retry and dead-letter ownership aligned with current PgQue semantics,
because retry rows already use `ev_owner = subscription.sub_id`.

Do not add a `pgque.subconsumer` table for 0.2. It would only duplicate state
that already exists in `pgque.subscription`, and it would add upgrade and bloat
surface before the feature has proved itself.

Mixed-version behavior must be explicit. A PgQue 0.1 client should not be
expected to understand cooperative rows. PgQue 0.2 docs must say cooperative
consumers require PgQue 0.2-aware clients, and downgrade after creating
subconsumers is unsupported unless subconsumers are unregistered first.

Add an explicit role marker column:

```sql
alter table pgque.subscription
  add column sub_role text not null default 'normal',
  add constraint subscription_sub_role_check
    check (sub_role in ('normal', 'coop_main', 'coop_member'));
```

Use roles as follows:

- `normal`: ordinary fan-out consumer row.
- `coop_main`: logical consumer group cursor row.
- `coop_member`: subconsumer row that can own an active cooperative batch.

The shared `sub_id` remains the ownership invariant for retry and dead-letter
routing. `sub_role` is for safety, diagnostics, filtering, and performance.

On upgrade, all existing `pgque.subscription` rows get `sub_role = 'normal'`.
No existing consumer is converted to `coop_main` unless
`register_subconsumer()` / `subscribe_subconsumer()` is called.

Reasons:

- Safety: cooperative scans can explicitly target `coop_member` rows and avoid
  accidentally treating ordinary fan-out consumers as cooperative workers.
- Correctness: `finish_batch()` and stale takeover can identify cooperative
  member rows without derived name or nullable-tick heuristics.
- Performance: checking a role marker is cheaper and clearer than repeated
  self-join or count checks on `sub_id`.
- Migration clarity: PgQue 0.2 introduces the feature, so an explicit schema
  marker is cleaner than hiding the state in legacy PgQ field combinations.

## SQL API

Add PgQue-native functions in the `pgque` schema.

Low-level compatibility-style API:

```sql
pgque.register_subconsumer(
  queue text,
  consumer text,
  subconsumer text
) returns integer

pgque.unregister_subconsumer(
  queue text,
  consumer text,
  subconsumer text,
  batch_handling integer default 0
) returns integer

pgque.next_batch(
  queue text,
  consumer text,
  subconsumer text
) returns bigint

pgque.next_batch(
  queue text,
  consumer text,
  subconsumer text,
  dead_interval interval
) returns bigint

pgque.next_batch_custom(
  in queue text,
  in consumer text,
  in subconsumer text,
  in min_lag interval,
  in min_count int4,
  in min_interval interval,
  in dead_interval interval default null,
  out batch_id bigint,
  out prev_tick_id bigint,
  out next_tick_id bigint
)
```

Modern API for applications and clients:

```sql
pgque.subscribe_subconsumer(
  queue text,
  consumer text,
  subconsumer text
) returns integer

pgque.unsubscribe_subconsumer(
  queue text,
  consumer text,
  subconsumer text,
  batch_handling integer default 0
) returns integer

pgque.receive_coop(
  queue text,
  consumer text,
  subconsumer text,
  max_return int default 100,
  dead_interval interval default null
) returns setof pgque.message

pgque.touch_subconsumer(
  queue text,
  consumer text,
  subconsumer text
) returns integer
```

`subscribe_subconsumer()` and `unsubscribe_subconsumer()` are modern aliases over
`register_subconsumer()` and `unregister_subconsumer()`.

State transitions:

```text
normal -> coop_main
  register first subconsumer for this logical consumer;
  fail if the normal consumer has an active batch.

coop_main -> normal
  unregister last coop_member, only when no active member batch remains or after
  active member batches have been safely nacked with batch_handling = 1.

coop_member -> deleted
  unregister subconsumer after active-batch safety checks.
```

Invariants:

- `coop_main` must never have `sub_batch is not null`.
- `coop_member` may have `sub_batch` only while actively owning a cooperative
  batch.
- `normal` keeps existing PgQue fan-out semantics.

`receive_coop()` and cooperative `next_batch(..., subconsumer, ...)`
auto-create the logical consumer and subconsumer if missing, using the same
validation and locking as `register_subconsumer()`.

`ack()` and `nack()` stay unchanged. `batch_id` remains the ownership token.

Once a logical consumer has one or more `coop_member` rows, normal
`receive(queue, consumer, ...)`, `next_batch(queue, consumer)`, and
`next_batch_custom(queue, consumer, ...)` for the `coop_main` consumer must raise
a clear error. The main row is the cooperative group cursor and must not also act
as a normal active consumer. To return to normal fan-out mode, unregister all
subconsumers first.

## Batch allocation algorithm

`pgque.next_batch(queue, consumer, subconsumer, ...)` should:

1. Ensure the main consumer exists on the queue.
2. Ensure the subconsumer exists on the queue.
3. Ensure the subconsumer subscription shares the main consumer's `sub_id`.
4. Lock the main subscription row `for update` before opening or advancing the
   group cursor.
5. Lock the current subconsumer row before checking its active state.
6. If the current subconsumer already has `sub_batch`, refresh `sub_active` and
   return the same batch id.
7. If `dead_interval` is provided, find a stale sibling subconsumer with an
   active batch and lock it. Stale takeover must allocate a fresh `batch_id`,
   copy the victim's tick window to the current subconsumer under that new
   ownership token, clear the victim's old `sub_batch`, and return the fresh
   `batch_id`. Never reuse the victim's old `batch_id`.
8. Otherwise call the existing main-consumer batch allocator for the logical
   consumer.
9. Immediately advance/close the main consumer row so the group cursor moves
   forward.
10. Copy the allocated batch/tick window into the subconsumer row.
11. Return the batch id.

The main subscription lock is mandatory. Without it, two workers can race and
allocate duplicate or skipped tick windows.

`batch_id` is the only token accepted by `ack()` and `nack()`. Any operation
that transfers or force-closes ownership must invalidate the old token in the
same transaction. Late `ack(old_batch_id)` or `nack(old_batch_id, ...)` must not
affect a new owner or a redelivered batch.

## `finish_batch()` behavior

`pgque.ack()` calls `pgque.finish_batch(batch_id)`, so `finish_batch()` must
become cooperative-aware.

Normal consumer batch:

```text
sub_last_tick = sub_next_tick
sub_next_tick = null
sub_batch = null
```

Cooperative subconsumer batch:

```text
sub_last_tick = null
sub_next_tick = null
sub_batch = null
```

The subconsumer must not advance its own cursor. The main consumer row owns the
logical group cursor.

Detection rule:

- If the target subscription row has `sub_role = 'coop_member'` and
  `sub_batch = batch_id`, treat it as a cooperative subconsumer batch.
- If the target subscription row has `sub_role = 'normal'`, use normal consumer
  behavior.
- `finish_batch()` must reject attempts to finish a `coop_main` row as an active
  normal consumer.

## Locking and concurrency

Required locks:

- `register_subconsumer()` locks the main subscription row before sharing its
  `sub_id`.
- `next_batch(..., subconsumer)` locks the main subscription row before opening
  a group batch.
- `next_batch(..., subconsumer)` locks the current subconsumer row before
  checking or returning an active batch.
- stale takeover locks the victim row before moving batch state.
- every cooperative path must lock in one deterministic order: first the
  `coop_main` row, then the current `coop_member` row, then candidate victim
  `coop_member` rows using `for update skip locked`.

Recommended stale takeover query behavior:

- consider only sibling rows with the same `sub_queue` and `sub_id`
- require `sub_role = 'coop_member'`
- require `sub_batch is not null`
- require `sub_active < now() - dead_interval`
- use deterministic ordering by `sub_active asc`
- use `for update skip locked` when scanning candidates

Prefer clearing stale sibling batch state over deleting the sibling row during
automatic takeover. Deletion should be reserved for explicit unsubscribe. On
takeover, clear the victim's old `sub_batch` so the old `batch_id` is no longer
accepted by `ack()` or `nack()`.

Batch allocation is serialized on the main subscription row. That is the right
correctness tradeoff, but it is not infinite scaling. Documentation must warn
that high worker counts can bottleneck on the `for update` lock, and that users
should tune batch size / tick cadence so each allocation does meaningful work.
Adding 50 subconsumers that each poll tiny batches will mostly benchmark row-lock
churn, not queue throughput.

Cooperative consumers fit CPU-bound or I/O-bound handlers where message
processing dominates allocation cost. Normal fan-out consumers remain better for
ultra-high-frequency, low-latency streams where each consumer should advance its
own cursor without coordinating with sibling workers.

`dead_interval` is not a visibility timeout. It is based on `sub_active`. A
healthy worker running a long handler can be stolen if it does not heartbeat and
`dead_interval` is too low. Set `dead_interval` above worst-case handler runtime
or call `pgque.touch_subconsumer()` from long-running handlers to refresh
`sub_active`.

## Active batch unregistration

The active-batch force path must never drop messages.

`unregister_subconsumer(..., batch_handling = 0)`:

- If the subconsumer has no active batch, unregister it.
- If the subconsumer has an active batch, raise an exception and leave all state
  unchanged.

`unregister_subconsumer(..., batch_handling = 1)`:

- If the subconsumer has no active batch, unregister it.
- If the subconsumer has an active batch, atomically route every message in that
  batch through retry/dead-letter handling for the shared `sub_id`, invalidate
  the active `batch_id`, then clear the batch state and unregister the
  subconsumer.
- If retry/DLQ routing fails for any message, abort the transaction and leave
  the subconsumer registered with its active batch intact.

Forced unregister is semantically equivalent to nacking every message in the
active batch with a PgQue-generated reason such as `subconsumer unregistered`.
It must follow the same retry-count, retry-delay, retry-after, and dead-letter
rules as `pgque.nack()`; do not bypass normal retry policy unless a future
design explicitly changes that policy. A plain "clear active batch" operation is data
loss because the main group cursor was already advanced when the batch was
assigned.

Forced unregister must invalidate the old active `batch_id` in the same
transaction that routes messages and removes the subconsumer row. Late
`ack(old_batch_id)` or `nack(old_batch_id, ...)` from the old worker must not
affect any new delivery.

## Name handling

The SQL layer should validate queue, consumer, and subconsumer names with the
same rules used by the existing PgQue API.

For PgQue 0.2, keep name handling simple and collision-free: reject `.` in both
logical consumer names and subconsumer names for cooperative APIs. This avoids
ambiguous internal names such as `billing.us.worker-1` meaning either
`consumer = billing.us, subconsumer = worker-1` or
`consumer = billing, subconsumer = us.worker-1`.

Clients must expose `consumer` and `subconsumer` as separate arguments. Do not
make users manually construct internal names.

Documentation should recommend globally stable, unique subconsumer names per
logical consumer, for example hostname, process id, or deployment instance id.

## Grants and roles

Cooperative consume functions are reader-side functions.

Grant to `pgque_reader`:

- `register_subconsumer`
- `unregister_subconsumer`
- `subscribe_subconsumer`
- `unsubscribe_subconsumer`
- cooperative `next_batch` overloads
- cooperative `next_batch_custom`
- `receive_coop`
- `touch_subconsumer`

Do not grant them to `pgque_writer`.

## Client library plan

All three client libraries need first-class support.

### Go

Add low-level methods:

```go
Subscribe(ctx, queue, consumer string) (int, error)
Unsubscribe(ctx, queue, consumer string) (int, error)
SubscribeSubconsumer(ctx, queue, consumer, subconsumer string) (int, error)
UnsubscribeSubconsumer(ctx, queue, consumer, subconsumer string, opts ...UnsubscribeOption) (int, error)
ReceiveCoop(ctx, queue, consumer, subconsumer string, opts ...ReceiveCoopOption) ([]Message, error)
TouchSubconsumer(ctx, queue, consumer, subconsumer string) (int, error)

ReceiveCoop options:
WithMaxMessages(n int)
WithDeadInterval(d time.Duration)

Unsubscribe options:
WithBatchHandling(mode int)
```

Add high-level option:

```go
client.NewConsumer(
    "orders",
    "billing",
    pgque.WithSubconsumer("worker-1"),
    pgque.WithDeadInterval(5*time.Minute),
)
```

If `WithSubconsumer()` is absent, keep using normal `Receive()`.

### Python

Add client methods:

```python
subscribe(queue, consumer) -> int
unsubscribe(queue, consumer) -> int
subscribe_subconsumer(queue, consumer, subconsumer) -> int
unsubscribe_subconsumer(queue, consumer, subconsumer, batch_handling=0) -> int
receive_coop(queue, consumer, subconsumer, max_messages=100, dead_interval=None) -> list[Message]
touch_subconsumer(queue, consumer, subconsumer) -> int
```

Add high-level constructor argument:

```python
Consumer(
    client,
    queue="orders",
    name="billing",
    subconsumer="worker-1",
    dead_interval="5 minutes",
)
```

If `subconsumer is None`, keep using normal `receive()`.

### TypeScript

Add client methods:

```ts
subscribeSubconsumer(queue, consumer, subconsumer): Promise<number>
unsubscribeSubconsumer(
  queue,
  consumer,
  subconsumer,
  options?: { batchHandling?: 0 | 1 },
): Promise<number>

receiveCoop(
  queue,
  consumer,
  subconsumer,
  options?: { maxMessages?: number; deadInterval?: string },
): Promise<Message[]>

touchSubconsumer(queue, consumer, subconsumer): Promise<number>
```

Add high-level consumer option:

```ts
client.newConsumer("orders", "billing", {
  subconsumer: "worker-1",
  deadInterval: "5 minutes",
})
```

If `subconsumer` is absent, keep using normal `receive()`.

## Documentation plan

Update:

- `README.md`
- `docs/tutorial.md`
- `docs/examples.md`
- `docs/reference.md`
- client README files

Add a section named "Fan-out vs cooperative consumers".

Every user-facing mention must mark the feature **experimental** for 0.2.

Document:

- normal consumers each receive every event
- subconsumers under one consumer split batches
- each worker should use a stable unique subconsumer name
- `ack()` still closes the whole batch
- `max_return` still has the existing partial-batch caveat
- `dead_interval` enables stale-worker takeover, but long handlers need either
  conservative intervals or `touch_subconsumer()` heartbeats
- `touch_subconsumer()` refreshes `sub_active` only for an existing
  `coop_member`; it may touch idle or active members, must not create rows, and
  stale takeover only considers active rows with `sub_batch is not null`
- `nack()` behavior is unchanged
- forced subconsumer unregister retries or dead-letters active messages; it does
  not discard them
- cooperative consumer throughput can bottleneck on the main subscription row
  lock if many workers poll tiny batches
- PgQue 0.1 clients and downgrade paths do not understand cooperative rows;
  unregister subconsumers before downgrade or mixed-version rollback

## Test plan

SQL tests:

1. `register_subconsumer()` is idempotent.
2. `receive_coop()` / cooperative `next_batch()` auto-create the main consumer
   and subconsumer when missing.
3. Two subconsumers under one logical consumer split batches without duplicate
   delivery.
4. Repeated receive by the same active subconsumer returns the same active batch.
5. `ack()` on a cooperative batch clears the subconsumer row without advancing a
   subconsumer cursor.
6. Stale takeover moves an active batch from a dead sibling to the current
   subconsumer under a fresh `batch_id`.
7. Unregistering an active subconsumer fails with `batch_handling = 0`.
8. Unregistering an active subconsumer with `batch_handling = 1` routes all
   active-batch messages through the same retry/dead-letter policy as
   `pgque.nack()` with a PgQue-generated reason, then unregisters the
   subconsumer. No message is skipped.
9. A forced unregister that fails during retry/DLQ routing leaves the active
   batch and subconsumer intact.
10. Unregistering the main consumer removes all sibling subconsumers only after
    applying the same active-batch safety rule to any active sibling batches.
11. `nack()` from a cooperative batch writes retry or dead-letter state using
    the shared `sub_id` and redelivery works.
12. Existing normal fan-out consumers still each receive all events.
13. Existing `receive()`, `ack()`, `nack()`, `subscribe()`, and `unsubscribe()`
    behavior remains unchanged.
14. Two-session allocation test proves concurrent subconsumers cannot allocate
    the same new batch.
15. Late `ack(old_batch_id)` after stale takeover does not finish the new
    owner's batch.
16. Late `nack(old_batch_id, ...)` after stale takeover does not retry/DLQ
    messages owned by the new batch token.
17. Late `ack()` / `nack()` after forced unregister does not affect any new
    delivery.
18. Normal `receive()` / `next_batch()` for a `coop_main` consumer raises while
    subconsumer rows exist; cooperative receive still works, and other normal
    consumers on the same queue are unaffected.
19. Dotted logical consumer or subconsumer names are rejected by cooperative
    APIs.
20. High-poll worker test documents main subscription lock contention without
    duplicate or skipped batches.
21. `touch_subconsumer()` refreshes `sub_active` for idle and active
    `coop_member` rows, does not create missing rows, and stale takeover ignores
    idle rows.
22. Upgrade/migration preserves existing normal subscriptions and assigns
    `sub_role = 'normal'`.
23. Registering the first subconsumer fails if the normal consumer has an active
    batch.

Client tests for each library:

1. Subscribe and unsubscribe subconsumer.
2. Low-level `receive_coop()` receives messages and `ack()` finishes them.
3. High-level consumer with subconsumer dispatches handlers and acks normally.
4. Handler failure still calls `nack()` and skips `ack()` if `nack()` fails.
5. Existing high-level consumer without subconsumer remains backward compatible.

## Implementation order

1. SQL core functions and grants.
2. Cooperative-aware `finish_batch()`.
3. SQL regression tests.
4. Reference docs and examples, all marked experimental.
5. Go client API and tests, docs marked experimental.
6. Python client API and tests, docs marked experimental.
7. TypeScript client API and tests, docs marked experimental.
8. README roadmap update showing experimental status.
9. Full SQL and client test suite.

## Parallelization plan

Use separate git worktrees from the same approved base branch. Keep SQL core as
the integration point and avoid overlapping edits.

Suggested work split after this blueprint is approved:

1. SQL core owner
   - files: `sql/pgque.sql`, `sql/pgque-tle.sql`, SQL tests, grants
   - must land first or expose a stable branch for client owners

2. Documentation owner
   - files: `README.md`, `docs/*.md`, client README examples
   - can start from this blueprint, but should wait for final SQL function names
     before finalizing reference docs

3. Go client owner
   - files: `clients/go/**`
   - depends on stable SQL signatures

4. Python client owner
   - files: `clients/python/**`
   - depends on stable SQL signatures

5. TypeScript client owner
   - files: `clients/typescript/**`
   - depends on stable SQL signatures

Do not run multiple agents in the same worktree. Do not edit the same files from
two worktrees unless one owner is explicitly rebasing after the other lands.

Before spawning implementation agents, check current open PgQue work on this VM
and on GitHub. In particular, avoid overlapping with active OpenClaw/Leo work on
client-library fixes and receive/ack semantics.
