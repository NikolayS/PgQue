# Cooperative consumers

Blueprint version: `0.2-draft.4`

## Change log

| Version | Date | Notes |
|---|---|---|
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
- release notes for PgQue core and all client packages
- roadmap table
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

Before implementation, decide whether to add a lightweight marker column such as
`subscription.sub_cooperative boolean not null default false` or expose the state
only through derived introspection. A marker makes mixed-version diagnostics and
filtering simpler, but it is still a schema change. If skipped, docs and
introspection functions must clearly identify subconsumer rows using the shared
`sub_id` invariant.

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
  queue text,
  consumer text,
  subconsumer text,
  min_lag interval,
  min_count int4,
  min_interval interval,
  dead_interval interval default null
) returns record
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
```

`subscribe_subconsumer()` and `unsubscribe_subconsumer()` are modern aliases over
`register_subconsumer()` and `unregister_subconsumer()`.

`ack()` and `nack()` stay unchanged. `batch_id` remains the ownership token.

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
   active batch, lock it, move the batch/tick state to the current subconsumer,
   clear the stale sibling, and return the stolen batch id.
8. Otherwise call the existing main-consumer batch allocator for the logical
   consumer.
9. Immediately advance/close the main consumer row so the group cursor moves
   forward.
10. Copy the allocated batch/tick window into the subconsumer row.
11. Return the batch id.

The main subscription lock is mandatory. Without it, two workers can race and
allocate duplicate or skipped tick windows.

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

- If the target subscription row shares its `sub_id` with more than one
  subscription row, and the target row is not the main cursor row, treat it as a
  cooperative subconsumer batch.
- Otherwise use normal `finish_batch()` behavior.

## Locking and concurrency

Required locks:

- `register_subconsumer()` locks the main subscription row before sharing its
  `sub_id`.
- `next_batch(..., subconsumer)` locks the main subscription row before opening
  a group batch.
- `next_batch(..., subconsumer)` locks the current subconsumer row before
  checking or returning an active batch.
- stale takeover locks the victim row before moving batch state.

Recommended stale takeover query behavior:

- consider only sibling rows with the same `sub_id`
- require `sub_batch is not null`
- require `sub_active < now() - dead_interval`
- use deterministic ordering by `sub_active asc`
- use `for update skip locked` when scanning candidates

Prefer clearing stale sibling batch state over deleting the sibling row during
automatic takeover. Deletion should be reserved for explicit unsubscribe.

Batch allocation is serialized on the main subscription row. That is the right
correctness tradeoff, but it is not infinite scaling. Documentation must warn
that high worker counts can bottleneck on the `for update` lock, and that users
should tune batch size / tick cadence so each allocation does meaningful work.
Adding 50 subconsumers that each poll tiny batches will mostly benchmark row-lock
churn, not queue throughput.

## Active batch unregistration

The active-batch force path must never drop messages.

`unregister_subconsumer(..., batch_handling = 0)`:

- If the subconsumer has no active batch, unregister it.
- If the subconsumer has an active batch, raise an exception and leave all state
  unchanged.

`unregister_subconsumer(..., batch_handling = 1)`:

- If the subconsumer has no active batch, unregister it.
- If the subconsumer has an active batch, atomically route every message in that
  batch through retry/dead-letter handling for the shared `sub_id`, then clear
  the batch state and unregister the subconsumer.
- If retry/DLQ routing fails for any message, abort the transaction and leave
  the subconsumer registered with its active batch intact.

The implementation may use an internal batch-level helper, but the semantics
should be equivalent to nacking each message with a reason such as
`subconsumer unregistered`: messages under retry budget go to immediate retry;
terminal messages go to `dead_letter`. A plain "clear active batch" operation is
data loss because the main group cursor was already advanced when the batch was
assigned.

## Name handling

The SQL layer should validate queue, consumer, and subconsumer names with the
same rules used by the existing PgQue API.

Internally the subconsumer's concrete `pgque.consumer.co_name` can use the
existing `consumer || '.' || subconsumer` convention, but clients must expose
`consumer` and `subconsumer` as separate arguments. Do not make users manually
construct internal names.

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

Do not grant them to `pgque_writer`.

## Client library plan

All three client libraries need first-class support.

### Go

Add low-level methods:

```go
Subscribe(ctx, queue, consumer string) (int, error)
Unsubscribe(ctx, queue, consumer string) (int, error)
SubscribeSubconsumer(ctx, queue, consumer, subconsumer string) (int, error)
UnsubscribeSubconsumer(ctx, queue, consumer, subconsumer string) (int, error)
ReceiveCoop(ctx, queue, consumer, subconsumer string, maxMessages int) ([]Message, error)
```

Add high-level option:

```go
client.NewConsumer("orders", "billing", pgque.WithSubconsumer("worker-1"))
```

If `WithSubconsumer()` is absent, keep using normal `Receive()`.

### Python

Add client methods:

```python
subscribe(queue, consumer) -> int
unsubscribe(queue, consumer) -> int
subscribe_subconsumer(queue, consumer, subconsumer) -> int
unsubscribe_subconsumer(queue, consumer, subconsumer) -> int
receive_coop(queue, consumer, subconsumer, max_messages=100) -> list[Message]
```

Add high-level constructor argument:

```python
Consumer(client, queue="orders", name="billing", subconsumer="worker-1")
```

If `subconsumer is None`, keep using normal `receive()`.

### TypeScript

Add client methods:

```ts
subscribeSubconsumer(queue, consumer, subconsumer): Promise<number>
unsubscribeSubconsumer(queue, consumer, subconsumer): Promise<number>
receiveCoop(queue, consumer, subconsumer, maxMessages = 100): Promise<Message[]>
```

Add high-level consumer option:

```ts
client.newConsumer("orders", "billing", { subconsumer: "worker-1" })
```

If `subconsumer` is absent, keep using normal `receive()`.

## Documentation plan

Update:

- `README.md`
- `docs/tutorial.md`
- `docs/examples.md`
- `docs/reference.md`
- client README files
- release notes for PgQue core and each client package that exposes the API

Add a section named "Fan-out vs cooperative consumers".

Every user-facing mention must mark the feature **experimental** for 0.2.

Document:

- normal consumers each receive every event
- subconsumers under one consumer split batches
- each worker should use a stable unique subconsumer name
- `ack()` still closes the whole batch
- `max_return` still has the existing partial-batch caveat
- `dead_interval` enables stale-worker takeover
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
2. `receive_coop()` can auto-create the main consumer and subconsumer if that is
   the chosen API behavior.
3. Two subconsumers under one logical consumer split batches without duplicate
   delivery.
4. Repeated receive by the same active subconsumer returns the same active batch.
5. `ack()` on a cooperative batch clears the subconsumer row without advancing a
   subconsumer cursor.
6. Stale takeover moves an active batch from a dead sibling to the current
   subconsumer.
7. Unregistering an active subconsumer fails with `batch_handling = 0`.
8. Unregistering an active subconsumer with `batch_handling = 1` routes all
   active-batch messages to immediate retry or dead letter, then unregisters the
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
15. High-poll worker test documents main subscription lock contention without
    duplicate or skipped batches.

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
5. Release notes, marked experimental.
6. Go client API and tests, docs marked experimental.
7. Python client API and tests, docs marked experimental.
8. TypeScript client API and tests, docs marked experimental.
9. README roadmap update showing experimental status.
10. Full SQL and client test suite.

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
