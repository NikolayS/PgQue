---
title: Producer idempotency
description: Safely retry PgQue sends with queue-scoped idempotency keys and bounded deduplication windows.
---

`pgque.send_idem()` lets a producer retry an enqueue after a timeout or lost
response without appending the same logical effect twice. The first call
appends an event and returns its id. Calls with the same queue and idempotency
key during the live TTL window return that original id without appending.

Producer idempotency prevents duplicate **sends**. It does not change PgQue's
at-least-once delivery contract, so consumers must still make external side
effects idempotent or commit database side effects and the batch ack in one
transaction.

## Send with an idempotency key

The text and JSON overloads have the same named signature:

```text
pgque.send_idem(queue_name, type_name, payload, idem_key,
                ttl default '1 hour', partition_key default null)
```

Use an explicit `::jsonb` cast when you want JSON validation and canonical
storage:

```sql
select *
from pgque.send_idem(
  queue_name := 'orders',
  type_name := 'order.created',
  payload := '{"order_id": 42}'::jsonb,
  idem_key := 'tenant-7:order-42:create:v1',
  ttl := interval '24 hours'
);
```

The first accepted call returns one row with `deduped = false`. A duplicate
during the same window returns the first call's `event_id` with
`deduped = true`:

```text
 event_id | deduped
----------+---------
       17 | f

 event_id | deduped
----------+---------
       17 | t
```

The claim and append happen in the caller's transaction. If the transaction
rolls back, both roll back, so a failed send never leaves a key claimed without
an event. Concurrent producers racing on the same key serialize to one append.

## Key and TTL semantics

Deduplication is an exact match on `(queue_name, idem_key)`:

- The same key may be used independently on different queues.
- PgQue does not compare event type, payload, TTL, or partition key on a
  duplicate. Reusing a live key with different content still returns the first
  event id and suppresses the new event.
- Choose a key for the intended effect, not only the entity. Include the tenant,
  operation, entity id, and an operation version where appropriate, for example
  `tenant-7:order-42:capture:v2`.
- `idem_key` must be non-null. `ttl` must be a positive interval and defaults
  to one hour.
- The window starts with the accepted send. Duplicate attempts do not extend
  it. After expiry, the same key can append a new event and start a new window.

The TTL is a retry-deduplication window, not a rate limiter. Use a new key for a
new logical effect even when it targets the same entity.

## Compose with partition keys

`partition_key` is optional and independent from deduplication. Supplying it
routes the accepted event through a partitioned consumer while the idempotency
key still controls whether the event is appended:

```sql
select *
from pgque.send_idem(
  queue_name := 'orders',
  type_name := 'order.updated',
  payload := '{"order_id": 42}'::jsonb,
  idem_key := 'tenant-7:order-42:update:8',
  ttl := interval '6 hours',
  partition_key := 'tenant-7:order-42'
);
```

See [Partition keys](partition-keys.md) for the required consumer setup and
worker loop.

## Maintenance and capacity

The first idempotent send on a queue registers `pgque.maint_idem` as an
extra-maintenance hook for that queue. A normal `pgque.maint()` schedule removes
expired rows in bounded batches. Expired keys are also reclaimable by a new
send immediately; maintenance controls table size, not correctness.

Operators can run cleanup directly:

```sql
select pgque.maint_idem('orders'); -- one queue
select pgque.maint_idem();         -- all queues
```

Both forms return `1` when they deleted a full 10,000-row batch and should be
called again, otherwise `0`. They require `pgque_admin`; `send_idem` requires
`pgque_writer`.

Live claim-table size is approximately the idempotent send rate multiplied by
the TTL. Keep the TTL only as long as producers can legitimately retry, run
`pgque.maint()` regularly, and monitor unexpectedly long windows or rapid key
growth.

The claim table is internal and is revoked from application roles, including
`pgque_admin`. The schema/install owner can inspect aggregate size without
exposing keys or payloads:

```sql
select q.queue_name, count(*) as claim_rows,
       min(i.expires_at) as next_expiry,
       max(i.expires_at) as last_expiry
from pgque.idem i
join pgque.queue q on q.queue_id = i.queue_id
group by q.queue_name
order by claim_rows desc;
```

## Failure checklist

- Treat a lost response as unknown and retry with the **same** key.
- Treat a new logical effect as new work and use a **new** key.
- Persist or deterministically derive keys so a process restart does not invent
  a replacement key for the same effect.
- Keep producer retries inside the TTL window you selected.
- Keep consumer handlers idempotent; producer deduplication cannot fence an
  external side effect after consumer redelivery.

For exact overloads and grants, see the [Function reference](reference.md#producer-idempotency).
