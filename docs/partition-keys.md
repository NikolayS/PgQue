---
title: Partition keys
description: Preserve per-key order while scaling a PgQue consumer across leased partition slots.
---

Partition keys split one logical consumer into a fixed number of hash slots.
Events with the same non-null key always route to the same slot, so that slot
preserves their stream order while different slots can be processed in
parallel.

This is a SQL-first API. The first-party clients do not yet provide partition
worker helpers, automatic claim loops, or rebalance logic. Applications call
the functions below through their Postgres driver and own the loop across all
slots.

## Set up every slot before producing

Create the whole logical consumer atomically:

```sql
select pgque.create_queue('orders');
select pgque.subscribe_partitioned(
  queue_name := 'orders',
  consumer := 'workers',
  n := 4
);
```

`subscribe_partitioned` pins `n` for `(queue_name, consumer)` and creates all
`n` engine subscriptions plus their lease rows at one shared starting tick.
The call is transactional: it creates the complete set or nothing. Repeating
the call with the same `n` is idempotent and does not reposition any cursor.
Changing `n` is rejected.

Run this setup before any event that the consumer must see is produced. A
subscription starts at the latest tick; creating or repairing a slot later
cannot recover events already covered by earlier ticks.

## Publish keyed events

The four-argument text and JSON `send` overloads accept a partition key:

```sql
select pgque.send(
  queue_name := 'orders',
  type_name := 'order.updated',
  payload := '{"order_id": 42}'::jsonb,
  partition_key := 'tenant-7:order-42'
);
```

`send_idem` accepts the same optional `partition_key`, so retry-safe publishing
and per-key ordering compose. See [Producer idempotency](producer-idempotency.md).

For a pinned slot count `n`, PgQue computes the slot as:

```sql
((hashtextextended(partition_key, 0) % n) + n) % n
```

The normalization keeps the result in `0..n-1` even when the hash is negative.
A null partition key routes to slot 0. Keyless `send` overloads therefore
remain visible to a partitioned consumer, but can concentrate load on slot 0.

Partition keys use the inherited event column `ev_extra1`. They are supported
for events published through `send` / `send_idem`; PgQue's change-data-capture
triggers already use `ev_extra1` for the table name and are not a partition-key
source.

## Claim and process a slot

Each live worker needs a stable, process-instance-unique id. Claim a slot
before receiving from it:

```sql
select pgque.claim_slot(
  queue_name := 'orders',
  consumer := 'workers',
  slot := 2,
  worker := 'orders-worker-7f3c',
  ttl := interval '30 seconds'
);
```

The call returns the slot's fencing epoch when the claim succeeds, or null when
another live worker owns the slot or its row is busy. A worker claim loop should
try other slots rather than block on one busy slot. Claiming again as the same
owner renews the lease without changing the epoch.

Receive and finish work with the same `(queue, consumer, slot, n, worker)`:

```sql
select *
from pgque.receive_partitioned(
  queue_name := 'orders',
  consumer := 'workers',
  slot := 2,
  n := 4,
  worker := 'orders-worker-7f3c',
  max_return := 500
);

-- On a per-message failure, before acknowledging the batch:
select pgque.nack_partitioned(
  queue_name := 'orders',
  consumer := 'workers',
  slot := 2,
  n := 4,
  worker := 'orders-worker-7f3c',
  msg := :message,
  retry_after := interval '60 seconds',
  reason := 'downstream timeout'
);

select pgque.ack_partitioned(
  queue_name := 'orders',
  consumer := 'workers',
  slot := 2,
  n := 4,
  worker := 'orders-worker-7f3c'
);

select pgque.release_slot(
  queue_name := 'orders',
  consumer := 'workers',
  slot := 2,
  worker := 'orders-worker-7f3c'
);
```

`receive_partitioned`, `nack_partitioned`, and `ack_partitioned` verify the
lease owner and renew the stored TTL. `release_slot` is optional during normal
polling and succeeds only for the owner. It is allowed only at a batch boundary:
ack the open batch first. A crashed worker should not release; expiry is the
recovery path.

As with normal `receive`, `max_return` limits rows returned, but
`ack_partitioned` finishes the whole underlying tick batch. Use a value at least
as large as `ticker_max_count` (500 by default), or otherwise guarantee that
the full batch was returned, before acknowledging it.

## Lease expiry, renewal, and fencing

The default lease TTL is 30 seconds and the minimum accepted TTL is one second.
Choose a TTL longer than ordinary batch processing, or renew it by calling
`claim_slot` again as the same worker in a committed transaction while work is
still running. Receiving and acknowledging also renew, but a long external
side effect between those calls can outlive the lease.

After expiry, another worker may take over and the slot epoch increments. The
old worker is then fenced: its next receive, nack, or ack raises because its
worker id no longer owns the lease. Make downstream effects idempotent because
fencing prevents stale queue acknowledgements, not an external request already
sent before takeover.

Leases are rows updated by normal transactions; there is no advisory lock or
connection-local state. The API therefore works through transaction-mode
poolers such as PgBouncer and Supavisor. Do not use a backend PID or pooled
connection identity as `worker`; use an application process/instance id that
stays stable across transactions and is unique among live workers.

## Poll every slot and budget for amplification

Slots are independent engine consumers named internally as
`<consumer>#<slot>/<n>`. Each one advances its own cursor over the full event
stream and applies the hash filter server-side. This has two operational
consequences:

- Steady-state reads are approximately `n` times the queue stream, even though
  each event is returned by only one slot. Choose the smallest `n` that meets
  the required parallelism.
- Every subscribed slot must be polled. A stopped slot retains an old cursor,
  pins table rotation for the queue, and can cause event-table growth even when
  all other slots are caught up.

An empty `receive_partitioned` result may mean that the current tick window had
no events for that hash class. PgQue advances that empty filtered window
automatically; keep polling.

Monitor all expected slots through `pgque.partition_slot_status`; see
[Monitoring and health](monitoring.md#partitioned-consumers).

## Incomplete setup and recovery

`unsubscribe_slot(queue_name, consumer, slot)` removes one slot. It can leave
the logical consumer incomplete, and it also removes that engine subscription's
retry and dead-letter state. Drain and ack work before controlled teardown.

An incomplete consumer is visible as `subscribed = false` in
`pgque.partition_slot_status`; its `last_tick` and `pending_events` are null
because its lag is unknowable. `subscribe_partitioned` rejects an incomplete
existing setup instead of silently moving a cursor.

`subscribe_slot(queue_name, consumer, slot, n)` exists for explicit repair. Use
it only when you have proved no required events were ticked while the slot was
missing. If history may have been missed, a late repair cannot reconstruct it:
replay or compensate from an application source of truth. Unsubscribing every
remaining slot and calling `subscribe_partitioned` again is safe only before
new required events are produced; it creates a new cursor at the latest tick.

To change `n`, fully remove the old logical consumer and atomically subscribe a
new one during a controlled production pause. Repartitioning changes the hash
mapping, so do not run old and new slot counts as one logical ordered consumer.

For all signatures, defaults, and grants, see the
[Function reference](reference.md#partitioned-consumers).
