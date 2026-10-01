---
title: Bounded batch processing
description: Durable page checkpoints, ownership fencing, and bounded one-shot consumption.
---

## Availability

This API is present only in the development installer, `devel/sql/pgque.sql`.
Existing whole-batch `receive` calls keep their complete-batch-or-error
behavior; `max_return` remains a safety ceiling, not pagination.

A page contains at most `page_size` events. Receiving a nonempty page does not
advance its durable checkpoint. The `advanced` result is the exception: receive
finishes one empty tick window internally. Only a committed `ack_page` advances
a nonempty page checkpoint; the final page ack finishes the underlying batch.
Never acknowledge a page before processing every message not explicitly listed
as failed.

## One invocation, at most N messages

Run setup and publishing as separate committed statements before consuming:

```sql
select pgque.create_queue('page_demo');
select pgque.subscribe('page_demo', 'jobs');
select pgque.send('page_demo', 'job', 'first'::text);
select pgque.send('page_demo', 'job', 'second'::text);
select pgque.force_next_tick('page_demo');
select pgque.ticker('page_demo');
```

Each worker supplies a unique process-instance identity. Keep it across retries
in that process; do not reuse a deployment name across competing processes.
The following executable example processes one page inside a transaction:

```sql
do $$
declare
    p pgque.batch_page;
    m pgque.message;
begin
    p := pgque.receive_page('page_demo', 'jobs', 'example-instance-1', 1);
    if p.status = 'page' then
        foreach m in array p.messages loop
            -- Replace this with the application operation.
            raise notice 'processing %: %', m.msg_id, m.payload;
        end loop;
        perform pgque.ack_page(p.page_token, 'example-instance-1');
    end if;
end $$;
```

Repeat the invocation to process the next page. Production callers should use a
random process-instance ID, such as a UUID string, instead of the example ID.
Within the trusted reader group, the worker name is the effective credential:
same-worker receive returns the pending page and its token. Keep the name stable
for the process lifetime, generate it randomly, do not reuse it across processes,
and omit it from logs. The token does not compensate for a guessable worker name.
For external work, commit receive first, process the returned messages, then ack
in another transaction. External effects are **at-least-once**: a crash can
repeat them, so handlers need idempotency. Database effects can share the ack
transaction for atomic checkpointing.

## SQL API

All public page functions are granted to `pgque_reader`. Subscribe explicitly
before receiving, including cooperative members and partition slots.

For cooperative paging, register each member first with
`pgque.register_subconsumer(queue, consumer, subconsumer)`. Queue, consumer and
subconsumer names must be nonempty; cooperative consumer and subconsumer names
must not contain `.`. The cooperative receive does not auto-register members.
Its default `dead_interval := null` disables takeover; pass a positive interval
to allow takeover from an inactive member after its page lease also expires.

| Function | Result |
| --- | --- |
| `receive_page(queue, consumer, worker, page_size := 100, lease := '60 seconds')` | `batch_page` |
| `receive_page_coop(queue, consumer, subconsumer, worker, page_size := 100, dead_interval := null, lease := '60 seconds')` | `batch_page` |
| `receive_page_partitioned(queue, consumer, slot, n, worker, page_size := 100)` | `batch_page` |
| `ack_page(page_token, worker, failures := '[]'::jsonb)` | One row: `status`, `batch_finished` |
| `renew_page(page_token, worker)` | New `timestamptz` lease deadline |

The table uses abbreviated argument names; positional calls are shown in examples.
The SQL parameter names have the `i_` prefix (`i_queue`, `i_consumer`,
`i_worker`, `i_page_size`, `i_lease`, and so on).

The `batch_page` envelope contains `status`, `batch_id`, `page_token`,
`page_number`, `is_last`, `messages`, `lease_until`, and `fence_epoch`.
Only `status = 'page'` supplies a token, page number, terminal flag and messages.
Other statuses have an empty message array:

- `idle`: no next tick window is available.
- `advanced`: one empty window was completed; another invocation may poll again.
- `busy`: in normal and cooperative modes, a different worker holds a live page
  lease; the deadline is exposed, but not that worker's identity or token.
  Partition mode uses slot ownership and does not return `busy`.

One invocation inspects at most one tick window. `advanced` is not proof the
queue is empty. A repeated receive by the same owner returns the same pending
page even if a different size is requested. Page size is a positive int4;
leases must be finite and positive.

## Leases, crashes and ownership

Normal and cooperative pages retain the TTL selected when the page was issued.
Same-worker receive and `renew_page` renew that TTL; changing the argument on a
retry does not change the outstanding page. An expired owner may still ack or
renew until a successor replaces its token. Once replaced, the old token is
fenced. Expiry alone cannot undo an external effect.

Cooperative takeover requires both the member dead interval and its outstanding
page lease to have expired. It preserves acknowledged progress and the pending
page boundary, but changes batch and page tokens. Do not persist batch IDs as
page acknowledgments.

Partition pages use the existing slot lease and epoch, not a second lease.
Call `subscribe_slot` and `claim_slot` before receiving. Every pending ack or
renewal checks the issued epoch, including ownership returning to the same
worker name after another owner. The partition hash filter runs before paging.

A handler failure leaves the page outstanding. If the ack response is lost,
retry the same token, worker and failure descriptors. The latest committed ack
receipt returns `already_acked` with its original `batch_finished` value, even
after takeover or final completion. It survives subsequent page issuance and
is replaced only by the next committed ack on that subscription. Subscription
deletion removes it. An older token becoming stale does **not** prove its ack
failed. Receipt replay proves a past commit; it does not authorize new work.

## Atomic retry and dead-letter routing

To checkpoint a page with selected failures, pass descriptors to `ack_page`:

```sql
-- Substitute the received token and the failed message ID.
-- IDs are decimal strings, not JSON numbers.
select * from pgque.ack_page(
    '00000000-0000-0000-0000-000000000001'::uuid,
    'example-instance-1',
    '[{"msg_id":"9007199254740993","retry_after_seconds":60,"reason":"timeout"}]'::jsonb
);
```

This illustrates the call shape; the placeholder token is not executable work.
Each ID must belong to the issued page. Duplicate IDs, unknown fields, malformed
values and out-of-page IDs are rejected. Delay is a nonnegative int4, default
60 seconds; reason is optional text. Canonical payload and retry count come
from the engine. Failure routing, page checkpoint and ack receipt commit
atomically. Retrying the identical normalized request never adds another retry;
a different request for the retained token is an error. Omitted messages are
asserted successfully processed. There is no separate page nack operation.
Failed messages retry up to the queue's `max_retries` setting (effective default
5); once the stored retry count reaches that ceiling, `ack_page` routes them to
`pgque.dead_letter` instead of scheduling another retry.

| SQLSTATE | Meaning |
| --- | --- |
| `22023` | Invalid page argument or failure descriptor |
| `55000` | Legacy mutation of an active page or incompatible active batch state |
| `PQP01` | Stale page token, wrong worker, or fenced partition epoch |
| `PQP02` | Retained ack replay with a different failure request |
| `21000` | Ambiguous duplicate event IDs or changed pending-page membership |
| `40001` | Concurrent routing changed, a takeover victim renewed, or administrative force-drop found a locked subscription/slot; retry the whole transaction |
| `P0001` | Cooperative membership or partition-slot setup is missing or incompatible at receive time |

Legacy whole-batch ack, finish, retry, cursor reset and unsubscribe cannot bypass
an active page, including the interval between page acknowledgments. Finish
paging before switching back to the whole-batch API.

`drop_queue(queue, true)` is administrative destruction, not an unregister or
acknowledgment path. In the development installer it deletes subscriptions and
their page checkpoints directly. It uses NOWAIT for every attached partition
slot and subscription, whether paged or ordinary: if any is locked, the entire
drop aborts with `40001` and commits no changes. Retry the whole transaction.
Pause consumers before force-drop when reliable removal of a busy queue is
required.

## Limits and client behavior

The returned page and buffered lookahead are bounded by N and N+1. This is not
an O(N) disk-I/O guarantee: the existing batch query can scan or sort more rows.
Partial batches still pin history needed by the consumer; monitor consumer lag.

Traversal assumes immutable engine-managed batch membership. Retry IDs are
reused, so an ambiguous duplicate in the bounded candidate set raises an error
before delivery. Privileged event-table mutation or rewinding behind an already
committed checkpoint is outside the protocol. Do not grant raw table mutation
to application roles.

Page tokens and worker names provide stale-owner fencing, not per-role
authorization. `pgque_reader` is a shared trust boundary: any holder can receive
from any queue, and a same-worker receive reveals that worker's pending token.
Do not share one PgQue install among mutually untrusted readers; isolate them by
database or enforce ownership in app-controlled wrappers.

The additive SDK page helpers process one page per invocation. They do not alter
existing whole-batch consumer loops, auto-nack on a handler exception, or start
background lease-renewal threads. Bound processing time below the lease or use
explicit `renew_page`. Ack errors propagate as ambiguous outcomes. SDK queries
expand composite messages into typed SQL scalar columns, preserving bigint IDs
without JSON-number rounding.
