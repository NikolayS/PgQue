# Durable paged batches for 0.3

Status: implemented candidate; CI, review and release acceptance pending.
Tracking: #364; bounded-invocation use case: #362.

## Goal and boundary

Process at most N events per invocation without acknowledging unseen events.
A committed page acknowledgment survives reconnect and process loss. An
unacknowledged page is redelivered. External side effects remain at-least-once;
applications need idempotency. Database effects can share the acknowledgment
transaction. Receiving a page is never proof that it was processed.

Keep the whole-batch receive API and its fail-closed ceiling unchanged. Do not
change the frozen 0.2.1 artifacts under `sql/`. This is an additive development
API, not an alteration of PgQ's snapshot membership or rotation algorithm.

## Implemented public SQL surface

- `receive_page(queue, consumer, worker, page_size := 100, lease := '60 seconds')`
- `receive_page_coop(queue, consumer, subconsumer, worker, page_size := 100,
  dead_interval := null, lease := '60 seconds')`
- `receive_page_partitioned(queue, consumer, slot, n, worker, page_size := 100)`
- `ack_page(page_token uuid, worker text, failures jsonb := '[]')`
- `renew_page(page_token uuid, worker text)`

All modes require prior explicit subscription setup; paged receive does not
auto-register identities. This avoids hidden registration mutations during a
busy-page poll. `worker` is an explicit unique process-instance identity, not a
reusable deployment name. Within the shared reader-role trust boundary, it is the
effective credential: same-worker receive returns the pending page and token.
Use a random ID per process, do not reuse it across processes, and omit it from
logs. A page token does not compensate for a guessable worker name.
`page_size` is a positive, non-null int4. It is a real page size, not the legacy
complete-batch safety ceiling. Leases must be finite positive intervals.

A receive returns one `batch_page` envelope:

- `status`: `page`, `idle`, `advanced`, or `busy`;
- `batch_id`: current PgQ ownership token, for diagnostics, not acknowledgment;
- `page_token`: opaque random UUID identifying this issued page and ownership;
- `page_number`: monotonically increasing within the logical batch;
- `is_last`: proven by one-row lookahead in the same filtered batch query;
- `messages`: an array of at most `page_size` existing `pgque.message` values;
- `lease_until`: current claim deadline, also returned for `busy`;
- `fence_epoch`: partition lease epoch, null for other modes.

Only `page` exposes a token/messages/page number/terminal flag. In normal and
cooperative modes, `busy` exposes no competing worker identity or token.
Partition mode uses slot ownership and never returns `busy`. `idle` and
`advanced` have empty message arrays and null claim metadata. Page numbers are
one-based bigint diagnostics; they survive cooperative transfer and reset only
on a new logical tick window.

SDKs do not decode composite arrays. They expand them in the same SQL statement:

```sql
select
    p.status, p.batch_id as page_batch_id, p.page_token::text,
    p.page_number, p.is_last, p.lease_until, p.fence_epoch,
    m.msg_id, m.batch_id, m.type, m.payload, m.retry_count,
    m.created_at, m.extra1, m.extra2, m.extra3, m.extra4, m.ordinality
from pgque.receive_page($1, $2, $3, $4, $5) as p
left join lateral unnest(p.messages) with ordinality as m on true
order by m.ordinality;
```

The left join preserves one metadata-only row for non-page statuses; a null
`msg_id` marks that row (real IDs are non-null, including zero/negative IDs).
Page metadata must agree across all rows. Standard scalar driver decoding keeps
int8 precision without JSON-number conversion.

Empty tick windows are finished internally, without manufacturing a nonempty
page. One invocation inspects at most one batch window, so long quiet histories
do not create an unbounded internal polling loop. `advanced` means an empty
window was finished and another invocation may poll immediately; `idle` means
no next window is currently available. Neither claims future producers are idle.

SQLSTATEs: `22023` for invalid arguments or failure descriptors; `55000` for
forbidden legacy mutation of a paged batch; `PQP01` for a stale/wrong-owner token
or fenced partition epoch; `PQP02` for an ack replay with different failure
descriptors; `21000` for ambiguous duplicate IDs or changed membership; `40001`
for concurrent routing changes, a renewed cooperative victim, or a busy
administrative force-drop; and `P0001` for receive-time cooperative membership
or partition-slot setup errors.

An ack returns `acked` or `already_acked`, plus `batch_finished`. Invalid,
reassigned or superseded tokens raise a distinct stale-page error. Idempotent
ack retries are guaranteed for the most recently acknowledged page retained
on a subscription. Keep this receipt after final batch completion and subsequent
page issuance; overwrite it only when a later page ack commits. Subscription
deletion ends the guarantee. An older token can become stale after that overwrite;
stale does not prove its earlier ack failed. This is not an unbounded receipt store.
Return the originally recorded `batch_finished` value on receipt replay. Check
receipt and its worker/request fingerprint under locks before rejecting a token.

## Durable state and transitions

One side-table row per `(sub_queue, sub_consumer)`, with a foreign key to the
subscription and cascade deletion. No per-event processing flags or UPDATEs.
Typed fields store the active batch/window, mode, acknowledged event key,
page number, pending page boundary/size/token, worker/lease, and the last ack
receipt. Partition mode also stores its slot context and issued lease epoch, but uses
`partition_slot` as its sole lease authority, not a second page-expiry clock.
The active paging guard lasts continuously from first paging allocation through
terminal ack, including intervals between pages with no outstanding token.

1. Lock/allocate the batch using its existing normal, cooperative or partition
   path. Lock the subscription before page state. The cooperative allocator uses
   the private paging-aware `_next_batch_coop(..., i_paged)` allocator so transfer
   remains atomic; a wrapper cannot reconstruct victim ownership after the
   existing allocator clears it. Normal and partition allocation use the private
   `_next_batch_custom(..., i_paged)` allocator. Both private functions are
   revoked from application and admin roles; public legacy wrappers call them
   with `i_paged = false`, while paged receive calls them with `true`.
2. An outstanding page held by another live worker returns `busy`. The same
   worker receives the same token, boundaries and rows, even if it supplies a
   different page size on retry. Pending-page size is immutable.
3. After a page lease expires, a new worker may take over. Preserve the last
   committed checkpoint and outstanding page boundary; issue a new token.
   The old token can no longer ack or nack. Lease expiry alone does not make
   an external side effect safe; applications still need idempotency/fencing.
4. With no outstanding page, query at most N+1 rows after the acknowledged key,
   issue N rows and persist their boundary plus terminal flag. Receipt does
   not advance the acknowledged key or the PgQ cursor.
5. `ack_page` revalidates token, worker, current batch ownership and (for a
   partition) lease epoch under locks. It commits the page checkpoint and
   releases the page claim. For an issued terminal page: record the receipt,
   mark the paging guard inactive, then call `finish_batch`, all while holding
   the subscription/page locks in the same transaction. A rollback restores
   both checkpoint and guard; no public bypass flag is permitted.
6. A crash before ack commit replays that page. A lost ack response can retry
   the same token against the retained receipt. A later receiver starts after
   the committed checkpoint, not at the beginning of the batch.

A page lease covers worker processing, not just the receive statement. Persist
the chosen TTL on issuance; retries cannot change it. Same-worker receive and
`renew_page` renew it using `clock_timestamp()`. An expired owner may renew/ack
if no successor has replaced its token. After replacement every old mutation
fails. Partition renewal uses the existing slot TTL and revalidates the issued
epoch under the slot lock. The low-level API exposes renewal; initial one-shot
SDK helpers do not start background heartbeat threads. Callers must bound handler
runtime below their chosen lease or use the low-level API and explicit renewal.

For cooperative cross-member takeover, both the existing dead-interval predicate
and any outstanding plain/coop page lease must have expired. A legacy allocator
must not steal paged victims. Page renewal also refreshes member `sub_active`.

## Ownership and legacy interoperability

- Cooperative takeover creates a new PgQ batch token. Transfer the checkpoint
  and pending boundary to the new member before clearing the victim, atomically
  in the allocator under main -> current member -> victim -> page locks. Invalidate
  the old page token. Transfer only active logical-batch fields, preserving the
  retained ack receipt on each member row. Upsert into the destination without
  erasing its receipt; leave the victim inactive with its own receipt intact.
  Do not key progress solely by shared `sub_id`.
- Partition takeover can retain the PgQ batch ID. Every page mutation must also
  compare the issued partition epoch, including an ABA return to the same
  worker name. Partition lock precedes subscription/page locks.
- Legacy `receive`, `receive_coop`, `receive_partitioned`, `ack`,
  `ack_partitioned`, `nack`, and `nack_partitioned` must reject an actively
  paged batch. The common `finish_batch` path must enforce the guard too;
  blocking only the convenience `ack` wrapper is insufficient.
- Low-level read access need not be hidden, but cannot authorize finishing an
  incomplete paged batch. Also guard `event_retry` overloads and `batch_retry`
  plus cursor-reset/move paths executable by application roles. Atomic page
  completion uses a private retry core after canonical page membership checks;
  it must not call a guarded public retry wrapper or use a caller-settable bypass.
- Reject normal/forced cooperative unregister while paging is active, regardless
  of `batch_handling`; finish or transfer the page first. Do not reroute already
  acknowledged pages. Explicit administrative queue destruction remains
  destructive and deletes checkpoints; do not silently treat application
  unsubscribe/reset as administrative force.
- Token-based partition mutations first perform a non-locking routing lookup,
  then lock the slot, subscription and page state in that order. Revalidate
  routing and receipt presence after taking the locks. First return any matching
  retained receipt (token, recorded worker and request) regardless of the current
  slot owner/epoch: that is read-only proof of a past commit. Current token, batch,
  owner and epoch are mandatory for pending mutations, renewal and terminal
  completion. Never lock the page before the slot.
- Force `drop_queue` is administrative destruction, not unregister or ack. It
  deletes subscriptions and cascading page state directly. The development
  implementation takes queue, then ordered partition-slot and subscription
  locks with NOWAIT. A lock held by an ordinary or paged consumer aborts the
  whole operation with `40001`; no deletion commits. Operators retry the whole
  transaction and pause consumers when reliable removal is required.

### Lock order

Normal page operations lock subscription before page state. Partition page
operations first resolve routing without a lock, then lock partition slot,
subscription and page state, and revalidate routing. Cooperative allocation and
takeover lock the main subscription, current member, victim and page state in
that order. Force-drop starts with the queue row and must not wait behind those
consumer paths; its subsequent ordered slot and subscription locks are NOWAIT,
turning contention into `40001` instead of a lock-order cycle. No path may lock
page state before its subscription, or a partition subscription/page before its
slot.

## Traversal identity and boundedness

Use dynamic SQL over `batch_event_sql`, retaining its snapshot and retry-owner
filters. Apply the partition hash filter, key predicate, ordering and lookahead
inside that query. Do not page over the materializing `get_batch_events` SRF,
and do not retain a server cursor across transactions or pooled connections.

`ev_id` is reused by retries and is not schema-enforced unique. Ordinary API
processing is expected to produce one visible occurrence per active batch,
but pre-existing raw inserts or deliberate replay windows may violate that.
Privileged mutation of event rows or rewind behind a committed checkpoint is
unsupported, not something a keyset protocol can repair. Application roles must
not gain those privileges. Proposed safe initial rule: order by `ev_id`, inspect duplicate IDs in
the bounded N+1 candidate set (including the page boundary), and fail closed
on ambiguity before issuing/advancing a page. Negative/zero IDs use a nullable
initial key, not an assumed positive sentinel. Retry, rotation and long-running
producer tests must establish that supported normal operation never trips the
guard. If that premise fails, use a genuinely durable occurrence identity;
`ctid`, table location, or an unproven `(ev_id, ev_txid)` pair are not substitutes.

Bound the returned messages and page buffer. This does not promise O(N) I/O:
the existing transaction-ID indexes and global ordering can still scan/sort a
larger fixed batch. Document that limitation; benchmark/index optimization is
separate and must use dedicated infrastructure.

## Retry and SDK behavior

Do not expose a separate non-atomic page nack in the initial API. `ack_page`
accepts optional failure descriptors and atomically validates all of them, routes
failed events to retry/DLQ, checkpoints the page and writes the ack receipt. This
works with Go/TypeScript pool-based clients without a transaction-affinity claim.

A descriptor contains `msg_id` as a decimal string (preserving bigint precision),
`retry_after_seconds` as a nonnegative int4 (default 60), and optional text
`reason`. Reject duplicate IDs, unknown fields, malformed IDs and IDs outside the
issued page. Canonical event data comes only from the issued immutable interval;
never trust caller-supplied payload or retry count. Messages omitted from failures
are asserted successfully processed. Store the normalized request JSON with
the ack receipt; a repeat token with different failures is an error, not another
retry insertion. Same-token retries return the receipt without rerouting events.

The simple one-shot helper supplies no failures: any handler exception leaves
the page outstanding and propagates, without nack or ack. A lost ack response
is an ambiguous outcome, not proof of rollback. Low-level users can choose the
explicit atomic failure-routing form. External side effects may repeat.

Add an independent Page type and low-level page methods to Python, Go,
TypeScript and Ruby. Do not extend existing Message types or break Go consumer
interfaces. Preserve bigint precision (particularly TypeScript); the array
must be expanded as typed SQL rows, not silently parsed as JSON numbers.

A one-shot page-processing helper accepts queue/consumer/worker/page-size/lease
and a handler, processes at most one page, acknowledges only after every handler
succeeds, and leaves the page outstanding on handler failure. It returns status,
processed count and batch-finished state; idle/advanced/busy return without another
receive. Ack errors propagate without claiming the page remains unacknowledged.
Worker IDs are caller-supplied random process-instance UUID strings, reused only
within that instance/retries, never generated invisibly per API call. Existing
whole-batch consumer loops remain compatible. Unknown handler types must be
explicit errors, not silently skipped acknowledged messages.

## Required acceptance

- Red/green N-1/N/N+1, multiple pages, N=1, invalid and integer-maximum input.
- Actual payload identities and cursor positions; terminal lookahead at exact N.
- Reconnect/process death before receive commit, after delivery, before/after ack
  commit; lost ack response; no replay of committed pages after reconnect.
- Concurrent receivers, lease expiry/takeover, stale tokens, same-worker ABA,
  cooperative ownership transfer and partition filtering before pagination.
- Lost partition-ack response followed by epoch takeover still replays the
  retained committed receipt; pending mutations with old epoch remain fenced.
- Cooperative takeover preserves independent retained receipts on both members.
- Legacy whole-batch ack/finish/nack bypass attempts while a page is outstanding
  and between pages; wrong worker, forged token, and reader-role execution.
- Retry/DLQ with transaction rollback and retry maintenance; duplicate event-ID
  boundary rejection; negative IDs; concurrent/long producer transactions.
- Rotation while a partially consumed batch pins old history.
- Install/reapply/upgrade preserves both active pages and ordinary subscriptions.
- SDK handler failure and ack failure; exact exported types and bigint handling.
- PostgreSQL 14–18 plus the separately labeled preview job; all existing suites.
- CI green, SamoRev on the exact head, executable public examples and posted
  evidence before merge. No 0.3 release or frozen-artifact promotion in this PR.
