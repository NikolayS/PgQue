# Idempotency & dedup: design decision and prior art

Status: internal design note (not for the README/docs; basis for the Slack
reply to Fabrizio and for the eventual feature PRs). Refs issue #293.

This note records *why* pgque's idempotency feature has the shape it does. The
short version: pgque is a **log**, not a job queue, and that single fact
determines the entire design.

---

## 1. TL;DR — the decision

Fabrizio asked for two things, framed as one ("idempotency keys") plus one
("one job at a time per partition key"). After working it through, they are
**two separate features at two different layers**, and the split is forced by
the log model:

1. **Producer idempotency = a TTL/window dedup, enforced at produce time.**
   A duplicate `send` with the same key inside a time window is a no-op that
   returns the original event id. Append-only, garbage-collected by the
   existing table rotation. This is what SQS, NATS JetStream, and the RabbitMQ
   dedup plugin all do — because they are logs/brokers too.

2. **"Free once processed" (pg-boss `singletonKey`) = a consumer-side,
   per-consumer key lease.** This is the "one in-flight per partition key"
   feature. It lives on the read side because that is the *only* place where
   "processed" is a well-defined fact.

The thing that *cannot* exist: a producer-side "reject the duplicate until the
prior one is processed" in a log. Section 3 explains why, three independent
ways. Section 5 is the prior-art evidence.

---

## 2. The model: pgque is a log, not a job queue

PgQ (and therefore pgque) is an append-only event **log** with independent
consumer cursors:

- Producers append events to the current data table. Events are **never**
  updated or deleted on consumption — `finish_batch` only advances a
  per-subscription tick cursor (`subscription.sub_last_tick`). Events physically
  vanish only when table rotation `TRUNCATE`s their child table.
- A queue can have **many independent consumers** (fan-out). Each has its own
  cursor. An event can be done for consumer A and still pending for consumer B.
- Rotation recycles the oldest of N child tables (default 3) every
  `queue_rotation_period` (default 2h), and only once no consumer still needs
  it. **Rotation is the only garbage collector.**

A job queue (pg-boss, Oban, River, Graphile Worker) is the opposite: each job
is a **mutable row** consumed once by one logical worker pool, carrying a
`state` column that is `UPDATE`d (`created → active → completed`). "Processed"
is a global, singular property of the row.

That difference is the whole story.

---

## 3. Why "free once processed" cannot be a producer feature in a log

Three independent arguments, all pointing the same way.

### 3.1 The model argument

"Processed" is a **per-consumer** fact. In a fan-out log the question "is key K
processed?" has no answer without naming a consumer — K can be processed by A
and pending for B simultaneously. A producer sits before the fan-out; it has no
single "processed" state to free a key against. The predicate is not just hard
to compute, it is **undefined** at the producer.

### 3.2 The mechanics argument

The engine *does* expose one aggregate signal a producer could read:
`min(sub_last_tick)` across all subscriptions (it is exactly what rotation uses
to decide when a table is safe to truncate). So one could, in principle, probe
"has every consumer's cursor passed K's event?" But:

- **"Free once ALL consumers processed"** means the key stays reserved until the
  *slowest* consumer drains it; a lagging or dead consumer **wedges the key**
  indefinitely (bounded only by a TTL backstop).
- **"Free once ANY consumer processed"** breaks the guarantee for the laggards:
  if B has not yet seen the first K and the producer re-sends K, B now has two
  in-flight copies of K — the exact duplicate the feature was meant to prevent.

Either way, **producer dedup behavior becomes a function of consumer lag.** That
is operationally surprising (a dead consumer silently changes whether your sends
deduplicate) and conceptually backwards for a log, whose entire value is that
producers and consumers are decoupled.

### 3.3 The prior-art argument

No system in the field does append-only "free once processed." Every system that
offers it is a job queue that pays for it with a per-row state `UPDATE`. Every
log that does business-key dedup uses a wall-clock TTL window instead. This is
not an oversight — it is structural: "free once processed" must *observe*
"processed," and "processed" is row state. See §5.

---

## 4. Recommended designs

### 4.1 Producer idempotency — TTL window dedup (variant 1)

Contract: `send` with an idempotency key is deduplicated against other sends
with the same key **within a time window**. Freeing is by wall clock, not by
consumption — identical to SQS's "tracking continues even after the message has
been received and deleted."

Why this is the right (and only coherent) producer-side option for a log: §3.

Why it does **not** reproduce pg-boss's bloat — the point that matters most for
Fabrizio: the dedup state is sized by **`throughput × window`**, not by the
backlog. pg-boss bloats because its state grows with the *pending pile* (millions
of stuck jobs, each an indexed mutable row). A TTL dedup ledger is bounded by the
send rate times a short window, completely independent of how far behind the
consumers are. The failure mode he is fleeing does not exist here even in the
naive implementation.

Shape (pseudocode-level; final SQL is a later PR):

```
-- non-rotated sidecar, or a rotation-partitioned sidecar (see GC fork below)
pgque.idempotency (queue, key, ev_id, expires_at)   -- unique (queue, key)

function pgque.send_idempotent(queue, key, payload, ttl):
    insert into pgque.idempotency (queue, key, ev_id, expires_at)
    values (queue, key, <pending>, now() + ttl)
    on conflict (queue, key) do nothing
    -- if inserted: produce the real event, record ev_id, return (ev_id, deduped=false)
    -- if conflict and not expired: return (existing ev_id, deduped=true)
    -- if conflict and expired: reclaim the row, produce, return (new ev_id, deduped=false)
```

**Return contract.** pg-boss returns `null` on a deduped send (the caller gets
nothing). The log brokers do better — SQS returns a fresh `MessageId`, NATS sets
`PubAck.duplicate = true`. pgque should **return the existing event id plus a
`deduplicated` boolean**: strictly more useful than pg-boss, and free since the
dedup row already stores the id.

**The one open engineering fork — how the ledger is GC'd:**

- **(X) Non-partitioned table, global `unique (queue, key)` + `expires_at`,
  pruned by a `maint`-cycle DELETE reaper.** Exact, predictable window; dedup is
  a single `on conflict`. Cost: per-row delete churn → autovacuum on a small hot
  table. (This is the in-tree precedent — `delayed_events` + a `maint_*` step,
  and the DLQ's unique-index-on-conflict pattern.)
- **(Y) Rotation-partitioned ledger (or the key carried in the event stream),
  GC'd by `TRUNCATE`/`DROP` of old buckets — append-only, zero vacuum.** Cost:
  Postgres requires the partition key inside any unique constraint, so
  uniqueness is per-bucket → a key can recur across buckets → dedup needs a probe
  across the live buckets (the "previous-child probe" / sawtooth window).

Net: **vacuum-churn (X) vs probe-cost (Y).** X's churn is window-bounded and
modest here (not pg-boss's monster); Y is append-only but pays a small
multi-bucket read per send and has a ragged window at the rotation boundary.
This is the single decision to make before writing the producer PR.

### 4.2 Free-once-processed — consumer-side per-key lease (the partition feature)

This is where "free once processed" legitimately lives, because a single
consumer's in-flight set is that consumer's own concern, small, and well-defined.

- Carry the partition/idempotency key on the event (`ev_extra1`, no schema
  change) via a `send_partitioned(queue, key, payload)` wrapper.
- A per-consumer lease sidecar: when a consumer receives an event for key K, it
  claims the lease (`insert ... on conflict do nothing`); a second event for K is
  **deferred** (re-queued via `event_retry`) until the first is acked, at which
  point the lease is released. Net effect: at most one in-flight job per key per
  consumer. Add a lease TTL reaper so a crashed worker cannot wedge a key.
- Policy knob: **drop** the duplicate (idempotency flavor) vs **defer** it
  (serialization flavor) — same machinery, two surfaces.

Because the lease is per-consumer, the fan-out ambiguity of §3.2 disappears:
each consumer enforces its own "one in-flight per key" without reference to any
other consumer.

---

## 5. Prior art (evidence for §3.3)

Sorted by the log-vs-job-queue axis. All facts are from primary sources
(source DDL / official docs); URLs inline.

### Logs / brokers that do business-key dedup → all variant-1 (TTL window), produce-side

| System | Mechanism | Freeing | Notes |
|---|---|---|---|
| **AWS SQS FIFO** | server dedup-ID set per queue (or SHA-256 of body) | fixed **5-min window**, wall-clock | docs: "continues tracking the deduplication ID **even after the message has been received and deleted**" — explicitly *not* free-once-processed. Returns success + a fresh `MessageId`. |
| **NATS JetStream** | per-stream table keyed by `Nats-Msg-Id` | configurable `duplicate_window`, **default 2 min** | `PubAck.duplicate = true` on a suppressed write. |
| **RabbitMQ** (noxdafox plugin) | in-mem cache keyed by `x-deduplication-header`, bounded by `x-cache-size` | TTL `x-cache-ttl` | core RabbitMQ has **no** dedup. |
| **Kafka** idempotent producer | per-(PID, partition) sequence numbers | n/a | **not** business-key dedup — only same-producer-instance retry dedup; new PID on restart. |
| **GCP Pub/Sub** | server `message_id` redelivery suppression | per-message, no redeliver after ack | **not** producer-key dedup — two `publish()` of the same logical message are two messages. |

Sources: SQS [using-messagededuplicationid](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html),
[FIFO exactly-once](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html);
NATS [model_deep_dive](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive);
RabbitMQ plugin [README](https://github.com/noxdafox/rabbitmq-message-deduplication);
Kafka [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging);
Pub/Sub [exactly-once-delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery).

### Job queues that do free-once-processed → all rely on a mutable per-row `state` column

| System | Mechanism | Freeing | Per-row mutation? |
|---|---|---|---|
| **pg-boss** | partial unique indexes on `(name, COALESCE(singleton_key,''))` **predicated on the mutable `state` column** (`job_i1/i2/i3/i6`, e.g. `WHERE state <= 'active'`) | `UPDATE ... SET state='completed'` pushes the row out of the index predicate | **Yes** — and the index-on-mutable-`state` is the documented bloat source (HOT updates defeated; terminal rows linger under retention until `DELETE` + vacuum). Returns `null` on dedup. |
| **Oban** | `pg_try_advisory_xact_lock` + `SELECT` over `state ∈ states` within `period` (no DB constraint) | state leaves the watched set, or `period` (default 60s) elapses | **Yes**, in-place state UPDATE. Docs admit it is "prone to race conditions." |
| **River** (v0.12+) | partial unique index on `unique_key`, predicate over a per-row `unique_states BIT(8)` bitmask | row's `state` leaves the bitmask → drops out of the index, no cleanup job | **Yes** — elegant ("free on completion for free") but works *only because* `state` is an UPDATE'd column. |
| **Graphile Worker** | `UNIQUE (key)` on the job row, `INSERT ... ON CONFLICT (key) DO UPDATE` | job completes → **row DELETEd** | **Yes** (replace/upsert + delete-on-complete). |
| **Hatchet** | side `WorkflowRunDedupe` table, `UNIQUE (tenantId, workflowId, value)`, reject on conflict | run reaches terminal state → dedup row removed | side-table registry as a lock. |

Sources: pg-boss [`src/plans.js`](https://cdn.jsdelivr.net/npm/pg-boss/src/plans.js) (partial unique indexes + `completeJobs`);
Oban [unique_jobs](https://hexdocs.pm/oban/unique_jobs.html) + `lib/oban/engines/basic.ex`;
River [unique-jobs](https://riverqueue.com/docs/unique-jobs) + migration `006_bulk_unique.up.sql`;
Graphile Worker [job-key](https://worker.graphile.org/docs/job-key);
Hatchet `WorkflowRunDedupe` migration (`20240726160629_v0_40_0.sql`).

### The peer that has nothing

**pgmq** — the closest architectural analog to pgque (simple single-extension
Postgres queue, `send`/`read`/`pop`/`archive`) — has **no dedup or idempotency
feature at all**. `send` always inserts; two identical sends yield two messages.
Source: [pgmq SQL functions](https://pgmq.github.io/pgmq/latest/api/sql/functions/).

**Takeaway:** logs do TTL-window dedup; job queues do state-based
free-once-processed and eat the per-row UPDATE for it; nobody does append-only
free-once-processed. pgque's nearest analog ships neither — so both of pgque's
planned features are genuine differentiators, not catch-up.

---

## 6. Open decisions (before writing PRs)

1. **Producer GC fork: (X) vacuum-reaper vs (Y) rotation-partitioned probe** (§4.1).
2. **Default TTL** for the producer window, and its relation to rotation period.
   Hard floor only matters for the consumer-lease variant; for pure window dedup
   the TTL is just "how long do duplicate sends collapse."
3. **Return contract** confirmation: existing id + `deduplicated` flag (recommended
   over pg-boss's `null`).
4. **Consumer lease**: drop-vs-defer policy surface; lease TTL reaper; whether the
   lease key reuses `ev_extra1` or gets a dedicated column.
5. **Two PRs, in order**: producer window-dedup first (self-contained, closes the
   spirit of #293), consumer lease second (the partition feature).

---

## 7. Slack-reply-ready summary (for Fabrizio)

> Great questions, and digging into them surfaced something important: pgque is
> a **log**, not a job queue (PgQ heritage — append-only events, independent
> consumer cursors, no per-row state). That changes how idempotency has to work.
>
> pg-boss's `singletonKey` ("dedupe until the job is processed") is implemented
> with a partial unique index on a mutable `state` column that gets `UPDATE`d to
> `completed` — and that index-on-mutable-state is *exactly* the write
> amplification / bloat you're migrating away from. We don't want to reintroduce
> it.
>
> In a log, "processed" is a per-consumer fact the producer can't see, so
> "free-once-processed" can't be a producer feature. So we'd split it:
>
> 1. **Producer idempotency** = a dedup **window** (like SQS's dedup ID or NATS's
>    `Nats-Msg-Id` window) — a duplicate `send` with the same key inside the
>    window is a no-op returning the original id. Append-only, GC'd by our table
>    rotation, and crucially **sized by throughput × window, not by backlog** —
>    so it can't bloat the way pg-boss does when consumers fall behind.
> 2. **"One in-flight per key" / free-once-processed** = a **consumer-side** key
>    lease (this is also your partitions ask). It lives on the read side because
>    that's the only place "processed" is defined, and it's per-consumer so
>    fan-out stays clean.
>
> Both are things our closest analog (pgmq) doesn't have, so we're keen on the
> contributions. Happy to pair on the produce-side window dedup first — it's
> self-contained and closes the core of your idempotency issue.

---

(Companion: the consumer-side partition feature in
`blueprints/partition-keys/SPEC.md`.)
