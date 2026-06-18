# Contributor guide: idempotency keys & partition keys

Status: design guidance for external PRs (refs #293).
Audience: contributors adding pg-boss-style **idempotency keys** and
**per-partition serialization** ("one job at a time per partition key") on top
of PgQue.

This document is the answer to "how should I approach these two features?". It
maps each feature onto PgQue's existing layering so a PR reduces cleanly to PgQ
primitives, survives table rotation, and does not touch the sacred engine.

---

## 0. The three facts that shape both designs

Before writing any SQL, internalize these properties of the engine. They are
what make the naive approaches wrong.

1. **Event data tables rotate and get truncated.** Each queue stores events in
   `queue_ntables` (default 3) round-robin data tables
   (`<queue_data_pfx>_<N>`). Rotation recycles the oldest table with
   `TRUNCATE` every `queue_rotation_period` (default 2h). Anything you want to
   remember *for longer than one rotation* — a dedup ledger, a partition lease —
   **cannot live in the event tables.** It must live in its own sidecar table.
   The existing `pgque.delayed_events` holding table (see
   `sql/experimental/delayed.sql`) is the precedent to copy.

2. **`send` must reduce to `insert_event`.** Design rule #3 in `CLAUDE.md`: any
   producer API must be explainable as "calls `pgque.insert_event(queue, type,
   data)` with these args." Both features are *wrappers around* `insert_event`,
   not replacements for it.

3. **The batch/tick/snapshot engine is sacred** (design rule #2). A batch is a
   snapshot window: `next_batch` + `get_batch_events` + `batch_event_sql`
   deliver *every* event committed in the window to the consumer at once. Do
   **not** modify `batch_event_sql`, `next_batch`, rotation, or consumer
   tracking. Partition serialization must be built *on top of* these
   primitives (consumer-side gating), never inside them.

### Where code goes

`build/transform.sh` assembles `sql/pgque.sql` from the transformed PgQ core
plus every file in `sql/pgque-additions/` (shipped in the default install) and
**excludes** `sql/experimental/`. So:

- Land new features in `sql/experimental/<feature>.sql` first (opt-in, not in
  the default single-file install), with tests in `tests/` registered in
  `tests/run_experimental.sql`.
- Graduate to `sql/pgque-additions/<feature>.sql` once the API is settled.
- Either way, regenerate `sql/pgque.sql` via `build/transform.sh` and commit
  the source and generated file together (keep them in sync — `CLAUDE.md`).
- Every `SECURITY DEFINER` function pins `SET search_path = pgque, pg_catalog`.
  Grant producer-side functions to `pgque_writer`, consumer-side to
  `pgque_reader`. Re-run the deny-by-default `revoke ... from public`.
- Red/green TDD: failing `tests/test_*.sql` first, then the implementation.
  CI runs PG 14–18.

Two features → **two PRs.** Keep changes surgical (one feature each).

---

## 1. Idempotency keys (issue #293)

> "the same send within a timeframe results in a no-op"

### Why not a unique index on the event table

That is the obvious move and it is wrong here: the unique index would live on a
rotating data table, so it (a) only dedups within the current table, and (b) is
destroyed on the next `TRUNCATE`. You would get non-deterministic dedup windows
tied to rotation timing. The dedup ledger must be a **separate, non-rotated
table** with an explicit TTL you control.

### Recommended shape

A sidecar table keyed by `(queue, idempotency_key)` with an expiry column, plus
a thin `send` wrapper that claims the key atomically before inserting the event.

```sql
create table if not exists pgque.idempotency_key (
    ik_queue_name  text        not null,
    ik_key         text        not null,
    ik_msg_id      bigint,                 -- event id produced on first send
    ik_expires_at  timestamptz not null,
    constraint idempotency_key_pkey primary key (ik_queue_name, ik_key)
);
create index if not exists ik_expires_idx
    on pgque.idempotency_key (ik_expires_at);
```

```sql
-- pgque.send_idempotent(queue, key, payload, ttl)
-- First send within the TTL window inserts the event and records the key.
-- Repeat sends with the same (queue, key) inside the window are a no-op and
-- return the original msg_id. Reduces to one insert_event() call.
create or replace function pgque.send_idempotent(
    i_queue text, i_key text, i_payload text,
    i_ttl interval default '1 hour')
returns bigint as $$
declare
    v_msg_id bigint;
    v_now    timestamptz := now();
begin
    -- Atomic claim: the unique index is the serialization point. A concurrent
    -- duplicate loses the race and takes the "already present" branch.
    insert into pgque.idempotency_key (ik_queue_name, ik_key, ik_expires_at)
    values (i_queue, i_key, v_now + i_ttl)
    on conflict (ik_queue_name, ik_key) do update
        -- only "win" the upsert if the prior key has expired
        set ik_expires_at = excluded.ik_expires_at,
            ik_msg_id     = null
        where pgque.idempotency_key.ik_expires_at <= v_now
    returning ik_msg_id into v_msg_id;

    if not found then
        -- live duplicate: row exists and is unexpired, upsert WHERE filtered it
        select ik_msg_id into v_msg_id
        from pgque.idempotency_key
        where ik_queue_name = i_queue and ik_key = i_key;
        return v_msg_id;            -- no-op, original id (may be the same event)
    end if;

    -- we own the claim (fresh or expired-and-reclaimed): produce the event
    v_msg_id := pgque.insert_event(i_queue, 'default', i_payload);
    update pgque.idempotency_key
        set ik_msg_id = v_msg_id
        where ik_queue_name = i_queue and ik_key = i_key;
    return v_msg_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

Design decisions to settle in the PR (call them out explicitly):

- **Return value on duplicate.** pg-boss returns `null` for a rejected
  duplicate. PgQue can do better by storing `ik_msg_id` and returning the
  original event id, so callers get an idempotent *result*, not just an
  idempotent *side effect*. Pick one and document it.
- **TTL semantics.** Is the window "since first send" (above) or "since last
  send" (sliding)? The above is fixed-from-first; a sliding window just bumps
  `ik_expires_at` on every hit. pg-boss's `singletonKey` is closer to
  fixed-window-per-slot — match the semantics you actually need.
- **Transaction visibility.** If the producer rolls back, the key insert rolls
  back with it (same transaction) — correct. Document that `send_idempotent`
  is meant to run in the caller's transaction.

### The part that actually fixes their bloat: expiry maintenance

Their pain is unbounded growth, so the ledger must self-prune. Add a maint
step modeled on `maint_deliver_delayed()` and hook it into `pgque.maint()`:

```sql
create or replace function pgque.maint_expire_idempotency()
returns integer as $$
declare cnt integer;
begin
    delete from pgque.idempotency_key where ik_expires_at <= now();
    get diagnostics cnt = row_count;
    return cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

This bounds the dedup table to roughly `throughput × TTL` rows regardless of
backlog — which is exactly the property they could not get from pg-boss.

### Tests (red first)

- send same `(queue, key)` twice inside TTL → exactly one event in the batch.
- send same key after TTL expiry (or after `maint_expire_idempotency()`) → a
  second event is produced.
- two concurrent `send_idempotent` with the same key → exactly one event
  (use the `tests/two_session_*.sh` pattern for the race).
- `maint_expire_idempotency()` deletes only expired rows and returns the count.

---

## 2. Partition keys — "one job at a time per partition key"

> "run 1 job at a time for a given partition key … the batch could contain 1
> job per partition key"

This is the harder request because it is about **consumption order /
concurrency control**, which lives in the sacred engine's territory. Split it
into two independent sub-problems and solve them separately.

### 2a. Carrying the partition key (easy, no engine change)

An event already has four free passthrough columns (`ev_extra1..ev_extra4`)
that survive batching and are returned by `get_batch_events` / `pgque.receive`.
Carry the partition key in one of them via the existing 7-arg
`insert_event(queue, type, data, extra1..4)`. A thin wrapper:

```sql
-- pgque.send_partitioned(queue, partition_key, payload)
create or replace function pgque.send_partitioned(
    i_queue text, i_partition_key text, i_payload text)
returns bigint as $$
begin
    -- partition key rides in ev_extra1; everything else is a normal send
    return pgque.insert_event(i_queue, 'default', i_payload,
                              i_partition_key, null, null, null);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

No schema change, still reduces to `insert_event`. The `pgque.message` type
already exposes `extra1`, so consumers see the key without API changes.

### 2b. Serializing per key (the real work)

The PgQ batch model hands a consumer *all* events in the tick window at once;
it has no built-in "skip events whose partition is busy." Do **not** try to add
that to `batch_event_sql`. Gate it **consumer-side**, on top of the existing
`next_batch` / `get_batch_events` / `event_retry` primitives. Three viable
approaches, in order of how well they match the request:

1. **Partition lease table (recommended for true "one at a time").**
   A non-rotated sidecar holding the currently in-flight key per queue:

   ```sql
   create table if not exists pgque.partition_lease (
       pl_queue_name    text        not null,
       pl_partition_key text        not null,
       pl_msg_id        bigint      not null,
       pl_leased_at     timestamptz not null default now(),
       constraint partition_lease_pkey primary key (pl_queue_name, pl_partition_key)
   );
   ```

   A partition-aware receive walks the batch in `ev_id` order and, for each
   distinct partition key, tries to claim the lease
   (`insert ... on conflict do nothing`). The first event for a free key is
   delivered; any further event whose key is already leased is **deferred**
   back into retry via `pgque.event_retry(batch, ev_id, delay)` instead of
   being returned. `ack`/`nack` for a leased message releases the lease
   (`delete from partition_lease`), letting the next event for that key through
   on a subsequent batch. Net effect: at most one in-flight job per key, across
   all workers, with no engine change. Add a TTL/`pl_leased_at` reaper to the
   maint cycle so a crashed worker's lease cannot wedge a partition forever.

2. **Cooperative consumers (already in the tree).**
   `sql/pgque-api/cooperative_consumers.sql` lets N members of one logical
   consumer split a queue. Hashing `partition_key → member` gives you
   *parallelism bounded by key* (all events for a key go to the same member),
   which is often what people actually want. It does **not** by itself
   guarantee strictly one in-flight per key within a member — combine with
   per-key ordering in the worker, or with approach 1, if strictness matters.

3. **At-most-one-per-key-per-batch filter.**
   A `receive` variant that returns at most one event per distinct
   `partition_key` in the current batch and defers the rest via `event_retry`.
   Simpler than the lease table but only serializes *within a batch*, not
   across concurrent consumers — weaker guarantee. Useful as a stepping stone.

### Ordering caveat to flag in the PR

PgQ batches are snapshot windows, so cross-batch ordering is by `ev_id` but a
deferred (retried) event reappears in a *later* batch. If they need strict
FIFO *within* a partition, the lease approach must also process a partition's
events in `ev_id` order and not advance the partition past a deferred event.
Make this an explicit, documented guarantee (or non-guarantee) — it is the
subtle part reviewers will care about.

### Tests (red first)

- two events, same partition key: first `receive` returns event 1 and leases
  the key; event 2 is deferred (not returned) until event 1 is acked.
- two events, different keys: both delivered in the same batch.
- two concurrent consumers, same key: only one gets the event (lease race).
- crashed worker (lease never released) → reaper frees it after TTL.

---

## 3. Suggested PR sequence

1. **PR 1 — idempotency keys.** Sidecar table + `send_idempotent` +
   `maint_expire_idempotency` hooked into `maint()`; tests; land in
   `sql/experimental/` first. Closes #293.
2. **PR 2 — partition keys.** `send_partitioned` (key in `ev_extra1`) +
   partition-aware receive over a lease table + lease reaper; tests. Open a
   tracking issue first to settle the ordering guarantee before coding.

Both follow the same rules: pin `search_path`, grant by role
(`send_*` → `pgque_writer`, partition receive → `pgque_reader`), keep
`pgque.sql` regenerated, never touch the batch/tick/rotation engine, and write
the failing test before the implementation.
