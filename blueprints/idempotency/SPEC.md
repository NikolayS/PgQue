# PgQue Producer Idempotency — Spec

- **Version:** v0.1 (draft)
- **Status:** design-ready. Separate, optional **send-layer** feature. Depends on
  nothing in partition-keys; composes with it. Rationale: `DESIGN.md`.
- **Slug:** producer-idempotency
- **Scope:** producer-side, business-key dedup over a **TTL window**. NOT
  consumer-side "free once processed" (that is a per-consumer fact — see
  `DESIGN.md` §1) and NOT partition keys.

---

## 1. Goal

Prevent duplicate **inserts**: a `send` carrying an idempotency key that was
already used within a time window is a no-op that reports "deduped" instead of
appending a second event. This keeps the log small when many producers race to
enqueue the same logical work (the migration-storm case: 20k concurrent
"migrate tenant T" requests → one event).

This is the SQS `MessageDeduplicationId` / NATS `Nats-Msg-Id` model — the only
producer-side business-key dedup that fits a **log** (`DESIGN.md`). It is
distinct from, and complementary to, consumer-side mutual exclusion (advisory
lock + idempotent handler), which prevents duplicate *work*. Use both: dedup
keeps the log small; mutual exclusion keeps it correct if a duplicate slips in.

## 2. The guarantee

- **I1 — windowed dedup.** For a key `(queue, idem_key)`, at most one event is
  appended per live window. A second `send` with the same key while the window
  is unexpired returns `deduped = true, event_id = null` and appends nothing.
- **I2 — multi-producer-safe.** Concurrent `send`s with the same key from
  different sessions/producers resolve to exactly one insert. Enforced by a
  unique key + atomic upsert; no advisory lock, no subtransaction.
- **I3 — atomic claim+append.** The dedup claim and the event insert happen in
  one transaction: either both or neither. A crash cannot leave a claimed key
  with no event (which would suppress a never-delivered job).
- **I4 — window expiry.** After the window expires the key is reusable; the
  claim row is GC'd (see §6). Append-only; no per-event UPDATE/DELETE churn on
  the hot path beyond the single claim upsert.

## 3. The key-scope rule (the part that bites)

**The idempotency key MUST represent the desired *effect*, not just the
*entity*.** If the desired effect changes, the key must change.

This is not a caveat — getting it wrong is a **correctness bug**:

> Key on the entity alone (`migrate:${tenant}`), ship migration **v1**, then
> ship **v2** inside the TTL window. The v2 `send` collides with v1's live key
> and is **silently suppressed** — the tenant never gets v2 until the window
> expires.

Correct key includes every dimension that changes the intended work:

```
idem_key = migrate:${tenant_id}:${target_schema_version}
                                 ^^^^^^^^^^^^^^^^^^^^^^^^ the effect, not just the entity
```

API guidance, in hard language:
- The key is `(queue, idem_key)` at minimum; `(queue, producer_scope, idem_key)`
  if producers need separate namespaces.
- `idem_key` must encode **tenant + operation kind + version/target** — whatever
  distinguishes "do this again because the work changed" from "this is a
  duplicate of the same work". A bare entity id is almost always wrong.
- The window (TTL) is for collapsing a *burst of the same effect*, not for
  rate-limiting distinct effects. Size it shorter than the cadence at which the
  effect legitimately changes — or, better, encode the effect in the key and
  stop relying on the TTL for correctness.

Reproduced both ways in `blueprints/partition-keys/repro/` (`--tier hazard`):
entity-only key drops the v2 wave (0 inserted); effect-scoped key delivers both.

## 4. API

```
pgque.send_idem(
    queue_name   text,
    type_name    text,
    payload      ...,                         -- jsonb / text overloads
    idem_key     text,                        -- the effect-scoped key (§3)
    ttl          interval default '1 hour',
    partition_key text default null   -- optional; composes with partition keys
) returns table(event_id bigint, deduped boolean)
```

- `deduped = false, event_id = <id>` — first use in the window; event appended.
- `deduped = true,  event_id = null` — duplicate within the window; nothing
  appended. (Returning the *original* event_id is possible but costs a lookup;
  v0.1 returns null and leaves "find the original" to the caller if needed.)
- SECURITY DEFINER, pinned `search_path = pgque, pg_catalog`; granted to
  `pgque_writer` (it is a producer surface).

## 5. Implementation

```sql
create table pgque.idem_key (
    queue_id   int4        not null references pgque.queue(queue_id) on delete cascade,
    idem_key   text        not null,
    expires_at timestamptz not null,
    primary key (queue_id, idem_key)
);
```

`send_idem` body (sketch — atomic claim+append, I2/I3):

```sql
insert into pgque.idem_key(queue_id, idem_key, expires_at)
  values (v_queue_id, i_idem_key, now() + i_ttl)
on conflict (queue_id, idem_key) do update
  set expires_at = excluded.expires_at
  where pgque.idem_key.expires_at <= now()    -- only an EXPIRED key can be reclaimed
returning true into v_claimed;

if v_claimed then
  event_id := pgque.insert_event(i_queue, i_type, i_payload, i_partition_key, i_idem_key, ...);
  deduped  := false;
else
  event_id := null; deduped := true;
end if;
```

The unique key serializes concurrent producers; the `where expires_at <= now()`
makes a *live* key un-reclaimable (dedup) while letting an *expired* key be
reused. No advisory lock, no `BEGIN..EXCEPTION` (Key Design Rule 4).

## 6. Garbage collection

The `idem_key` table is append-ish (one row per live key, upserted). Expired
rows are dead weight and must be reaped, or the dedup table becomes the bloat
pgque exists to avoid. Options, in order of preference:

1. **Piggyback on maintenance/rotation.** Delete `where expires_at < now()` in
   the same scheduled maintenance that drives ticking/rotation (pg_cron job).
   Bounded, batched, off the hot path.
2. **Partition by expiry window** and drop whole partitions (if volume warrants).

GC cadence must keep the table's live set ~= count of distinct keys in one TTL
window. Document the expected size = `produce_rate * ttl` so operators can size
it. (For the migration case: tens of thousands of rows for a 1h window — tiny.)

## 7. Relationship to partition keys

Orthogonal. `send_idem` works on any queue. It may *also* set `partition_key`
(it rides `ev_extra1`; idem_key can ride `ev_extra2`) so a deduped producer can
feed an ordered partitioned consumer — but neither feature requires the other.
The migration recipe = `send_idem` (this spec) + consumer advisory lock
(mutual exclusion) on a **plain** queue; no partition slots involved.

## 8. Tests (red/green, CI PG 14–18)

- **T-I1 dedup:** two `send_idem` same `(queue, idem_key)` within TTL → 1 event,
  second returns `deduped=true`.
- **T-I2 race:** N concurrent sessions, same key → exactly 1 insert (count the
  event table; assert `deduped=true` for N-1).
- **T-I3 atomic:** inject failure between claim and append → assert no claimed
  key without its event (no permanently-suppressed job).
- **T-I4 expiry:** key reusable after `ttl`; pre-expiry it is not.
- **T-keyscope HAZARD:** entity-only key, v1 then v2 within TTL → v2 suppressed
  (demonstrates the footgun); effect-scoped key → both inserted. (Mirrors the
  repro `--tier hazard`.)
- **T-gc:** expired rows reaped by the maintenance pass; live set bounded.
- **T-no-churn:** happy path does one upsert + one insert per accepted send,
  zero UPDATE/DELETE on the event log.

## 9. Phasing

Ships as **Phase 1B** — after partition-keys Phase 1A (partition_key producer +
fixed-N slots + skip), independently. Not gated on it. Migration-storm load test
is part of 1B's acceptance.

## 10. Changelog

- **v0.1 (draft):** initial feature spec split out of the partition-keys brief
  after review (Max / consumer Q&A). Key-scope rule + version-suppression hazard
  made first-class; atomic claim+append; `(queue, idem_key)` scoping; rotation/
  maintenance GC; orthogonality to partition keys.

## User stories

Canonical user-story layer for this feature — the shared language the spec, the
public brief, and the acceptance suite all speak. All five are proven by
`tests/acceptance/us13_producer_idempotency.sql`.

**Contract note (supersedes the §4 v0.1 sketch).** US-13.1 pins the dedup return
as the **ORIGINAL** `event_id` with `deduped = true` — not `null`. Returning the
first event's id requires the dedup claim row to carry that id (a small addition
to the §5 `idem_key` table, e.g. an `event_id` column set on the accepted insert),
which is the resolved contract the acceptance suite tests against.

- **US-13.1 — TTL dedup.** As a scheduler, `pgque.send_idem(queue, type, payload,
  idem_key, ttl)` inserts once per `(queue, idem_key)` within the window; a
  duplicate attempt inserts nothing and returns the ORIGINAL `event_id` with
  `deduped=true`.
  - *Accept:* first `send_idem` returns `deduped=false` + an id; a second within
    the TTL returns `deduped=true` + the same id; the log carries exactly one event.
  - *Test:* `us13_producer_idempotency.sql` — US-13.1.
- **US-13.2 — Effect-scoped keys.** dedup is exact-match on `(queue, idem_key)` —
  key `migrate:t1:v2` is NOT suppressed by `migrate:t1:v1`.
  - *Accept:* two distinct effect keys both insert (`deduped=false`) and yield
    distinct `event_id`s.
  - *Test:* `us13_producer_idempotency.sql` — US-13.2.
- **US-13.3 — Window expiry.** after the TTL passes, the same key inserts a new
  event again.
  - *Accept:* a duplicate inside a live 1-second window is deduped; after the
    window lapses the same key inserts a new, distinct event.
  - *Test:* `us13_producer_idempotency.sql` — US-13.3.
- **US-13.4 — GC.** expired dedup rows are purged by `pgque.maint()`, so the dedup
  table cannot grow unbounded.
  - *Accept:* after a row expires and `pgque.maint()` runs, no expired
    `pgque.idem_key` rows remain for the queue.
  - *Test:* `us13_producer_idempotency.sql` — US-13.4.
- **US-13.5 — Consumer mutual-exclusion recipe** (docs/acceptance only, no new
  SQL): per-key `pg_try_advisory_xact_lock` + idempotent handler = at most one
  concurrent migration per tenant.
  - *Accept:* the recipe derives a deterministic per-tenant lock key and its
    idempotent handler leaves exactly one effect even when its body runs twice;
    cross-session exclusion is a two-session property (honesty note in the test).
  - *Test:* `us13_producer_idempotency.sql` — US-13.5.
