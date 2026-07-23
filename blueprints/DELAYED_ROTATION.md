# Delayed-delivery rotation

Blueprint version: `0.1-draft.2`

## Change log

| Version | Date | Notes |
|---|---|---|
| `0.1-draft.2` | 2026-05-19 | Promote cancellation to first-class. `cancel_scheduled()` uses a row DELETE; document that the bloat-free property is conditional on low cancellation rate and that high-cancel workloads should be discouraged in docs and function comments. |
| `0.1-draft.1` | 2026-05-19 | First draft. Two-table TRUNCATE rotation for `pgque.delayed_events`, modeled on PgQ's `event_N` rotation. Replaces the current single-heap design before promotion of `send_at()` out of experimental. |

## Goal

Make `pgque.send_at()` viable as a primary write path, not just a sparingly
used scheduling hook.

The experimental design in `sql/experimental/delayed.sql` stores every
scheduled message in a single `pgque.delayed_events` heap, INSERTs on
`send_at()`, and DELETEs on delivery. That works while delayed delivery is a
rare path, but it is the exact MVCC-on-hot-path pattern PgQ exists to avoid.
At throughputs where `send_at()` becomes the main write path (scheduled
notifications, reminders, "fire at T+N" workloads), dead-tuple churn and
index bloat would put pgque squarely back into the same operational hole
that pg_boss / Delayed::Job / typical SKIP LOCKED queues already occupy.

This blueprint specifies a replacement storage scheme that:

- never DELETEs and never UPDATEs rows on the **delivery** hot path
- never requires `VACUUM` to keep up with delivery throughput
- never grows the pg_class / pg_inherits catalog
- keeps `send_at()` as a pure INSERT
- delivers events at the granularity of the maint tick (no bucket-width
  latency floor)
- supports out-of-band cancellation via `pgque.cancel_scheduled(de_id)`
  as an explicit, low-volume DELETE path with documented caveats

The bloat-free property holds when cancellation is rare relative to
delivery. The design does not attempt to make cancellation itself
bloat-free; high-cancel workloads (e.g., > a few percent of scheduled
messages cancelled) are explicitly out of the supported envelope and
must be called out in user-facing docs and function comments.

## Non-goals

- No new C extension and no `shared_preload_libraries` dependency.
- No change to `pgque.send()` / `pgque.receive()` / `pgque.ack()` /
  `pgque.nack()` API.
- No change to the PgQ-style `event_N` rotation for in-flight messages.
- No support for high-cancel workloads. `cancel_scheduled()` exists,
  but the no-bloat guarantee is contingent on cancellation being a
  rare exception, not a routine path (see "Cancellation").

## Why not native range partitioning

Range-partitioning `pgque.delayed_events` on `de_deliver_at` (one partition
per minute / hour, drop-and-create on rotation) does work and was the first
shape considered. It has two costs the two-table scheme avoids:

1. **Catalog churn.** A 1-minute bucket with year-long horizons is ~525k
   live partition rows over a year and a steady stream of `create table` /
   `drop table` DDL on the maintenance path. Planning time on the parent
   scales with partition count.
2. **Latency floor.** An event at the start of a bucket waits for the
   entire bucket window to close before it can be delivered without
   reintroducing per-row DELETE-on-delivery (which is what breaks the
   no-bloat invariant). At 1-minute buckets that is up to ~60 s extra
   delivery latency.

Both costs are avoidable. PgQ already solved this exact "deliver a stream
of events without per-row MVCC churn" problem for `event_N`, and the
same solution applies here.

## Design

Two physical tables, `pgque.delayed_events_a` and `pgque.delayed_events_b`,
each with the same columns as today's `pgque.delayed_events`. At any
moment one is the **drainer** (rows scheduled for the current rotation
window) and the other is the **future** table (rows scheduled for the next
window or beyond). Roles are stored in a one-row state table and flipped
at rotation.

```text
       send_at(t)
            |
            v
+----------+   +----------+
|  drainer |   |  future  |
| (today)  |   | (tomorrow+) |
+----------+   +----------+
     |               ^
     | scan rows     | re-INSERT rows whose
     | with          | actual_deliver_at falls
     | actual_       | beyond the next window
     | deliver_at    |
     | <= now()      |
     v               |
  insert_event()  ----+
```

The rotation period (the meaning of "today") is configurable. Default is
24 h aligned to UTC midnight. Per-queue overrides are out of scope for
0.2.

### Tables

```sql
create table pgque.delayed_events_a (
    de_id           bigserial primary key,
    de_queue_name   text not null,
    de_deliver_at   timestamptz not null,
    de_type         text,
    de_data         text,
    de_extra1       text,
    de_extra2       text,
    de_extra3       text,
    de_extra4       text
);

create index de_a_deliver_idx
    on pgque.delayed_events_a (de_deliver_at);

-- delayed_events_b is identical
```

The parent name `pgque.delayed_events` is kept as a view that unions the
two tables, so introspection (`select * from pgque.delayed_events`) still
shows every scheduled row.

### State

```sql
create table pgque.delayed_state (
    singleton           boolean primary key default true,
    drainer_table       regclass not null,
    future_table        regclass not null,
    window_start        timestamptz not null,
    window_end          timestamptz not null,
    deliver_watermark   timestamptz not null,
    constraint delayed_state_singleton check (singleton)
);
```

`deliver_watermark` advances during the drain scan. `window_end` is the
boundary that decides "drainer vs future" for new inserts and triggers
rotation. The `singleton` column with a CHECK constraint guarantees at most
one row.

### `send_at()`

```text
function send_at(queue, type, payload, deliver_at):
    if deliver_at <= now():
        return insert_event(queue, type, payload)

    state = select_for_share from delayed_state
    if deliver_at <= state.window_end:
        target = state.drainer_table
    else:
        target = state.future_table

    insert into target (de_queue_name, de_deliver_at, de_type, de_data, ...)
        values (queue, deliver_at, type, payload, ...)

    return currval('pgque.delayed_events_<a|b>_de_id_seq')
```

Routing is decided at INSERT time against the current window boundary.
No partition pruning at planning time, no dynamic SQL, just a single
INSERT into one of two named tables.

The shared lock on `delayed_state` is light (no row modification) and
serializes only against rotation, which acquires an exclusive lock for
its single transaction.

### Drain scan (called from `pgque.maint()`)

```text
function maint_deliver_delayed():
    begin
        state = select * from delayed_state for update
        for row in select * from drainer
                  where actual_deliver_at > state.deliver_watermark
                    and actual_deliver_at <= now()
                  order by actual_deliver_at
                  limit drain_batch_size
        loop
            insert_event(row.de_queue_name, row.de_type, row.de_data, ...)
        end loop
        update delayed_state set deliver_watermark = now()
    commit
```

Key invariant: the watermark advance and the `insert_event()` calls are in
the same transaction. Either the batch is delivered AND the watermark
advanced, or neither.

The watermark is monotonic within a rotation window and reset on rotation.

### Rotation

Triggered when `now() >= state.window_end`. Single transaction:

```text
function maint_rotate_delayed():
    begin
        state = select * from delayed_state for update
        if now() < state.window_end:
            return  -- another worker rotated us

        -- final drain pass for anything scheduled before window_end
        drain(state.drainer_table)

        -- carry far-future rows from the future table forward
        new_window_end = state.window_end + rotation_interval
        insert into state.drainer_table (...)
            select ... from state.future_table
            where de_deliver_at > new_window_end

        -- the drainer is wiped; far-future rows we just inserted survive
        -- in the future table, which is about to become the new drainer
        truncate state.drainer_table

        -- swap roles
        update delayed_state set
            drainer_table = state.future_table,
            future_table = state.drainer_table,
            window_start = state.window_end,
            window_end = new_window_end,
            deliver_watermark = state.window_end
    commit
```

A row scheduled inside the next window survives unchanged in the (newly
named) drainer table. A row scheduled beyond the next window was re-INSERTed
into the (newly named) future table before truncate, and will be carried
forward again at the next rotation if its delivery time is still beyond
that window.

If the system is paused longer than one rotation interval, `maint_rotate_delayed()`
must loop until `now() < window_end`. Each iteration is its own transaction so
catch-up does not hold a long lock.

### Cancellation

```sql
create or replace function pgque.cancel_scheduled(i_de_id bigint)
returns boolean as $$
declare
    rows_deleted integer;
begin
    delete from pgque.delayed_events_a where de_id = i_de_id;
    get diagnostics rows_deleted = row_count;
    if rows_deleted > 0 then
        return true;
    end if;

    delete from pgque.delayed_events_b where de_id = i_de_id;
    get diagnostics rows_deleted = row_count;
    return rows_deleted > 0;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
```

Cancellation is the **only** path that performs a row-level DELETE
against `pgque.delayed_events_a` / `pgque.delayed_events_b`. The
DELETE produces a dead tuple in whichever physical table holds the
row. That dead tuple is reclaimed at the next TRUNCATE (when the
table next rotates out as the drainer-becoming-future), so the
bloat is bounded above by **(cancellation rate) × (rotation interval)**.

Concretely: with default 24 h rotation, dead tuples from cancels
accumulate for at most one rotation cycle before being TRUNCATEd
away with the rest of the table. Autovacuum may also reclaim them
sooner; nothing in the design depends on that.

The bloat-free guarantee is therefore conditional:

> `pgque.send_at()` is bloat-free **provided cancellation is rare**.
> A workload that cancels a large fraction of scheduled messages
> (rule of thumb: more than a few percent) is outside the supported
> envelope; consider an application-level "skip on consume" pattern
> instead, where messages are delivered and then dropped by the
> consumer based on application state.

This caveat must appear:

- in the SQL comment on `pgque.cancel_scheduled()` itself
- in `docs/reference.md` next to the function entry
- in any tutorial / example that introduces cancellation

Behavioral notes:

- Cancellation by `de_id` is O(1) given the primary-key lookup; the
  function tries the drainer first (likely target for soon-to-deliver
  rows), then the future table.
- A cancellation that races a delivery scan can lose: if the drain
  transaction commits first, the event has already been delivered
  and `cancel_scheduled()` returns `false`. The function's return
  value indicates "row was still pending and is now gone."
  Cancellation has no effect on already-delivered events; the
  consumer-side `ack`/`nack` flow remains the only post-delivery
  control.
- Concurrent cancellations of the same `de_id` are safe — the second
  DELETE simply matches zero rows.
- `cancel_scheduled()` does not take any state-table locks; it
  contends only with the row it targets.

## Properties

1. **No DELETE on the delivery hot path.** Drain writes only to event
   tables (via `insert_event()`) and to the state row. The only
   row-level DELETE on delayed tables is `cancel_scheduled()`, which
   is an off-hot-path exception (see "Cancellation").
2. **No UPDATE on a delayed-event row.** State row is updated; row data
   is immutable from INSERT to either delivery or cancellation.
3. **VACUUM-independent under nominal use.** All reclamation on the
   delivery path is via TRUNCATE. Cancellation introduces dead tuples,
   but they are bounded by one rotation interval and TRUNCATEd away;
   `VACUUM` only matters if the cancellation rate is high enough to
   pressure the table between rotations.
4. **Stable catalog.** Two tables, one state table, one view. Forever.
5. **Delivery latency.** Bounded by `maint()` cadence, not by rotation
   interval. A row scheduled for `now() + 5 s` delivers on the next tick
   after `now() + 5 s`.
6. **Write amplification.** A row scheduled `N` rotation intervals out
   incurs one extra INSERT per rotation crossed. For 24 h rotation and a
   30-day-out message: ~30 INSERTs total. For 1 h rotation: ~720.
7. **Crash safety.** Drain, rotation, watermark advance, and
   cancellation are each a single transaction. Recovery is automatic.
8. **Concurrent safety.** `select for update` on the singleton state row
   serializes rotation; drain holds it for at most one batch.
   Cancellation does not touch the state row.

## Tradeoffs

### Conditional bloat-free guarantee

The no-VACUUM-on-hot-path property holds for workloads where cancellation
is rare. A workload that cancels a large fraction of `send_at()` rows
(say > a few percent) pushes dead tuples into the physical tables faster
than rotation can TRUNCATE them away. In that regime, autovacuum has to
keep up — at which point the design has lost its primary advantage over
the original single-heap.

This is an explicit, documented contract, not a hidden limitation:

- The SQL comment on `cancel_scheduled()` warns about the cancellation
  rate.
- `docs/reference.md` repeats the caveat next to the function entry and
  in the "When to use `send_at()`" preamble.
- Tutorials and examples that introduce cancellation must include the
  caveat inline.
- Function comment on `send_at()` mentions that bloat-free behavior
  assumes cancellation is exceptional.

For workloads with very high cancel rates, the documented alternative is
the application-level "skip on consume" pattern: always `send_at()`,
always deliver, and let the consumer drop the message based on
application state at receive time. That keeps cancels off the storage
layer entirely.

### Write amplification under long horizons

A workload that mixes mostly-soon delivery with a small population of
multi-month scheduled messages pays write-amp on the long-tail rows only,
once per rotation. Concretely: at 24 h rotation, 1M scheduled-far-future
messages cause 1M extra INSERTs per day, batched into the rotation
transaction. That is acceptable; it can be chunked if the single TX is
too large in practice. The hot path (soon-to-deliver messages) is
untouched.

### Rotation interval choice

24 h (UTC midnight) is the default. Two alternative pickers worth
mentioning:

- **Shorter rotation (e.g., 1 h or 6 h)** reduces the "future" table's
  steady-state population at the cost of higher write amplification on
  long-horizon rows.
- **Adaptive rotation** based on observed long-horizon population. Out of
  scope for 0.2. Mention only.

### Comparison vs partition+drop

| | this design | declarative partition + drop |
|---|---|---|
| catalog rows | 2 tables, 1 view, 1 state row, stable | partition per bucket, churns |
| hot-path DDL | none | `create` / `drop` per bucket |
| `DELETE` / `UPDATE` on rows | only `cancel_scheduled()` | only `cancel_scheduled()` |
| `VACUUM` dependence | none (assuming low cancel rate) | none (assuming low cancel rate) |
| latency floor | maint tick | bucket width |
| write amplification | `1 + rotations_crossed` per row | 1× always |
| code complexity | low (2 tables + state) | medium (partition manager) |

Both designs satisfy the no-bloat goal under the same cancellation
assumption. The two-table scheme wins on catalog stability, hot-path
DDL, and latency floor. It loses on write amplification for
long-horizon rows.

## Open questions

1. **Rotation cadence configurability.** 0.2 ships UTC-midnight 24 h
   rotation, hard-coded. Per-queue rotation interval would need a column
   on `pgque.queue` and a more elaborate state model (per queue, not
   singleton).
2. **Far-future migration chunking.** Default is one big `insert ... select`
   inside the rotation transaction. Above some threshold (say 100k rows)
   it should split into chunks. Threshold is a tuning knob; default
   probably 50k.
3. **View vs. parent table.** The public `pgque.delayed_events` name is
   exposed as a view here. Alternative: keep the existing parent name as
   the active drainer (via a synonym-ish mechanism). View is simpler and
   stable; reconsider only if introspection latency turns out to matter.
4. **`pg_cron` integration.** Rotation must run shortly after
   `window_end`. The default pgque ticker calls `maint()` frequently
   enough that lazy rotation (triggered on the first maint after
   `now() >= window_end`) is sufficient and does not need a dedicated
   pg_cron job. Document this.
5. **High-cancel observability.** Should `pgque.queue_stats()` expose
   a cancel-rate metric so users can detect when they have drifted out
   of the supported envelope? Cheap to add; defer to the implementation
   PR.

## Implementation plan

Sequenced TDD slices on the branch
`claude/check-delayed-messages-IGSpF`:

1. **Failing acceptance test.** Extend `tests/acceptance/us4_delayed_delivery.sql`
   with a "no dead tuples after N deliveries" assertion against
   `pg_stat_user_tables.n_dead_tup` on both tables. Existing test must
   continue to pass.
2. **Schema + state.** Add `pgque.delayed_events_a`, `pgque.delayed_events_b`,
   `pgque.delayed_state`, and the `pgque.delayed_events` view. Drop the
   old `pgque.delayed_events` table inside the same migration (experimental
   file, no preserved data needed).
3. **`send_at()` rewrite.** Route INSERTs by window boundary.
4. **`maint_deliver_delayed()` rewrite.** Watermark-driven drain.
5. **`maint_rotate_delayed()` and `maint()` wrapper.** Lazy rotation
   call sequenced before the drain pass.
6. **`cancel_scheduled()`.** Add the function, the SQL comment with the
   rate caveat, and an acceptance test that asserts both the
   "row-gone-after-cancel" success case and the "no-op-after-delivery"
   race case.
7. **Bench harness.** Reuse `benchmark/` to measure delivery rate and dead
   tuple counts under sustained `send_at()` load, plus a separate run
   under sustained `send_at()` + steady cancel rate to validate the
   bounded-bloat claim. Capture numbers in the PR description.
8. **Docs.** Update `docs/reference.md` experimental section, including
   the cancellation caveat in both the `cancel_scheduled()` entry and
   the `send_at()` preamble. Promote `send_at()` documentation into
   `docs/tutorial.md` / `docs/examples.md` only after benchmarks
   confirm the no-bloat property under nominal (low-cancel) workload.

## Acceptance criteria

- `pg_stat_user_tables.n_dead_tup` on both `pgque.delayed_events_a` and
  `pgque.delayed_events_b` remains `0` after any number of `send_at` /
  drain / rotation cycles with no cancellations.
- With a low cancellation rate (≤ 1% of scheduled rows), dead tuples
  on either physical table never exceed the count of cancellations
  performed since that table last served as the rotation TRUNCATE
  target, and return to `0` after the next rotation that moves through
  it.
- `pg_class` row count contributed by delayed-delivery objects stays
  constant across one full week of simulated traffic.
- `send_at()` followed by `select pgque.maint()` after the scheduled time
  produces exactly one event in the target queue (no duplicates, no
  losses) under concurrent producers and concurrent maint workers.
- A row scheduled `N` rotation intervals out is delivered exactly once on
  the maint pass following its `de_deliver_at` and incurs exactly `N+1`
  total row writes to delayed tables.
- `cancel_scheduled(id)` against a pending row returns `true` and the
  row is not subsequently delivered. Against an already-delivered or
  non-existent row, returns `false` and is a no-op.
- Pausing the ticker for `> 1` rotation interval and resuming does not
  lose, duplicate, or reorder any scheduled rows.

## Out of scope

- High-cancel workloads as a supported use case (see "Cancellation"
  and "Tradeoffs").
- Per-queue rotation cadence.
- Migration from a populated 0.1 / 0.2-pre-rotation `delayed_events`
  table. The experimental file's existing table is dropped on upgrade.
  The first stable release of `send_at()` will need a real migration
  story; that lives in its own blueprint.
