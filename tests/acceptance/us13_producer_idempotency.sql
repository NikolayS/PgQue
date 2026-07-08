\set ON_ERROR_STOP on

-- US-13: Producer idempotency (Fabrizio Case 1 -- tenant migrations, duplicate
-- requests turning into connection storms)
-- As a scheduler firing the same logical work from many instances, I want a
-- business-key dedup over a TTL window so a burst of identical "migrate tenant T"
-- sends collapses to a single appended event -- the SQS MessageDeduplicationId /
-- NATS Nats-Msg-Id model, the only producer-side dedup that fits a log.
--
-- Covers US-13.1, 13.2, 13.3, 13.4, and US-13.5 (the consumer mutual-exclusion
-- recipe -- docs/acceptance only, no new SQL).
--
-- blueprints/idempotency/SPEC.md (User stories section)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- CRITICAL: send and tick MUST be in separate DO blocks (PgQ snapshot batching);
-- and the TTL-expiry check MUST cross a transaction boundary, because now() is
-- fixed at transaction start -- so pg_sleep() runs in its own statement and the
-- post-expiry send_idem runs in a fresh transaction that sees the advanced clock.

-- ===========================================================================
-- US-13.1 -- TTL dedup: send_idem inserts once per (queue, idem_key) within the
-- window; a duplicate attempt inserts nothing and returns the ORIGINAL event_id
-- with deduped=true.
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us13_dedup');
  perform pgque.subscribe('us13_dedup', 'reader');
end $$;

do $$
declare
  v_id1    bigint;
  v_dedup1 boolean;
  v_id2    bigint;
  v_dedup2 boolean;
begin
  select event_id, deduped into v_id1, v_dedup1
  from pgque.send_idem('us13_dedup', 'migrate', '{"tenant":"t1"}'::jsonb, 'migrate:t1:v1', '1 hour');
  assert v_dedup1 = false, 'US-13.1: first send in a fresh window must not be deduped';
  assert v_id1 is not null, 'US-13.1: first send must return an event_id';

  select event_id, deduped into v_id2, v_dedup2
  from pgque.send_idem('us13_dedup', 'migrate', '{"tenant":"t1"}'::jsonb, 'migrate:t1:v1', '1 hour');
  assert v_dedup2 = true, 'US-13.1: a duplicate within the TTL must be deduped';
  assert v_id2 = v_id1,
    format('US-13.1: dedup must return the ORIGINAL event_id %s, got %s', v_id1, coalesce(v_id2::text, 'NULL'));
end $$;

-- The log must carry exactly one event for the deduped key
do $$ begin
  perform pgque.force_next_tick('us13_dedup');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('us13_dedup', 'reader', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;
  assert v_count = 1,
    'US-13.1: exactly one event must be appended for a deduped key, got ' || v_count;
  if v_batch is not null then
    perform pgque.ack(v_batch);
  end if;
  raise notice 'PASS: US-13.1 TTL dedup (one insert, original event_id, deduped=true)';
end $$;

do $$ begin
  perform pgque.unsubscribe('us13_dedup', 'reader');
  perform pgque.drop_queue('us13_dedup');
end $$;

-- ===========================================================================
-- US-13.2 -- Effect-scoped keys: dedup is exact-match on (queue, idem_key), so
-- 'migrate:t1:v2' is NOT suppressed by 'migrate:t1:v1'. (The key-scope footgun:
-- a bare entity key would swallow v2 as a "duplicate".)
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us13_effect');
end $$;

do $$
declare
  v_id1 bigint;
  v_d1  boolean;
  v_id2 bigint;
  v_d2  boolean;
begin
  select event_id, deduped into v_id1, v_d1
  from pgque.send_idem('us13_effect', 'migrate', '{"target":"v1"}'::jsonb, 'migrate:t1:v1', '1 hour');
  select event_id, deduped into v_id2, v_d2
  from pgque.send_idem('us13_effect', 'migrate', '{"target":"v2"}'::jsonb, 'migrate:t1:v2', '1 hour');

  assert v_d1 = false and v_d2 = false,
    'US-13.2: two distinct effect keys must both insert (neither deduped)';
  assert v_id1 is not null and v_id2 is not null and v_id1 <> v_id2,
    format('US-13.2: v2 key must not be suppressed by v1 key (id1=%s, id2=%s)', coalesce(v_id1::text, 'NULL'), coalesce(v_id2::text, 'NULL'));
  raise notice 'PASS: US-13.2 effect-scoped keys (v2 not suppressed by v1)';
end $$;

do $$ begin
  perform pgque.drop_queue('us13_effect');
end $$;

-- ===========================================================================
-- US-13.3 -- Window expiry: after the TTL passes, the same key inserts a new
-- event again.
-- ===========================================================================

drop table if exists _us13_expiry;
create temporary table _us13_expiry (
  label    text,
  event_id bigint
);

-- First send opens a fresh 1-second window
do $$
declare
  v_id bigint;
  v_d  boolean;
begin
  perform pgque.create_queue('us13_expiry');
  select event_id, deduped into v_id, v_d
  from pgque.send_idem('us13_expiry', 'job', '{"x":1}'::jsonb, 'burst:x', '1 second');
  assert v_d = false, 'US-13.3: first send in a fresh window must insert';
  insert into _us13_expiry values ('first', v_id);
end $$;

-- A duplicate inside the live window is deduped
do $$
declare
  v_id bigint;
  v_d  boolean;
begin
  select event_id, deduped into v_id, v_d
  from pgque.send_idem('us13_expiry', 'job', '{"x":1}'::jsonb, 'burst:x', '1 second');
  assert v_d = true, 'US-13.3: a duplicate within the live window must be deduped';
end $$;

-- Let the 1-second window lapse (own statement so the next txn sees a later now())
select pg_sleep(1.5);

-- The same key now inserts a brand-new event
do $$
declare
  v_id    bigint;
  v_d     boolean;
  v_first bigint;
begin
  select event_id, deduped into v_id, v_d
  from pgque.send_idem('us13_expiry', 'job', '{"x":2}'::jsonb, 'burst:x', '1 second');
  assert v_d = false, 'US-13.3: after the TTL lapses, the same key must insert again';
  select event_id into v_first from _us13_expiry where label = 'first';
  assert v_id is not null and v_id <> v_first,
    format('US-13.3: post-expiry insert must be a NEW event (first=%s, new=%s)', v_first, coalesce(v_id::text, 'NULL'));
  raise notice 'PASS: US-13.3 window expiry (dedup inside window, new insert after)';
end $$;

do $$ begin
  perform pgque.drop_queue('us13_expiry');
end $$;
drop table if exists _us13_expiry;

-- ===========================================================================
-- US-13.4 -- GC: expired dedup rows are purged by pgque.maint(), so the dedup
-- table (pgque.idem) cannot grow unbounded.
-- Depends on: the dedup claim table named pgque.idem (SPEC section 5) and the
-- expired-row reap being wired into pgque.maint() (SPEC section 6).
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us13_gc');
end $$;

do $$
declare
  v_id bigint;
  v_d  boolean;
begin
  select event_id, deduped into v_id, v_d
  from pgque.send_idem('us13_gc', 'job', '{"k":1}'::jsonb, 'gc:key:1', '1 second');
  assert v_d = false, 'US-13.4: setup send must insert a dedup claim row';
  assert exists (
    select 1
    from pgque.idem ik
    join pgque.queue q on q.queue_id = ik.queue_id
    where q.queue_name = 'us13_gc'
      and ik.idem_key = 'gc:key:1'
  ), 'US-13.4: the dedup claim row must exist before GC';
end $$;

-- Let the row expire, then run maintenance
select pg_sleep(1.5);

do $$ begin
  perform pgque.maint();
end $$;

do $$
declare v_remaining int;
begin
  select count(*) into v_remaining
  from pgque.idem ik
  join pgque.queue q on q.queue_id = ik.queue_id
  where q.queue_name = 'us13_gc'
    and ik.expires_at < now();
  assert v_remaining = 0,
    format('US-13.4: pgque.maint() must purge expired dedup rows, %s remain', v_remaining);
  raise notice 'PASS: US-13.4 GC (expired dedup rows purged by maint)';
end $$;

do $$ begin
  perform pgque.drop_queue('us13_gc');
end $$;

-- ===========================================================================
-- US-13.5 -- Consumer mutual-exclusion recipe (docs/acceptance only, no new SQL):
-- per-key pg_try_advisory_xact_lock + idempotent handler = at most one concurrent
-- migration per tenant. This is the plain-queue migration recipe: dedup (US-13.1)
-- keeps the log small; this keeps it correct if a duplicate slips through.
--
/* Honesty note: mutual exclusion is a CROSS-SESSION property. Proving that a
   second concurrent transaction's pg_try_advisory_xact_lock returns false needs
   two live sessions -- advisory locks are re-entrant within one session, so a
   single session can never observe its own lock as contended. What a single
   session CAN prove, and does below, is the recipe's two moving parts:
     1. the per-tenant lock key is derived deterministically from the tenant, and
     2. the idempotent handler (ON CONFLICT DO NOTHING) leaves exactly one effect
        even if its body runs more than once.
   Under real concurrency the lock lets one worker run the migration while
   contended workers ack-and-drop, and the idempotent effect makes even a lost
   race converge to a single run. The concurrent-exclusion facet is exercised by
   the two-session harness. */
-- ===========================================================================

drop table if exists _us13_migration_runs;
create temporary table _us13_migration_runs (
  tenant text primary key,
  ran_at timestamptz not null default now()
);

do $$
declare
  v_tenant text := 'tenant-t1';
  v_lock   bigint := hashtextextended('migrate:' || v_tenant, 0);
  v_got    boolean;
begin
  -- Recipe step 1: take the per-key lock; if held elsewhere, ack-and-drop instead
  v_got := pg_try_advisory_xact_lock(v_lock);
  assert v_got, 'US-13.5: an uncontended per-tenant lock must be acquirable';

  -- Recipe step 2: idempotent handler -- safe to run more than once, effect once
  insert into _us13_migration_runs (tenant) values (v_tenant) on conflict (tenant) do nothing;
  insert into _us13_migration_runs (tenant) values (v_tenant) on conflict (tenant) do nothing;

  assert (select count(*) from _us13_migration_runs where tenant = v_tenant) = 1,
    'US-13.5: idempotent handler must leave exactly one migration run per tenant';
  raise notice 'PASS: US-13.5 mutual-exclusion recipe (per-key lock + idempotent handler)';
end $$;
-- advisory xact lock auto-releases at DO-block transaction end -- no unlock needed

drop table if exists _us13_migration_runs;

\echo 'US-13: PASSED'
