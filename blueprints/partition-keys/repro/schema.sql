-- Partition-keys reproduction — demo schema (NOT part of pgque core).
-- Installs alongside sql/pgque.sql into a throwaway database. Everything here
-- is a thin recipe over existing engine primitives, to validate the two-tier
-- design from blueprints/partition-keys/SPEC.md empirically.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create schema if not exists demo;

-- ---------------------------------------------------------------------------
-- Invariant-checking logs
-- ---------------------------------------------------------------------------

-- Tier A (mutual exclusion / G2): each time a worker *runs* a job for a key it
-- records the processing window. G2 holds iff no two windows for the same key,
-- owned by different workers, overlap in time.
create table if not exists demo.mutex_log (
    id         bigserial primary key,
    part_key   text not null,
    worker     text not null,
    started_at timestamptz not null default clock_timestamp(),
    ended_at   timestamptz
);

-- Tier A idempotency target: "tenant is on latest migration". A real handler
-- runs the migration; here we record that it ran. `runs` > 1 for any tenant
-- would itself be a mutual-exclusion failure (two workers both ran it).
create table if not exists demo.tenant_migrated (
    tenant      text primary key,
    migrated_at timestamptz not null default clock_timestamp(),
    runs        int not null default 1
);

-- Tier B (ordered per key / G1): every delivered event, in consume order, with
-- the slot that owned it. G1 holds iff, per key, msg_id is non-decreasing in
-- seq order and every row for a key carries the same slot.
create table if not exists demo.consume_log (
    seq         bigserial primary key,
    part_key    text not null,
    msg_id      bigint not null,
    slot        int not null,
    worker      text not null,
    consumed_at timestamptz not null default clock_timestamp()
);

create or replace function demo.reset() returns void language plpgsql as $$
begin
  truncate demo.mutex_log, demo.consume_log;
  delete from demo.tenant_migrated;
end $$;

-- ---------------------------------------------------------------------------
-- BENCH ONLY: a minimal pg-boss-style mutable job table, used by bench.ts to
-- contrast against PgQue's append+rotation model. A job's life is
-- insert(created) -> update(active) -> delete(complete): every processed job
-- leaves dead tuples that vacuum must reclaim. This is the row churn PgQue
-- avoids (events are append-only; rotation TRUNCATEs whole tables).
-- Not part of any pgque feature.
-- ---------------------------------------------------------------------------
create table if not exists demo.jobq (
    id         bigserial primary key,
    key        text,
    state      text not null default 'created',
    payload    text,
    created_at timestamptz not null default now()
);
create index if not exists jobq_state_id on demo.jobq (state, id);

-- ---------------------------------------------------------------------------
-- Keyed producer (decision D1/D6): the partition key rides in ev_extra1.
-- This is the only "new" producer surface the design needs; here it is just a
-- one-line wrapper over the existing 7-arg insert_event.
-- ---------------------------------------------------------------------------
create or replace function demo.produce(
    i_queue text, i_type text, i_payload text, i_key text)
returns bigint language plpgsql as $$
begin
  return pgque.insert_event(i_queue, i_type, i_payload, i_key, null, null, null);
end $$;

-- ---------------------------------------------------------------------------
-- PRODUCER-SIDE IDEMPOTENCY (Case 1, the literal ask): a TTL dedup window
-- (nik's option 1; the SQS/NATS model). This is what prevents duplicate
-- *inserts* — distinct from the consumer-side mutual exclusion below, which
-- only prevents duplicate *work*. The two are complementary layers.
--
-- Multi-producer-safe: the unique key + atomic upsert serialize concurrent
-- producers, so exactly one caller wins per idempotency key per window. The
-- table is append-only-ish (one row per live key) and GC'd by expiry — in a
-- real install this maps onto a window GC'd by rotation (see the idempotency
-- design note).
-- ---------------------------------------------------------------------------
create table if not exists demo.idem (
    idem_key   text primary key,
    expires_at timestamptz not null
);

create or replace function demo.send_idem(
    i_queue text, i_type text, i_payload text, i_key text,
    i_idem text, i_ttl interval,
    out event_id bigint, out deduped boolean)
language plpgsql as $$
declare
  v_claimed boolean;
begin
  -- Claim the idempotency key for this window. Fresh key -> insert wins;
  -- expired key -> the WHERE lets the update win; live key -> 0 rows.
  insert into demo.idem(idem_key, expires_at)
    values (i_idem, now() + i_ttl)
  on conflict (idem_key) do update
    set expires_at = excluded.expires_at
    where demo.idem.expires_at <= now()
  returning true into v_claimed;

  if v_claimed then
    event_id := pgque.insert_event(i_queue, i_type, i_payload, i_key, i_idem, null, null);
    deduped := false;
  else
    event_id := null;        -- duplicate within the window: NOT inserted
    deduped := true;
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- TIER A — mutual exclusion via cooperative consumers + per-key advisory lock.
--
-- Consume one cooperative batch (the engine spreads batches across workers),
-- serialize per key with a NON-BLOCKING per-key advisory lock, and ack-drop
-- contended or duplicate events. No new slots, no N, no assignment.
--
--   i_work_ms : simulated work while holding the key (widens the window so
--               concurrent collisions are observable / measurable).
-- returns: got = events seen, ran = jobs actually executed, dropped = ack-drop
--          (either contended on the lock, or duplicate of an already-done key)
-- ---------------------------------------------------------------------------
create or replace function demo.tier_a_consume(
    i_queue text, i_main text, i_worker text, i_max int, i_work_ms int default 0,
    out got int, out ran int, out dropped int)
language plpgsql as $$
declare
  m         pgque.message;
  v_batch   bigint := null;
  v_log_id  bigint;
  v_already boolean;
begin
  got := 0; ran := 0; dropped := 0;
  for m in select * from pgque.receive_coop(i_queue, i_main, i_worker, i_max) loop
    v_batch := m.batch_id;
    got := got + 1;

    -- per-key mutual exclusion: try, never block (the non-blocking claim).
    if pg_try_advisory_xact_lock(hashtextextended(m.extra1, 0)) then
      -- idempotency: a duplicate for an already-migrated tenant collapses to a
      -- no-op (free-once-done, without any producer-side dedup).
      select true into v_already from demo.tenant_migrated where tenant = m.extra1;
      if v_already then
        dropped := dropped + 1;
      else
        insert into demo.mutex_log(part_key, worker) values (m.extra1, i_worker)
          returning id into v_log_id;
        if i_work_ms > 0 then perform pg_sleep(i_work_ms / 1000.0); end if;
        insert into demo.tenant_migrated(tenant) values (m.extra1)
          on conflict (tenant) do update set runs = demo.tenant_migrated.runs + 1;
        update demo.mutex_log set ended_at = clock_timestamp() where id = v_log_id;
        ran := ran + 1;
      end if;
    else
      dropped := dropped + 1;  -- another worker holds this key right now
    end if;
  end loop;

  if v_batch is not null then
    perform pgque.ack(v_batch);
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- TIER B — ordered per key via N hash-routed slot subscriptions.
--
-- Each slot is its own consumer (own cursor). It reads the full tick window and
-- server-side-filters to its hash class through the existing get_batch_cursor
-- extra_where hook. The engine is untouched.
--
-- returns: scanned   = events in the tick window this slot read (read-amp num)
--          delivered = events that passed the hash filter (read-amp denom)
-- ---------------------------------------------------------------------------
create or replace function demo.tier_b_consume(
    i_queue text, i_slot_consumer text, i_k int, i_n int, i_max int,
    out scanned int, out delivered int)
language plpgsql as $$
declare
  v_batch  bigint;
  e        record;
  v_cursor text;
begin
  scanned := 0; delivered := 0;
  v_batch := pgque.next_batch(i_queue, i_slot_consumer);
  if v_batch is null then
    return;
  end if;

  -- read amplification: the whole tick window is scanned by every slot.
  select count(*) into scanned from pgque.get_batch_events(v_batch);

  v_cursor := 'pk_cur_' || i_slot_consumer || '_' || v_batch::text;
  for e in
    select *
    from pgque.get_batch_cursor(
      v_batch, v_cursor, i_max,
      format('(hashtextextended(ev_extra1, 0) %% %s + %s) %% %s = %s',
             i_n, i_n, i_n, i_k))
  loop
    insert into demo.consume_log(part_key, msg_id, slot, worker)
      values (e.ev_extra1, e.ev_id, i_k, i_slot_consumer);
    delivered := delivered + 1;
  end loop;

  perform pgque.ack(v_batch);
end $$;
