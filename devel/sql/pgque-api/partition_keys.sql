-- pgque-api/partition_keys.sql -- Partition keys (Phase 1A)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Partition keys (blueprints/partition-keys/SPEC.md, Phase 1A):
--   pgque.send(queue, type, payload, partition_key)  -- jsonb + text overloads
--   pgque.subscribe_slot(queue, consumer, slot, n)
--   pgque.unsubscribe_slot(queue, consumer, slot)
--   pgque.receive_partitioned(queue, consumer, slot, n, max)
--   pgque.ack_partitioned(queue, consumer, slot, n)
--   pgque.slot_lock_key / claim_slot / release_slot
--   view pgque.partition_slot_status
--
-- Producer idempotency (Phase 1B) is the separate, orthogonal
-- sql/pgque-api/send_idem.sql; it composes with this feature via
-- send_idem(..., partition_key) but neither requires the other.
--
-- Design notes:
--
-- * The mechanism is N independent slot consumers. Slot k of consumer C with
--   fixed slot count N is the engine consumer "C#k/N": its own subscription,
--   its own cursor. Every slot scans the full stream and filters server-side
--   to its hash class -- read amplification is ~N x steady (SPEC R2), and a
--   stalled slot pins rotation for the whole queue (SPEC R7): monitor
--   pgque.partition_slot_status and alert on lag.
--
-- * OWNERSHIP INVARIANT (SPEC section 6): receive_partitioned reaches the
--   admin-only pgque.get_batch_cursor(4) i_extra_where hook because it is a
--   SECURITY DEFINER function owned by the SAME role that ran sql/pgque.sql
--   (a function owner may execute its own functions regardless of grants).
--   This file MUST be installed by the pgque install owner. It does not
--   depend on that owner holding pgque_admin.
--
-- * Partition keys ride ev_extra1, so this works for send()-sourced queues
--   only (triggers use ev_extra1 for the table name -- SPEC D1/R5). Events
--   with a NULL partition key (e.g. sent through the keyless send()
--   overloads) route to slot 0, so no event is ever silently dropped by the
--   hash filter.
--
-- * The slot claim (claim_slot/release_slot) is a session-scoped advisory
--   lock held across the receive -> process -> ack loop; release only at a
--   batch boundary. Under transaction-mode pooling (PgBouncer/Supavisor) the
--   claim connection must be session-mode/direct (SPEC section 15).

-- ---------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------

/*
 * Pinned slot count per (queue, consumer) -- SPEC D3. Written only inside
 * SECURITY DEFINER functions; revoked from app roles below (the dead_letter
 * pattern). A changed n is rejected, never silently applied.
 */
create table if not exists pgque.partition_consumer (
    queue_id    int4    not null references pgque.queue (queue_id) on delete cascade,
    co_name     text    not null,
    n           int4    not null check (n >= 1),
    primary key (queue_id, co_name)
);

-- ---------------------------------------------------------------------------
-- Internal helpers
-- ---------------------------------------------------------------------------

-- Engine consumer name for slot k of consumer C with slot count N: "C#k/N".
create or replace function pgque._slot_name(
    i_consumer text, i_slot int, i_n int)
returns text as $$
select i_consumer || '#' || i_slot::text || '/' || i_n::text;
$$ language sql immutable;

/*
 * Validate (queue, consumer, slot, n) against the pinned slot count BEFORE
 * touching the engine (US-12.7): a worker calling with the wrong N is
 * rejected with a clear error, never silently misrouted. Returns pinned n.
 */
create or replace function pgque._partition_n(
    i_queue text, i_consumer text, i_slot int, i_n int)
returns int4 as $$
declare
    v_n int4;
begin
    if i_queue is null or i_consumer is null or i_slot is null or i_n is null then
        raise exception 'queue, consumer, slot, and n must not be null';
    end if;

    select pc.n into v_n
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = i_queue
      and pc.co_name = i_consumer;
    if not found then
        raise exception 'consumer % on queue % is not a partitioned consumer; call pgque.subscribe_slot() first',
            i_consumer, i_queue;
    end if;

    if i_n <> v_n then
        raise exception 'wrong slot count for consumer % on queue %: pinned n=%, caller passed n=%',
            i_consumer, i_queue, v_n, i_n;
    end if;
    if i_slot < 0 or i_slot >= v_n then
        raise exception 'slot % out of range for consumer % on queue % (valid: 0..%)',
            i_slot, i_consumer, i_queue, v_n - 1;
    end if;

    return v_n;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Producer: keyed send (US-12.1)
-- ---------------------------------------------------------------------------

-- pgque.send(queue, type, payload jsonb, partition_key) -- keyed JSON send
create or replace function pgque.send(
    i_queue text, i_type text, i_payload jsonb, i_partition_key text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload::text,
        i_partition_key, null, null, null);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send(text, text, jsonb, text) from public;

-- pgque.send(queue, type, payload text, partition_key) -- keyed fast path.
-- Same verbatim-bytes contract as pgque.send(text, text, text): no jsonb
-- parse/canonicalization round-trip; see sql/pgque-api/send.sql.
create or replace function pgque.send(
    i_queue text, i_type text, i_payload text, i_partition_key text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload,
        i_partition_key, null, null, null);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send(text, text, text, text) from public;

-- ---------------------------------------------------------------------------
-- Slot registration (US-12.7: enforced N)
-- ---------------------------------------------------------------------------

create or replace function pgque.subscribe_slot(
    i_queue text, i_consumer text, i_slot int, i_n int)
returns void as $$
declare
    v_queue_id int4;
    v_n int4;
begin
    if i_queue is null or i_queue = '' then
        raise exception 'queue name must not be empty';
    end if;
    if i_consumer is null or i_consumer = '' then
        raise exception 'consumer name must not be empty';
    end if;
    if position('#' in i_consumer) > 0 then
        raise exception 'partitioned consumer name must not contain #: %', i_consumer;
    end if;
    if i_n is null or i_n < 1 then
        raise exception 'slot count n must be >= 1, got %', i_n;
    end if;
    if i_slot is null or i_slot < 0 or i_slot >= i_n then
        raise exception 'slot % out of range for n=% (valid: 0..%)', i_slot, i_n, i_n - 1;
    end if;

    select queue_id into v_queue_id
    from pgque.queue
    where queue_name = i_queue;
    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    /*
     * First call for (queue, consumer) pins n; later calls must match it
     * (SPEC D3). The no-op conflict update locks the existing row and
     * returns the pinned value race-free.
     */
    insert into pgque.partition_consumer as pc (queue_id, co_name, n)
    values (v_queue_id, i_consumer, i_n)
    on conflict (queue_id, co_name) do update set n = pc.n
    returning pc.n into v_n;
    if v_n <> i_n then
        raise exception 'consumer % on queue % is pinned to n=%; got n=% (unsubscribe all slots to change the slot count)',
            i_consumer, i_queue, v_n, i_n;
    end if;

    -- register_consumer is idempotent for an existing subscription.
    perform pgque.register_consumer(i_queue, pgque._slot_name(i_consumer, i_slot, i_n));
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.subscribe_slot(text, text, int, int) from public;

/*
 * Drop one slot subscription. NOTE: unregister_consumer cascades the slot's
 * retry rows and its dead_letter audit (SPEC section 8, teardown). When the
 * last slot of (queue, consumer) is dropped, the pinned-N row is removed so
 * a fresh subscribe_slot may choose a new N.
 */
create or replace function pgque.unsubscribe_slot(
    i_queue text, i_consumer text, i_slot int)
returns void as $$
declare
    v_queue_id int4;
    v_n int4;
begin
    select pc.n, q.queue_id into v_n, v_queue_id
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = i_queue
      and pc.co_name = i_consumer
    for update of pc;
    if not found then
        return;
    end if;

    if i_slot is null or i_slot < 0 or i_slot >= v_n then
        raise exception 'slot % out of range for consumer % on queue % (valid: 0..%)',
            i_slot, i_consumer, i_queue, v_n - 1;
    end if;

    perform pgque.unregister_consumer(i_queue, pgque._slot_name(i_consumer, i_slot, v_n));

    perform 1
    from pgque.subscription as s
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where s.sub_queue = v_queue_id
      and c.co_name in (
          select pgque._slot_name(i_consumer, g, v_n)
          from generate_series(0, v_n - 1) as g);
    if not found then
        delete from pgque.partition_consumer
        where queue_id = v_queue_id
          and co_name = i_consumer;
    end if;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.unsubscribe_slot(text, text, int) from public;

-- ---------------------------------------------------------------------------
-- Slot consume path (US-12.2, US-12.3, US-12.4)
-- ---------------------------------------------------------------------------

/*
 * pgque.receive_partitioned() -- receive the slot's hash class of the stream.
 *
 * Wraps next_batch + the admin-only get_batch_cursor(4) i_extra_where hook.
 * The filter predicate is assembled with format() from the VALIDATED
 * integers n and slot only -- caller strings are never interpolated into
 * the trusted-SQL sink. NULL partition keys route to slot 0 (see header).
 *
 * Works under SECURITY DEFINER through co-ownership with get_batch_cursor
 * (see the OWNERSHIP INVARIANT in the file header) -- this is NOT the
 * receive()/nack() pattern of calling reader-granted internals.
 *
 * Contract mirrors pgque.receive(): returns up to i_max messages of the
 * slot's current batch; ack_partitioned finishes the WHOLE batch even after
 * a partial receive; a batch left unacked is re-issued idempotently (the
 * engine receive lock -- US-12.4). A batch whose filtered slice is empty is
 * finished immediately so the slot cursor keeps advancing.
 */
create or replace function pgque.receive_partitioned(
    i_queue text, i_consumer text, i_slot int, i_n int, i_max int default 100)
returns setof pgque.message as $$
declare
    v_n int4;
    v_batch_id bigint;
    v_cname text;
    ev record;
    cnt int := 0;
begin
    if i_max is null or i_max < 1 then
        raise exception 'pgque.receive_partitioned: max must be >= 1, got %', i_max;
    end if;

    v_n := pgque._partition_n(i_queue, i_consumer, i_slot, i_n);

    v_batch_id := pgque.next_batch(i_queue, pgque._slot_name(i_consumer, i_slot, v_n));
    if v_batch_id is null then
        return;
    end if;

    v_cname := 'pgque_part_' || v_batch_id::text;
    for ev in
        select ev_id, ev_time, ev_retry, ev_type, ev_data,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
        from pgque.get_batch_cursor(v_batch_id, v_cname, i_max,
            format('(case when ev_extra1 is null then 0 else (pg_catalog.hashtextextended(ev_extra1, 0) %% %s + %s) %% %s end) = %s',
                v_n, v_n, v_n, i_slot))
    loop
        return next row(
            ev.ev_id, v_batch_id, ev.ev_type, ev.ev_data,
            ev.ev_retry, ev.ev_time,
            ev.ev_extra1, ev.ev_extra2, ev.ev_extra3, ev.ev_extra4
        )::pgque.message;
        cnt := cnt + 1;
    end loop;

    -- get_batch_cursor leaves the cursor open; close it so a repeated call
    -- in the same transaction does not collide on the cursor name.
    execute 'close ' || quote_ident(v_cname);

    -- Empty filtered batch: finish immediately to advance the slot cursor.
    if cnt = 0 then
        perform pgque.finish_batch(v_batch_id);
    end if;

    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.receive_partitioned(text, text, int, int, int) from public;

-- pgque.ack_partitioned() -- finish the slot's active batch (same ack path
-- as pgque.ack(): finish_batch advances the slot cursor). Returns 1 if a
-- batch was finished, 0 if the slot had no active batch.
create or replace function pgque.ack_partitioned(
    i_queue text, i_consumer text, i_slot int, i_n int)
returns int as $$
declare
    v_n int4;
    v_batch_id bigint;
begin
    v_n := pgque._partition_n(i_queue, i_consumer, i_slot, i_n);

    select s.sub_batch into v_batch_id
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = i_queue
      and c.co_name = pgque._slot_name(i_consumer, i_slot, v_n);
    if not found then
        raise exception 'slot % of consumer % is not subscribed to queue %',
            i_slot, i_consumer, i_queue;
    end if;

    if v_batch_id is null then
        return 0;
    end if;
    return pgque.finish_batch(v_batch_id);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.ack_partitioned(text, text, int, int) from public;

-- ---------------------------------------------------------------------------
-- Slot claim (US-12.4, US-12.5) -- SPEC D7/D8/section 15
-- ---------------------------------------------------------------------------

/*
 * Stateless, shared advisory-lock namespace so all clients (SQL, Go, Python,
 * TS, CLI) agree on the per-slot lock key and cannot silently collide.
 * Deliberately independent of the pinned n: the claim addresses a slot
 * identity, not a slot-count epoch.
 */
create or replace function pgque.slot_lock_key(
    i_queue text, i_consumer text, i_slot int)
returns bigint as $$
select pg_catalog.hashtextextended(
    'pgque.slot:' || i_queue || '/' || i_consumer || '/' || i_slot::text, 0);
$$ language sql immutable parallel safe;
revoke execute on function pgque.slot_lock_key(text, text, int) from public;

/*
 * Non-blocking, session-scoped slot claim. Hold it across the whole
 * receive -> process -> ack loop; release only at a batch boundary
 * (release_slot). If the claiming session dies, PostgreSQL releases the
 * lock and the slot is immediately claimable (US-12.5). The claim is
 * load-bearing for G2: it closes the process -> ack gap the engine receive
 * lock does not span (SPEC section 2).
 */
create or replace function pgque.claim_slot(
    i_queue text, i_consumer text, i_slot int)
returns boolean as $$
select pg_catalog.pg_try_advisory_lock(pgque.slot_lock_key(i_queue, i_consumer, i_slot));
$$ language sql;
revoke execute on function pgque.claim_slot(text, text, int) from public;

-- Release a claimed slot at a batch boundary. Returns false (with a
-- PostgreSQL warning) when this session does not hold the claim.
create or replace function pgque.release_slot(
    i_queue text, i_consumer text, i_slot int)
returns boolean as $$
select pg_catalog.pg_advisory_unlock(pgque.slot_lock_key(i_queue, i_consumer, i_slot));
$$ language sql;
revoke execute on function pgque.release_slot(text, text, int) from public;

-- ---------------------------------------------------------------------------
-- Observability (US-12.6) -- SPEC D10: writeless owner + lag view
-- ---------------------------------------------------------------------------

/*
 * One row per slot 0..n-1 of every partitioned consumer (including slots
 * not yet registered/claimed -- an unpolled slot is exactly what the R7
 * rotation-pinning alert must catch).
 *
 *   owner_pid      -- backend holding the slot claim (pg_locks advisory
 *                     match on slot_lock_key); null if unclaimed.
 *   last_tick      -- the slot subscription's cursor (sub_last_tick).
 *   pending_events -- approximate lag: events in the queue between the
 *                     slot's cursor tick and the latest tick, BEFORE hash
 *                     filtering (tick_event_seq delta). It over-counts a
 *                     single slot's own share by ~n x, but 0 means "caught
 *                     up" exactly, and growth means the slot is stalling.
 */
create or replace view pgque.partition_slot_status as
select
    q.queue_name,
    pc.co_name as consumer,
    gs.slot,
    pc.n,
    lk.pid as owner_pid,
    s.sub_last_tick as last_tick,
    greatest(coalesce(latest.tick_event_seq - cur.tick_event_seq, 0), 0) as pending_events
from pgque.partition_consumer as pc
join pgque.queue as q on q.queue_id = pc.queue_id
cross join lateral generate_series(0, pc.n - 1) as gs(slot)
cross join lateral (
    select pgque.slot_lock_key(q.queue_name, pc.co_name, gs.slot) as key
) as k
left join pgque.consumer as c
    on c.co_name = pgque._slot_name(pc.co_name, gs.slot, pc.n)
left join pgque.subscription as s
    on s.sub_queue = pc.queue_id
    and s.sub_consumer = c.co_id
left join pgque.tick as cur
    on cur.tick_queue = pc.queue_id
    and cur.tick_id = s.sub_last_tick
left join lateral (
    select t.tick_event_seq
    from pgque.tick as t
    where t.tick_queue = pc.queue_id
    order by t.tick_id desc
    limit 1
) as latest on true
left join lateral (
    select l.pid
    from pg_catalog.pg_locks as l
    where l.locktype = 'advisory'
      and l.granted
      and l.objsubid = 1
      and l.database = (
          select d.oid
          from pg_catalog.pg_database as d
          where d.datname = pg_catalog.current_database())
      and l.classid::bigint = ((k.key >> 32) & 4294967295)
      and l.objid::bigint = (k.key & 4294967295)
    limit 1
) as lk on true;

-- ---------------------------------------------------------------------------
-- Grants (SPEC section 8)
-- ---------------------------------------------------------------------------
-- Producer surfaces -> pgque_writer; slot/claim/observability surfaces ->
-- pgque_reader (consumer-side). Internal tables stay off app roles entirely
-- (written only inside SECURITY DEFINER functions); pgque_admin keeps
-- read-only visibility for ops. get_batch_cursor stays admin-only -- the
-- wrapper works through co-ownership, not a grant.

revoke all on pgque.partition_consumer from public, pgque_reader, pgque_writer;
grant select on pgque.partition_consumer to pgque_admin;

grant execute on function pgque.send(text, text, jsonb, text)  to pgque_writer;
grant execute on function pgque.send(text, text, text, text)   to pgque_writer;

grant execute on function pgque.subscribe_slot(text, text, int, int)   to pgque_reader;
grant execute on function pgque.unsubscribe_slot(text, text, int)      to pgque_reader;
grant execute on function pgque.receive_partitioned(text, text, int, int, int) to pgque_reader;
grant execute on function pgque.ack_partitioned(text, text, int, int)  to pgque_reader;
grant execute on function pgque.slot_lock_key(text, text, int)         to pgque_reader;
grant execute on function pgque.claim_slot(text, text, int)            to pgque_reader;
grant execute on function pgque.release_slot(text, text, int)          to pgque_reader;

grant select on pgque.partition_slot_status to pgque_reader;
grant select on pgque.partition_slot_status to pgque_admin;

-- Internal helpers: SECURITY DEFINER callees only.
revoke execute on function pgque._slot_name(text, int, int) from public, pgque_reader, pgque_writer;
revoke execute on function pgque._partition_n(text, text, int, int) from public, pgque_reader, pgque_writer;

-- Re-apply deny-by-default: functions created in this file would otherwise
-- keep PostgreSQL's default PUBLIC EXECUTE (see sql/pgque-api/send.sql).
revoke execute on all functions in schema pgque from public;
