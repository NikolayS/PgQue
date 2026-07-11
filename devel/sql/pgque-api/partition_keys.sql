-- pgque-api/partition_keys.sql -- Partition keys (Phase 1A)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Partition keys (blueprints/partition-keys/SPEC.md, Phase 1A):
--   pgque.send(queue, type, payload, partition_key)  -- jsonb + text overloads
--   pgque.subscribe_partitioned(queue, consumer, n)
--   pgque.subscribe_slot(queue, consumer, slot, n)
--   pgque.unsubscribe_slot(queue, consumer, slot)
--   pgque.claim_slot(queue, consumer, slot, worker, ttl)
--   pgque.release_slot(queue, consumer, slot, worker)
--   pgque.receive_partitioned(queue, consumer, slot, n, worker, max)
--   pgque.ack_partitioned(queue, consumer, slot, n, worker)
--   view pgque.partition_slot_status
--
-- Producer idempotency (Phase 1B) is the separate, orthogonal
-- sql/pgque-api/send_idem.sql; it composes with this feature via
-- send_idem(..., partition_key) but neither requires the other.
--
/*
 * Design notes:
 *
 * * The mechanism is N independent slot consumers. Slot k of consumer C with
 *   fixed slot count N is the engine consumer "C#k/N": its own subscription,
 *   its own cursor. Every slot scans the full stream and filters server-side
 *   to its hash class -- read amplification is ~N x steady (SPEC R2), and a
 *   stalled slot pins rotation for the whole queue (SPEC R7): monitor
 *   pgque.partition_slot_status and alert on lag.
 *
 * * OWNERSHIP INVARIANT (SPEC section 6): receive_partitioned reaches the
 *   admin-only pgque.get_batch_cursor(4) i_extra_where hook because it is a
 *   SECURITY DEFINER function owned by the SAME role that ran sql/pgque.sql
 *   (a function owner may execute its own functions regardless of grants).
 *   This file MUST be installed by the pgque install owner. It does not
 *   depend on that owner holding pgque_admin.
 *
 * * Partition keys ride ev_extra1, so this works for send()-sourced queues
 *   only (triggers use ev_extra1 for the table name -- SPEC D1/R5). Events
 *   with a NULL partition key (e.g. sent through the keyless send()
 *   overloads) route to slot 0, so no event is ever silently dropped by the
 *   hash filter.
 *
 * * Slot ownership is a batch-granularity LEASE (worker id + TTL + epoch
 *   fencing token) stored in pgque.partition_slot. It is plain transactional
 *   DML -- no session state -- so it works under transaction-mode pooling
 *   (PgBouncer/Supavisor): any pooled connection can hold or renew a lease.
 *   receive_partitioned and ack_partitioned renew the lease and fence any
 *   worker that does not hold it. Crash recovery is lease expiry:
 *   a dead worker never releases, so after the TTL remainder another worker
 *   takes over. Takeover bumps the epoch, fencing the zombie -- its next
 *   receive/ack raises rather than double-acking (SPEC section 15).
 */

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
    n           int4    not null,
    constraint partition_consumer_n_check check (n between 1 and 256),
    primary key (queue_id, co_name)
);

/* Keep the constraint definition synchronized on idempotent installs. */
alter table pgque.partition_consumer
    drop constraint if exists partition_consumer_n_check;
alter table pgque.partition_consumer
    add constraint partition_consumer_n_check check (n between 1 and 256);

/*
 * Per-slot lease -- SPEC D7/D8/section 15. A slot is owned by a worker id
 * for a TTL window; epoch is the monotonic fencing token, bumped on every
 * takeover of an expired lease. Written only inside SECURITY DEFINER
 * functions (same pattern as partition_consumer); revoked from app roles.
 * One row per subscribed slot, created by subscribe_partitioned or repaired
 * individually by subscribe_slot.
 */
create table if not exists pgque.partition_slot (
    queue_id    int4 not null,
    co_name     text not null,
    slot        int4 not null,
    lease_owner text,
    lease_until timestamptz,
    lease_ttl   interval,
    epoch       bigint not null default 0,
    primary key (queue_id, co_name, slot),
    foreign key (queue_id, co_name) references pgque.partition_consumer (queue_id, co_name) on delete cascade
);

/*
 * These drops must precede the new objects: the view's column set changes
 * (create or replace view cannot alter it) and the older advisory-lock slot
 * functions are replaced entirely by the lease model.
 */
drop view if exists pgque.partition_slot_status;
drop function if exists pgque.slot_lock_key(text, text, int);
drop function if exists pgque.claim_slot(text, text, int);
drop function if exists pgque.release_slot(text, text, int);
drop function if exists pgque.receive_partitioned(text, text, int, int, int);
drop function if exists pgque.ack_partitioned(text, text, int, int);
drop function if exists pgque._partition_n(text, text, int, int);
drop function if exists pgque._touch_lease(int4, text, text, int, text);

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
 * Shared guard for the consume path (receive/ack/nack_partitioned). Two
 * ordered checks, then a lease renewal; returns the pinned n.
 *
 * 1. Validate (queue, consumer, slot, n) against the pinned slot count
 *    BEFORE any lease error: a wrong-N caller is rejected clearly, never
 *    silently misrouted.
 * 2. Fence the lease: the caller must already hold it as i_worker. An
 *    expired lease still owned by the SAME worker (no successor took over)
 *    is renewed, not rejected -- no zombie exists. clock_timestamp() keeps
 *    lease math correct inside a long transaction.
 */
create or replace function pgque._slot_guard(
    i_queue text, i_consumer text, i_slot int, i_n int, i_worker text)
returns int4 as $$
declare
    v_n int4;
    v_queue_id int4;
    v_owner text;
    v_ttl interval;
begin
    if i_queue is null or i_consumer is null or i_slot is null or i_n is null then
        raise exception 'queue, consumer, slot, and n must not be null';
    end if;

    select pc.n, pc.queue_id into v_n, v_queue_id
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = i_queue
      and pc.co_name = i_consumer;
    if not found then
        raise exception 'consumer % on queue % is not a partitioned consumer; call pgque.subscribe_partitioned() first',
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

    select lease_owner, lease_ttl into v_owner, v_ttl
    from pgque.partition_slot
    where queue_id = v_queue_id
      and co_name = i_consumer
      and slot = i_slot
    for update;
    if not found then
        raise exception 'slot % of consumer % on queue % is not subscribed; call pgque.subscribe_slot()',
            i_slot, i_consumer, i_queue;
    end if;

    if v_owner is null then
        raise exception 'slot % of consumer % on queue % is not leased; call pgque.claim_slot() first',
            i_slot, i_consumer, i_queue;
    end if;
    if v_owner <> i_worker then
        raise exception 'lease on slot % of consumer % on queue % is held by worker %, not %; fenced',
            i_slot, i_consumer, i_queue, v_owner, i_worker;
    end if;

    -- Owner (possibly with an expired-but-un-taken-over lease): renew.
    update pgque.partition_slot
    set lease_until = clock_timestamp() + v_ttl
    where queue_id = v_queue_id
      and co_name = i_consumer
      and slot = i_slot;

    return v_n;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- The slot's open engine batch (null when none); raises if unsubscribed.
create or replace function pgque._slot_batch(
    i_queue text, i_consumer text, i_slot int, i_n int)
returns bigint as $$
declare
    v_batch_id bigint;
begin
    select s.sub_batch into v_batch_id
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = i_queue
      and c.co_name = pgque._slot_name(i_consumer, i_slot, i_n);
    if not found then
        raise exception 'slot % of consumer % is not subscribed to queue %',
            i_slot, i_consumer, i_queue;
    end if;
    return v_batch_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Producer: keyed send
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

/*
 * pgque.send(queue, type, payload text, partition_key) -- keyed fast path.
 * Same verbatim-bytes contract as pgque.send(text, text, text): no jsonb
 * round-trip; see sql/pgque-api/send.sql.
 */
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
-- Slot registration (enforced N)
-- ---------------------------------------------------------------------------

/*
 * Atomically materialize every slot for a partitioned consumer at one shared
 * tick. Existing complete setup is idempotent and never repositions cursors;
 * existing partial setup requires an explicit repair decision.
 */
create or replace function pgque.subscribe_partitioned(
    i_queue text, i_consumer text, i_n int)
returns void as $$
declare
    v_queue_id int4;
    v_n int4;
    v_start_tick bigint;
    v_complete_slots int4;
    v_registered int4;
    v_slot int4;
    v_slot_name text;
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
    if i_n is null or i_n < 1 or i_n > 256 then
        raise exception 'slot count n must be between 1 and 256, got %', i_n;
    end if;

    select queue_id into v_queue_id
    from pgque.queue
    where queue_name = i_queue;
    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    /*
     * ON CONFLICT waits for a concurrent creator. The follow-up row lock makes
     * complete-state inspection serialize with subscribe_slot and another
     * subscribe_partitioned call for the same logical consumer.
     */
    insert into pgque.partition_consumer (queue_id, co_name, n)
    values (v_queue_id, i_consumer, i_n)
    on conflict (queue_id, co_name) do nothing
    returning n into v_n;
    if not found then
        select pc.n into v_n
        from pgque.partition_consumer as pc
        where pc.queue_id = v_queue_id
          and pc.co_name = i_consumer
        for update;

        if v_n <> i_n then
            raise exception 'consumer % on queue % is pinned to n=%; got n=% (unsubscribe all slots to change the slot count)',
                i_consumer, i_queue, v_n, i_n;
        end if;

        select count(*) into v_complete_slots
        from generate_series(0, i_n - 1) as gs(slot)
        join pgque.partition_slot as ps
            on ps.queue_id = v_queue_id
            and ps.co_name = i_consumer
            and ps.slot = gs.slot
        join pgque.consumer as c
            on c.co_name = pgque._slot_name(i_consumer, gs.slot, i_n)
        join pgque.subscription as s
            on s.sub_queue = v_queue_id
            and s.sub_consumer = c.co_id;
        if v_complete_slots <> i_n then
            raise exception 'partitioned consumer % on queue % has incomplete setup (% of % slots subscribed); repair with pgque.subscribe_slot() or recreate it before producing',
                i_consumer, i_queue, v_complete_slots, i_n;
        end if;

        return;
    end if;

    select tick_id into v_start_tick
    from pgque.tick
    where tick_queue = v_queue_id
    order by tick_id desc
    limit 1;
    if not found then
        raise exception 'no ticks for queue: %', i_queue;
    end if;

    for v_slot in 0..i_n - 1 loop
        v_slot_name := pgque._slot_name(i_consumer, v_slot, i_n);
        v_registered := pgque.register_consumer_at(
            i_queue, v_slot_name, v_start_tick);
        if v_registered <> 1 then
            raise exception 'consumer name % is already registered on queue %; cannot reuse it for partition slot %/%',
                v_slot_name, i_queue, v_slot, i_n;
        end if;

        insert into pgque.partition_slot (queue_id, co_name, slot)
        values (v_queue_id, i_consumer, v_slot);
    end loop;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.subscribe_partitioned(text, text, int) from public;

/*
 * Alpha-compatible single-slot registration. Prefer subscribe_partitioned for
 * new consumers; use this only for explicit repair or controlled setup work.
 */
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
    if i_n is null or i_n < 1 or i_n > 256 then
        raise exception 'slot count n must be between 1 and 256, got %', i_n;
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

    -- Materialize the slot's lease row (unleased). Idempotent re-subscribe.
    insert into pgque.partition_slot (queue_id, co_name, slot)
    values (v_queue_id, i_consumer, i_slot)
    on conflict (queue_id, co_name, slot) do nothing;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.subscribe_slot(text, text, int, int) from public;

/*
 * Drop one slot subscription. NOTE: unregister_consumer cascades the slot's
 * retry rows and its dead_letter audit (SPEC section 8, teardown). When the
 * last slot of (queue, consumer) is dropped, the pinned-N row is removed --
 * which cascades any remaining partition_slot rows via the FK -- so a fresh
 * subscribe_slot may choose a new N.
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

    delete from pgque.partition_slot
    where queue_id = v_queue_id
      and co_name = i_consumer
      and slot = i_slot;

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
-- Slot lease -- SPEC D7/D8/section 15
-- ---------------------------------------------------------------------------

/*
 * Claim (or renew) the lease on a slot for a worker id. Returns the epoch
 * fencing token, or null if the slot is currently leased by another live
 * worker (the caller's claim loop then moves to the next slot). The lease
 * lives in pgque.partition_slot -- plain transactional DML, no session state
 * -- so it survives transaction-mode pooling. Uses clock_timestamp() so a
 * pg_sleep inside a transaction correctly ages a short TTL.
 *
 *   owner re-claim  -- renew the window, epoch UNCHANGED.
 *   free / expired  -- takeover: epoch += 1, return the NEW (fencing) epoch.
 *   live, other own -- return null.
 */
create or replace function pgque.claim_slot(
    i_queue text, i_consumer text, i_slot int, i_worker text,
    i_ttl interval default '30 seconds')
returns bigint as $$
declare
    v_queue_id int4;
    v_n int4;
    v_owner text;
    v_until timestamptz;
    v_epoch bigint;
begin
    if i_worker is null or i_worker = '' then
        raise exception 'worker id must not be empty';
    end if;
    if i_ttl is null or not isfinite(i_ttl) or i_ttl < interval '1 second' then
        raise exception 'lease ttl must be a finite interval >= 1 second, got %', i_ttl;
    end if;

    select pc.queue_id, pc.n into v_queue_id, v_n
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = i_queue
      and pc.co_name = i_consumer;
    if not found then
        raise exception 'consumer % on queue % is not a partitioned consumer; call pgque.subscribe_partitioned() first',
            i_consumer, i_queue;
    end if;
    if i_slot is null or i_slot < 0 or i_slot >= v_n then
        raise exception 'slot % out of range for consumer % on queue % (valid: 0..%)',
            i_slot, i_consumer, i_queue, v_n - 1;
    end if;

    select lease_owner, lease_until, epoch into v_owner, v_until, v_epoch
    from pgque.partition_slot
    where queue_id = v_queue_id
      and co_name = i_consumer
      and slot = i_slot
    for update skip locked;
    if not found then
        /* A matching row that could not be locked is busy in another
           transaction. Steer the caller to its next candidate slot. */
        perform 1
        from pgque.partition_slot
        where queue_id = v_queue_id
          and co_name = i_consumer
          and slot = i_slot;
        if found then
            return null;
        end if;

        raise exception 'slot % of consumer % on queue % is not subscribed; call pgque.subscribe_slot()',
            i_slot, i_consumer, i_queue;
    end if;

    if v_owner = i_worker then
        -- Renew: same epoch, no takeover.
        update pgque.partition_slot
        set lease_until = clock_timestamp() + i_ttl,
            lease_ttl = i_ttl
        where queue_id = v_queue_id
          and co_name = i_consumer
          and slot = i_slot;
        return v_epoch;
    elsif v_owner is null or v_until <= clock_timestamp() then
        -- Takeover of a free or expired lease: bump the fencing epoch.
        v_epoch := v_epoch + 1;
        update pgque.partition_slot
        set lease_owner = i_worker,
            lease_until = clock_timestamp() + i_ttl,
            lease_ttl = i_ttl,
            epoch = v_epoch
        where queue_id = v_queue_id
          and co_name = i_consumer
          and slot = i_slot;
        return v_epoch;
    end if;

    -- Leased by another live worker.
    return null;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.claim_slot(text, text, int, text, interval) from public;

/*
 * Release a lease at a batch boundary. Only the owning worker may release
 * (a non-owner call returns false, never raises). The epoch is KEPT so a
 * later takeover still advances the fencing token monotonically. Returns
 * true if this worker held the lease and it was cleared.
 */
create or replace function pgque.release_slot(
    i_queue text, i_consumer text, i_slot int, i_worker text)
returns boolean as $$
declare
    v_queue_id int4;
    v_n int4;
    v_owner text;
    v_batch_id bigint;
begin
    select ps.queue_id, ps.lease_owner, pc.n into v_queue_id, v_owner, v_n
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    join pgque.partition_consumer as pc
      on pc.queue_id = ps.queue_id
     and pc.co_name = ps.co_name
    where q.queue_name = i_queue
      and ps.co_name = i_consumer
      and ps.slot = i_slot
    for update of ps;
    if not found then
        raise exception 'slot % of consumer % on queue % is not subscribed; call pgque.subscribe_slot()',
            i_slot, i_consumer, i_queue;
    end if;

    if v_owner is null or v_owner <> i_worker then
        return false;
    end if;

    /* The lease is the process-to-ack fence. Clearing it with an open batch
       would let a successor receive that same batch while this worker may
       still be processing it. Crash recovery must go through lease expiry. */
    v_batch_id := pgque._slot_batch(i_queue, i_consumer, i_slot, v_n);
    if v_batch_id is not null then
        raise exception 'cannot release slot % of consumer % on queue % while batch % is open; ack the batch first',
            i_slot, i_consumer, i_queue, v_batch_id;
    end if;

    update pgque.partition_slot
    set lease_owner = null,
        lease_until = null,
        lease_ttl = null
    where queue_id = v_queue_id
      and co_name = i_consumer
      and slot = i_slot;
    return true;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.release_slot(text, text, int, text) from public;

-- ---------------------------------------------------------------------------
-- Slot consume path
-- ---------------------------------------------------------------------------

/*
 * pgque.receive_partitioned() -- receive the slot's hash class of the stream.
 * Wraps next_batch + the admin-only get_batch_cursor(4) i_extra_where hook.
 * Works under SECURITY DEFINER through co-ownership with get_batch_cursor
 * (see the OWNERSHIP INVARIANT in the file header), NOT the receive()/nack()
 * pattern of calling reader-granted internals.
 *
 * The filter predicate is built with format() from the VALIDATED integers n
 * and slot only -- caller strings never reach the trusted-SQL sink. NULL
 * partition keys route to slot 0 (see header).
 *
 * Contract mirrors pgque.receive(): the lease is fenced and renewed first
 * (after N validation); returns up to i_max messages of the slot's current
 * batch; ack_partitioned finishes the WHOLE batch even after a partial
 * receive; an unacked batch is re-issued idempotently. An empty filtered
 * slice is finished immediately so the slot cursor keeps advancing.
 */
create or replace function pgque.receive_partitioned(
    i_queue text, i_consumer text, i_slot int, i_n int, i_worker text,
    i_max int default 100)
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

    -- Reject wrong-N, fence the lease, renew it for this batch.
    v_n := pgque._slot_guard(i_queue, i_consumer, i_slot, i_n, i_worker);

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

    /* get_batch_cursor leaves the cursor open; close it so a repeated call
       in the same transaction does not collide on the cursor name. */
    execute 'close ' || quote_ident(v_cname);

    -- Empty filtered batch: finish immediately to advance the slot cursor.
    if cnt = 0 then
        perform pgque.finish_batch(v_batch_id);
    end if;

    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.receive_partitioned(text, text, int, int, text, int) from public;

/*
 * pgque.ack_partitioned() -- finish the slot's active batch (same ack path
 * as pgque.ack(): finish_batch advances the slot cursor). Fences a worker
 * that does not hold the lease. Returns 1 if a batch was finished, 0 if the
 * slot had no active batch.
 */
create or replace function pgque.ack_partitioned(
    i_queue text, i_consumer text, i_slot int, i_n int, i_worker text)
returns int as $$
declare
    v_n int4;
    v_batch_id bigint;
begin
    -- Reject wrong-N, fence the lease, renew it.
    v_n := pgque._slot_guard(i_queue, i_consumer, i_slot, i_n, i_worker);

    v_batch_id := pgque._slot_batch(i_queue, i_consumer, i_slot, v_n);
    if v_batch_id is null then
        return 0;
    end if;
    return pgque.finish_batch(v_batch_id);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.ack_partitioned(text, text, int, int, text) from public;

/*
 * pgque.nack_partitioned() -- lease-fenced per-message retry/DLQ for a slot.
 * The plain pgque.nack() refuses slot consumers (no worker id to lease-check);
 * this is the partitioned equivalent. Fences a worker that does not hold the
 * lease, then routes the message through the shared retry/DLQ core
 * (pgque._nack_batch_event) against the slot's open batch. Raises if the slot
 * has no open batch. Retried keyed events keep ev_extra1, so redelivery stays
 * on the same slot (SPEC section 9).
 */
create or replace function pgque.nack_partitioned(
    i_queue text, i_consumer text, i_slot int, i_n int, i_worker text,
    i_msg pgque.message,
    i_retry_after interval default '60 seconds',
    i_reason text default null)
returns integer as $$
declare
    v_n int4;
    v_batch_id bigint;
begin
    -- Reject wrong-N, fence the lease, renew it.
    v_n := pgque._slot_guard(i_queue, i_consumer, i_slot, i_n, i_worker);

    v_batch_id := pgque._slot_batch(i_queue, i_consumer, i_slot, v_n);
    if v_batch_id is null then
        raise exception 'slot % of consumer % on queue % has no open batch to nack',
            i_slot, i_consumer, i_queue;
    end if;

    return pgque._nack_batch_event(v_batch_id, i_msg, i_retry_after, i_reason);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.nack_partitioned(text, text, int, int, text, pgque.message, interval, text) from public;

-- ---------------------------------------------------------------------------
-- Observability -- SPEC D10: writeless owner + lag view
-- ---------------------------------------------------------------------------

/*
 * One row per expected slot 0..n-1 of every partitioned consumer, including
 * missing subscriptions and slots not yet leased. An unpolled subscribed slot
 * is exactly what the R7 rotation-pinning alert must catch.
 *
 *   subscribed     -- true when the engine subscription exists; false means
 *                     setup is incomplete and cursor lag is unknown.
 *   lease_owner    -- worker holding a LIVE lease on the slot; null when
 *                     unleased OR the lease has expired (lease_until in the
 *                     past). A stale owner is never shown as current.
 *   lease_until    -- lease expiry; epoch -- current fencing token.
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
    s.sub_id is not null as subscribed,
    case when ps.lease_owner is not null and ps.lease_until > clock_timestamp()
         then ps.lease_owner
         else null
    end as lease_owner,
    ps.lease_until,
    coalesce(ps.epoch, 0) as epoch,
    s.sub_last_tick as last_tick,
    case
        when s.sub_id is null then null
        else greatest(coalesce(latest.tick_event_seq - cur.tick_event_seq, 0), 0)
    end as pending_events
from pgque.partition_consumer as pc
join pgque.queue as q on q.queue_id = pc.queue_id
cross join lateral generate_series(0, pc.n - 1) as gs(slot)
left join pgque.partition_slot as ps
    on ps.queue_id = pc.queue_id
    and ps.co_name = pc.co_name
    and ps.slot = gs.slot
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
) as latest on true;

-- ---------------------------------------------------------------------------
-- Grants (SPEC section 8)
-- ---------------------------------------------------------------------------
/*
 * Producer surfaces -> pgque_writer; slot/lease/observability surfaces ->
 * pgque_reader (consumer-side). Internal tables stay off app roles (written
 * only inside SECURITY DEFINER functions); pgque_admin keeps read-only
 * visibility for ops. get_batch_cursor stays admin-only -- the wrapper works
 * through co-ownership, not a grant.
 */

revoke all on pgque.partition_consumer from public, pgque_reader, pgque_writer;
grant select on pgque.partition_consumer to pgque_admin;

revoke all on pgque.partition_slot from public, pgque_reader, pgque_writer;
grant select on pgque.partition_slot to pgque_admin;

grant execute on function pgque.send(text, text, jsonb, text)  to pgque_writer;
grant execute on function pgque.send(text, text, text, text)   to pgque_writer;

grant execute on function pgque.subscribe_partitioned(text, text, int) to pgque_reader;
grant execute on function pgque.subscribe_slot(text, text, int, int)   to pgque_reader;
grant execute on function pgque.unsubscribe_slot(text, text, int)      to pgque_reader;
grant execute on function pgque.claim_slot(text, text, int, text, interval)    to pgque_reader;
grant execute on function pgque.release_slot(text, text, int, text)            to pgque_reader;
grant execute on function pgque.receive_partitioned(text, text, int, int, text, int) to pgque_reader;
grant execute on function pgque.ack_partitioned(text, text, int, int, text)    to pgque_reader;
grant execute on function pgque.nack_partitioned(text, text, int, int, text, pgque.message, interval, text) to pgque_reader;

grant select on pgque.partition_slot_status to pgque_reader;
grant select on pgque.partition_slot_status to pgque_admin;

-- Internal helpers: SECURITY DEFINER callees only.
revoke execute on function pgque._slot_name(text, int, int) from public, pgque_reader, pgque_writer;
revoke execute on function pgque._slot_guard(text, text, int, int, text) from public, pgque_reader, pgque_writer;
revoke execute on function pgque._slot_batch(text, text, int, int) from public, pgque_reader, pgque_writer;

/* Re-apply deny-by-default: functions created in this file would otherwise
   keep PostgreSQL's default PUBLIC EXECUTE (see sql/pgque-api/send.sql). */
revoke execute on all functions in schema pgque from public;
