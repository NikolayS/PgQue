-- metadata_rotation.sql -- 3-table rotation for pgque.subscription and pgque.tick
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Converts the upstream PgQ single-table pgque.subscription and pgque.tick into
-- 3-child UNION ALL views to cap held-xmin bloat, mirroring the event-table
-- rotation pattern already used for pgque.event_<queue>_0/1/2.
--
-- Load order: AFTER config.sql (needs pgque.config table), AFTER all PgQ-derived
-- function files (overrides maint_tables_to_vacuum, maint_operations,
-- unregister_consumer).

-- ======================================================================
-- Step 1: Rotation-pointer singleton
-- ======================================================================
--
-- Declared before the views so the subscription view's WHERE clause can
-- reference it. Keep this table tiny and hot: it is updated on every
-- rotation cycle and kept separate from pgque.config (which is updated
-- only at start()/stop() time).

create table if not exists pgque.meta_rotation (
    singleton                bool      primary key default true check (singleton),
    cur_subscription_table   smallint  not null default 0,
    cur_tick_table           smallint  not null default 0,
    last_rotation_time       timestamptz not null default now(),
    last_rotation_step1_txid bigint    not null default pg_current_xact_id()::text::bigint,
    last_rotation_step2_txid bigint             default pg_current_xact_id()::text::bigint
);

insert into pgque.meta_rotation (singleton) values (true)
on conflict (singleton) do nothing;

-- ======================================================================
-- Step 2: Replace pgque.tick with 3-child rotation
-- ======================================================================
--
-- The upstream PgQ tables.sql defines pgque.tick as a plain table.
-- We need to replace it with a view over three child tables. On a fresh
-- install the table exists but is empty, so we can drop it safely.
-- On reinstall (idempotent) the table may already be a view; drop is a
-- no-op in that case because we use CREATE OR REPLACE VIEW below.
--
-- Strategy: rename the base table to tick_tmpl (template/schema carrier),
-- then build the three child tables from it, then create the view.
-- We use DO blocks to make each step idempotent.

do $$
begin
    -- Convert pgque.tick (base table) to pgque.tick_tmpl if not already done.
    -- After first install: tick is a view, tick_tmpl already exists — skip.
    if exists (
        select 1 from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'pgque' and c.relname = 'tick' and c.relkind = 'r'
    ) then
        -- Drop constraints that reference child tables won't need on template
        alter table pgque.tick drop constraint if exists tick_pkey;
        alter table pgque.tick drop constraint if exists tick_queue_fkey;
        alter table pgque.tick rename to tick_tmpl;
    end if;
end;
$$;

-- Template table: holds the column layout for LIKE inheritance.
-- Never holds rows; only the children do.
create table if not exists pgque.tick_tmpl (
    tick_queue      int4        not null,
    tick_id         bigint      not null,
    tick_time       timestamptz not null default now(),
    tick_snapshot   pg_snapshot not null default pg_current_snapshot(),
    tick_event_seq  bigint      not null,
    constraint tick_tmpl_queue_fkey foreign key (tick_queue)
                               references pgque.queue (queue_id)
);

-- Three physical children, one per rotation slot.
create table if not exists pgque.tick_0 (
    like pgque.tick_tmpl including defaults,
    constraint tick_0_pkey primary key (tick_queue, tick_id),
    constraint tick_0_queue_fkey foreign key (tick_queue)
                               references pgque.queue (queue_id)
);
create table if not exists pgque.tick_1 (
    like pgque.tick_tmpl including defaults,
    constraint tick_1_pkey primary key (tick_queue, tick_id),
    constraint tick_1_queue_fkey foreign key (tick_queue)
                               references pgque.queue (queue_id)
);
create table if not exists pgque.tick_2 (
    like pgque.tick_tmpl including defaults,
    constraint tick_2_pkey primary key (tick_queue, tick_id),
    constraint tick_2_queue_fkey foreign key (tick_queue)
                               references pgque.queue (queue_id)
);

-- Migrate any rows from the template (on fresh install this is empty).
do $$
begin
    if exists (
        select 1 from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'pgque' and c.relname = 'tick_tmpl' and c.relkind = 'r'
    ) then
        insert into pgque.tick_0
            (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
        select tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq
          from pgque.tick_tmpl
        on conflict do nothing;
        truncate pgque.tick_tmpl;
    end if;
end;
$$;

-- Drop the base table view if it already exists from a previous install,
-- then create the UNION ALL view.
-- (CREATE OR REPLACE VIEW handles the view case idempotently.)
create or replace view pgque.tick as
      select 0::smallint as tick_child_table, * from pgque.tick_0
union all
      select 1::smallint as tick_child_table, * from pgque.tick_1
union all
      select 2::smallint as tick_child_table, * from pgque.tick_2;

-- ======================================================================
-- Step 3: Replace pgque.subscription with 3-child rotation
-- ======================================================================

do $$
begin
    -- Convert pgque.subscription (base table) to pgque.subscription_tmpl.
    -- After first install: subscription is a view — skip.
    if exists (
        select 1 from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'pgque' and c.relname = 'subscription' and c.relkind = 'r'
    ) then
        alter table pgque.subscription drop constraint if exists subscription_pkey;
        alter table pgque.subscription drop constraint if exists subscription_batch_idx;
        alter table pgque.subscription drop constraint if exists sub_queue_fkey;
        alter table pgque.subscription drop constraint if exists sub_consumer_fkey;
        alter table pgque.subscription rename to subscription_tmpl;
    end if;
end;
$$;

-- Shared sequence for sub_id: survives rotation so sub_id values remain
-- stable across rotations.
create sequence if not exists pgque.subscription_sub_id_seq;

-- Sync the sequence floor with any pre-existing max sub_id (idempotent).
do $$
declare
    v_max bigint;
begin
    -- If the template table still holds rows (just-renamed), find the max.
    if exists (
        select 1 from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'pgque' and c.relname = 'subscription_tmpl' and c.relkind = 'r'
    ) then
        select max(sub_id) into v_max from pgque.subscription_tmpl;
        if v_max is not null then
            perform setval('pgque.subscription_sub_id_seq', v_max, true);
        end if;
    end if;
end;
$$;

-- Template table: column layout only, no PK/FK (those live on children).
create table if not exists pgque.subscription_tmpl (
    sub_id          int4        not null default nextval('pgque.subscription_sub_id_seq'),
    sub_queue       int4        not null,
    sub_consumer    int4        not null,
    sub_last_tick   bigint,
    sub_active      timestamptz not null default now(),
    sub_batch       bigint,
    sub_next_tick   bigint
);

create table if not exists pgque.subscription_0 (
    like pgque.subscription_tmpl including defaults,
    constraint subscription_0_pkey primary key (sub_queue, sub_consumer),
    constraint subscription_0_batch_uq unique (sub_batch),
    constraint sub_0_queue_fkey foreign key (sub_queue)
                               references pgque.queue (queue_id),
    constraint sub_0_consumer_fkey foreign key (sub_consumer)
                               references pgque.consumer (co_id)
);
create table if not exists pgque.subscription_1 (
    like pgque.subscription_tmpl including defaults,
    constraint subscription_1_pkey primary key (sub_queue, sub_consumer),
    constraint subscription_1_batch_uq unique (sub_batch),
    constraint sub_1_queue_fkey foreign key (sub_queue)
                               references pgque.queue (queue_id),
    constraint sub_1_consumer_fkey foreign key (sub_consumer)
                               references pgque.consumer (co_id)
);
create table if not exists pgque.subscription_2 (
    like pgque.subscription_tmpl including defaults,
    constraint subscription_2_pkey primary key (sub_queue, sub_consumer),
    constraint subscription_2_batch_uq unique (sub_batch),
    constraint sub_2_queue_fkey foreign key (sub_queue)
                               references pgque.queue (queue_id),
    constraint sub_2_consumer_fkey foreign key (sub_consumer)
                               references pgque.consumer (co_id)
);

-- Migrate any pre-existing rows from the template into child_0.
do $$
begin
    if exists (
        select 1 from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'pgque' and c.relname = 'subscription_tmpl' and c.relkind = 'r'
    ) then
        insert into pgque.subscription_0
            (sub_id, sub_queue, sub_consumer, sub_last_tick,
             sub_active, sub_batch, sub_next_tick)
        select sub_id, sub_queue, sub_consumer, sub_last_tick,
               sub_active, sub_batch, sub_next_tick
          from pgque.subscription_tmpl
        on conflict do nothing;
        truncate pgque.subscription_tmpl;
    end if;
end;
$$;

-- Compatibility view named pgque.subscription — same shape as upstream PgQ's
-- table so the rest of the SQL layer keeps working unchanged.
-- Only the *active* child is exposed; the non-active children hold stale row
-- versions from previous rotations and must not be visible to readers.
create or replace view pgque.subscription as
      select 0::smallint as sub_child_table, s.*
        from pgque.subscription_0 s
       where (select cur_subscription_table from pgque.meta_rotation) = 0
union all
      select 1::smallint, s.*
        from pgque.subscription_1 s
       where (select cur_subscription_table from pgque.meta_rotation) = 1
union all
      select 2::smallint, s.*
        from pgque.subscription_2 s
       where (select cur_subscription_table from pgque.meta_rotation) = 2;

-- ======================================================================
-- Step 4: Routing trigger functions for the views
-- ======================================================================

-- pgque._subscription_route() -- INSTEAD OF trigger for pgque.subscription view.
-- Routes INSERT/UPDATE/DELETE to the currently-active child table.
create or replace function pgque._subscription_route()
returns trigger language plpgsql as $$
declare
    v_cur smallint;
begin
    select cur_subscription_table into v_cur from pgque.meta_rotation;
    if tg_op = 'INSERT' then
        if v_cur = 0 then
            insert into pgque.subscription_0
                (sub_id, sub_queue, sub_consumer, sub_last_tick, sub_active,
                 sub_batch, sub_next_tick)
            values (coalesce(new.sub_id, nextval('pgque.subscription_sub_id_seq')),
                    new.sub_queue, new.sub_consumer, new.sub_last_tick,
                    coalesce(new.sub_active, now()), new.sub_batch, new.sub_next_tick);
        elsif v_cur = 1 then
            insert into pgque.subscription_1
                (sub_id, sub_queue, sub_consumer, sub_last_tick, sub_active,
                 sub_batch, sub_next_tick)
            values (coalesce(new.sub_id, nextval('pgque.subscription_sub_id_seq')),
                    new.sub_queue, new.sub_consumer, new.sub_last_tick,
                    coalesce(new.sub_active, now()), new.sub_batch, new.sub_next_tick);
        else
            insert into pgque.subscription_2
                (sub_id, sub_queue, sub_consumer, sub_last_tick, sub_active,
                 sub_batch, sub_next_tick)
            values (coalesce(new.sub_id, nextval('pgque.subscription_sub_id_seq')),
                    new.sub_queue, new.sub_consumer, new.sub_last_tick,
                    coalesce(new.sub_active, now()), new.sub_batch, new.sub_next_tick);
        end if;
        return new;
    elsif tg_op = 'UPDATE' then
        -- Between rotations, ALL live rows live on exactly one child (the
        -- active one). Rotation's truncate+copy step guarantees this.
        -- Update only the active child; use (sub_queue, sub_consumer) as key.
        if v_cur = 0 then
            update pgque.subscription_0
               set sub_id        = new.sub_id,
                   sub_queue     = new.sub_queue,
                   sub_consumer  = new.sub_consumer,
                   sub_last_tick = new.sub_last_tick,
                   sub_active    = new.sub_active,
                   sub_batch     = new.sub_batch,
                   sub_next_tick = new.sub_next_tick
             where sub_queue    = old.sub_queue
               and sub_consumer = old.sub_consumer;
        elsif v_cur = 1 then
            update pgque.subscription_1
               set sub_id        = new.sub_id,
                   sub_queue     = new.sub_queue,
                   sub_consumer  = new.sub_consumer,
                   sub_last_tick = new.sub_last_tick,
                   sub_active    = new.sub_active,
                   sub_batch     = new.sub_batch,
                   sub_next_tick = new.sub_next_tick
             where sub_queue    = old.sub_queue
               and sub_consumer = old.sub_consumer;
        else
            update pgque.subscription_2
               set sub_id        = new.sub_id,
                   sub_queue     = new.sub_queue,
                   sub_consumer  = new.sub_consumer,
                   sub_last_tick = new.sub_last_tick,
                   sub_active    = new.sub_active,
                   sub_batch     = new.sub_batch,
                   sub_next_tick = new.sub_next_tick
             where sub_queue    = old.sub_queue
               and sub_consumer = old.sub_consumer;
        end if;
        return new;
    elsif tg_op = 'DELETE' then
        -- Delete from every child so stale rows left on non-active children
        -- (e.g. within the xmin window of a just-completed rotation) go away.
        delete from pgque.subscription_0
         where sub_queue = old.sub_queue and sub_consumer = old.sub_consumer;
        delete from pgque.subscription_1
         where sub_queue = old.sub_queue and sub_consumer = old.sub_consumer;
        delete from pgque.subscription_2
         where sub_queue = old.sub_queue and sub_consumer = old.sub_consumer;
        return old;
    end if;
    return null;
end;
$$;

drop trigger if exists subscription_route on pgque.subscription;
create trigger subscription_route
    instead of insert or update or delete on pgque.subscription
    for each row execute function pgque._subscription_route();

-- pgque._tick_route() -- INSTEAD OF trigger for pgque.tick view.
create or replace function pgque._tick_route()
returns trigger language plpgsql as $$
declare
    v_cur smallint;
begin
    select cur_tick_table into v_cur from pgque.meta_rotation;
    if tg_op = 'INSERT' then
        if v_cur = 0 then
            insert into pgque.tick_0
                (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
            values (new.tick_queue, new.tick_id,
                    coalesce(new.tick_time, now()),
                    coalesce(new.tick_snapshot, pg_current_snapshot()),
                    new.tick_event_seq);
        elsif v_cur = 1 then
            insert into pgque.tick_1
                (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
            values (new.tick_queue, new.tick_id,
                    coalesce(new.tick_time, now()),
                    coalesce(new.tick_snapshot, pg_current_snapshot()),
                    new.tick_event_seq);
        else
            insert into pgque.tick_2
                (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
            values (new.tick_queue, new.tick_id,
                    coalesce(new.tick_time, now()),
                    coalesce(new.tick_snapshot, pg_current_snapshot()),
                    new.tick_event_seq);
        end if;
        return new;
    elsif tg_op = 'DELETE' then
        -- maint_rotate_tables_step1 DELETEs old ticks by xmin predicate.
        -- Propagate to all three children.
        delete from pgque.tick_0
         where tick_queue = old.tick_queue and tick_id = old.tick_id;
        delete from pgque.tick_1
         where tick_queue = old.tick_queue and tick_id = old.tick_id;
        delete from pgque.tick_2
         where tick_queue = old.tick_queue and tick_id = old.tick_id;
        return old;
    elsif tg_op = 'UPDATE' then
        -- Ticks are immutable in PgQ design; treat UPDATE as delete+insert.
        delete from pgque.tick_0
         where tick_queue = old.tick_queue and tick_id = old.tick_id;
        delete from pgque.tick_1
         where tick_queue = old.tick_queue and tick_id = old.tick_id;
        delete from pgque.tick_2
         where tick_queue = old.tick_queue and tick_id = old.tick_id;
        if v_cur = 0 then
            insert into pgque.tick_0
                (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
            values (new.tick_queue, new.tick_id, new.tick_time,
                    new.tick_snapshot, new.tick_event_seq);
        elsif v_cur = 1 then
            insert into pgque.tick_1
                (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
            values (new.tick_queue, new.tick_id, new.tick_time,
                    new.tick_snapshot, new.tick_event_seq);
        else
            insert into pgque.tick_2
                (tick_queue, tick_id, tick_time, tick_snapshot, tick_event_seq)
            values (new.tick_queue, new.tick_id, new.tick_time,
                    new.tick_snapshot, new.tick_event_seq);
        end if;
        return new;
    end if;
    return null;
end;
$$;

drop trigger if exists tick_route on pgque.tick;
create trigger tick_route
    instead of insert or update or delete on pgque.tick
    for each row execute function pgque._tick_route();

-- ======================================================================
-- Step 5: Override maint_tables_to_vacuum() to list the children
-- ======================================================================
--
-- pgque transformation: the upstream PgQ version lists 'subscription' and
-- 'tick' as tables to vacuum. With 3-child rotation, those are now views.
-- Replace them with the three physical children.

create or replace function pgque.maint_tables_to_vacuum()
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_tables_to_vacuum(0)
--
--      Returns list of tablenames that need frequent vacuuming.
--
-- pgque transformation: subscription and tick are UNION ALL views over
-- three children each. List the children instead of the views.
-- ----------------------------------------------------------------------
declare
    scm text;
    tbl text;
    fqname text;
begin
    -- assume autovacuum handles them fine
    if current_setting('autovacuum') = 'on' then
        return;
    end if;

    for scm, tbl in values
        ('pgque', 'subscription_0'),
        ('pgque', 'subscription_1'),
        ('pgque', 'subscription_2'),
        ('pgque', 'consumer'),
        ('pgque', 'queue'),
        ('pgque', 'tick_0'),
        ('pgque', 'tick_1'),
        ('pgque', 'tick_2'),
        ('pgque', 'retry_queue'),
        ('pgq_ext', 'completed_tick'),
        ('pgq_ext', 'completed_batch'),
        ('pgq_ext', 'completed_event'),
        ('pgq_ext', 'partial_batch'),
        --('pgq_node', 'node_location'),
        --('pgq_node', 'node_info'),
        ('pgq_node', 'local_state'),
        --('pgq_node', 'subscriber_info'),
        --('londiste', 'table_info'),
        ('londiste', 'seq_info'),
        --('londiste', 'applied_execute'),
        --('londiste', 'pending_fkeys'),
        ('txid', 'epoch'),
        ('londiste', 'completed')
    loop
        select n.nspname || '.' || t.relname into fqname
            from pg_class t, pg_namespace n
            where n.oid = t.relnamespace
                and n.nspname = scm
                and t.relname = tbl;
        if found then
            return next fqname;
        end if;
    end loop;
    return;
end;
$$ language plpgsql;

-- ======================================================================
-- Step 6: Override maint_operations() to emit metadata rotation calls
-- ======================================================================
--
-- pgque transformation: emit pgque.maint_rotate_metadata and
-- pgque.maint_rotate_metadata_step2 in the operations list. The
-- maint_rotate_metadata_step2 entry is filtered out by pgque.maint()
-- (same pattern as maint_rotate_tables_step2) and run in a separate
-- transaction by pgque.start()'s pgque_rotate_step2 cron job.

create or replace function pgque.maint_operations(out func_name text, out func_arg text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_operations(0)
--
--      Returns list of functions to call for maintenance.
--
-- pgque transformation: adds metadata rotation entries.
-- ----------------------------------------------------------------------
declare
    ops text[];
    nrot int4;
begin
    -- rotate event tables step 1
    nrot := 0;
    func_name := 'pgque.maint_rotate_tables_step1';
    for func_arg in
        select queue_name from pgque.queue
            where queue_rotation_period is not null
                and queue_switch_step2 is not null
                and queue_switch_time + queue_rotation_period < current_timestamp
            order by 1
    loop
        nrot := nrot + 1;
        return next;
    end loop;

    -- rotate event tables step 2
    if nrot = 0 then
        select count(1) from pgque.queue
            where queue_rotation_period is not null
                and queue_switch_step2 is null
            into nrot;
    end if;
    if nrot > 0 then
        func_name := 'pgque.maint_rotate_tables_step2';
        func_arg := NULL;
        return next;
    end if;

    -- metadata rotation (pgque transformation): rotate subscription
    -- and tick child tables to cap held-xmin bloat.
    func_name := 'pgque.maint_rotate_metadata';
    func_arg := NULL;
    return next;
    func_name := 'pgque.maint_rotate_metadata_step2';
    func_arg := NULL;
    return next;

    -- check if extra field exists
    perform 1 from pg_attribute
      where attrelid = 'pgque.queue'::regclass
        and attname = 'queue_extra_maint';
    if found then
        -- add extra ops
        for func_arg, ops in
            select q.queue_name, queue_extra_maint from pgque.queue q
             where queue_extra_maint is not null
             order by 1
        loop
            for i in array_lower(ops, 1) .. array_upper(ops, 1)
            loop
                func_name = ops[i];
                return next;
            end loop;
        end loop;
    end if;

    -- vacuum tables
    func_name := 'vacuum';
    for func_arg in
        select * from pgque.maint_tables_to_vacuum()
    loop
        return next;
    end loop;

    return;
end;
$$ language plpgsql;

-- ======================================================================
-- Step 7: Override unregister_consumer() — drop FOR UPDATE OF s
-- ======================================================================
--
-- pgque transformation: pgque.subscription is now a UNION ALL view and
-- does not accept row locks. Drop "FOR UPDATE OF s"; keep "FOR UPDATE OF c"
-- (consumer row lock is still required). This is safe: unregister_consumer
-- callers are rare and single-threaded in practice; the subsequent DELETE
-- provides the necessary row lock.

create or replace function pgque.unregister_consumer(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.unregister_consumer(2)
--
--      Unsubscribe consumer from the queue.
--      Also consumer's retry events are deleted.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      number of (sub)consumers unregistered
--
-- pgque transformation: FOR UPDATE OF s dropped because pgque.subscription
-- is now a UNION ALL view (see 3-table rotation). The view does not accept
-- row locks. FOR UPDATE OF c (consumer row lock) is still enforced.
-- ----------------------------------------------------------------------
declare
    x_sub_id integer;
    _sub_id_cnt integer;
    _consumer_id integer;
    _is_subconsumer boolean;
begin
    select s.sub_id, c.co_id,
           -- subconsumers: both null or both not-null for main consumer
           (s.sub_last_tick is null and s.sub_next_tick is null)
               or (s.sub_last_tick is not null and s.sub_next_tick is not null)
      into x_sub_id, _consumer_id, _is_subconsumer
      from pgque.subscription s, pgque.consumer c, pgque.queue q
     where s.sub_queue = q.queue_id
       and s.sub_consumer = c.co_id
       and q.queue_name = x_queue_name
       and c.co_name = x_consumer_name
       for update of c;
    if not found then
        return 0;
    end if;

    -- consumer + subconsumer count
    select count(*) into _sub_id_cnt
        from pgque.subscription
       where sub_id = x_sub_id;

    -- delete only one subconsumer
    if _sub_id_cnt > 1 and _is_subconsumer then
        delete from pgque.subscription
              where sub_id = x_sub_id
                and sub_consumer = _consumer_id;
        return 1;
    else
        -- delete main consumer (including possible subconsumers)

        -- retry events
        delete from pgque.retry_queue
            where ev_owner = x_sub_id;

        -- this will drop subconsumers too
        delete from pgque.subscription
            where sub_id = x_sub_id;

        perform 1 from pgque.subscription
            where sub_consumer = _consumer_id;
        if not found then
            delete from pgque.consumer
                where co_id = _consumer_id;
        end if;

        return _sub_id_cnt;
    end if;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ======================================================================
-- Step 8: Metadata rotation functions
-- ======================================================================

create or replace function pgque.maint_rotate_metadata()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_rotate_metadata(0)
--
--      Rotate subscription + tick child tables to cap held-xmin bloat.
--
--      This is the metadata-table analogue of maint_rotate_tables_step1 +
--      step2. Unlike event tables, subscription and tick storage is NOT
--      per-queue — there is one set of three children total.
--
--      Subscription procedure:
--        next := (cur + 1) % 3
--        TRUNCATE pgque.subscription_<next>   (was the oldest slot)
--        INSERT INTO pgque.subscription_<next>
--            SELECT * FROM pgque.subscription_<cur>
--        flip cur := next
--
--      Tick procedure (conditional):
--        next := (cur + 1) % 3
--        Only truncate+flip if no live sub_last_tick references land in
--        the target slot. The tick slot is skipped this cycle if any
--        consumer's sub_last_tick would be stranded.
--
-- Returns:
--      1 if a rotation happened, 0 if skipped (too soon, or gated).
-- ----------------------------------------------------------------------
declare
    mr              record;
    rotation_period interval;
    next_sub        smallint;
    next_tick       smallint;
begin
    rotation_period := coalesce(
        current_setting('pgque.meta_rotation_period', true)::interval,
        interval '30 seconds'
    );

    select * into mr from pgque.meta_rotation for update;

    -- Too soon?
    if now() < mr.last_rotation_time + rotation_period then
        return 0;
    end if;

    -- Held-xmin gate: require that the previous rotation's step2 txid has
    -- been acknowledged in a separate transaction before rotating again.
    if mr.last_rotation_step2_txid is null then
        return 0;
    end if;

    -- Compute targets.
    next_sub  := (mr.cur_subscription_table + 1) % 3;
    next_tick := (mr.cur_tick_table + 1) % 3;

    -- Subscription rotation: lock current + target (nowait), truncate target,
    -- copy live rows from current to target, then flip pointer.
    begin
        execute format('lock table pgque.subscription_%s in exclusive mode nowait',
                       mr.cur_subscription_table);
        execute format('lock table pgque.subscription_%s in exclusive mode nowait',
                       next_sub);
        execute format('truncate pgque.subscription_%s', next_sub);
        execute format(
            'insert into pgque.subscription_%s '
         || '  (sub_id, sub_queue, sub_consumer, sub_last_tick, '
         || '   sub_active, sub_batch, sub_next_tick) '
         || 'select sub_id, sub_queue, sub_consumer, sub_last_tick, '
         || '       sub_active, sub_batch, sub_next_tick '
         || '  from pgque.subscription_%s',
            next_sub, mr.cur_subscription_table);
    exception
        when lock_not_available then
            return 0;
    end;

    -- Tick rotation: conditional on no live sub_last_tick references in the
    -- target slot. Tick rows are referenced by subscription rows; truncating
    -- a slot that still has referenced tick rows would strand consumers.
    if not exists (
        select 1
          from pgque.subscription s
          join pgque.tick_0 t0 on t0.tick_queue = s.sub_queue and t0.tick_id = s.sub_last_tick
         where next_tick = 0
         union all
        select 1
          from pgque.subscription s
          join pgque.tick_1 t1 on t1.tick_queue = s.sub_queue and t1.tick_id = s.sub_last_tick
         where next_tick = 1
         union all
        select 1
          from pgque.subscription s
          join pgque.tick_2 t2 on t2.tick_queue = s.sub_queue and t2.tick_id = s.sub_last_tick
         where next_tick = 2
    ) then
        begin
            execute format('lock table pgque.tick_%s in exclusive mode nowait', next_tick);
            execute format('truncate pgque.tick_%s', next_tick);
        exception
            when lock_not_available then
                next_tick := mr.cur_tick_table; -- keep tick pointer this cycle
        end;
    else
        next_tick := mr.cur_tick_table; -- defer tick rotation this cycle
    end if;

    -- Flip pointers atomically.
    update pgque.meta_rotation
       set cur_subscription_table   = next_sub,
           cur_tick_table           = next_tick,
           last_rotation_time       = now(),
           last_rotation_step1_txid = pg_current_xact_id()::text::bigint,
           last_rotation_step2_txid = null
     where singleton;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.maint_rotate_metadata_step2()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_rotate_metadata_step2(0)
--
--      Must run in a separate transaction from maint_rotate_metadata()
--      so that its txid is visible to all new snapshots before the next
--      rotation decides whether it is safe to truncate the next slot.
--      Mirrors the role of maint_rotate_tables_step2 for event tables.
-- ----------------------------------------------------------------------
begin
    update pgque.meta_rotation
       set last_rotation_step2_txid = pg_current_xact_id()::text::bigint
     where last_rotation_step2_txid is null;
    return 0;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ======================================================================
-- Step 9: Grants for new functions / objects
-- ======================================================================

grant select on pgque.meta_rotation to pgque_reader, pgque_writer, pgque_admin;
grant insert, update on pgque.meta_rotation to pgque_admin;

-- Views: re-apply the same SELECT grants that PgQ's grants.sql put on the
-- base tables. After the rename-to-tmpl + view-creation above, the view
-- objects are new and have no grants yet. Mirror the upstream grants.
grant select on pgque.subscription to public;
grant select on pgque.tick to public;
-- Writer role: pgque.subscription needs insert/update/delete via the view
-- for the INSTEAD OF trigger to work when called from SECURITY DEFINER
-- functions that execute as the install owner. The trigger functions
-- themselves target the child tables directly, but the INSTEAD OF trigger
-- fires on the view, so the caller needs INSERT/UPDATE/DELETE on the view.
grant insert, update, delete on pgque.subscription to pgque_writer, pgque_admin;
grant insert, update, delete on pgque.tick to pgque_admin;

grant select on pgque.subscription_0, pgque.subscription_1, pgque.subscription_2
    to pgque_reader, pgque_writer, pgque_admin;
grant insert, update, delete on pgque.subscription_0, pgque.subscription_1,
    pgque.subscription_2 to pgque_admin;
grant usage on pgque.subscription_sub_id_seq to pgque_admin;

grant select on pgque.tick_0, pgque.tick_1, pgque.tick_2
    to pgque_reader, pgque_writer, pgque_admin;
grant insert, update, delete on pgque.tick_0, pgque.tick_1, pgque.tick_2
    to pgque_admin;

grant execute on function pgque.maint_rotate_metadata() to pgque_admin;
grant execute on function pgque.maint_rotate_metadata_step2() to pgque_admin;
