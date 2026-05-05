-- pgque-api/cooperative_consumers.sql -- Experimental cooperative consumers API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Cooperative-aware overrides extend PgQ-derived primitives.
-- New cooperative APIs are clean-room; no code copied from pgq-coop.

-- Cooperative consumer state marker. Existing rows remain normal on upgrade.
alter table pgque.subscription
  add column if not exists sub_role text not null default 'normal';

do $$
begin
    if not exists (
        select
            1
        from
            pg_catalog.pg_constraint
        where
            conrelid = 'pgque.subscription'::regclass
            and conname = 'subscription_sub_role_check'
    ) then
        alter table pgque.subscription
            add constraint subscription_sub_role_check
            check (sub_role in ('normal', 'coop_main', 'coop_member'));
    end if;
end $$;

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
-- Calls:
--      None
-- Tables directly manipulated:
--      delete - pgque.retry_queue
--      delete - pgque.subscription
-- ----------------------------------------------------------------------
declare
    x_sub_id integer;
    _sub_id_cnt integer;
    _consumer_id integer;
    _sub_role text;
begin
    select
        s.sub_id,
        c.co_id,
        s.sub_role
    into
        x_sub_id,
        _consumer_id,
        _sub_role
    from
        pgque.subscription as s
        inner join pgque.queue as q
            on q.queue_id = s.sub_queue
        inner join pgque.consumer as c
            on c.co_id = s.sub_consumer
    where
        q.queue_name = x_queue_name
        and c.co_name = x_consumer_name
    for update of s, c;
    if not found then
        return 0;
    end if;

    -- consumer + subconsumer count
    select
        count(*)
    into
        _sub_id_cnt
    from
        pgque.subscription
    where
        sub_id = x_sub_id;

    -- delete only one cooperative subconsumer
    if _sub_id_cnt > 1 and _sub_role = 'coop_member' then
        perform
            1
        from
            pgque.subscription
        where
            sub_id = x_sub_id
            and sub_consumer = _consumer_id
            and sub_batch is not null;
        if found then
            raise exception 'cannot unregister active cooperative subconsumer without forced batch handling';
        end if;

        delete from pgque.subscription
        where
            sub_id = x_sub_id
            and sub_consumer = _consumer_id;

        perform
            1
        from
            pgque.subscription
        where
            sub_consumer = _consumer_id;
        if not found then
            delete from pgque.consumer
            where
                co_id = _consumer_id;
        end if;

        if not exists (
            select
                1
            from
                pgque.subscription
            where
                sub_id = x_sub_id
                and sub_role = 'coop_member'
        ) then
            update pgque.subscription
            set
                sub_role = 'normal',
                sub_active = now()
            where
                sub_id = x_sub_id
                and sub_role = 'coop_main';
        end if;

        return 1;
    else
        -- Refuse implicit cooperative teardown through the legacy main
        -- consumer API. Members must be unregistered explicitly so one
        -- caller cannot wipe sibling subconsumers by guessing the main name.
        if _sub_role = 'coop_main' then
            perform
                1
            from
                pgque.subscription
            where
                sub_id = x_sub_id
                and sub_role = 'coop_member';
            if found then
                raise exception 'cannot unregister cooperative main consumer with registered subconsumers';
            end if;
        end if;

        -- delete main consumer (or a legacy single-row subscription)
        perform
            1
        from
            pgque.subscription
        where
            sub_id = x_sub_id
            and sub_role = 'coop_member'
            and sub_batch is not null;
        if found then
            raise exception 'cannot unregister cooperative consumer with active subconsumer batches';
        end if;

        -- retry events
        delete from pgque.retry_queue
        where
            ev_owner = x_sub_id;

        -- this will drop subconsumers too
        delete from pgque.subscription
        where
            sub_id = x_sub_id;

        perform
            1
        from
            pgque.subscription
        where
            sub_consumer = _consumer_id;
        if not found then
            delete from pgque.consumer
            where
                co_id = _consumer_id;
        end if;

        return _sub_id_cnt;
    end if;

end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.next_batch_custom(
    in i_queue_name text,
    in i_consumer_name text,
    in i_min_lag interval,
    in i_min_count int4,
    in i_min_interval interval,
    out batch_id int8,
    out cur_tick_id int8,
    out prev_tick_id int8,
    out cur_tick_time timestamptz,
    out prev_tick_time timestamptz,
    out cur_tick_event_seq int8,
    out prev_tick_event_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgque.next_batch_custom(5)
--
--      Makes next block of events active.  Block size can be tuned
--      with i_min_count, i_min_interval parameters.  Events age can
--      be tuned with i_min_lag.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep then.
--
--      The values from event_id sequence may give hint how big the
--      batch may be.  But they are inexact, they do not give exact size.
--      Client *MUST NOT* use them to detect whether the batch contains any
--      events at all - the values are unfit for that purpose.
--
-- Note:
--      i_min_lag together with i_min_interval/i_min_count is inefficient.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--      i_min_lag           - Consumer wants events older than that
--      i_min_count         - Consumer wants batch to contain at least this many events
--      i_min_interval      - Consumer wants batch to cover at least this much time
--
-- Returns:
--      batch_id            - Batch ID or NULL if there are no more events available.
--      cur_tick_id         - End tick id.
--      cur_tick_time       - End tick time.
--      cur_tick_event_seq  - Value from event id sequence at the time tick was issued.
--      prev_tick_id        - Start tick id.
--      prev_tick_time      - Start tick time.
--      prev_tick_event_seq - value from event id sequence at the time tick was issued.
-- Calls:
--      pgque.find_tick_helper
-- Tables directly manipulated:
--      update - pgque.subscription
-- ----------------------------------------------------------------------
declare
    errmsg text;
    queue_id integer;
    cur_sub_id integer;
    cons_id integer;
    sub_role text;
begin
    select
        s.sub_queue,
        s.sub_consumer,
        s.sub_id,
        s.sub_batch,
        s.sub_role,
        t1.tick_id,
        t1.tick_time,
        t1.tick_event_seq,
        t2.tick_id,
        t2.tick_time,
        t2.tick_event_seq
    into
        queue_id,
        cons_id,
        cur_sub_id,
        batch_id,
        sub_role,
        prev_tick_id,
        prev_tick_time,
        prev_tick_event_seq,
        cur_tick_id,
        cur_tick_time,
        cur_tick_event_seq
    from
        pgque.subscription as s
        inner join pgque.queue as q
            on q.queue_id = s.sub_queue
        inner join pgque.consumer as c
            on c.co_id = s.sub_consumer
        left join pgque.tick as t1
            on t1.tick_queue = s.sub_queue
            and t1.tick_id = s.sub_last_tick
        left join pgque.tick as t2
            on t2.tick_queue = s.sub_queue
            and t2.tick_id = s.sub_next_tick
    where
        q.queue_name = i_queue_name
        and c.co_name = i_consumer_name;
    if not found then
        errmsg := 'Not subscriber to queue: '
            || coalesce(i_queue_name, 'NULL')
            || '/'
            || coalesce(i_consumer_name, 'NULL');
        raise exception '%', errmsg;
    end if;

    if sub_role = 'coop_main' and exists (
        select
            1
        from
            pgque.subscription as sx
        where
            sx.sub_queue = queue_id
            and sx.sub_id = cur_sub_id
            and sx.sub_role = 'coop_member'
    ) then
        raise exception 'consumer % on queue % is a cooperative main consumer; use cooperative receive/next_batch with a subconsumer', i_consumer_name, i_queue_name;
    end if;

    -- sanity check
    if prev_tick_id is null then
        raise exception 'PgQ corruption: Consumer % on queue % does not see tick %', i_consumer_name, i_queue_name, prev_tick_id;
    end if;

    -- has already active batch
    if batch_id is not null then
        return;
    end if;

    if i_min_interval is null and i_min_count is null then
        -- find next tick
        select
            tick_id,
            tick_time,
            tick_event_seq
        into
            cur_tick_id,
            cur_tick_time,
            cur_tick_event_seq
        from
            pgque.tick
        where
            tick_id > prev_tick_id
            and tick_queue = queue_id
        order by
            tick_queue asc,
            tick_id asc
        limit 1;
    else
        -- find custom tick
        select
            next_tick_id,
            next_tick_time,
            next_tick_seq
        into
            cur_tick_id,
            cur_tick_time,
            cur_tick_event_seq
        from
            pgque.find_tick_helper(
                queue_id,
                prev_tick_id,
                prev_tick_time,
                prev_tick_event_seq,
                i_min_count,
                i_min_interval
            );
    end if;

    if i_min_lag is not null then
        -- enforce min lag
        if now() - cur_tick_time < i_min_lag then
            cur_tick_id := null;
            cur_tick_time := null;
            cur_tick_event_seq := null;
        end if;
    end if;

    if cur_tick_id is null then
        -- nothing to do
        prev_tick_id := null;
        prev_tick_time := null;
        prev_tick_event_seq := null;
        return;
    end if;

    -- get next batch
    batch_id := nextval('pgque.batch_id_seq');
    update pgque.subscription
    set
        sub_batch = batch_id,
        sub_next_tick = cur_tick_id,
        sub_active = now()
    where
        sub_queue = queue_id
        and sub_consumer = cons_id;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.finish_batch(
    x_batch_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.finish_batch(1)
--
--      Closes a batch.  No more operations can be done with events
--      of this batch.
--
-- Parameters:
--      x_batch_id      - id of batch.
--
-- Returns:
--      1 if batch was found, 0 otherwise.
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgque.subscription
-- ----------------------------------------------------------------------
declare
    v_sub record;
begin
    select
        *
    into
        v_sub
    from
        pgque.subscription
    where
        sub_batch = x_batch_id
    for update;
    if not found then
        raise warning 'finish_batch: batch % not found', x_batch_id;
        return 0;
    end if;

    if v_sub.sub_role = 'coop_main' then
        raise exception 'cannot finish cooperative main consumer batch % as normal active consumer', x_batch_id;
    elsif v_sub.sub_role = 'coop_member' then
        perform pgque._clear_member_cursor(v_sub.sub_queue, v_sub.sub_consumer);
    else
        update pgque.subscription
        set
            sub_active = now(),
            sub_last_tick = sub_next_tick,
            sub_next_tick = null,
            sub_batch = null
        where
            sub_queue = v_sub.sub_queue
            and sub_consumer = v_sub.sub_consumer;
    end if;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque cooperative consumers (experimental in PgQue 0.2)
create or replace function pgque._validate_coop_names(
    i_queue text,
    i_consumer text,
    i_subconsumer text)
returns void as $$
begin
    if i_queue is null or i_queue = '' then
        raise exception 'queue name must not be empty';
    end if;
    if i_consumer is null or i_consumer = '' then
        raise exception 'consumer name must not be empty';
    end if;
    if i_subconsumer is null or i_subconsumer = '' then
        raise exception 'subconsumer name must not be empty';
    end if;
    if position('.' in i_consumer) > 0 then
        raise exception 'cooperative consumer name must not contain dot: %', i_consumer;
    end if;
    if position('.' in i_subconsumer) > 0 then
        raise exception 'cooperative subconsumer name must not contain dot: %', i_subconsumer;
    end if;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- Reset a coop_member subscription's batch token + tick window. Member rows
-- never advance sub_last_tick on their own — the main consumer owns the
-- cursor — so clearing both ticks releases the member without losing position.
create or replace function pgque._clear_member_cursor(
    p_queue_id int4,
    p_consumer_id int4)
returns void as $$
begin
    update pgque.subscription
    set
        sub_active = now(),
        sub_last_tick = null,
        sub_next_tick = null,
        sub_batch = null
    where
        sub_queue = p_queue_id
        and sub_consumer = p_consumer_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

drop function if exists pgque.subscribe_subconsumer(text, text, text);
drop function if exists pgque.register_subconsumer(text, text, text);

create or replace function pgque.register_subconsumer(
    i_queue text,
    i_consumer text,
    i_subconsumer text,
    i_convert_normal boolean default false)
returns integer as $$
declare
    v_queue_id int4;
    v_main_consumer_id int4;
    v_member_consumer_id int4;
    v_main record;
    v_member record;
    v_member_name text;
    v_last_tick bigint;
    v_created integer := 0;
begin
    perform pgque._validate_coop_names(i_queue, i_consumer, i_subconsumer);
    v_member_name := i_consumer || '.' || i_subconsumer;

    select
        queue_id
    into
        v_queue_id
    from
        pgque.queue
    where
        queue_name = i_queue;
    if not found then
        raise exception 'Event queue not created yet';
    end if;

    select
        co_id
    into
        v_main_consumer_id
    from
        pgque.consumer
    where
        co_name = i_consumer
    for update;
    if not found then
        insert into pgque.consumer (co_name)
        values (i_consumer)
        returning co_id into v_main_consumer_id;
    end if;

    select
        *
    into
        v_main
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_main_consumer_id
    for update;
    if not found then
        select
            tick_id
        into
            v_last_tick
        from
            pgque.tick
        where
            tick_queue = v_queue_id
        order by
            tick_queue desc,
            tick_id desc
        limit 1;
        if not found then
            raise exception 'No ticks for this queue.  Please run ticker on database.';
        end if;

        insert into pgque.subscription (
            sub_queue,
            sub_consumer,
            sub_last_tick,
            sub_role
        )
        values (
            v_queue_id,
            v_main_consumer_id,
            v_last_tick,
            'coop_main'
        )
        returning * into v_main;
        v_created := 1;
    elsif v_main.sub_role = 'normal' then
        if not i_convert_normal then
            raise exception 'consumer % on queue % is already a normal consumer; explicit conversion is required', i_consumer, i_queue;
        end if;
        if v_main.sub_batch is not null then
            raise exception 'cannot convert active normal consumer % on queue % to cooperative main', i_consumer, i_queue;
        end if;

        update pgque.subscription
        set
            sub_role = 'coop_main',
            sub_active = now()
        where
            sub_queue = v_queue_id
            and sub_consumer = v_main_consumer_id
        returning * into v_main;
    elsif v_main.sub_role <> 'coop_main' then
        raise exception 'consumer % on queue % is not a cooperative main consumer', i_consumer, i_queue;
    end if;

    select
        co_id
    into
        v_member_consumer_id
    from
        pgque.consumer
    where
        co_name = v_member_name
    for update;
    if not found then
        insert into pgque.consumer (co_name)
        values (v_member_name)
        returning co_id into v_member_consumer_id;
    end if;

    select
        *
    into
        v_member
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_member_consumer_id
    for update;
    if found then
        if v_member.sub_role <> 'coop_member' or v_member.sub_id <> v_main.sub_id then
            raise exception 'consumer name % on queue % is already registered incompatibly', v_member_name, i_queue;
        end if;

        update pgque.subscription
        set
            sub_active = now()
        where
            sub_queue = v_queue_id
            and sub_consumer = v_member_consumer_id;
        return v_created;
    end if;

    insert into pgque.subscription (
        sub_id,
        sub_queue,
        sub_consumer,
        sub_last_tick,
        sub_active,
        sub_batch,
        sub_next_tick,
        sub_role
    )
    values (
        v_main.sub_id,
        v_queue_id,
        v_member_consumer_id,
        null,
        now(),
        null,
        null,
        'coop_member'
    );
    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.subscribe_subconsumer(
    i_queue text,
    i_consumer text,
    i_subconsumer text,
    i_convert_normal boolean default false)
returns integer as $$
begin
    return pgque.register_subconsumer(i_queue, i_consumer, i_subconsumer, i_convert_normal);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.touch_subconsumer(
    i_queue text,
    i_consumer text,
    i_subconsumer text)
returns integer as $$
declare
    v_member_name text;
    v_cnt integer;
begin
    perform pgque._validate_coop_names(i_queue, i_consumer, i_subconsumer);
    v_member_name := i_consumer || '.' || i_subconsumer;

    update pgque.subscription as s
    set
        sub_active = clock_timestamp()
    from
        pgque.queue as q
        cross join pgque.consumer as c
    where
        q.queue_name = i_queue
        and c.co_name = v_member_name
        and s.sub_queue = q.queue_id
        and s.sub_consumer = c.co_id
        and s.sub_role = 'coop_member';
    get diagnostics v_cnt = row_count;
    return v_cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.next_batch_custom(
    in i_queue text,
    in i_consumer text,
    in i_subconsumer text,
    in i_min_lag interval,
    in i_min_count int4,
    in i_min_interval interval,
    in i_dead_interval interval default null,
    out batch_id bigint,
    out prev_tick_id bigint,
    out next_tick_id bigint)
as $$
declare
    v_queue_id int4;
    v_main_consumer_id int4;
    v_member_consumer_id int4;
    v_member_name text;
    v_main record;
    v_member record;
    v_victim record;
    v_prev_tick_time timestamptz;
    v_prev_tick_event_seq bigint;
    v_next_tick_time timestamptz;
    v_next_tick_event_seq bigint;
begin
    perform pgque.register_subconsumer(i_queue, i_consumer, i_subconsumer);
    v_member_name := i_consumer || '.' || i_subconsumer;

    select
        q.queue_id,
        c.co_id
    into
        v_queue_id,
        v_main_consumer_id
    from
        pgque.queue as q
        cross join pgque.consumer as c
    where
        q.queue_name = i_queue
        and c.co_name = i_consumer;

    select
        *
    into
        v_main
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_main_consumer_id
        and sub_role = 'coop_main'
    for update;
    if not found then
        raise exception 'cooperative main consumer not found: %/%', i_queue, i_consumer;
    end if;

    select
        co_id
    into
        v_member_consumer_id
    from
        pgque.consumer
    where
        co_name = v_member_name;

    select
        *
    into
        v_member
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_member_consumer_id
        and sub_id = v_main.sub_id
        and sub_role = 'coop_member'
    for update;
    if not found then
        raise exception 'cooperative subconsumer not found: %/%/%', i_queue, i_consumer, i_subconsumer;
    end if;

    if v_member.sub_batch is not null then
        update pgque.subscription
        set
            sub_active = now()
        where
            sub_queue = v_member.sub_queue
            and sub_consumer = v_member.sub_consumer;
        batch_id := v_member.sub_batch;
        prev_tick_id := v_member.sub_last_tick;
        next_tick_id := v_member.sub_next_tick;
        return;
    end if;

    if i_dead_interval is not null then
        select
            *
        into
            v_victim
        from
            pgque.subscription
        where
            sub_queue = v_main.sub_queue
            and sub_id = v_main.sub_id
            and sub_role = 'coop_member'
            and sub_consumer <> v_member.sub_consumer
            and sub_batch is not null
            and sub_active < now() - i_dead_interval
        order by
            sub_active asc,
            sub_consumer asc
        for update skip locked
        limit 1;
        if found then
            batch_id := nextval('pgque.batch_id_seq');
            update pgque.subscription
            set
                sub_active = now(),
                sub_last_tick = v_victim.sub_last_tick,
                sub_next_tick = v_victim.sub_next_tick,
                sub_batch = batch_id
            where
                sub_queue = v_member.sub_queue
                and sub_consumer = v_member.sub_consumer;
            perform pgque._clear_member_cursor(v_victim.sub_queue, v_victim.sub_consumer);
            prev_tick_id := v_victim.sub_last_tick;
            next_tick_id := v_victim.sub_next_tick;
            return;
        end if;
    end if;

    if v_main.sub_batch is not null then
        raise exception 'cooperative main consumer %/% has an unexpected active batch %', i_queue, i_consumer, v_main.sub_batch;
    end if;
    if v_main.sub_last_tick is null then
        raise exception 'PgQ corruption: cooperative main consumer % on queue % has no cursor', i_consumer, i_queue;
    end if;

    select
        tick_time,
        tick_event_seq
    into
        v_prev_tick_time,
        v_prev_tick_event_seq
    from
        pgque.tick
    where
        tick_queue = v_queue_id
        and tick_id = v_main.sub_last_tick;
    if not found then
        raise exception 'PgQ corruption: cooperative main consumer % on queue % does not see tick %', i_consumer, i_queue, v_main.sub_last_tick;
    end if;

    if i_min_interval is null and i_min_count is null then
        select
            tick_id,
            tick_time,
            tick_event_seq
        into
            next_tick_id,
            v_next_tick_time,
            v_next_tick_event_seq
        from
            pgque.tick
        where
            tick_id > v_main.sub_last_tick
            and tick_queue = v_queue_id
        order by
            tick_queue asc,
            tick_id asc
        limit 1;
    else
        select
            h.next_tick_id,
            h.next_tick_time,
            h.next_tick_seq
        into
            next_tick_id,
            v_next_tick_time,
            v_next_tick_event_seq
        from
            pgque.find_tick_helper(
                v_queue_id,
                v_main.sub_last_tick,
                v_prev_tick_time,
                v_prev_tick_event_seq,
                i_min_count,
                i_min_interval
            ) as h;
    end if;

    if i_min_lag is not null and next_tick_id is not null then
        if now() - v_next_tick_time < i_min_lag then
            next_tick_id := null;
        end if;
    end if;

    if next_tick_id is null then
        prev_tick_id := null;
        return;
    end if;

    prev_tick_id := v_main.sub_last_tick;
    batch_id := nextval('pgque.batch_id_seq');

    update pgque.subscription
    set
        sub_active = now(),
        sub_last_tick = next_tick_id,
        sub_next_tick = null,
        sub_batch = null
    where
        sub_queue = v_main.sub_queue
        and sub_consumer = v_main.sub_consumer;

    update pgque.subscription
    set
        sub_active = now(),
        sub_last_tick = prev_tick_id,
        sub_next_tick = next_tick_id,
        sub_batch = batch_id
    where
        sub_queue = v_member.sub_queue
        and sub_consumer = v_member.sub_consumer;

    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.next_batch(
    in i_queue text,
    in i_consumer text,
    in i_subconsumer text,
    in i_dead_interval interval default null)
returns bigint as $$
declare
    v_batch_id bigint;
begin
    select
        batch_id
    into
        v_batch_id
    from
        pgque.next_batch_custom(
            i_queue,
            i_consumer,
            i_subconsumer,
            null,
            null,
            null,
            i_dead_interval
        );
    return v_batch_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.unregister_subconsumer(
    i_queue text,
    i_consumer text,
    i_subconsumer text,
    i_batch_handling integer default 0)
returns integer as $$
declare
    v_queue_id int4;
    v_main_consumer_id int4;
    v_member_consumer_id int4;
    v_member_name text;
    v_main record;
    v_member record;
    v_ev record;
    v_max_retries int4;
    v_remaining integer;
begin
    perform pgque._validate_coop_names(i_queue, i_consumer, i_subconsumer);
    if i_batch_handling not in (0, 1) then
        raise exception 'unsupported batch_handling value: %', i_batch_handling;
    end if;
    v_member_name := i_consumer || '.' || i_subconsumer;

    select
        q.queue_id,
        c.co_id,
        coalesce(q.queue_max_retries, 5)
    into
        v_queue_id,
        v_main_consumer_id,
        v_max_retries
    from
        pgque.queue as q
        cross join pgque.consumer as c
    where
        q.queue_name = i_queue
        and c.co_name = i_consumer;
    if not found then
        return 0;
    end if;

    select
        *
    into
        v_main
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_main_consumer_id
        and sub_role = 'coop_main'
    for update;
    if not found then
        return 0;
    end if;

    select
        co_id
    into
        v_member_consumer_id
    from
        pgque.consumer
    where
        co_name = v_member_name;
    if not found then
        return 0;
    end if;

    select
        *
    into
        v_member
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_member_consumer_id
        and sub_id = v_main.sub_id
        and sub_role = 'coop_member'
    for update;
    if not found then
        return 0;
    end if;

    if v_member.sub_batch is not null then
        if i_batch_handling = 0 then
            raise exception 'cannot unregister active cooperative subconsumer %/%/% without batch_handling = 1', i_queue, i_consumer, i_subconsumer;
        end if;

        for v_ev in
            select
                ev_id,
                ev_time,
                ev_txid,
                ev_retry,
                ev_type,
                ev_data,
                ev_extra1,
                ev_extra2,
                ev_extra3,
                ev_extra4
            from
                pgque.get_batch_events(v_member.sub_batch)
        loop
            if coalesce(v_ev.ev_retry, 0) >= v_max_retries then
                perform pgque.event_dead(
                    v_member.sub_batch,
                    v_ev.ev_id,
                    'subconsumer unregistered',
                    v_ev.ev_time,
                    v_ev.ev_txid::text::xid8,
                    v_ev.ev_retry,
                    v_ev.ev_type,
                    v_ev.ev_data,
                    v_ev.ev_extra1,
                    v_ev.ev_extra2,
                    v_ev.ev_extra3,
                    v_ev.ev_extra4
                );
            else
                perform pgque.event_retry(v_member.sub_batch, v_ev.ev_id, 60);
            end if;
        end loop;

        perform pgque._clear_member_cursor(v_member.sub_queue, v_member.sub_consumer);
    end if;

    delete from pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_consumer = v_member_consumer_id;

    perform
        1
    from
        pgque.subscription
    where
        sub_consumer = v_member_consumer_id;
    if not found then
        delete from pgque.consumer
        where
            co_id = v_member_consumer_id;
    end if;

    select
        count(*)
    into
        v_remaining
    from
        pgque.subscription
    where
        sub_queue = v_queue_id
        and sub_id = v_main.sub_id
        and sub_role = 'coop_member';
    if v_remaining = 0 then
        update pgque.subscription
        set
            sub_role = 'normal',
            sub_active = now()
        where
            sub_queue = v_queue_id
            and sub_consumer = v_main_consumer_id
            and sub_role = 'coop_main';
    end if;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.unsubscribe_subconsumer(
    i_queue text,
    i_consumer text,
    i_subconsumer text,
    i_batch_handling integer default 0)
returns integer as $$
begin
    return pgque.unregister_subconsumer(i_queue, i_consumer, i_subconsumer, i_batch_handling);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.receive_coop(
    i_queue text,
    i_consumer text,
    i_subconsumer text,
    i_max_return int default 100,
    i_dead_interval interval default null)
returns setof pgque.message as $$
declare
    v_batch_id bigint;
    ev record;
    cnt int := 0;
begin
    if i_max_return < 1 then
        raise exception 'pgque.receive_coop: max_return must be >= 1, got %', i_max_return;
    end if;

    v_batch_id := pgque.next_batch(i_queue, i_consumer, i_subconsumer, i_dead_interval);
    if v_batch_id is null then
        return;
    end if;

    for ev in
        select
            ev_id,
            ev_type,
            ev_data,
            ev_retry,
            ev_time,
            ev_extra1,
            ev_extra2,
            ev_extra3,
            ev_extra4
        from
            pgque.get_batch_events(v_batch_id)
    loop
        return next row(
            ev.ev_id,
            v_batch_id,
            ev.ev_type,
            ev.ev_data,
            ev.ev_retry,
            ev.ev_time,
            ev.ev_extra1,
            ev.ev_extra2,
            ev.ev_extra3,
            ev.ev_extra4
        )::pgque.message;
        cnt := cnt + 1;
        exit when cnt >= i_max_return;
    end loop;

    -- Empty batch: release the member token so the subconsumer is not wedged
    -- on a tick window with no visible events. finish_batch on a coop_member
    -- clears sub_batch + sub_last_tick + sub_next_tick (it does not advance
    -- the main cursor, which already moved when the batch was allocated).
    if cnt = 0 then
        perform pgque.finish_batch(v_batch_id);
    end if;

    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Experimental API comments + grants
-- ---------------------------------------------------------------------------
-- Cooperative consumer functions are consumer-side and go to pgque_reader.
-- See sql/pgque-additions/roles.sql for the producer/consumer split.

comment on function pgque.register_subconsumer(text, text, text, boolean) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.unregister_subconsumer(text, text, text, integer) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.subscribe_subconsumer(text, text, text, boolean) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.unsubscribe_subconsumer(text, text, text, integer) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.next_batch(text, text, text, interval) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.next_batch_custom(text, text, text, interval, int4, interval, interval) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.receive_coop(text, text, text, int, interval) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';
comment on function pgque.touch_subconsumer(text, text, text) is
    'Experimental in PgQue 0.2. Function names, edge-case behavior, and client API shape may change before this feature is marked stable. Do not use this as the only processing path for critical workloads without idempotent handlers and stale-worker takeover tests.';

grant execute on function pgque.register_subconsumer(text, text, text, boolean) to pgque_reader;
grant execute on function pgque.unregister_subconsumer(text, text, text, integer) to pgque_reader;
grant execute on function pgque.subscribe_subconsumer(text, text, text, boolean) to pgque_reader;
grant execute on function pgque.unsubscribe_subconsumer(text, text, text, integer) to pgque_reader;
grant execute on function pgque.next_batch(text, text, text, interval) to pgque_reader;
grant execute on function pgque.next_batch_custom(text, text, text, interval, int4, interval, interval) to pgque_reader;
grant execute on function pgque.receive_coop(text, text, text, int, interval) to pgque_reader;
grant execute on function pgque.touch_subconsumer(text, text, text) to pgque_reader;
