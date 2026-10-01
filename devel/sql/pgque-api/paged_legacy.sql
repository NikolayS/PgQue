-- pgque-api/paged_legacy.sql -- Guard legacy mutations during paged delivery
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).

/* Internal retry primitive for callers that already validated page ownership. */
create or replace function pgque._event_retry_core(
    i_batch_id bigint,
    i_event_id bigint,
    i_retry_time timestamptz)
returns integer as $$
declare
    v_sql text;
    v_count integer;
begin
    v_sql := pgque.batch_event_sql(i_batch_id);
    v_sql :=
        'insert into pgque.retry_queue (ev_retry_after, ev_queue, '
        || 'ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data, '
        || 'ev_extra1, ev_extra2, ev_extra3, ev_extra4) '
        || 'select $1, s.sub_queue, e.ev_id, e.ev_time, null, s.sub_id, '
        || 'coalesce(e.ev_retry, 0) + 1, e.ev_type, e.ev_data, '
        || 'e.ev_extra1, e.ev_extra2, e.ev_extra3, e.ev_extra4 '
        || 'from (' || v_sql || ') as e '
        || 'cross join pgque.subscription as s '
        || 'where s.sub_batch = $2 and e.ev_id = $3';

    execute v_sql using i_retry_time, i_batch_id, i_event_id;
    get diagnostics v_count = row_count;
    if v_count = 0 then
        raise exception 'event not found';
    end if;
    return 1;
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_time timestamptz)
returns integer as $$
begin
    perform pgque._assert_unpaged(x_batch_id);
    return pgque._event_retry_core(x_batch_id, x_event_id, x_retry_time);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_seconds integer)
returns integer as $$
declare
    v_retry_time timestamptz;
begin
    perform pgque._assert_unpaged(x_batch_id);
    v_retry_time := current_timestamp
        + ((x_retry_seconds::text || ' seconds')::interval);
    return pgque._event_retry_core(x_batch_id, x_event_id, v_retry_time);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.batch_retry(
    i_batch_id bigint,
    i_retry_seconds integer)
returns integer as $$
declare
    v_retry timestamptz;
    v_count integer;
    v_subscription record;
begin
    perform pgque._assert_unpaged(i_batch_id);
    v_retry := current_timestamp
        + ((i_retry_seconds::text || ' seconds')::interval);

    select * into v_subscription
    from pgque.subscription
    where sub_batch = i_batch_id;
    if not found then
        raise exception 'batch_retry: batch % not found', i_batch_id;
    end if;

    insert into pgque.retry_queue (
        ev_retry_after, ev_queue, ev_id, ev_time, ev_txid, ev_owner,
        ev_retry, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select distinct
        v_retry, v_subscription.sub_queue, b.ev_id, b.ev_time, null::xid8,
        v_subscription.sub_id, coalesce(b.ev_retry, 0) + 1,
        b.ev_type, b.ev_data, b.ev_extra1, b.ev_extra2,
        b.ev_extra3, b.ev_extra4
    from pgque.get_batch_events(i_batch_id) as b
    left join pgque.retry_queue as rq
        on rq.ev_id = b.ev_id
        and rq.ev_owner = v_subscription.sub_id
        and rq.ev_queue = v_subscription.sub_queue
    where rq.ev_id is null;

    get diagnostics v_count = row_count;
    return v_count;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

/*
 * Route one already-validated paged event without invoking a guarded public
 * retry wrapper. The lookup is restricted by event ID inside batch_event_sql.
 */
create or replace function pgque._nack_paged_event(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval,
    i_reason text)
returns integer as $$
declare
    v_event record;
    v_max_retries int4;
    v_sql text;
begin
    select coalesce(q.queue_max_retries, 5)
    into v_max_retries
    from pgque.subscription as s
    inner join pgque.queue as q on q.queue_id = s.sub_queue
    where s.sub_batch = i_batch_id;
    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    v_sql := pgque.batch_event_sql(i_batch_id);
    execute
        'select e.ev_id, e.ev_time, e.ev_txid, e.ev_retry, '
        || 'e.ev_type, e.ev_data, e.ev_extra1, e.ev_extra2, '
        || 'e.ev_extra3, e.ev_extra4 '
        || 'from (' || v_sql || ') as e where e.ev_id = $1'
    into strict v_event
    using i_msg.msg_id;

    if coalesce(v_event.ev_retry, 0) >= v_max_retries then
        perform pgque.event_dead(
            i_batch_id,
            v_event.ev_id,
            coalesce(i_reason, 'max retries exceeded'),
            v_event.ev_time,
            v_event.ev_txid::text::xid8,
            v_event.ev_retry,
            v_event.ev_type,
            v_event.ev_data,
            v_event.ev_extra1,
            v_event.ev_extra2,
            v_event.ev_extra3,
            v_event.ev_extra4
        );
    else
        perform pgque._event_retry_core(
            i_batch_id,
            v_event.ev_id,
            current_timestamp + i_retry_after
        );
    end if;
    return 1;
exception
    when no_data_found then
        raise exception 'msg_id % not found in batch %', i_msg.msg_id, i_batch_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

/* Preserve register_consumer_at(), except a real cursor move cannot erase a page. */
create or replace function pgque.register_consumer_at(
    x_queue_name text,
    x_consumer_name text,
    x_tick_pos bigint)
returns integer as $$
declare
    last_tick bigint;
    x_queue_id integer;
    x_consumer_id integer;
    sub record;
    v_member record;
begin
    select queue_id into x_queue_id
    from pgque.queue
    where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not created yet';
    end if;

    select co_id into x_consumer_id
    from pgque.consumer
    where co_name = x_consumer_name
    for update;
    if not found then
        insert into pgque.consumer (co_name) values (x_consumer_name);
        x_consumer_id := currval('pgque.consumer_co_id_seq');
    end if;

    if x_tick_pos is not null then
        perform 1
        from pgque.tick
        where tick_queue = x_queue_id and tick_id = x_tick_pos;
        if not found then
            raise exception 'cannot reposition, tick not found: %', x_tick_pos;
        end if;
    end if;

    select sub_last_tick, sub_batch, sub_id, sub_role into sub
    from pgque.subscription
    where sub_consumer = x_consumer_id and sub_queue = x_queue_id
    for update;
    if found then
        if x_tick_pos is not null then
            perform pgque._assert_unpaged(sub.sub_batch);
            if sub.sub_role = 'coop_main' then
                for v_member in
                    select m.sub_batch
                    from pgque.subscription as m
                    where
                        m.sub_id = sub.sub_id
                        and m.sub_role = 'coop_member'
                    order by m.sub_consumer
                    for update
                loop
                    perform pgque._assert_unpaged(v_member.sub_batch);
                end loop;
            end if;
            update pgque.subscription
            set
                sub_last_tick = x_tick_pos,
                sub_batch = null,
                sub_next_tick = null,
                sub_active = now()
            where
                sub_consumer = x_consumer_id
                and sub_queue = x_queue_id;
        end if;
        return 0;
    end if;

    if x_tick_pos is null then
        select tick_id into last_tick
        from pgque.tick
        where tick_queue = x_queue_id
        order by tick_queue desc, tick_id desc
        limit 1;
        if not found then
            raise exception 'No ticks for this queue.  Please run ticker on database.';
        end if;
    else
        last_tick := x_tick_pos;
    end if;

    insert into pgque.subscription (sub_queue, sub_consumer, sub_last_tick)
    values (x_queue_id, x_consumer_id, last_tick);
    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

revoke execute on function pgque._event_retry_core(bigint, bigint, timestamptz)
    from public, pgque_reader, pgque_writer, pgque_admin;
revoke execute on function pgque._nack_paged_event(bigint, pgque.message, interval, text)
    from public, pgque_reader, pgque_writer, pgque_admin;

/*
 * Administrative force destroys subscriptions, rather than acknowledging them.
 * NOWAIT prevents queue -> subscription waiting from deadlocking an ack that
 * already holds its subscription and is inserting a queue-referencing retry.
 */
create or replace function pgque.drop_queue(x_queue_name text, x_force boolean)
returns integer as $$
declare
    v_queue pgque.queue%rowtype;
    v_consumers int4[];
    v_table text;
begin
    select * into v_queue
    from pgque.queue
    where queue_name = x_queue_name
    for update;
    if not found then
        raise exception 'No such event queue';
    end if;
    if x_force then
        perform 1 from pgque.partition_slot
        where queue_id = v_queue.queue_id
        order by co_name, slot
        for update nowait;
        perform 1 from pgque.subscription
        where sub_queue = v_queue.queue_id
        order by sub_consumer
        for update nowait;
        select array_agg(sub_consumer) into v_consumers
        from pgque.subscription
        where sub_queue = v_queue.queue_id;
        delete from pgque.retry_queue where ev_queue = v_queue.queue_id;
        delete from pgque.subscription where sub_queue = v_queue.queue_id;
        /* Concurrent registration owns its consumer row; leave that identity
           in place rather than waiting while holding the queue lock. */
        with orphaned as (
            select c.co_id
            from pgque.consumer as c
            where c.co_id = any(v_consumers)
                and not exists (
                    select 1 from pgque.subscription as s
                    where s.sub_consumer = c.co_id
                )
            for update of c skip locked
        )
        delete from pgque.consumer as c
        using orphaned as o
        where c.co_id = o.co_id;
    elsif exists (
        select 1 from pgque.subscription where sub_queue = v_queue.queue_id
    ) then
        raise exception 'cannot drop queue, consumers still attached';
    end if;
    for i in 0 .. (v_queue.queue_ntables - 1) loop
        v_table := v_queue.queue_data_pfx || '_' || i::text;
        execute 'drop table ' || pgque.quote_fqname(v_table);
    end loop;
    execute 'drop table ' || pgque.quote_fqname(v_queue.queue_data_pfx);
    delete from pgque.tick where tick_queue = v_queue.queue_id;
    execute 'drop sequence ' || pgque.quote_fqname(v_queue.queue_tick_seq);
    execute 'drop sequence ' || pgque.quote_fqname(v_queue.queue_event_seq);
    delete from pgque.queue where queue_id = v_queue.queue_id;
    return 1;
exception when lock_not_available then
    raise exception 'queue is in use; retry administrative force drop'
        using errcode = '40001';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

revoke execute on function pgque.drop_queue(text, boolean)
    from public, pgque_reader, pgque_writer;
grant execute on function pgque.drop_queue(text, boolean) to pgque_admin;
