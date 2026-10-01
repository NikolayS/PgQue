-- Durable bounded consumption; see blueprints/PAGED_BATCHES.md (#364).
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
    if to_regtype('pgque.batch_page') is null then
        create type pgque.batch_page as (
            status text,
            batch_id bigint,
            page_token uuid,
            page_number bigint,
            is_last boolean,
            messages pgque.message[],
            lease_until timestamptz,
            fence_epoch bigint
        );
    end if;
end $$;

-- The engine owns membership. Bound the buffer, not the underlying scan/sort.
create or replace function pgque._page_messages(
    i_state pgque.page_state, i_size int4)
returns setof pgque.message as $$
declare
    v_sql text;
    v_filter text := '';
    v_ev record;
    v_previous bigint;
begin
    if i_state.mode = 'partition' then
        v_filter := format(
            ' and (case when ev_extra1 is null then 0 else '
            || '(pg_catalog.hashtextextended(ev_extra1, 0) %% %s + %s) %% %s end) = %s',
            i_state.partition_n, i_state.partition_n,
            i_state.partition_n, i_state.partition_slot);
    end if;
    v_sql := 'select * from (' || pgque.batch_event_sql(i_state.active_batch_id)
        || ') as events where ($1 is null or ev_id > $1)'
        || ' and ($2 is null or ev_id <= $2)' || v_filter
        || ' order by ev_id limit $3';
    for v_ev in execute v_sql using i_state.acked_event_id,
        i_state.pending_last_event_id, i_size::bigint + 1
    loop
        if v_previous = v_ev.ev_id then
            raise exception 'ambiguous duplicate event ID % in paged batch', v_ev.ev_id
                using errcode = '21000';
        end if;
        v_previous := v_ev.ev_id;
        return next row(v_ev.ev_id, i_state.active_batch_id, v_ev.ev_type,
            v_ev.ev_data, v_ev.ev_retry, v_ev.ev_time, v_ev.ev_extra1,
            v_ev.ev_extra2, v_ev.ev_extra3, v_ev.ev_extra4)::pgque.message;
    end loop;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- Allocators already hold the subscription lock (and slot lock if applicable).
create or replace function pgque._receive_page(
    i_batch bigint, i_mode text, i_worker text, i_size int4, i_lease interval,
    i_partition_consumer text default null, i_slot int4 default null,
    i_n int4 default null)
returns pgque.batch_page as $$
declare
    v_sub pgque.subscription%rowtype;
    v_state pgque.page_state%rowtype;
    v_slot pgque.partition_slot%rowtype;
    v_result pgque.batch_page;
    v_messages pgque.message[];
    v_size int4;
    v_count int4;
begin
    v_result.status := 'idle';
    v_result.messages := array[]::pgque.message[];
    if i_batch is null then
        return v_result;
    end if;
    select * into strict v_sub from pgque.subscription
    where sub_batch = i_batch for update;
    insert into pgque.page_state (queue_id, consumer_id)
    values (v_sub.sub_queue, v_sub.sub_consumer)
    on conflict do nothing;
    select * into strict v_state from pgque.page_state
    where queue_id = v_sub.sub_queue and consumer_id = v_sub.sub_consumer
    for update;
    if v_state.active_batch_id is null then
        v_state.active_batch_id := i_batch;
        v_state.prev_tick_id := v_sub.sub_last_tick;
        v_state.next_tick_id := v_sub.sub_next_tick;
        v_state.mode := i_mode;
        v_state.partition_co_name := i_partition_consumer;
        v_state.partition_slot := i_slot;
        v_state.partition_n := i_n;
    elsif v_state.active_batch_id <> i_batch or v_state.mode <> i_mode then
        raise exception 'incompatible active paged batch' using errcode = '55000';
    end if;
    if i_mode = 'partition' then
        -- Caller locked/validated the slot before locking this subscription.
        select * into strict v_slot from pgque.partition_slot
        where queue_id = v_sub.sub_queue and co_name = i_partition_consumer
            and slot = i_slot;
    elsif v_state.pending_token is not null
        and v_state.pending_worker <> i_worker
        and v_state.pending_lease_until > clock_timestamp() then
        v_result.status := 'busy';
        v_result.batch_id := i_batch;
        v_result.lease_until := v_state.pending_lease_until;
        return v_result;
    end if;

    v_size := coalesce(v_state.pending_page_size, i_size);
    select coalesce(array_agg(m order by m.msg_id), array[]::pgque.message[])
    into v_messages from pgque._page_messages(v_state, v_size) as m;
    v_count := cardinality(v_messages);
    if v_count = 0 then
        if v_state.pending_last_event_id is not null then
            raise exception 'pending page membership changed' using errcode = '21000';
        end if;
        perform pgque._clear_paged_active(v_sub.sub_queue, v_sub.sub_consumer);
        perform pgque.finish_batch(i_batch);
        v_result.status := 'advanced';
        return v_result;
    end if;
    if v_state.pending_last_event_id is null then
        v_state.pending_final := v_count <= v_size;
        v_messages := v_messages[1:v_size];
        v_state.pending_last_event_id := (v_messages[cardinality(v_messages)]).msg_id;
        v_state.pending_page_size := v_size;
        v_state.pending_lease_ttl := i_lease;
    elsif v_count > v_size
        or (v_messages[v_count]).msg_id <> v_state.pending_last_event_id then
        raise exception 'pending page membership changed' using errcode = '21000';
    end if;
    if v_state.pending_token is null
        or v_state.pending_worker is distinct from i_worker
        or (i_mode = 'partition' and v_state.partition_epoch is distinct from v_slot.epoch) then
        v_state.pending_token := gen_random_uuid();
        v_state.pending_worker := i_worker;
    end if;
    if i_mode = 'partition' then
        v_state.partition_epoch := v_slot.epoch;
        v_state.pending_lease_until := null;
        v_state.pending_lease_ttl := null;
        v_result.lease_until := v_slot.lease_until;
        v_result.fence_epoch := v_slot.epoch;
    else
        v_state.pending_lease_until := clock_timestamp() + v_state.pending_lease_ttl;
        v_result.lease_until := v_state.pending_lease_until;
    end if;
    update pgque.page_state set
        active_batch_id = v_state.active_batch_id,
        prev_tick_id = v_state.prev_tick_id, next_tick_id = v_state.next_tick_id,
        mode = v_state.mode,
        pending_token = v_state.pending_token,
        pending_last_event_id = v_state.pending_last_event_id,
        pending_page_size = v_state.pending_page_size,
        pending_final = v_state.pending_final,
        pending_worker = v_state.pending_worker,
        pending_lease_ttl = v_state.pending_lease_ttl,
        pending_lease_until = v_state.pending_lease_until,
        partition_co_name = v_state.partition_co_name,
        partition_slot = v_state.partition_slot, partition_n = v_state.partition_n,
        partition_epoch = v_state.partition_epoch
    where queue_id = v_state.queue_id and consumer_id = v_state.consumer_id;
    update pgque.subscription set sub_active = clock_timestamp()
    where sub_queue = v_state.queue_id and sub_consumer = v_state.consumer_id;
    v_result.status := 'page';
    v_result.batch_id := i_batch;
    v_result.page_token := v_state.pending_token;
    v_result.page_number := v_state.acked_page_number + 1;
    v_result.is_last := v_state.pending_final;
    v_result.messages := v_messages;
    return v_result;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque._validate_page_args(
    i_queue text, i_consumer text, i_worker text, i_size int4, i_lease interval)
returns void as $$
begin
    if i_queue is null or i_queue = '' or i_consumer is null or i_consumer = ''
        or i_worker is null or i_worker = '' or i_size is null or i_size < 1
        or i_lease is null or i_lease <= interval '0' then
        raise exception 'nonempty queue/consumer/worker, positive page size and lease required'
            using errcode = '22023';
    end if;
    -- Timestamp arithmetic also rejects unsupported non-finite/overflow TTLs.
    if not isfinite(clock_timestamp() + i_lease) then
        raise exception 'lease must be finite' using errcode = '22023';
    end if;
exception when datetime_field_overflow then
    raise exception 'lease out of range' using errcode = '22023';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.receive_page(
    i_queue text, i_consumer text, i_worker text,
    i_page_size int4 default 100, i_lease interval default '60 seconds')
returns pgque.batch_page as $$
declare
    v_batch bigint;
begin
    perform pgque._validate_page_args(i_queue, i_consumer, i_worker, i_page_size, i_lease);
    if position('#' in i_consumer) > 0 then
        raise exception 'use receive_page_partitioned for slot consumers' using errcode = '22023';
    end if;
    v_batch := pgque.next_batch(i_queue, i_consumer);
    return pgque._receive_page(v_batch, 'normal', i_worker, i_page_size, i_lease);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.receive_page_coop(
    i_queue text, i_consumer text, i_subconsumer text, i_worker text,
    i_page_size int4 default 100, i_dead_interval interval default null,
    i_lease interval default '60 seconds')
returns pgque.batch_page as $$
declare
    v_batch bigint;
begin
    perform pgque._validate_page_args(i_queue, i_consumer, i_worker, i_page_size, i_lease);
    if i_dead_interval <= interval '0' then
        raise exception 'dead interval must be positive' using errcode = '22023';
    end if;
    select batch_id into v_batch from pgque._next_batch_coop(
        i_queue, i_consumer, i_subconsumer, null, null, null, i_dead_interval, true);
    return pgque._receive_page(v_batch, 'coop', i_worker, i_page_size, i_lease);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.receive_page_partitioned(
    i_queue text, i_consumer text, i_slot int4, i_n int4, i_worker text,
    i_page_size int4 default 100)
returns pgque.batch_page as $$
declare
    v_batch bigint;
begin
    perform pgque._validate_page_args(i_queue, i_consumer, i_worker, i_page_size, interval '1 second');
    perform pgque._slot_guard(i_queue, i_consumer, i_slot, i_n, i_worker);
    v_batch := pgque.next_batch(i_queue, pgque._slot_name(i_consumer, i_slot, i_n));
    return pgque._receive_page(v_batch, 'partition', i_worker, i_page_size,
        null, i_consumer, i_slot, i_n);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- Route without locks, then slot -> subscription -> page. Re-read after locks.
-- Receipt-only inactive rows have no slot context and need no slot lock.
create or replace function pgque._lock_page(i_token uuid)
returns pgque.page_state as $$
declare
    v_route pgque.page_state%rowtype;
    v_state pgque.page_state%rowtype;
begin
    select * into v_route from pgque.page_state
    where pending_token = i_token or last_ack_token = i_token;
    if not found then
        raise exception 'stale page token' using errcode = 'PQP01';
    end if;
    if v_route.mode = 'partition' then
        perform 1 from pgque.partition_slot
        where queue_id = v_route.queue_id and co_name = v_route.partition_co_name
            and slot = v_route.partition_slot for update;
    end if;
    perform 1 from pgque.subscription
    where sub_queue = v_route.queue_id and sub_consumer = v_route.consumer_id
    for update;
    select * into v_state from pgque.page_state
    where queue_id = v_route.queue_id and consumer_id = v_route.consumer_id
    for update;
    if not found or (v_state.pending_token is distinct from i_token
        and v_state.last_ack_token is distinct from i_token) then
        raise exception 'stale page token' using errcode = 'PQP01';
    end if;
    -- If routing changed while acquiring locks, do not acquire a slot late.
    if (v_state.mode = 'partition') and (
        v_route.mode is distinct from v_state.mode
        or v_route.partition_co_name is distinct from v_state.partition_co_name
        or v_route.partition_slot is distinct from v_state.partition_slot) then
        raise exception 'page routing changed; retry transaction' using errcode = '40001';
    end if;
    return v_state;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque._validate_pending_page(
    i_state pgque.page_state, i_token uuid, i_worker text)
returns void as $$
begin
    if i_worker is null or i_state.pending_token is distinct from i_token
        or i_state.pending_worker is distinct from i_worker
        or not exists (select 1 from pgque.subscription
            where sub_queue = i_state.queue_id and sub_consumer = i_state.consumer_id
                and sub_batch = i_state.active_batch_id) then
        raise exception 'stale page token or wrong worker' using errcode = 'PQP01';
    end if;
    if i_state.mode = 'partition' and not exists (
        select 1 from pgque.partition_slot
        where queue_id = i_state.queue_id and co_name = i_state.partition_co_name
            and slot = i_state.partition_slot and lease_owner = i_worker
            and epoch = i_state.partition_epoch
    ) then
        raise exception 'page partition epoch fenced' using errcode = 'PQP01';
    end if;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.renew_page(i_page_token uuid, i_worker text)
returns timestamptz as $$
declare
    v_state pgque.page_state%rowtype;
    v_until timestamptz;
begin
    v_state := pgque._lock_page(i_page_token);
    perform pgque._validate_pending_page(v_state, i_page_token, i_worker);
    if v_state.mode = 'partition' then
        update pgque.partition_slot set lease_until = clock_timestamp() + lease_ttl
        where queue_id = v_state.queue_id and co_name = v_state.partition_co_name
            and slot = v_state.partition_slot
        returning lease_until into v_until;
    else
        v_until := clock_timestamp() + v_state.pending_lease_ttl;
        update pgque.page_state set pending_lease_until = v_until
        where queue_id = v_state.queue_id and consumer_id = v_state.consumer_id;
    end if;
    update pgque.subscription set sub_active = clock_timestamp()
    where sub_queue = v_state.queue_id and sub_consumer = v_state.consumer_id;
    return v_until;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque._page_failures(i_failures jsonb)
returns jsonb as $$
declare
    v_item jsonb;
    v_normalized jsonb := '[]';
    v_id bigint;
    v_seconds int4;
begin
    if i_failures is null or jsonb_typeof(i_failures) <> 'array' then
        raise exception 'failures must be an array' using errcode = '22023';
    end if;
    for v_item in select value from jsonb_array_elements(i_failures) loop
        if jsonb_typeof(v_item) <> 'object' then
            raise exception 'failure must be an object' using errcode = '22023';
        end if;
        if exists (select 1 from jsonb_object_keys(v_item) as k
            where k not in ('msg_id', 'retry_after_seconds', 'reason'))
            or jsonb_typeof(v_item->'msg_id') is distinct from 'string'
            or (v_item->>'msg_id') !~ '^-?[0-9]+$'
            or (v_item ? 'retry_after_seconds' and (
                jsonb_typeof(v_item->'retry_after_seconds') <> 'number'
                or (v_item->>'retry_after_seconds') !~ '^[0-9]+$'))
            or (v_item ? 'reason' and jsonb_typeof(v_item->'reason') not in ('string', 'null')) then
            raise exception 'invalid failure descriptor' using errcode = '22023';
        end if;
        v_id := (v_item->>'msg_id')::bigint;
        v_seconds := coalesce((v_item->>'retry_after_seconds')::int4, 60);
        if exists (select 1 from jsonb_array_elements(v_normalized) as f
            where f->>'msg_id' = v_id::text) then
            raise exception 'duplicate failure ID' using errcode = '22023';
        end if;
        v_normalized := v_normalized || jsonb_build_array(jsonb_build_object(
            'msg_id', v_id::text, 'retry_after_seconds', v_seconds,
            'reason', v_item->>'reason'));
    end loop;
    select coalesce(jsonb_agg(f order by (f->>'msg_id')::bigint), '[]')
    into v_normalized from jsonb_array_elements(v_normalized) as f;
    return v_normalized;
exception when numeric_value_out_of_range or invalid_text_representation then
    raise exception 'failure number out of range' using errcode = '22023';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.ack_page(
    i_page_token uuid, i_worker text, i_failures jsonb default '[]')
returns table(status text, batch_finished boolean) as $$
declare
    v_state pgque.page_state%rowtype;
    v_request jsonb;
    v_failure jsonb;
    v_messages pgque.message[];
    v_message pgque.message;
begin
    v_request := pgque._page_failures(i_failures);
    v_state := pgque._lock_page(i_page_token);
    if v_state.last_ack_token = i_page_token then
        if v_state.last_ack_worker is distinct from i_worker then
            raise exception 'wrong receipt worker' using errcode = 'PQP01';
        end if;
        if v_state.last_ack_request is distinct from v_request then
            raise exception 'ack receipt request differs' using errcode = 'PQP02';
        end if;
        return query select 'already_acked'::text, v_state.last_ack_finished;
        return;
    end if;
    perform pgque._validate_pending_page(v_state, i_page_token, i_worker);
    if jsonb_array_length(v_request) > 0 then
        select array_agg(m) into v_messages
        from pgque._page_messages(v_state, v_state.pending_page_size) as m;
        for v_failure in select value from jsonb_array_elements(v_request) loop
            select m.* into v_message from unnest(v_messages) as m
            where m.msg_id = (v_failure->>'msg_id')::bigint;
            if not found then
                raise exception 'failure ID is outside the issued page' using errcode = '22023';
            end if;
            perform pgque._nack_paged_event(v_state.active_batch_id, v_message,
                make_interval(secs => (v_failure->>'retry_after_seconds')::int4),
                v_failure->>'reason');
        end loop;
    end if;
    perform pgque.renew_page(i_page_token, i_worker);
    update pgque.page_state set
        acked_event_id = v_state.pending_last_event_id,
        acked_page_number = acked_page_number + 1,
        last_ack_token = i_page_token, last_ack_worker = i_worker,
        last_ack_request = v_request, last_ack_finished = v_state.pending_final,
        pending_token = null, pending_last_event_id = null,
        pending_page_size = null, pending_final = null, pending_worker = null,
        pending_lease_ttl = null, pending_lease_until = null
    where queue_id = v_state.queue_id and consumer_id = v_state.consumer_id;
    if v_state.pending_final then
        perform pgque._clear_paged_active(v_state.queue_id, v_state.consumer_id);
        perform pgque.finish_batch(v_state.active_batch_id);
    end if;
    return query select 'acked'::text, v_state.pending_final;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

revoke execute on function pgque._page_messages(pgque.page_state, int4),
    pgque._receive_page(bigint, text, text, int4, interval, text, int4, int4),
    pgque._validate_page_args(text, text, text, int4, interval),
    pgque._lock_page(uuid), pgque._validate_pending_page(pgque.page_state, uuid, text),
    pgque._page_failures(jsonb)
from public, pgque_reader, pgque_writer, pgque_admin;
revoke execute on function pgque.receive_page(text, text, text, int4, interval),
    pgque.receive_page_coop(text, text, text, text, int4, interval, interval),
    pgque.receive_page_partitioned(text, text, int4, int4, text, int4),
    pgque.ack_page(uuid, text, jsonb), pgque.renew_page(uuid, text)
from public, pgque_writer;
grant execute on function pgque.receive_page(text, text, text, int4, interval),
    pgque.receive_page_coop(text, text, text, text, int4, interval, interval),
    pgque.receive_page_partitioned(text, text, int4, int4, text, int4),
    pgque.ack_page(uuid, text, jsonb), pgque.renew_page(uuid, text)
to pgque_reader;
