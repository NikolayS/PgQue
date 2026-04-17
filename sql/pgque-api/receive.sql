-- pgque.receive(), pgque.ack(), pgque.nack() -- modern consume API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- These functions wrap PgQ primitives (next_batch, get_batch_events,
-- finish_batch, event_retry) into a simpler receive/ack/nack interface.
-- See SPECx.md sections 4.2 and 4.3.

-- pgque.message type (idempotent creation)
do $$ begin
    create type pgque.message as (
        msg_id      bigint,
        batch_id    bigint,
        type        text,
        payload     text,
        retry_count int4,
        created_at  timestamptz,
        extra1      text,
        extra2      text,
        extra3      text,
        extra4      text
    );
exception when duplicate_object then null;
end $$;

-- pgque.receive() -- wraps next_batch + get_batch_events
create or replace function pgque.receive(
    i_queue text, i_consumer text, i_max_return int default 100)
returns setof pgque.message as $$
declare
    v_batch_id bigint;
    ev record;
    cnt int := 0;
begin
    -- Get next batch (may return NULL if no events)
    v_batch_id := pgque.next_batch(i_queue, i_consumer);
    if v_batch_id is null then
        return;
    end if;

    -- Yield messages from the batch
    for ev in
        select ev_id, ev_type, ev_data, ev_retry, ev_time,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
        from pgque.get_batch_events(v_batch_id)
    loop
        return next row(
            ev.ev_id, v_batch_id, ev.ev_type, ev.ev_data,
            ev.ev_retry, ev.ev_time,
            ev.ev_extra1, ev.ev_extra2, ev.ev_extra3, ev.ev_extra4
        )::pgque.message;
        cnt := cnt + 1;
        exit when cnt >= i_max_return;
    end loop;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.ack() -- finishes the batch, advances consumer position
create or replace function pgque.ack(i_batch_id bigint)
returns integer as $$
begin
    return pgque.finish_batch(i_batch_id);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.nack() -- retry or route to DLQ based on retry_count vs max_retries
create or replace function pgque.nack(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval default '60 seconds',
    i_reason text default null)
returns integer as $$
declare
    v_max_retries int4;
begin
    -- Single lookup: subscription -> queue config
    select coalesce(q.queue_max_retries, 5) into v_max_retries
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    where s.sub_batch = i_batch_id;

    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    if coalesce(i_msg.retry_count, 0) >= v_max_retries then
        -- Move to dead letter queue (pass event fields, no re-query)
        perform pgque.event_dead(i_batch_id, i_msg.msg_id,
            coalesce(i_reason, 'max retries exceeded'),
            i_msg.created_at, null::xid8, i_msg.retry_count,
            i_msg.type, i_msg.payload,
            i_msg.extra1, i_msg.extra2, i_msg.extra3, i_msg.extra4);
    else
        -- Retry after delay
        perform pgque.event_retry(i_batch_id, i_msg.msg_id,
            extract(epoch from i_retry_after)::integer);
    end if;
    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------
-- Colocated here (not in pgque-additions/roles.sql) because roles.sql is
-- assembled before pgque-api/, so these functions do not yet exist when
-- roles.sql runs. Same convention as sql/pgque-api/send.sql.

grant execute on function pgque.receive(text, text, int)                      to pgque_writer;
grant execute on function pgque.ack(bigint)                                   to pgque_writer;
grant execute on function pgque.nack(bigint, pgque.message, interval, text)   to pgque_writer;
