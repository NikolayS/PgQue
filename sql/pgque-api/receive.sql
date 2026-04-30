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
    if i_max_return < 1 then
        raise exception 'pgque.receive: max_return must be >= 1, got %', i_max_return;
    end if;

    -- Get next batch (may return NULL if no tick window is ready)
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

    -- Empty batch: finish immediately to advance the consumer cursor.
    if cnt = 0 then
        perform pgque.finish_batch(v_batch_id);
    end if;

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
--
-- Fix #98: re-query the canonical event row from the active batch using
-- msg_id, instead of trusting caller-supplied pgque.message fields.
-- A caller with an active batch could otherwise forge DLQ rows by
-- supplying arbitrary ev_id / ev_type / ev_data in the composite.
--
-- Fix #104: DLQ insert is idempotent via ON CONFLICT in event_dead().
-- Repeated nack() calls for the same terminal message produce exactly one
-- dead_letter row.
create or replace function pgque.nack(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval default '60 seconds',
    i_reason text default null)
returns integer as $$
declare
    v_max_retries int4;
    v_ev          record;
begin
    -- Lookup: subscription -> queue config
    select coalesce(q.queue_max_retries, 5) into v_max_retries
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    where s.sub_batch = i_batch_id;

    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    -- Re-query the canonical event from the active batch (#98).
    -- This ignores caller-supplied payload/type/extras and uses the real
    -- values stored in the queue data tables.
    select ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
           ev_extra1, ev_extra2, ev_extra3, ev_extra4
    into v_ev
    from pgque.get_batch_events(i_batch_id)
    where ev_id = i_msg.msg_id;

    if not found then
        raise exception 'msg_id % not found in batch %', i_msg.msg_id, i_batch_id;
    end if;

    if coalesce(v_ev.ev_retry, 0) >= v_max_retries then
        -- Move to dead letter queue using canonical event data (#98).
        -- event_dead() uses ON CONFLICT DO NOTHING for idempotency (#104).
        -- ev_txid is bigint in get_batch_events (legacy PgQ signature); text
        -- round-trip is the codebase convention to widen to xid8 without loss.
        perform pgque.event_dead(i_batch_id, v_ev.ev_id,
            coalesce(i_reason, 'max retries exceeded'),
            v_ev.ev_time, v_ev.ev_txid::text::xid8, v_ev.ev_retry,
            v_ev.ev_type, v_ev.ev_data,
            v_ev.ev_extra1, v_ev.ev_extra2, v_ev.ev_extra3, v_ev.ev_extra4);
    else
        -- Retry after delay
        perform pgque.event_retry(i_batch_id, v_ev.ev_id,
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
