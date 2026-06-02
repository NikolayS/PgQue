-- pgque.receive(), pgque.ack(), pgque.nack() -- modern consume API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- These functions wrap PgQ primitives (next_batch, get_batch_events,
-- finish_batch, event_retry) into a simpler receive/ack/nack interface.
-- See SPECx.md sections 4.2 and 4.3.

-- pgque.message type (idempotent creation)
do $$
begin
    if to_regtype('pgque.message') is null then
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
    end if;
end $$;

-- Tracking table for #134: pgque.receive() records the msg_ids it actually
-- yielded so pgque.ack() can re-queue any unreturned events from the
-- underlying PgQ batch instead of silently dropping them. The row is keyed
-- by batch_id and cleared by ack(); finish_batch() callers that bypass
-- pgque.receive() are unaffected (no row → no re-queue, legacy behavior).
create table if not exists pgque.batch_returned (
    batch_id         bigint primary key,
    returned_msg_ids bigint[] not null default '{}'::bigint[]
);

-- pgque.receive() -- wraps next_batch + get_batch_events
--
-- Fix #134: records returned msg_ids in pgque.batch_returned so ack() can
-- re-queue events the underlying PgQ batch contained but max_return clipped.
-- Without this, ack(batch_id) → finish_batch advances sub_last_tick past the
-- whole tick window and the unreturned events become unreachable.
create or replace function pgque.receive(
    i_queue text, i_consumer text, i_max_return int default 100)
returns setof pgque.message as $$
declare
    v_batch_id bigint;
    v_returned bigint[] := '{}'::bigint[];
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
        v_returned := v_returned || ev.ev_id;
        cnt := cnt + 1;
        exit when cnt >= i_max_return;
    end loop;

    -- Empty batch: finish immediately to advance the consumer cursor.
    if cnt = 0 then
        perform pgque.finish_batch(v_batch_id);
        return;
    end if;

    -- Record which msg_ids the caller actually saw so ack() can re-queue
    -- the rest. Upsert guards against a re-open of the same batch within
    -- the active subscription (next_batch returns the existing batch_id
    -- if one is already active; the latest receive() wins).
    insert into pgque.batch_returned (batch_id, returned_msg_ids)
    values (v_batch_id, v_returned)
    on conflict (batch_id) do update
        set returned_msg_ids = excluded.returned_msg_ids;

    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.ack() -- finishes the batch, advances consumer position
--
-- Fix #134: before finishing the batch, re-queue any events the underlying
-- PgQ batch contained but pgque.receive() did not yield (because of the
-- max_return cap). Re-queue uses the existing pgque.retry_queue path with
-- ev_retry_after = now() so the events are eligible for the next
-- maint_retry_events() cycle, and ev_retry is preserved (these events were
-- never delivered to a handler — they must not count as a retry attempt).
--
-- Backward compatibility: callers that opened the batch via lower-level
-- primitives (next_batch + finish_batch) leave no row in pgque.batch_returned,
-- so ack() falls through to plain finish_batch as before.
create or replace function pgque.ack(i_batch_id bigint)
returns integer as $$
declare
    v_returned   bigint[];
    v_sub_id     int4;
    v_sub_queue  int4;
begin
    select returned_msg_ids into v_returned
    from pgque.batch_returned
    where batch_id = i_batch_id;

    if found then
        select sub_id, sub_queue into v_sub_id, v_sub_queue
        from pgque.subscription
        where sub_batch = i_batch_id;

        if v_sub_id is not null then
            insert into pgque.retry_queue (
                ev_retry_after, ev_queue, ev_id, ev_time, ev_txid, ev_owner,
                ev_retry, ev_type, ev_data,
                ev_extra1, ev_extra2, ev_extra3, ev_extra4)
            select now(), v_sub_queue, b.ev_id, b.ev_time, NULL::xid8, v_sub_id,
                   coalesce(b.ev_retry, 0), b.ev_type, b.ev_data,
                   b.ev_extra1, b.ev_extra2, b.ev_extra3, b.ev_extra4
            from pgque.get_batch_events(i_batch_id) b
            where not (b.ev_id = any(coalesce(v_returned, '{}'::bigint[])))
            on conflict (ev_owner, ev_id) do nothing;
        end if;

        delete from pgque.batch_returned where batch_id = i_batch_id;
    end if;

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
-- receive/ack/nack are consumer-side: they open/close batches and route
-- failed events to retry/DLQ. They go to pgque_reader, not pgque_writer.
-- Apps that both produce and consume must hold both roles. See
-- sql/pgque-additions/roles.sql for the producer/consumer split rationale
-- (refs #102, #106; producer→consumer half. Consumer→consumer ownership
-- is tracked separately in #164.)
--
-- Upgrade path: pre-#163 installs granted these to pgque_writer. Postgres
-- preserves function-level grants across `create or replace function`, so
-- explicitly revoke before re-granting on the new role.
revoke execute on function pgque.receive(text, text, int)                    from pgque_writer;
revoke execute on function pgque.ack(bigint)                                 from pgque_writer;
revoke execute on function pgque.nack(bigint, pgque.message, interval, text) from pgque_writer;
grant execute on function pgque.receive(text, text, int)                      to pgque_reader;
grant execute on function pgque.ack(bigint)                                   to pgque_reader;
grant execute on function pgque.nack(bigint, pgque.message, interval, text)   to pgque_reader;
