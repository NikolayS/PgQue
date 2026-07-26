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

/* A `#` name is a slot only when subscribe_slot materialized matching
   partition metadata. Ordinary consumers created before `#` was reserved
   remain ordinary consumers after upgrade. */
create or replace function pgque._is_partition_slot_consumer(
    i_queue_id int4,
    i_consumer text)
returns boolean as $$
begin
    if i_queue_id is null
        or i_consumer is null
        or to_regclass('pgque.partition_consumer') is null
        or to_regclass('pgque.partition_slot') is null
    then
        return false;
    end if;

    return exists (
        select 1
        from pgque.partition_consumer as pc
        join pgque.partition_slot as ps
            on ps.queue_id = pc.queue_id
            and ps.co_name = pc.co_name
        where pc.queue_id = i_queue_id
          and pc.co_name || '#' || ps.slot::text || '/' || pc.n::text = i_consumer
    );
end;
$$ language plpgsql stable security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque._is_partition_slot_consumer(int4, text)
    from public, pgque_reader, pgque_writer;

-- pgque.receive() -- wraps next_batch + get_batch_events
create or replace function pgque.receive(
    i_queue text, i_consumer text, i_max_return int default 100)
returns setof pgque.message as $$
declare
    v_queue_id int4;
    v_batch_id bigint;
    ev record;
    cnt int := 0;
begin
    if i_max_return < 1 then
        raise exception 'pgque.receive: max_return must be >= 1, got %', i_max_return;
    end if;

    /*
     * The plain receive path applies neither the lease fence nor the hash
     * filter. Refuse cataloged slots while preserving ordinary `#` consumers
     * that predate the reserved namespace.
     */
    if position('#' in i_consumer) > 0 then
        select q.queue_id
        into v_queue_id
        from pgque.queue as q
        where q.queue_name = i_queue;

        if pgque._is_partition_slot_consumer(v_queue_id, i_consumer) then
            raise exception 'consumer % is a partition slot consumer; use pgque.receive_partitioned()', i_consumer;
        end if;
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
declare
    v_queue_id int4;
    v_cname text;
begin
    /*
     * Guard the raw slot consumer (see pgque.receive). A partition slot batch
     * must be finished through the lease-fenced ack_partitioned(); plain ack()
     * lets a fenced zombie double-ack it. Resolve the batch's consumer and
     * reject when the partition catalogs identify it as a slot. An unknown
     * batch id falls through to finish_batch(), preserving not-found behavior.
     */
    select
        s.sub_queue,
        c.co_name
    into
        v_queue_id,
        v_cname
    from pgque.subscription as s
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where s.sub_batch = i_batch_id;
    if found
        and position('#' in v_cname) > 0
        and pgque._is_partition_slot_consumer(v_queue_id, v_cname)
    then
        raise exception 'batch % belongs to partition slot consumer %; use pgque.ack_partitioned()', i_batch_id, v_cname;
    end if;

    return pgque.finish_batch(i_batch_id);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

/*
 * pgque._nack_batch_event() -- shared retry/DLQ core for a single event of an
 * open batch. Called by pgque.nack() (after the raw-slot guard) and by
 * pgque.nack_partitioned() (after the lease fence). Internal only; the caller
 * owns whatever consumer-name / lease authorization applies.
 *
 * Re-queries the canonical event row from the active batch by msg_id rather
 * than trusting caller-supplied pgque.message fields -- otherwise a caller
 * with an active batch could forge DLQ rows with arbitrary ev_id/type/data.
 * The DLQ insert is idempotent (ON CONFLICT in event_dead()), so repeated
 * calls for one terminal message produce exactly one dead_letter row.
 */
create or replace function pgque._nack_batch_event(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval,
    i_reason text)
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

    /* Re-query the canonical event from the active batch, ignoring
       caller-supplied payload/type/extras. */
    select ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
           ev_extra1, ev_extra2, ev_extra3, ev_extra4
    into v_ev
    from pgque.get_batch_events(i_batch_id)
    where ev_id = i_msg.msg_id;

    if not found then
        raise exception 'msg_id % not found in batch %', i_msg.msg_id, i_batch_id;
    end if;

    if coalesce(v_ev.ev_retry, 0) >= v_max_retries then
        /* Move to DLQ with canonical event data; event_dead() is idempotent
           via ON CONFLICT DO NOTHING. ev_txid is bigint in get_batch_events
           (legacy PgQ signature) -- the text round-trip widens it to xid8
           without loss, per codebase convention. */
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

-- pgque.nack() -- retry or route to DLQ based on retry_count vs max_retries
create or replace function pgque.nack(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval default '60 seconds',
    i_reason text default null)
returns integer as $$
declare
    v_queue_id int4;
    v_cname text;
begin
    /*
     * Guard the raw slot consumer (see pgque.receive). Slot batches are acked
     * and retried through the lease-fenced partitioned API (ack_partitioned /
     * nack_partitioned), not plain nack(). An unknown batch id falls through to
     * the shared core, which raises the same 'batch not found' as before.
     */
    select
        s.sub_queue,
        c.co_name
    into
        v_queue_id,
        v_cname
    from pgque.subscription as s
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where s.sub_batch = i_batch_id;
    if found
        and position('#' in v_cname) > 0
        and pgque._is_partition_slot_consumer(v_queue_id, v_cname)
    then
        raise exception 'batch % belongs to partition slot consumer %; retry slot batches via pgque.nack_partitioned()', i_batch_id, v_cname;
    end if;

    return pgque._nack_batch_event(i_batch_id, i_msg, i_retry_after, i_reason);
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

-- Shared retry/DLQ core: SECURITY DEFINER callee only (nack / nack_partitioned).
revoke execute on function pgque._nack_batch_event(bigint, pgque.message, interval, text) from public, pgque_reader, pgque_writer;
