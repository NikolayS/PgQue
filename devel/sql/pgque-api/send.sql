-- pgque-api/send.sql -- Modern send/subscribe API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements default v0.1 API surface:
--   pgque.message type
--   pgque.send(queue, payload)                -- text + jsonb overloads
--   pgque.send(queue, type, payload)          -- text + jsonb overloads
--   pgque.send_batch(queue, type, payloads[]) -- text[] + jsonb[] overloads
--   pgque.subscribe(queue, consumer)
--   pgque.unsubscribe(queue, consumer)

/*
 * Overload resolution: an untyped string literal resolves to the text
 * overload (unknown -> text needs no cast, unknown -> jsonb does), so the
 * text path is the default; opt into jsonb with an explicit ::jsonb cast.
 *
 *   select pgque.send('orders', '{"k":1}');        -- send(text, text)
 *   select pgque.send('orders', '{"k":1}'::jsonb); -- send(text, jsonb)
 *
 * text stores bytes verbatim (no parse/canonicalization) but cannot hold NUL,
 * so true binary (protobuf, msgpack, bytea dumps) must be caller-encoded.
 * jsonb validates and canonicalizes at parse time. Storage (ev_data text) is
 * identical either way.
 */

-- pgque.message type (idempotent creation)
do $$
begin
    if to_regtype('pgque.message') is null then
        create type pgque.message as (
            msg_id      bigint,       -- ev_id
            batch_id    bigint,       -- batch containing this message
            type        text,         -- ev_type
            payload     text,         -- ev_data (caller casts to jsonb if needed)
            retry_count int4,         -- ev_retry (NULL for first delivery)
            created_at  timestamptz,  -- ev_time
            extra1      text,         -- ev_extra1
            extra2      text,         -- ev_extra2
            extra3      text,         -- ev_extra3
            extra4      text          -- ev_extra4
        );
    end if;
end $$;

/*
 * Recreating a wrapper whose argument names changed (v0.1.0 i_* -> v0.2 names)
 * needs drop-then-create -- create or replace rejects a name change. Drop only
 * the renamed wrappers (a blanket drop would break dependents and churn OIDs)
 * and capture each owner so a superuser-run upgrade keeps the original owner.
 */
create temporary table if not exists pgque_v01_wrapper_owners (
    sig text primary key,
    owner_name name not null
);

do $$
declare
    v_sig text;
    proc regprocedure;
    args text;
    v_owner_name name;
begin
    foreach v_sig in array array[
        'pgque.send(text,jsonb)',
        'pgque.send(text,text)',
        'pgque.send(text,text,jsonb)',
        'pgque.send(text,text,text)',
        'pgque.send_batch(text,text,jsonb[])',
        'pgque.send_batch(text,text,text[])',
        'pgque.subscribe(text,text)',
        'pgque.unsubscribe(text,text)'
    ] loop
        proc := to_regprocedure(v_sig);
        if proc is null then
            continue;
        end if;

        args := pg_get_function_arguments(proc);
        if args like 'i\_%' escape '\' then
            select r.rolname
            into v_owner_name
            from pg_proc as p
            join pg_roles as r on r.oid = p.proowner
            where p.oid = proc::oid;

            insert into pg_temp.pgque_v01_wrapper_owners (sig, owner_name)
            values (v_sig, v_owner_name)
            on conflict (sig) do update
                set owner_name = excluded.owner_name;

            execute format('drop function %s', proc);
        end if;
    end loop;
end $$;

-- pgque.send(queue, payload jsonb) -- send with default type, JSON payload
create or replace function pgque.send(queue_name text, payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(queue_name, 'default', payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send(text, jsonb) from public;

-- pgque.send(queue, payload text) -- fast path, opaque textual payload.
create or replace function pgque.send(queue_name text, payload text)
returns bigint as $$
begin
    return pgque.insert_event(queue_name, 'default', payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send(text, text) from public;

-- pgque.send(queue, type, payload jsonb) -- send with explicit type, JSON payload
create or replace function pgque.send(queue_name text, type_name text, payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(queue_name, type_name, payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send(text, text, jsonb) from public;

-- pgque.send(queue, type, payload text) -- fast path with explicit type
create or replace function pgque.send(queue_name text, type_name text, payload text)
returns bigint as $$
begin
    return pgque.insert_event(queue_name, type_name, payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send(text, text, text) from public;

-- pgque.insert_event_bulk(queue, type, payloads text[]) -- internal set-based primitive
/* One-pass bulk insert (not insert_event_raw per row); keep its queue lookup
   and queue_disable_insert replica-bypass in sync with insert_event_raw(). */
create or replace function pgque.insert_event_bulk(
    queue_name text, ev_type text, ev_data_list text[])
returns bigint[] as $$
declare
    -- Local aliases avoid ambiguity with table columns inside SQL statements.
    _queue_name alias for $1;
    _ev_type alias for $2;
    _ev_data_list alias for $3;
    qstate record;
    v_ids bigint[];
begin
    select
        pgque.quote_fqname(q.queue_data_pfx || '_' || q.queue_cur_table::text) as cur_table_name,
        q.queue_event_seq::regclass as queue_event_seq,
        q.queue_disable_insert
    into qstate
    from pgque.queue q
    where q.queue_name = _queue_name;

    if not found then
        raise exception 'queue not found: %', _queue_name;
    end if;

    if qstate.queue_disable_insert then
        /* Match insert_event_raw(): a disabled queue still accepts inserts
           under session_replication_role = 'replica' (replication paths). */
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Insert into queue disallowed';
        end if;
    end if;

    execute format($sql$
        with input as materialized (
            select
                u.ord,
                u.payload as ev_data
            from unnest($2::text[]) with ordinality as u(payload, ord)
        ), numbered as materialized (
            select
                ord,
                nextval($1) as ev_id,
                ev_data
            from input
            order by ord
        ), ins as (
            insert into %s (
                ev_id, ev_time, ev_owner, ev_retry,
                ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
            )
            select
                ev_id, $3, null, null,
                $4, ev_data, null, null, null, null
            from numbered
            /* Return order comes from array_agg(order by ord) below; this
               just keeps physical insertion roughly aligned with input. */
            order by ord
            returning ev_id
        )
        select coalesce(array_agg(numbered.ev_id order by numbered.ord), '{}'::bigint[])
        from numbered
        join ins using (ev_id)
    $sql$, qstate.cur_table_name)
    into v_ids
    using qstate.queue_event_seq, _ev_data_list, now(), _ev_type;

    return v_ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, payloads jsonb[]) -- default-type batch send
create or replace function pgque.send_batch(queue_name text, payloads jsonb[])
returns bigint[] as $$
begin
    return pgque.send_batch(queue_name, 'default', payloads);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send_batch(text, jsonb[]) from public;

-- pgque.send_batch(queue, type, payloads jsonb[]) -- set-based batch send
create or replace function pgque.send_batch(
    queue_name text, type_name text, payloads jsonb[])
returns bigint[] as $$
begin
    if payloads is null then
        raise exception 'payloads must not be null';
    end if;
    if cardinality(payloads) = 0 then
        return '{}'::bigint[];
    end if;

    return pgque.insert_event_bulk(queue_name, type_name, payloads::text[]);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send_batch(text, text, jsonb[]) from public;

-- pgque.send_batch(queue, payloads text[]) -- default-type fast-path batch send
create or replace function pgque.send_batch(queue_name text, payloads text[])
returns bigint[] as $$
begin
    return pgque.send_batch(queue_name, 'default', payloads);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send_batch(text, text[]) from public;

-- pgque.send_batch(queue, type, payloads text[]) -- set-based fast-path batch send
create or replace function pgque.send_batch(
    queue_name text, type_name text, payloads text[])
returns bigint[] as $$
begin
    if payloads is null then
        raise exception 'payloads must not be null';
    end if;
    if cardinality(payloads) = 0 then
        return '{}'::bigint[];
    end if;

    return pgque.insert_event_bulk(queue_name, type_name, payloads);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.send_batch(text, text, text[]) from public;

-- pgque.subscribe(queue, consumer) -- wrapper for register_consumer
create or replace function pgque.subscribe(queue text, consumer text)
returns integer as $$
begin
    /*
     * Reserve '#' for partition slot consumers ("<consumer>#<slot>/<n>", see
     * pgque.subscribe_slot). The plain receive/ack/nack guards treat any '#'
     * name as a slot consumer and refuse it, so registering a plain consumer
     * whose name contains '#' would permanently lock it out of those calls.
     * Reject the reserved character here instead of at first use.
     */
    if position('#' in consumer) > 0 then
        raise exception 'consumer name must not contain the reserved character #: %', consumer;
    end if;

    return pgque.register_consumer(queue, consumer);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.subscribe(text, text) from public;

-- pgque.unsubscribe(queue, consumer) -- wrapper for unregister_consumer
create or replace function pgque.unsubscribe(queue text, consumer text)
returns integer as $$
begin
    return pgque.unregister_consumer(queue, consumer);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.unsubscribe(text, text) from public;

-- Restore owners for wrappers that had to be dropped during v0.1.0 upgrade.
do $$
declare
    rec record;
    proc regprocedure;
begin
    if to_regclass('pg_temp.pgque_v01_wrapper_owners') is null then
        return;
    end if;

    for rec in select sig, owner_name from pg_temp.pgque_v01_wrapper_owners
    loop
        proc := to_regprocedure(rec.sig);
        if proc is not null then
            execute format('alter function %s owner to %I', proc, rec.owner_name);

            /* A restored non-superuser wrapper still calls the locked-down
               bulk primitive, so grant its owner execute -- deliberately
               persistent across idempotent reinstalls. */
            if rec.sig in (
                'pgque.send_batch(text,text,jsonb[])',
                'pgque.send_batch(text,text,text[])'
            ) then
                execute format(
                    'grant execute on function pgque.insert_event_bulk(text, text, text[]) to %I',
                    rec.owner_name
                );
            end if;
        end if;
    end loop;
end $$;

drop table if exists pg_temp.pgque_v01_wrapper_owners;

/* send* -> pgque_writer (producer); subscribe/unsubscribe -> pgque_reader
   (consumer). See sql/pgque-additions/roles.sql for the split rationale. */
grant execute on function pgque.send(text, jsonb)               to pgque_writer;
grant execute on function pgque.send(text, text)                to pgque_writer;
grant execute on function pgque.send(text, text, jsonb)         to pgque_writer;
grant execute on function pgque.send(text, text, text)          to pgque_writer;
grant execute on function pgque.send_batch(text, jsonb[])       to pgque_writer;
grant execute on function pgque.send_batch(text, text[])        to pgque_writer;
grant execute on function pgque.send_batch(text, text, jsonb[]) to pgque_writer;
grant execute on function pgque.send_batch(text, text, text[])  to pgque_writer;
/* Revoke before re-granting on pgque_reader: create or replace preserves
   function-level grants, so an in-place upgrade must clear the old writer
   grant explicitly. */
revoke execute on function pgque.subscribe(text, text)         from pgque_writer;
revoke execute on function pgque.unsubscribe(text, text)       from pgque_writer;
grant execute on function pgque.subscribe(text, text)           to pgque_reader;
grant execute on function pgque.unsubscribe(text, text)         to pgque_reader;

/* Internal primitive for the SECURITY DEFINER send_batch wrappers only. Not
   granted to pgque_admin (it inherits pgque_writer): writers enter through the
   audited send_batch() wrappers, not this low-level primitive. */
revoke execute on function pgque.insert_event_bulk(text, text, text[])
    from public, pgque_reader, pgque_writer, pgque_admin;

/* Re-apply deny-by-default: roles.sql's blanket revoke runs before the
   pgque-api/ files, so functions created here would otherwise keep
   PostgreSQL's default PUBLIC EXECUTE. */
revoke execute on all functions in schema pgque from public;

