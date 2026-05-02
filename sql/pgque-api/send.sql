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
--
-- Overload resolution note: PostgreSQL resolves untyped string literals
-- (type `unknown`) to the `text` overload because `unknown -> text` needs
-- no implicit cast, while `unknown -> jsonb` does. Consequently:
--
--   select pgque.send('orders', '{"k":1}');           -- picks send(text, text)
--   select pgque.send('orders', '{"k":1}'::jsonb);    -- picks send(text, jsonb)
--
-- The `text` overloads are the default for untyped literals: bytes flow
-- through verbatim (no parse, no canonicalization, key order preserved)
-- for *textual* payloads -- JSON, XML, CSV, or binary that has already
-- been base64/hex-encoded. PostgreSQL `text` cannot store NUL (\x00),
-- so true binary payloads (raw protobuf, msgpack, Avro, bytea dumps)
-- must be caller-encoded before `send()` -- otherwise PG rejects the
-- insert with `invalid byte sequence`.
-- The `jsonb` overloads are opt-in via explicit `::jsonb` cast: PG
-- validates JSON at parse time and stores the canonical form.
-- Storage (ev_data TEXT) is identical in both paths.

-- pgque.message type (idempotent creation)
do $$ begin
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
exception when duplicate_object then null;
end $$;

-- pgque.send(queue, payload jsonb) -- send with default type, JSON payload
create or replace function pgque.send(i_queue text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, payload text) -- fast path, opaque textual payload
-- Skips the jsonb parse + canonical reserialize round-trip. Use this when
-- the payload is text (JSON, XML, CSV, base64/hex-encoded binary) or when
-- the caller has already validated the payload. Raw binary with NUL bytes
-- (protobuf, msgpack, Avro wire format) is not accepted by PG `text` --
-- encode first.
create or replace function pgque.send(i_queue text, i_payload text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, type, payload jsonb) -- send with explicit type, JSON payload
create or replace function pgque.send(i_queue text, i_type text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, type, payload text) -- fast path with explicit type
create or replace function pgque.send(i_queue text, i_type text, i_payload text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, type, payloads jsonb[]) -- set-based batch send
create or replace function pgque.send_batch(
    i_queue text, i_type text, i_payloads jsonb[])
returns bigint[] as $$
declare
    qstate record;
    v_ids bigint[];
begin
    if i_payloads is null then
        raise exception 'payloads must not be null';
    end if;

    select q.queue_id,
           pgque.quote_fqname(q.queue_data_pfx || '_' || q.queue_cur_table::text) as cur_table_name,
           q.queue_event_seq,
           q.queue_disable_insert
      into qstate
      from pgque.queue q
     where q.queue_name = i_queue;

    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    if qstate.queue_disable_insert then
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Insert into queue disallowed';
        end if;
    end if;

    execute format($sql$
        with input as materialized (
            select u.ord,
                   nextval($1::regclass) as ev_id,
                   u.payload::text as ev_data
              from unnest($2::jsonb[]) with ordinality as u(payload, ord)
        ), ins as (
            insert into %s
                (ev_id, ev_time, ev_owner, ev_retry,
                 ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
            select ev_id, $3, null, null,
                   $4, ev_data, null, null, null, null
              from input
             order by ord
            returning ev_id
        )
        select coalesce(array_agg(input.ev_id order by input.ord), '{}'::bigint[])
          from input
          join ins using (ev_id)
    $sql$, qstate.cur_table_name)
    into v_ids
    using qstate.queue_event_seq, i_payloads, now(), i_type;

    return v_ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, type, payloads text[]) -- set-based fast-path batch send
create or replace function pgque.send_batch(
    i_queue text, i_type text, i_payloads text[])
returns bigint[] as $$
declare
    qstate record;
    v_ids bigint[];
begin
    if i_payloads is null then
        raise exception 'payloads must not be null';
    end if;

    select q.queue_id,
           pgque.quote_fqname(q.queue_data_pfx || '_' || q.queue_cur_table::text) as cur_table_name,
           q.queue_event_seq,
           q.queue_disable_insert
      into qstate
      from pgque.queue q
     where q.queue_name = i_queue;

    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    if qstate.queue_disable_insert then
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Insert into queue disallowed';
        end if;
    end if;

    execute format($sql$
        with input as materialized (
            select u.ord,
                   nextval($1::regclass) as ev_id,
                   u.payload as ev_data
              from unnest($2::text[]) with ordinality as u(payload, ord)
        ), ins as (
            insert into %s
                (ev_id, ev_time, ev_owner, ev_retry,
                 ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
            select ev_id, $3, null, null,
                   $4, ev_data, null, null, null, null
              from input
             order by ord
            returning ev_id
        )
        select coalesce(array_agg(input.ev_id order by input.ord), '{}'::bigint[])
          from input
          join ins using (ev_id)
    $sql$, qstate.cur_table_name)
    into v_ids
    using qstate.queue_event_seq, i_payloads, now(), i_type;

    return v_ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.subscribe(queue, consumer) -- wrapper for register_consumer
create or replace function pgque.subscribe(i_queue text, i_consumer text)
returns integer as $$
begin
    return pgque.register_consumer(i_queue, i_consumer);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.unsubscribe(queue, consumer) -- wrapper for unregister_consumer
create or replace function pgque.unsubscribe(i_queue text, i_consumer text)
returns integer as $$
begin
    return pgque.unregister_consumer(i_queue, i_consumer);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- Grants for the send* + subscribe/unsubscribe family.
-- send* are producer-side (insert events) -> pgque_writer.
-- subscribe/unsubscribe are consumer-side (manage subscription cursor) ->
-- pgque_reader. See sql/pgque-additions/roles.sql for the producer/consumer
-- split rationale.
grant execute on function pgque.send(text, jsonb)               to pgque_writer;
grant execute on function pgque.send(text, text)                to pgque_writer;
grant execute on function pgque.send(text, text, jsonb)         to pgque_writer;
grant execute on function pgque.send(text, text, text)          to pgque_writer;
grant execute on function pgque.send_batch(text, text, jsonb[]) to pgque_writer;
grant execute on function pgque.send_batch(text, text, text[])  to pgque_writer;
-- Upgrade path: pre-#163 installs granted subscribe/unsubscribe to
-- pgque_writer. Revoke explicitly before re-granting on pgque_reader so
-- in-place upgrades clear the old grants (create or replace function
-- preserves function-level grants).
revoke execute on function pgque.subscribe(text, text)         from pgque_writer;
revoke execute on function pgque.unsubscribe(text, text)       from pgque_writer;
grant execute on function pgque.subscribe(text, text)           to pgque_reader;
grant execute on function pgque.unsubscribe(text, text)         to pgque_reader;

-- Re-apply deny-by-default after all API functions are defined.
-- roles.sql's blanket revoke runs before pgque-api/ files are loaded, so
-- functions created here would otherwise inherit PostgreSQL's default
-- PUBLIC EXECUTE. This second pass covers everything.
revoke execute on all functions in schema pgque from public;

