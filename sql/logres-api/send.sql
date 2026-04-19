-- logres-api/send.sql -- Modern send/subscribe API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements default v0.1 API surface:
--   logres.message type
--   logres.send(queue, payload)                -- text + jsonb overloads
--   logres.send(queue, type, payload)          -- text + jsonb overloads
--   logres.send_batch(queue, type, payloads[]) -- text[] + jsonb[] overloads
--   logres.subscribe(queue, consumer)
--   logres.unsubscribe(queue, consumer)
--
-- Overload resolution note: PostgreSQL resolves untyped string literals
-- (type `unknown`) to the `text` overload because `unknown -> text` needs
-- no implicit cast, while `unknown -> jsonb` does. Consequently:
--
--   select logres.send('orders', '{"k":1}');           -- picks send(text, text)
--   select logres.send('orders', '{"k":1}'::jsonb);    -- picks send(text, jsonb)
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

-- logres.message type (idempotent creation)
do $$ begin
    create type logres.message as (
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

-- logres.send(queue, payload jsonb) -- send with default type, JSON payload
create or replace function logres.send(i_queue text, i_payload jsonb)
returns bigint as $$
begin
    return logres.insert_event(i_queue, 'default', i_payload::text);
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.send(queue, payload text) -- fast path, opaque textual payload
-- Skips the jsonb parse + canonical reserialize round-trip. Use this when
-- the payload is text (JSON, XML, CSV, base64/hex-encoded binary) or when
-- the caller has already validated the payload. Raw binary with NUL bytes
-- (protobuf, msgpack, Avro wire format) is not accepted by PG `text` --
-- encode first.
create or replace function logres.send(i_queue text, i_payload text)
returns bigint as $$
begin
    return logres.insert_event(i_queue, 'default', i_payload);
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.send(queue, type, payload jsonb) -- send with explicit type, JSON payload
create or replace function logres.send(i_queue text, i_type text, i_payload jsonb)
returns bigint as $$
begin
    return logres.insert_event(i_queue, i_type, i_payload::text);
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.send(queue, type, payload text) -- fast path with explicit type
create or replace function logres.send(i_queue text, i_type text, i_payload text)
returns bigint as $$
begin
    return logres.insert_event(i_queue, i_type, i_payload);
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.send_batch(queue, type, payloads jsonb[]) -- batch send, JSON payloads
create or replace function logres.send_batch(
    i_queue text, i_type text, i_payloads jsonb[])
returns bigint[] as $$
declare
    ids bigint[] := '{}';
    p jsonb;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            logres.insert_event(i_queue, i_type, p::text));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.send_batch(queue, type, payloads text[]) -- fast-path batch send
create or replace function logres.send_batch(
    i_queue text, i_type text, i_payloads text[])
returns bigint[] as $$
declare
    ids bigint[] := '{}';
    p text;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            logres.insert_event(i_queue, i_type, p));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.subscribe(queue, consumer) -- wrapper for register_consumer
create or replace function logres.subscribe(i_queue text, i_consumer text)
returns integer as $$
begin
    return logres.register_consumer(i_queue, i_consumer);
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- logres.unsubscribe(queue, consumer) -- wrapper for unregister_consumer
create or replace function logres.unsubscribe(i_queue text, i_consumer text)
returns integer as $$
begin
    return logres.unregister_consumer(i_queue, i_consumer);
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

-- Explicit writer grants for the send* + subscribe/unsubscribe family.
-- Colocated here (not in logres-additions/roles.sql) because roles.sql
-- runs before the logres-api section during build, so API-layer grants
-- must live alongside the function definitions they apply to.
grant execute on function logres.send(text, jsonb)               to logres_writer;
grant execute on function logres.send(text, text)                to logres_writer;
grant execute on function logres.send(text, text, jsonb)         to logres_writer;
grant execute on function logres.send(text, text, text)          to logres_writer;
grant execute on function logres.send_batch(text, text, jsonb[]) to logres_writer;
grant execute on function logres.send_batch(text, text, text[])  to logres_writer;
grant execute on function logres.subscribe(text, text)           to logres_writer;
grant execute on function logres.unsubscribe(text, text)         to logres_writer;

