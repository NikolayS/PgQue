-- pgque-api/send.sql -- Modern send/subscribe API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements default v0.1 API surface:
--   pgque.message type
--   pgque.send(queue, payload)                -- jsonb + text overloads
--   pgque.send(queue, type, payload)          -- jsonb + text overloads
--   pgque.send_batch(queue, type, payloads[]) -- jsonb + text overloads
--   pgque.subscribe(queue, consumer)
--   pgque.unsubscribe(queue, consumer)
--
-- The jsonb overloads are the ergonomic default: PG validates JSON at parse
-- time and the caller gets a clear error for malformed payloads. The text
-- overloads skip the jsonb parse + canonical reserialization round-trip
-- (material for large payloads) and unlock non-JSON encodings: protobuf,
-- msgpack, Avro, XML. Storage (ev_data TEXT) is identical in both paths.

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

-- pgque.send(queue, payload text) -- fast path, opaque payload
-- Skips the jsonb parse + canonical reserialize round-trip. Use this when
-- the payload is not JSON (protobuf, msgpack, Avro, XML) or when the caller
-- has already validated the payload and wants to avoid the extra CPU.
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

-- pgque.send_batch(queue, type, payloads jsonb[]) -- batch send, JSON payloads
create or replace function pgque.send_batch(
    i_queue text, i_type text, i_payloads jsonb[])
returns bigint[] as $$
declare
    ids bigint[];
    p jsonb;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            pgque.insert_event(i_queue, i_type, p::text));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, type, payloads text[]) -- fast-path batch send
create or replace function pgque.send_batch(
    i_queue text, i_type text, i_payloads text[])
returns bigint[] as $$
declare
    ids bigint[];
    p text;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            pgque.insert_event(i_queue, i_type, p));
    end loop;
    return ids;
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

