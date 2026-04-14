-- pgque-api/send.sql -- Modern send/subscribe API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements default v0.1 API surface:
--   pgque.send(queue, payload)
--   pgque.send(queue, type, payload)
--   pgque.send_batch(queue, type, payloads[])
--   pgque.subscribe(queue, consumer)
--   pgque.unsubscribe(queue, consumer)

-- pgque.send(queue, payload) -- send with default type
create or replace function pgque.send(i_queue text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, type, payload) -- send with explicit type
create or replace function pgque.send(i_queue text, i_type text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, type, payloads[]) -- batch send
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

