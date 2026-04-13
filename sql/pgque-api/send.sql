-- pgque-api/send.sql -- Modern send/subscribe API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements SPECx.md sections 4.1, 4.4, 4.7:
--   pgque.message type
--   pgque.send(queue, payload)
--   pgque.send(queue, type, payload)
--   pgque.send_batch(queue, type, payloads[])
--   pgque.subscribe(queue, consumer)
--   pgque.unsubscribe(queue, consumer)
--   pgque.create_queue(queue, options) JSONB overload
--   pgque.pause_queue(queue)
--   pgque.resume_queue(queue)

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

-- pgque.create_queue(queue, options) -- JSONB overload
-- Maps JSONB keys to set_queue_config calls and queue_max_retries column.
create or replace function pgque.create_queue(i_queue text, i_options jsonb)
returns integer as $$
declare
    v_ret integer;
    v_key text;
    v_val text;
begin
    -- Create the queue using the existing PgQ function
    v_ret := pgque.create_queue(i_queue);

    -- Apply options from JSONB
    for v_key, v_val in select key, value #>> '{}' from jsonb_each(i_options)
    loop
        if v_key = 'max_retries' then
            update pgque.queue
            set queue_max_retries = v_val::int4
            where queue_name = i_queue;
        else
            -- Map known option names to PgQ config params
            perform pgque.set_queue_config(
                i_queue,
                case v_key
                    when 'rotation_period' then 'rotation_period'
                    when 'ticker_max_count' then 'ticker_max_count'
                    when 'ticker_max_lag' then 'ticker_max_lag'
                    when 'ticker_idle_period' then 'ticker_idle_period'
                    when 'ticker_paused' then 'ticker_paused'
                    else v_key
                end,
                v_val
            );
        end if;
    end loop;

    return v_ret;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.pause_queue(queue) -- convenience wrapper
create or replace function pgque.pause_queue(i_queue text)
returns void as $$
begin
    perform pgque.set_queue_config(i_queue, 'ticker_paused', 'true');
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.resume_queue(queue) -- convenience wrapper
create or replace function pgque.resume_queue(i_queue text)
returns void as $$
begin
    perform pgque.set_queue_config(i_queue, 'ticker_paused', 'false');
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
