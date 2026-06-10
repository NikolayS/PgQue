-- experimental config sugar API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- pgque.create_queue(queue, options) -- JSONB overload
create or replace function pgque.create_queue(i_queue text, i_options jsonb)
returns integer as $$
declare
    v_ret integer;
    v_key text;
    v_val text;
begin
    v_ret := pgque.create_queue(i_queue);

    -- Route every option through pgque.set_queue_config() so its
    -- per-parameter validation (e.g. max_retries >= 0) always applies.
    for v_key, v_val in select key, value #>> '{}' from jsonb_each(i_options)
    loop
        perform pgque.set_queue_config(i_queue, v_key, v_val);
    end loop;

    return v_ret;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.pause_queue(i_queue text)
returns void as $$
begin
    perform pgque.set_queue_config(i_queue, 'ticker_paused', 'true');
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.resume_queue(i_queue text)
returns void as $$
begin
    perform pgque.set_queue_config(i_queue, 'ticker_paused', 'false');
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
