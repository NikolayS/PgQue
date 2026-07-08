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

    for v_key, v_val in select key, value #>> '{}' from jsonb_each(i_options)
    loop
        if v_key = 'max_retries' then
            update pgque.queue
            set queue_max_retries = v_val::int4
            where queue_name = i_queue;
        else
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
