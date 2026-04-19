-- experimental config sugar API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- logres.create_queue(queue, options) -- JSONB overload
create or replace function logres.create_queue(i_queue text, i_options jsonb)
returns integer as $$
declare
    v_ret integer;
    v_key text;
    v_val text;
begin
    v_ret := logres.create_queue(i_queue);

    for v_key, v_val in select key, value #>> '{}' from jsonb_each(i_options)
    loop
        if v_key = 'max_retries' then
            update logres.queue
            set queue_max_retries = v_val::int4
            where queue_name = i_queue;
        else
            perform logres.set_queue_config(
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
$$ language plpgsql security definer set search_path = logres, pg_catalog;

create or replace function logres.pause_queue(i_queue text)
returns void as $$
begin
    perform logres.set_queue_config(i_queue, 'ticker_paused', 'true');
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

create or replace function logres.resume_queue(i_queue text)
returns void as $$
begin
    perform logres.set_queue_config(i_queue, 'ticker_paused', 'false');
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;
