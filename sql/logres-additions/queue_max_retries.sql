-- Add queue_max_retries column to logres.queue
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- The queue table is defined in PgQ's tables.sql.  After the transformed
-- PgQ schema is installed, we add this logres-specific column.

do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'logres' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ) then
        alter table logres.queue add column queue_max_retries int4;
    end if;
end $$;

-- Override set_queue_config to also accept queue_max_retries
create or replace function logres.set_queue_config(
    x_queue_name    text,
    x_param_name    text,
    x_param_value   text)
returns integer as $$
declare
    v_param_name    text;
begin
    -- discard NULL input
    if x_queue_name is null or x_param_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if queue exists
    perform 1 from logres.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'No such event queue';
    end if;

    -- check if valid parameter name
    v_param_name := 'queue_' || x_param_name;
    if v_param_name not in (
        'queue_ticker_max_count',
        'queue_ticker_max_lag',
        'queue_ticker_idle_period',
        'queue_ticker_paused',
        'queue_rotation_period',
        'queue_external_ticker',
        'queue_max_retries')
    then
        raise exception 'cannot change parameter "%s"', x_param_name;
    end if;

    execute 'update logres.queue set '
        || v_param_name || ' = ' || quote_literal(x_param_value)
        || ' where queue_name = ' || quote_literal(x_queue_name);

    return 1;
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;
