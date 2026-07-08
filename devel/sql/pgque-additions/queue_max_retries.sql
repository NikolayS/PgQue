-- Add queue_max_retries column to pgque.queue
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- The queue table is defined in PgQ's tables.sql.  After the transformed
-- PgQ schema is installed, we add this pgque-specific column.

do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'pgque' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ) then
        alter table pgque.queue add column queue_max_retries int4;
    end if;
end $$;

-- Override set_queue_config to also accept queue_max_retries.
--
-- #100: validate parameter values before writing them to pgque.queue.
-- The base PgQ implementation accepts any string PostgreSQL can cast to the
-- column type, so a typo like ticker_max_count=0 or rotation_period=-1h
-- silently produced broken ticker / rotation behavior. Reject nonsensical
-- values up front with a clear error.
create or replace function pgque.set_queue_config(
    x_queue_name    text,
    x_param_name    text,
    x_param_value   text)
returns integer as $$
declare
    v_param_name    text;
    v_int_val       int8;
    v_interval_val  interval;
begin
    -- discard NULL input
    if x_queue_name is null or x_param_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if queue exists
    perform 1 from pgque.queue where queue_name = x_queue_name;
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

    -- Per-parameter semantic validation (#100). Type errors (non-numeric for
    -- integer params, non-interval for interval params) still surface as
    -- PostgreSQL cast errors during the UPDATE; this block adds the
    -- range/sign checks that PG cannot infer from the column type alone.
    -- NULL values pass through to reset the column to its DEFAULT.
    if x_param_value is not null then
        case v_param_name
            when 'queue_max_retries' then
                v_int_val := x_param_value::int8;
                if v_int_val < 0 then
                    raise exception 'set_queue_config: max_retries must be >= 0, got %', v_int_val;
                end if;
            when 'queue_ticker_max_count' then
                v_int_val := x_param_value::int8;
                if v_int_val <= 0 then
                    raise exception 'set_queue_config: ticker_max_count must be > 0, got %', v_int_val;
                end if;
            when 'queue_ticker_max_lag' then
                v_interval_val := x_param_value::interval;
                if v_interval_val <= interval '0' then
                    raise exception 'set_queue_config: ticker_max_lag must be > 0, got %', v_interval_val;
                end if;
            when 'queue_ticker_idle_period' then
                v_interval_val := x_param_value::interval;
                if v_interval_val <= interval '0' then
                    raise exception 'set_queue_config: ticker_idle_period must be > 0, got %', v_interval_val;
                end if;
            when 'queue_rotation_period' then
                v_interval_val := x_param_value::interval;
                if v_interval_val <= interval '0' then
                    raise exception 'set_queue_config: rotation_period must be > 0, got %', v_interval_val;
                end if;
            else
                -- queue_ticker_paused / queue_external_ticker: bool, validated by cast.
                null;
        end case;
    end if;

    execute 'update pgque.queue set '
        || v_param_name || ' = '
        || case when x_param_value is null then 'DEFAULT' else quote_literal(x_param_value) end
        || ' where queue_name = ' || quote_literal(x_queue_name);

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
