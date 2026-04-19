-- logres lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function logres.start()
returns void as $$
declare
    v_ticker_id bigint;
    v_retry_id bigint;
    v_maint_id bigint;
    v_step2_id bigint;
    v_dbname text;
begin
    -- pg_cron is optional; start() specifically requires it because it schedules jobs.
    if not exists (select 1 from pg_extension where extname = 'pg_cron') then
        raise exception 'pg_cron extension is not installed. '
            'logres itself works without pg_cron, but logres.start() schedules cron jobs. '
            'Install pg_cron first, or run logres.ticker() and logres.maint() manually.';
    end if;

    -- Idempotent: stop existing jobs first
    perform logres.stop();

    v_dbname := current_database();

    -- Ticker: every 1 second (matches pgqd cadence; requires pg_cron >= 1.5
    -- for sub-minute scheduling)
    select cron.schedule_in_database(
        'logres_ticker',
        '1 second',
        $sql$SET statement_timeout = '950ms'; SELECT logres.ticker()$sql$,
        v_dbname
    ) into v_ticker_id;

    -- Retry events: every 30 seconds (move nack'd events from the retry
    -- queue back into the main event stream for the next tick).
    -- logres.maint() / maint_operations() does NOT include retry handling,
    -- so this has to be scheduled separately — matches pgqd cadence.
    select cron.schedule_in_database(
        'logres_retry_events',
        '30 seconds',
        $sql$set statement_timeout = '25s'; select logres.maint_retry_events()$sql$,
        v_dbname
    ) into v_retry_id;

    -- Maintenance: every 30 seconds (rotation step 1 and vacuum).
    select cron.schedule_in_database(
        'logres_maint',
        '30 seconds',
        $sql$SET statement_timeout = '25s'; SELECT logres.maint()$sql$,
        v_dbname
    ) into v_maint_id;

    -- Rotation step2: every 10 seconds, SEPARATE transaction from step1.
    -- PgQ requires step1 and step2 in different transactions so that
    -- step2's txid is guaranteed to be visible to all new transactions.
    select cron.schedule_in_database(
        'logres_rotate_step2',
        '10 seconds',
        $sql$SELECT logres.maint_rotate_tables_step2()$sql$,
        v_dbname
    ) into v_step2_id;

    -- Store job IDs in config (retry + rotate_step2 unscheduled by name)
    update logres.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id;

    raise notice 'logres started: ticker=%, retry_events=%, maint=%, rotate_step2=%',
        v_ticker_id, v_retry_id, v_maint_id, v_step2_id;
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

create or replace function logres.stop()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_has_pgcron bool;
begin
    -- Read current job IDs
    select ticker_job_id, maint_job_id
    into v_ticker_id, v_maint_id
    from logres.config;

    -- Check if pg_cron is available
    select exists (select 1 from pg_extension where extname = 'pg_cron')
    into v_has_pgcron;

    if v_has_pgcron then
        -- Unschedule ticker if it exists
        if v_ticker_id is not null then
            perform cron.unschedule(v_ticker_id);
        end if;

        -- Unschedule maint if it exists
        if v_maint_id is not null then
            perform cron.unschedule(v_maint_id);
        end if;

        -- Unschedule retry_events by name (job ID not stored in config).
        -- Ignore if job doesn't exist (first run or already removed).
        begin
            perform cron.unschedule('logres_retry_events');
        exception when others then
            raise notice 'logres.stop: retry_events job not found (OK on first install)';
        end;

        -- Unschedule rotate_step2 by name (job ID not stored in config)
        -- Ignore if job doesn't exist (first run or already removed)
        begin
            perform cron.unschedule('logres_rotate_step2');
        exception when others then
            raise notice 'logres.stop: rotate_step2 job not found (OK on first install)';
        end;
    end if;

    -- Clear job IDs regardless (even if pg_cron is gone)
    update logres.config
    set ticker_job_id = null,
        maint_job_id = null;
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

create or replace function logres.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        perform logres.stop();
    end if;
    -- Drop everything
    drop schema logres cascade;
    -- Note: roles are not dropped here (they may be in use by other databases)
    raise notice 'logres uninstalled. Run DROP ROLE IF EXISTS logres_reader, logres_writer, logres_admin; manually if needed.';
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

create or replace function logres.version()
returns text as $$
begin
    return '1.0.0-dev';
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;

create or replace function logres.status()
returns table (
    component text,
    status text,
    detail text
) as $$
begin
    -- PostgreSQL version
    return query select 'postgresql'::text, 'info'::text, pg_catalog.version()::text;

    -- logres version
    return query select 'logres'::text, 'info'::text, logres.version();

    -- pg_cron status
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        return query
        select 'ticker'::text,
            case when c.ticker_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.ticker_job_id is not null
                then 'job_id=' || c.ticker_job_id::text
                else 'not scheduled'
            end
        from logres.config c;

        return query
        select 'maintenance'::text,
            case when c.maint_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.maint_job_id is not null
                then 'job_id=' || c.maint_job_id::text
                else 'not scheduled'
            end
        from logres.config c;
    else
        return query select 'pg_cron'::text, 'unavailable'::text,
            'pg_cron not installed -- call logres.ticker() and logres.maint() manually'::text;
    end if;

    -- Queue count
    return query select 'queues'::text, 'info'::text,
        (select count(*)::text from logres.queue) || ' queues configured';

    -- Consumer count
    return query select 'consumers'::text, 'info'::text,
        (select count(*)::text from logres.subscription) || ' active subscriptions';
end;
$$ language plpgsql security definer set search_path = logres, pg_catalog;
