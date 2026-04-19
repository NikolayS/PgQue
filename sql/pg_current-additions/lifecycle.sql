-- pg_current lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function pg_current.start()
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
            'pg_current itself works without pg_cron, but pg_current.start() schedules cron jobs. '
            'Install pg_cron first, or run pg_current.ticker() and pg_current.maint() manually.';
    end if;

    -- Idempotent: stop existing jobs first
    perform pg_current.stop();

    v_dbname := current_database();

    -- Ticker: every 1 second (matches pgqd cadence; requires pg_cron >= 1.5
    -- for sub-minute scheduling)
    select cron.schedule_in_database(
        'pg_current_ticker',
        '1 second',
        $sql$SET statement_timeout = '950ms'; SELECT pg_current.ticker()$sql$,
        v_dbname
    ) into v_ticker_id;

    -- Retry events: every 30 seconds (move nack'd events from the retry
    -- queue back into the main event stream for the next tick).
    -- pg_current.maint() / maint_operations() does NOT include retry handling,
    -- so this has to be scheduled separately — matches pgqd cadence.
    select cron.schedule_in_database(
        'pg_current_retry_events',
        '30 seconds',
        $sql$set statement_timeout = '25s'; select pg_current.maint_retry_events()$sql$,
        v_dbname
    ) into v_retry_id;

    -- Maintenance: every 30 seconds (rotation step 1 and vacuum).
    select cron.schedule_in_database(
        'pg_current_maint',
        '30 seconds',
        $sql$SET statement_timeout = '25s'; SELECT pg_current.maint()$sql$,
        v_dbname
    ) into v_maint_id;

    -- Rotation step2: every 10 seconds, SEPARATE transaction from step1.
    -- PgQ requires step1 and step2 in different transactions so that
    -- step2's txid is guaranteed to be visible to all new transactions.
    select cron.schedule_in_database(
        'pg_current_rotate_step2',
        '10 seconds',
        $sql$SELECT pg_current.maint_rotate_tables_step2()$sql$,
        v_dbname
    ) into v_step2_id;

    -- Store job IDs in config (retry + rotate_step2 unscheduled by name)
    update pg_current.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id;

    raise notice 'pg_current started: ticker=%, retry_events=%, maint=%, rotate_step2=%',
        v_ticker_id, v_retry_id, v_maint_id, v_step2_id;
end;
$$ language plpgsql security definer set search_path = pg_current, pg_catalog;

create or replace function pg_current.stop()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_has_pgcron bool;
begin
    -- Read current job IDs
    select ticker_job_id, maint_job_id
    into v_ticker_id, v_maint_id
    from pg_current.config;

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
            perform cron.unschedule('pg_current_retry_events');
        exception when others then
            raise notice 'pg_current.stop: retry_events job not found (OK on first install)';
        end;

        -- Unschedule rotate_step2 by name (job ID not stored in config)
        -- Ignore if job doesn't exist (first run or already removed)
        begin
            perform cron.unschedule('pg_current_rotate_step2');
        exception when others then
            raise notice 'pg_current.stop: rotate_step2 job not found (OK on first install)';
        end;
    end if;

    -- Clear job IDs regardless (even if pg_cron is gone)
    update pg_current.config
    set ticker_job_id = null,
        maint_job_id = null;
end;
$$ language plpgsql security definer set search_path = pg_current, pg_catalog;

create or replace function pg_current.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        perform pg_current.stop();
    end if;
    -- Drop everything
    drop schema pg_current cascade;
    -- Note: roles are not dropped here (they may be in use by other databases)
    raise notice 'pg_current uninstalled. Run DROP ROLE IF EXISTS pg_current_reader, pg_current_writer, pg_current_admin; manually if needed.';
end;
$$ language plpgsql security definer set search_path = pg_current, pg_catalog;

create or replace function pg_current.version()
returns text as $$
begin
    return '1.0.0-dev';
end;
$$ language plpgsql security definer set search_path = pg_current, pg_catalog;

create or replace function pg_current.status()
returns table (
    component text,
    status text,
    detail text
) as $$
begin
    -- PostgreSQL version
    return query select 'postgresql'::text, 'info'::text, pg_catalog.version()::text;

    -- pg_current version
    return query select 'pg_current'::text, 'info'::text, pg_current.version();

    -- pg_cron status
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        return query
        select 'ticker'::text,
            case when c.ticker_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.ticker_job_id is not null
                then 'job_id=' || c.ticker_job_id::text
                else 'not scheduled'
            end
        from pg_current.config c;

        return query
        select 'maintenance'::text,
            case when c.maint_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.maint_job_id is not null
                then 'job_id=' || c.maint_job_id::text
                else 'not scheduled'
            end
        from pg_current.config c;
    else
        return query select 'pg_cron'::text, 'unavailable'::text,
            'pg_cron not installed -- call pg_current.ticker() and pg_current.maint() manually'::text;
    end if;

    -- Queue count
    return query select 'queues'::text, 'info'::text,
        (select count(*)::text from pg_current.queue) || ' queues configured';

    -- Consumer count
    return query select 'consumers'::text, 'info'::text,
        (select count(*)::text from pg_current.subscription) || ' active subscriptions';
end;
$$ language plpgsql security definer set search_path = pg_current, pg_catalog;
