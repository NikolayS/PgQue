-- pgque lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function pgque.start()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_step2_id bigint;
    v_dbname text;
begin
    -- pg_cron is optional; start() specifically requires it because it schedules jobs.
    if not exists (select 1 from pg_extension where extname = 'pg_cron') then
        raise exception 'pg_cron extension is not installed. '
            'PgQue itself works without pg_cron, but pgque.start() schedules cron jobs. '
            'Install pg_cron first, or run pgque.ticker() and pgque.maint() manually.';
    end if;

    -- Idempotent: stop existing jobs first
    perform pgque.stop();

    v_dbname := current_database();

    -- Ticker: every 2 seconds (pg_cron >= 1.5 for sub-minute scheduling)
    select cron.schedule_in_database(
        'pgque_ticker',
        '2 seconds',
        $sql$SET statement_timeout = '1500ms'; SELECT pgque.ticker()$sql$,
        v_dbname
    ) into v_ticker_id;

    -- Maintenance: every 30 seconds (rotation step1, retry, vacuum)
    select cron.schedule_in_database(
        'pgque_maint',
        '30 seconds',
        $sql$SET statement_timeout = '25s'; SELECT pgque.maint()$sql$,
        v_dbname
    ) into v_maint_id;

    -- Rotation step2: every 10 seconds, SEPARATE transaction from step1.
    -- PgQ requires step1 and step2 in different transactions so that
    -- step2's txid is guaranteed to be visible to all new transactions.
    select cron.schedule_in_database(
        'pgque_rotate_step2',
        '10 seconds',
        $sql$SELECT pgque.maint_rotate_tables_step2()$sql$,
        v_dbname
    ) into v_step2_id;

    -- Store job IDs in config
    update pgque.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id;

    raise notice 'pgque started: ticker job=%, maint job=%, rotate_step2 job=%',
        v_ticker_id, v_maint_id, v_step2_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.stop()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_has_pgcron bool;
begin
    -- Read current job IDs
    select ticker_job_id, maint_job_id
    into v_ticker_id, v_maint_id
    from pgque.config;

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

        -- Unschedule rotate_step2 by name (job ID not stored in config)
        -- Ignore if job doesn't exist (first run or already removed)
        begin
            perform cron.unschedule('pgque_rotate_step2');
        exception when others then
            raise notice 'pgque.stop: rotate_step2 job not found (OK on first install)';
        end;
    end if;

    -- Clear job IDs regardless (even if pg_cron is gone)
    update pgque.config
    set ticker_job_id = null,
        maint_job_id = null;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        perform pgque.stop();
    end if;
    -- Drop everything
    drop schema pgque cascade;
    -- Note: roles are not dropped here (they may be in use by other databases)
    raise notice 'pgque uninstalled. Run DROP ROLE IF EXISTS pgque_reader, pgque_writer, pgque_admin; manually if needed.';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.version()
returns text as $$
begin
    return '1.0.0-dev';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.status()
returns table (
    component text,
    status text,
    detail text
) as $$
begin
    -- PostgreSQL version
    return query select 'postgresql'::text, 'info'::text, pg_catalog.version()::text;

    -- pgque version
    return query select 'pgque'::text, 'info'::text, pgque.version();

    -- pg_cron status
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        return query
        select 'ticker'::text,
            case when c.ticker_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.ticker_job_id is not null
                then 'job_id=' || c.ticker_job_id::text
                else 'not scheduled'
            end
        from pgque.config c;

        return query
        select 'maintenance'::text,
            case when c.maint_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.maint_job_id is not null
                then 'job_id=' || c.maint_job_id::text
                else 'not scheduled'
            end
        from pgque.config c;
    else
        return query select 'pg_cron'::text, 'unavailable'::text,
            'pg_cron not installed -- call pgque.ticker() and pgque.maint() manually'::text;
    end if;

    -- Queue count
    return query select 'queues'::text, 'info'::text,
        (select count(*)::text from pgque.queue) || ' queues configured';

    -- Consumer count
    return query select 'consumers'::text, 'info'::text,
        (select count(*)::text from pgque.subscription) || ' active subscriptions';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
