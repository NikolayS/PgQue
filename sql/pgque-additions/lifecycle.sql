-- pgque lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function pgque.start()
returns void as $$
begin
    raise notice 'pgque.start(): pg_cron integration will be implemented in Sprint 2';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.stop()
returns void as $$
begin
    raise notice 'pgque.stop(): pg_cron integration will be implemented in Sprint 2';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    -- Sprint 2: stop() must handle its own errors when pg_cron integration lands.
    perform pgque.stop();
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
            case when c.ticker_job_id is not null then 'running' else 'stopped' end,
            case when c.ticker_job_id is not null
                then 'job_id=' || c.ticker_job_id::text
                else 'not scheduled'
            end
        from pgque.config c;

        return query
        select 'maintenance'::text,
            case when c.maint_job_id is not null then 'running' else 'stopped' end,
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
