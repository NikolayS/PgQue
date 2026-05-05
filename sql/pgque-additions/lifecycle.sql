-- pgque lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- pgque.ticker_loop()
--
-- Sub-second tick driver: runs inside one pg_cron slot (1 second cadence) and
-- internally invokes pgque.ticker() at the rate configured in
-- pgque.config.tick_period_ms (default 100 ms = 10 ticks/sec).
--
-- Implemented as a PROCEDURE so it can `commit` between iterations: every
-- pgque.ticker() call thereby gets its own transaction and the per-iteration
-- xmin is released, preserving the rotation behaviour the metadata tables
-- depend on.
-- Note: a procedure that uses COMMIT cannot also carry a SET clause (Postgres
-- restriction), so search_path is not pinned at the procedure level.  All
-- references inside the body are fully schema-qualified, and the procedure
-- only invokes SECURITY DEFINER functions (pgque.ticker / pgque.config) that
-- pin their own search_path. ticker_loop itself is SECURITY INVOKER and
-- callable only by pgque_admin / superuser (see grants below).
--
-- statement_timeout: NOT enforced from inside this procedure. Two reasons:
-- (a) statement_timeout is a top-level-statement timer — the CALL is the
--     statement, and its timer is fixed at invocation, so set_config inside
--     the body changes the GUC value but does not restart or apply the
--     timer to mid-procedure work; pg_sleep / ticker() run unguarded.
-- (b) the obvious workaround of "SET statement_timeout = ...; CALL ..." in
--     the pg_cron command is rejected at runtime: pg_cron concatenates the
--     two statements into one multi-statement transaction, and the
--     procedure's COMMIT then raises "invalid transaction termination".
-- The loop's clock_timestamp()-based budget below limits how many additional
-- iterations a slow run can chain together, but it cannot cancel a stuck
-- ticker() call. A hung ticker() will pin the pg_cron worker until an admin
-- pg_cancel_backend()s it. ticker() is short, well-trodden, and has no
-- code paths that block indefinitely under normal operation; we accept the
-- residual risk rather than ship a guardrail that doesn't actually fire.
create or replace procedure pgque.ticker_loop()
language plpgsql
as $$
declare
    v_period_ms     integer;
    v_window_ms     constant integer := 1000;
    v_started_at    timestamptz := clock_timestamp();
    v_elapsed_ms    double precision;
    v_iter_budget   integer;
    i               integer;
begin
    select tick_period_ms into v_period_ms from pgque.config;
    if v_period_ms is null or v_period_ms < 1 then
        v_period_ms := 100;
    end if;
    if v_period_ms > v_window_ms then
        v_period_ms := v_window_ms;
    end if;

    v_iter_budget := greatest(1, v_window_ms / v_period_ms);

    for i in 1 .. v_iter_budget loop
        perform pgque.ticker();
        commit;

        if i = v_iter_budget then
            exit;
        end if;

        v_elapsed_ms := extract(epoch from (clock_timestamp() - v_started_at)) * 1000.0;
        if v_elapsed_ms + v_period_ms >= v_window_ms then
            exit;
        end if;

        perform pg_sleep(v_period_ms / 1000.0);
    end loop;
end;
$$;

-- pgque.set_tick_period_ms(ms)
--
-- Configure how often pgque.ticker_loop() invokes pgque.ticker(). Default is
-- 100 ms (10 ticks/sec). Lower values cut producer→consumer latency for non-LISTEN
-- consumers; higher values reduce WAL volume and metadata churn.
--
-- Takes effect on the next pg_cron slot (≤1 s) without rescheduling.
create or replace function pgque.set_tick_period_ms(p_period_ms integer)
returns integer as $$
begin
    -- 1..1000 ms: pg_cron's minimum schedule is 1 s, so any value > 1000
    -- collapses to "one tick per pg_cron slot" (i.e. 1 tick/sec). Reject
    -- it explicitly rather than silently clamping. To tick less often than
    -- once per second, change the pg_cron schedule string directly.
    if p_period_ms is null or p_period_ms < 1 or p_period_ms > 1000 then
        raise exception 'tick_period_ms must be between 1 and 1000 (got %)',
            coalesce(p_period_ms::text, 'NULL');
    end if;
    update pgque.config set tick_period_ms = p_period_ms;
    return p_period_ms;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.start()
returns void as $$
declare
    v_ticker_id bigint;
    v_retry_id bigint;
    v_maint_id bigint;
    v_step2_id bigint;
    v_dbname text;
    v_period_ms integer;
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
    select tick_period_ms into v_period_ms from pgque.config;

    -- Ticker: pg_cron fires every 1 second; pgque.ticker_loop() then
    -- internally re-ticks at pgque.config.tick_period_ms cadence (default
    -- 100 ms = 10 ticks/sec). Tune via pgque.set_tick_period_ms(ms).
    --
    -- Bare CALL: NO `SET statement_timeout = ...;` prefix. pg_cron
    -- concatenates SET + CALL into one multi-statement transaction, and a
    -- procedure that issues COMMIT inside that wrapper raises "invalid
    -- transaction termination". See ticker_loop's source comment for the
    -- full reasoning on why a per-iteration statement_timeout cannot be
    -- enforced from inside the procedure either.
    select cron.schedule_in_database(
        'pgque_ticker',
        '1 second',
        $sql$CALL pgque.ticker_loop()$sql$,
        v_dbname
    ) into v_ticker_id;

    -- Retry events: every 30 seconds (move nack'd events from the retry
    -- queue back into the main event stream for the next tick).
    -- pgque.maint() / maint_operations() does NOT include retry handling,
    -- so this has to be scheduled separately — matches pgqd cadence.
    select cron.schedule_in_database(
        'pgque_retry_events',
        '30 seconds',
        $sql$set statement_timeout = '25s'; select pgque.maint_retry_events()$sql$,
        v_dbname
    ) into v_retry_id;

    -- Maintenance: every 30 seconds (rotation step 1 and vacuum).
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

    -- Store job IDs in config (retry + rotate_step2 unscheduled by name)
    update pgque.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id;

    raise notice 'pgque started: ticker=% (% ticks/sec), retry_events=%, maint=%, rotate_step2=%',
        v_ticker_id, (1000.0 / v_period_ms)::numeric(10, 2),
        v_retry_id, v_maint_id, v_step2_id;
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

        -- Unschedule retry_events by name (job ID not stored in config).
        -- Ignore if job doesn't exist (first run or already removed).
        begin
            perform cron.unschedule('pgque_retry_events');
        exception when others then
            raise notice 'pgque.stop: retry_events job not found (OK on first install)';
        end;

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
    return '0.2.0-dev';
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
                then format('job_id=%s, tick_period_ms=%s (%s ticks/sec)',
                    c.ticker_job_id,
                    c.tick_period_ms,
                    (1000.0 / c.tick_period_ms)::numeric(10, 2))
                else format('not scheduled (tick_period_ms=%s)', c.tick_period_ms)
            end
        from pgque.config c;

        return query
        select 'maintenance'::text,
            case when c.maint_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.maint_job_id is not null
                then format('job_id=%s', c.maint_job_id)
                else 'not scheduled'
            end
        from pgque.config c;
    else
        return query select 'pg_cron'::text, 'unavailable'::text,
            format('pg_cron not installed -- call pgque.ticker() and pgque.maint() manually (current tick_period_ms=%s)',
                (select tick_period_ms from pgque.config));
    end if;

    -- Queue count
    return query select 'queues'::text, 'info'::text,
        (select count(*)::text from pgque.queue) || ' queues configured';

    -- Consumer count
    return query select 'consumers'::text, 'info'::text,
        (select count(*)::text from pgque.subscription) || ' active subscriptions';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
