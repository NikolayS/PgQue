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
    -- 1..1000 ms and an exact divisor of the 1000 ms pg_cron slot: ticker_loop
    -- uses integer iteration counts, so arbitrary values (for example 251 or
    -- 750) would report an ideal cadence that cannot actually run in one slot.
    -- Reject them rather than silently flooring the effective rate.
    if p_period_ms is null or p_period_ms < 1 or p_period_ms > 1000 then
        raise exception 'tick_period_ms must be an exact divisor of 1000 between 1 and 1000 (got %)',
            coalesce(p_period_ms::text, 'NULL');
    end if;
    if 1000 % p_period_ms <> 0 then
        raise exception 'tick_period_ms must be an exact divisor of 1000 between 1 and 1000 (got %)',
            p_period_ms;
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
    perform pgque.stop_timetable();
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
        maint_job_id = v_maint_id,
        scheduler = 'pg_cron';

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
    v_scheduler text;
begin
    -- Read current job IDs
    select ticker_job_id, maint_job_id, scheduler
    into v_ticker_id, v_maint_id, v_scheduler
    from pgque.config;

    -- stop() is the generic PgQue stop entrypoint; delegate when pg_timetable
    -- owns the active jobs.
    if v_scheduler = 'pg_timetable' then
        perform pgque.stop_timetable();
        return;
    end if;

    -- Check if pg_cron is available
    select exists (select 1 from pg_extension where extname = 'pg_cron')
    into v_has_pgcron;

    if v_has_pgcron and (v_scheduler is null or v_scheduler = 'pg_cron') then
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

    -- Clear pg_cron job IDs (only when scheduler is pg_cron or unset).
    update pgque.config
    set ticker_job_id = null,
        maint_job_id = null,
        scheduler = null
    where scheduler is null or scheduler = 'pg_cron';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.start_timetable(i_ticks_per_second integer default 10)
returns void as $$
declare
    v_ticker_id bigint;
    v_retry_id bigint;
    v_maint_id bigint;
    v_step2_id bigint;
    v_period_ms integer;
    v_timetable_owner oid;
    v_add_job_reg regprocedure;
    v_delete_job_reg regprocedure;
    v_owner_super bool;
begin
    -- pg_timetable is optional and external (standalone scheduler). It creates
    -- the timetable schema on first run; PgQue only needs its SQL API here.
    if to_regnamespace('timetable') is null then
        raise exception 'pg_timetable schema/API is not installed. '
            'Run pg_timetable against this database first, or use pgque.start() for pg_cron.';
    end if;

    -- Support both modern pg_timetable (12-argument add_job with job_on_error)
    -- and older v4-style installs (11-argument add_job). The named-argument
    -- calls below only use parameters common to both versions.
    v_add_job_reg := coalesce(
        to_regprocedure('timetable.add_job(text,timetable.cron,text,jsonb,timetable.command_kind,text,integer,boolean,boolean,boolean,boolean,text)'),
        to_regprocedure('timetable.add_job(text,timetable.cron,text,jsonb,timetable.command_kind,text,integer,boolean,boolean,boolean,boolean)')
    );
    v_delete_job_reg := to_regprocedure('timetable.delete_job(text)');
    if v_add_job_reg is null or v_delete_job_reg is null then
        raise exception 'pg_timetable schema/API is not installed. '
            'Run pg_timetable against this database first, or use pgque.start() for pg_cron.';
    end if;

    -- start_timetable() is SECURITY DEFINER, so do not invoke arbitrary code
    -- from any schema named "timetable". Trust only a pg_timetable schema owned
    -- by the PgQue owner or by a superuser, and require the called functions to
    -- share that owner. This prevents a low-privilege fake timetable schema from
    -- becoming a definer-privilege trampoline.
    select nspowner into v_timetable_owner
    from pg_namespace where oid = 'timetable'::regnamespace;
    select rolsuper into v_owner_super
    from pg_roles where oid = v_timetable_owner;
    if v_timetable_owner <> current_user::regrole and not coalesce(v_owner_super, false) then
        raise exception 'untrusted pg_timetable schema owner: %', v_timetable_owner::regrole;
    end if;
    if exists (
        select 1
        from pg_proc
        where oid in (v_add_job_reg::oid, v_delete_job_reg::oid)
          and proowner <> v_timetable_owner
    ) then
        raise exception 'untrusted pg_timetable API owner: add_job/delete_job must be owned by timetable schema owner';
    end if;

    if i_ticks_per_second is null or i_ticks_per_second < 1 or i_ticks_per_second > 1000
       or 1000 % i_ticks_per_second <> 0 then
        raise exception 'ticks_per_second must be an exact divisor of 1000 between 1 and 1000 (got %)',
            coalesce(i_ticks_per_second::text, 'NULL');
    end if;

    v_period_ms := 1000 / i_ticks_per_second;
    perform pgque.set_tick_period_ms(v_period_ms);

    -- Idempotent: remove any old PgQue jobs from both schedulers.  This avoids
    -- double-ticking if an operator switches from pg_cron to pg_timetable.
    perform pgque.stop_timetable();
    perform pgque.stop();

    execute $sql$
        select timetable.add_job(
            job_name => 'pgque_ticker',
            job_schedule => '@every 1 second'::timetable.cron,
            job_command => 'CALL pgque.ticker_loop()',
            job_kind => 'SQL'::timetable.command_kind,
            job_max_instances => 1,
            job_ignore_errors => false
        )
    $sql$ into v_ticker_id;

    execute $sql$
        select timetable.add_job(
            job_name => 'pgque_retry_events',
            job_schedule => '@every 30 seconds'::timetable.cron,
            job_command => 'select pgque.maint_retry_events()',
            job_kind => 'SQL'::timetable.command_kind,
            job_max_instances => 1,
            job_ignore_errors => false
        )
    $sql$ into v_retry_id;

    execute $sql$
        select timetable.add_job(
            job_name => 'pgque_maint',
            job_schedule => '@every 30 seconds'::timetable.cron,
            job_command => 'select pgque.maint()',
            job_kind => 'SQL'::timetable.command_kind,
            job_max_instances => 1,
            job_ignore_errors => false
        )
    $sql$ into v_maint_id;

    execute $sql$
        select timetable.add_job(
            job_name => 'pgque_rotate_step2',
            job_schedule => '@every 10 seconds'::timetable.cron,
            job_command => 'select pgque.maint_rotate_tables_step2()',
            job_kind => 'SQL'::timetable.command_kind,
            job_max_instances => 1,
            job_ignore_errors => false
        )
    $sql$ into v_step2_id;

    update pgque.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id,
        scheduler = 'pg_timetable';

    raise notice 'pgque started with pg_timetable: ticker=% (% ticks/sec), retry_events=%, maint=%, rotate_step2=%',
        v_ticker_id, i_ticks_per_second, v_retry_id, v_maint_id, v_step2_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.stop_timetable()
returns void as $$
declare
    v_has_timetable bool;
    v_timetable_owner oid;
    v_delete_job_reg regprocedure;
    v_owner_super bool;
    v_scheduler text;
begin
    select scheduler into v_scheduler from pgque.config;

    v_has_timetable := to_regnamespace('timetable') is not null
        and to_regprocedure('timetable.delete_job(text)') is not null;

    if v_has_timetable then
        v_delete_job_reg := to_regprocedure('timetable.delete_job(text)');
        select nspowner into v_timetable_owner
        from pg_namespace where oid = 'timetable'::regnamespace;
        select rolsuper into v_owner_super
        from pg_roles where oid = v_timetable_owner;
        if v_timetable_owner <> current_user::regrole and not coalesce(v_owner_super, false) then
            if v_scheduler = 'pg_timetable' then
                raise exception 'untrusted pg_timetable schema owner: %', v_timetable_owner::regrole;
            end if;
            return;
        end if;
        if exists (
            select 1
            from pg_proc
            where oid = v_delete_job_reg::oid
              and proowner <> v_timetable_owner
        ) then
            if v_scheduler = 'pg_timetable' then
                raise exception 'untrusted pg_timetable API owner: delete_job must be owned by timetable schema owner';
            end if;
            return;
        end if;

        -- delete_job(name) returns false when absent; no exception noise needed.
        execute $sql$select timetable.delete_job('pgque_ticker')$sql$;
        execute $sql$select timetable.delete_job('pgque_retry_events')$sql$;
        execute $sql$select timetable.delete_job('pgque_maint')$sql$;
        execute $sql$select timetable.delete_job('pgque_rotate_step2')$sql$;
    end if;

    update pgque.config
    set ticker_job_id = null,
        maint_job_id = null,
        scheduler = null
    where scheduler = 'pg_timetable';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        perform pgque.stop();
    end if;
    if to_regnamespace('timetable') is not null then
        perform pgque.stop_timetable();
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
    return '0.2.0';
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

    -- Scheduler status (new summary row).
    return query
    select 'scheduler'::text,
        coalesce(c.scheduler, 'manual')::text,
        format('ticker_job_id=%s, maint_job_id=%s, tick_period_ms=%s (%s ticks/sec)',
            coalesce(c.ticker_job_id::text, 'NULL'),
            coalesce(c.maint_job_id::text, 'NULL'),
            c.tick_period_ms,
            (1000.0 / c.tick_period_ms)::numeric(10, 2))
    from pgque.config c;

    -- Backward-compatible rows retained for scripts that parse status() by
    -- component name.
    return query
    select 'ticker'::text,
        case when c.ticker_job_id is not null then 'scheduled' else 'stopped' end,
        case when c.ticker_job_id is not null
            then format('scheduler=%s, job_id=%s, tick_period_ms=%s (%s ticks/sec)',
                coalesce(c.scheduler, 'manual'),
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
            then format('scheduler=%s, job_id=%s', coalesce(c.scheduler, 'manual'), c.maint_job_id)
            else 'not scheduled'
        end
    from pgque.config c;

    if not exists (select 1 from pg_extension where extname = 'pg_cron') then
        return query select 'pg_cron'::text, 'unavailable'::text,
            'use pgque.start_timetable() for pg_timetable, or call pgque.ticker() / pgque.maint() manually'::text;
    end if;

    if to_regnamespace('timetable') is null then
        return query select 'pg_timetable'::text, 'unavailable'::text,
            'run pg_timetable against this database, then call pgque.start_timetable()'::text;
    end if;

    -- Queue count
    return query select 'queues'::text, 'info'::text,
        (select count(*)::text from pgque.queue) || ' queues configured';

    -- Consumer count
    return query select 'consumers'::text, 'info'::text,
        (select count(*)::text from pgque.subscription) || ' active subscriptions';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
