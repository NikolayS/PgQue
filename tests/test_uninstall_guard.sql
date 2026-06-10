-- test_uninstall_guard.sql -- Uninstall scripts must stop scheduler jobs first
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Scheduler jobs (pg_cron, pg_timetable) are catalog rows, not dependent
-- objects: dropping the pgque schema or extension does not remove them, so
-- both uninstall scripts must call pgque.stop() before dropping, and a real
-- stop() failure must abort the uninstall instead of being swallowed.
--
-- These tests swap pgque.stop() for instrumented fakes; the real definition
-- is saved first and restored at the end, so the rest of the suite is
-- unaffected. Runs without pg_cron / pg_tle. The TLE uninstall sub-tests
-- only run against a plain (non-extension) install; see the gate below.
--
-- Run from the repo root: the \i commands below resolve relative to cwd.

-- Save the real pgque.stop() so it can be restored at the end.
create table pg_temp.saved_stop as
select pg_catalog.pg_get_functiondef('pgque.stop()'::pg_catalog.regprocedure) as def;

-- Test 1 (C6): a real pgque.stop() failure must abort sql/pgque_uninstall.sql
-- before the schema drop (no silent "when others" swallowing).
create or replace function pgque.stop()
returns void as $$
begin
    raise exception 'simulated stop() failure (test_uninstall_guard)';
end;
$$ language plpgsql;

\set ON_ERROR_STOP off
\i sql/pgque_uninstall.sql
\set ON_ERROR_STOP on

do $$
begin
    assert exists (select 1 from pg_catalog.pg_namespace where nspname = 'pgque'),
        'pgque schema must survive when pgque.stop() raises during uninstall';
    raise notice 'PASS: pgque_uninstall.sql aborts before drop when stop() fails';
end $$;

-- Tests 2 and 3 execute sql/pgque-tle-uninstall.sql, which (correctly)
-- drops the pgque extension -- taking the whole schema with it -- and
-- unregisters pgque from pg_tle. Against an extension install (the pg_tle
-- CI job) that would destroy the install mid-suite, so the script must not
-- run at all there: skip both sub-tests. The extension path of the script
-- is covered by tests/test_tle_install.sql.
select exists (select 1 from pg_catalog.pg_extension where extname = 'pgque') as pgque_is_extension
\gset

\if :pgque_is_extension

\echo 'SKIP: pgque is installed as an extension; TLE uninstall sub-tests need a plain install'

\else

-- Test 2 (C4): sql/pgque-tle-uninstall.sql must call pgque.stop() before
-- drop extension, so pg_cron / pg_timetable jobs do not outlive the schema.
create table pg_temp.tle_stop_called (called bool);

create or replace function pgque.stop()
returns void as $$
begin
    insert into pg_temp.tle_stop_called values (true);
end;
$$ language plpgsql;

\i sql/pgque-tle-uninstall.sql

do $$
begin
    assert exists (select 1 from pg_temp.tle_stop_called),
        'pgque-tle-uninstall.sql must call pgque.stop() before drop extension';
    assert exists (select 1 from pg_catalog.pg_namespace where nspname = 'pgque'),
        'plain (non-extension) install must survive the TLE uninstall script';
    raise notice 'PASS: pgque-tle-uninstall.sql calls stop() before drop extension';
end $$;

-- Test 3 (C4/C6): a real stop() failure must abort the TLE uninstall script
-- before drop extension as well.
create or replace function pgque.stop()
returns void as $$
begin
    raise exception 'simulated stop() failure (test_uninstall_guard)';
end;
$$ language plpgsql;

\set ON_ERROR_STOP off
\i sql/pgque-tle-uninstall.sql
\set ON_ERROR_STOP on

do $$
begin
    assert exists (select 1 from pg_catalog.pg_namespace where nspname = 'pgque'),
        'pgque schema must survive when stop() raises during TLE uninstall';
    raise notice 'PASS: pgque-tle-uninstall.sql aborts before drop when stop() fails';
end $$;

drop table pg_temp.tle_stop_called;

\endif

-- Restore the real pgque.stop() and verify the restoration.
do $$
declare
    v_def text;
begin
    select def into v_def from pg_temp.saved_stop;
    execute v_def;
    assert pg_catalog.pg_get_functiondef('pgque.stop()'::pg_catalog.regprocedure) = v_def,
        'pgque.stop() must be restored to its original definition';
    raise notice 'PASS: original pgque.stop() restored';
end $$;

drop table pg_temp.saved_stop;
