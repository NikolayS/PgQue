-- test_maint_vacuum.sql -- Regression: maint() must not execute VACUUM inside PL/pgSQL
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Issue #110: pgque.maint() had an elsif branch that called EXECUTE 'vacuum ...',
-- which PostgreSQL forbids inside any function (or transaction block).
-- The branch fires whenever maint_tables_to_vacuum() returns rows, normally
-- when autovacuum = off; here we force it by temporarily overriding that function.
--
-- Fix (Option A): drop the vacuum branch from maint(); rely on autovacuum or a
-- separate pg_cron-scheduled VACUUM job for metadata-table bloat control.
--
-- Red until fix: maint() errors when the vacuum path is triggered.

-- -------------------------------------------------------------------------
-- Setup: override maint_tables_to_vacuum() to always return pgque.queue,
-- bypassing the autovacuum=off guard.  This deterministically triggers the
-- vacuum branch in maint_operations() / maint() without any server restart.
-- -------------------------------------------------------------------------
create or replace function pgque.maint_tables_to_vacuum()
returns setof text as $$
begin
    -- Always return pgque.queue so maint_operations() emits a vacuum row.
    return next 'pgque.queue';
end;
$$ language plpgsql;

-- Verify that maint_operations() now emits at least one vacuum row.
do $$
declare
  v_cnt int;
begin
  select count(*) into v_cnt
  from pgque.maint_operations()
  where func_name = 'vacuum';

  assert v_cnt > 0,
    'test precondition failed: maint_operations() returned no vacuum rows '
    || '(got ' || v_cnt::text || ' rows)';

  raise notice 'precondition OK: maint_operations() returned % vacuum row(s)', v_cnt;
end $$;

-- -------------------------------------------------------------------------
-- Core assertion: maint() must complete without error.
-- Before the fix this raises:
--   ERROR: VACUUM cannot run inside a transaction block
--   CONTEXT: SQL statement "vacuum pgque.queue"
--   PL/pgSQL function maint() ...
-- After the fix (Option A) it silently skips vacuum rows and returns normally.
-- -------------------------------------------------------------------------
do $$
declare
  v_result integer;
begin
  v_result := pgque.maint();
  raise notice 'PASS: maint_vacuum - maint() completed without error (returned %)', v_result;
end $$;

-- -------------------------------------------------------------------------
-- Mixed-case assertion: non-VACUUM ops still execute when VACUUM rows are
-- present in maint_operations().  We verify this by registering a
-- queue_extra_maint hook that inserts a row into a sentinel table and returns
-- 1, then asserting both v_result >= 1 (hook return accumulated) AND that the
-- sentinel table received a row (hook actually ran).
-- -------------------------------------------------------------------------

-- Sentinel table: each call to pgque_test_extra_maint() appends a row here.
create table if not exists pgque_test_sentinel (
    called_at timestamptz default clock_timestamp()
);

-- Sentinel function: bumps the sentinel and returns 1 so maint() accumulates it.
-- Owned by the current role (install owner) so any ownership check passes.
-- Table reference is schema-qualified so it resolves under maint()'s search_path.
-- Note: language plpgsql so 'return' syntax is valid; language sql would need
-- a bare 'select 1' as its final statement (no 'return' keyword).
create or replace function pgque_test_extra_maint(i_queue text)
returns int4 as $$
begin
  insert into public.pgque_test_sentinel default values;
  return 1;
end;
$$ language plpgsql;

do $$
declare
  v_result integer;
  v_sentinel_count integer;
begin
  -- Create a test queue and register the sentinel hook.
  perform pgque.create_queue('maint_vacuum_mixtest');

  update pgque.queue
     set queue_extra_maint = array['public.pgque_test_extra_maint']
   where queue_name = 'maint_vacuum_mixtest';

  -- Run maint() while the vacuum override is still active (so maint_operations()
  -- emits both vacuum rows and the extra_maint hook row).
  v_result := pgque.maint();

  -- Assert 1: the extra_maint hook returned 1, so total must be >= 1.
  assert v_result >= 1,
    format('mixed-case FAIL: expected v_result >= 1, got %', v_result);

  -- Assert 2: sentinel table must have at least one row (hook actually ran).
  select count(*) into v_sentinel_count from public.pgque_test_sentinel;
  assert v_sentinel_count >= 1,
    format('mixed-case FAIL: sentinel not bumped, count = %', v_sentinel_count);

  raise notice 'PASS: maint_vacuum_mixed - maint() ran extra_maint hook (v_result=%, sentinel=%)',
    v_result, v_sentinel_count;

  -- Cleanup test queue.
  perform pgque.drop_queue('maint_vacuum_mixtest');
end $$;

-- Cleanup sentinel objects.
drop function if exists pgque_test_extra_maint(text);
drop table if exists pgque_test_sentinel;

-- -------------------------------------------------------------------------
-- Cleanup: restore original maint_tables_to_vacuum() from pgque.sql source.
-- -------------------------------------------------------------------------
create or replace function pgque.maint_tables_to_vacuum()
returns setof text as $$
declare
    scm text;
    tbl text;
    fqname text;
begin
    -- assume autovacuum handles them fine
    if current_setting('autovacuum') = 'on' then
        return;
    end if;

    for scm, tbl in values
        ('pgque', 'subscription'),
        ('pgque', 'consumer'),
        ('pgque', 'queue'),
        ('pgque', 'tick'),
        ('pgque', 'retry_queue'),
        ('pgq_ext', 'completed_tick'),
        ('pgq_ext', 'completed_batch'),
        ('pgq_ext', 'completed_event'),
        ('pgq_ext', 'partial_batch'),
        ('pgq_node', 'local_state'),
        ('londiste', 'seq_info'),
        ('txid', 'epoch'),
        ('londiste', 'completed')
    loop
        select n.nspname || '.' || t.relname into fqname
            from pg_class t, pg_namespace n
            where n.oid = t.relnamespace
                and n.nspname = scm
                and t.relname = tbl;
        if found then
            return next fqname;
        end if;
    end loop;
    return;
end;
$$ language plpgsql;
