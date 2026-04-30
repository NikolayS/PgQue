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
-- present in maint_operations().  We verify this by running maint() while
-- the vacuum override is still active (so maint_operations() returns both
-- vacuum rows and real work rows), then checking that at least one real
-- maintenance operation was counted (returned integer > 0 is sufficient when
-- a queue exists, since rotation step1 always counts as at least 1 op).
-- -------------------------------------------------------------------------
do $$
declare
  v_result integer;
begin
  -- Create a test queue to ensure maint_operations() returns real work rows.
  perform pgque.create_queue('maint_vacuum_mixtest');
  -- Force a tick so rotation step1 has something to do.
  perform pgque.force_tick('maint_vacuum_mixtest');

  v_result := pgque.maint();

  -- maint() must not error (covered by prior test) AND must have executed at
  -- least one non-VACUUM operation.
  assert v_result > 0,
    'mixed-case FAIL: maint() returned 0 — non-VACUUM ops were not executed '
    || '(got ' || v_result::text || ')';

  raise notice 'PASS: maint_vacuum_mixed - maint() executed non-VACUUM ops (returned %)', v_result;

  -- Cleanup test queue.
  perform pgque.drop_queue('maint_vacuum_mixtest');
end $$;

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
