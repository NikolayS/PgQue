-- test_security_get_batch_cursor.sql -- Regression: get_batch_cursor restricted to pgque_admin
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Issue #108: pgque.get_batch_cursor(batch_id, cursor_name, quick_limit, extra_where)
-- concatenates `i_extra_where` directly into dynamic SQL. A caller can inject
-- predicate fragments (e.g. `false UNION ALL SELECT ...`) and forge rows
-- returned to the application.
--
-- The PgQ engine body is sacred (SPECx Key Design Rule #2). The chosen fix is
-- a boundary-level lockdown: keep the function pgque_admin-only (trusted SQL)
-- rather than parsing/sanitizing the predicate. This test asserts the grant
-- posture and documents the threat model.
--
-- Posture asserted:
--   1. pgque_reader cannot call either get_batch_cursor overload.
--   2. pgque_writer cannot call either get_batch_cursor overload.
--   3. pgque_admin (or members) can call both overloads.
--   4. UNION-ALL forgery still works *inside* admin context — we are not
--      sanitizing extra_where; we are restricting *who* can pass it.

-- =========================================================================
-- Setup: probe roles
-- =========================================================================

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'pgque_test_reader') then
    execute 'create role pgque_test_reader login';
    execute 'grant pgque_reader to pgque_test_reader';
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pgque_test_writer') then
    execute 'create role pgque_test_writer login';
    execute 'grant pgque_writer to pgque_test_writer';
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pgque_test_admin') then
    execute 'create role pgque_test_admin login';
    execute 'grant pgque_admin to pgque_test_admin';
  end if;
end $$;

-- =========================================================================
-- Test A: pgque_reader is blocked from get_batch_cursor (both overloads)
-- =========================================================================

do $$
declare
  v_sqlstate text;
begin
  set role pgque_test_reader;
  begin
    perform pgque.get_batch_cursor(1::bigint, 'probe_cursor_r3', 0);
    raise exception 'expected insufficient_privilege calling get_batch_cursor/3 as pgque_reader, got success';
  exception
    when insufficient_privilege then
      v_sqlstate := sqlstate;
  end;
  reset role;

  assert v_sqlstate = '42501',
    'expected sqlstate 42501 (insufficient_privilege) for reader/3, got ' || coalesce(v_sqlstate, 'NULL');

  raise notice 'PASS: security_get_batch_cursor/A1 - pgque_reader blocked from get_batch_cursor/3';
end $$;

do $$
declare
  v_sqlstate text;
begin
  set role pgque_test_reader;
  begin
    perform pgque.get_batch_cursor(1::bigint, 'probe_cursor_r4', 0, 'true');
    raise exception 'expected insufficient_privilege calling get_batch_cursor/4 as pgque_reader, got success';
  exception
    when insufficient_privilege then
      v_sqlstate := sqlstate;
  end;
  reset role;

  assert v_sqlstate = '42501',
    'expected sqlstate 42501 (insufficient_privilege) for reader/4, got ' || coalesce(v_sqlstate, 'NULL');

  raise notice 'PASS: security_get_batch_cursor/A2 - pgque_reader blocked from get_batch_cursor/4 (extra_where overload)';
end $$;

-- =========================================================================
-- Test B: pgque_writer is blocked from get_batch_cursor (both overloads)
-- =========================================================================

do $$
declare
  v_sqlstate text;
begin
  set role pgque_test_writer;
  begin
    perform pgque.get_batch_cursor(1::bigint, 'probe_cursor_w3', 0);
    raise exception 'expected insufficient_privilege calling get_batch_cursor/3 as pgque_writer, got success';
  exception
    when insufficient_privilege then
      v_sqlstate := sqlstate;
  end;
  reset role;

  assert v_sqlstate = '42501',
    'expected sqlstate 42501 (insufficient_privilege) for writer/3, got ' || coalesce(v_sqlstate, 'NULL');

  raise notice 'PASS: security_get_batch_cursor/B1 - pgque_writer blocked from get_batch_cursor/3';
end $$;

do $$
declare
  v_sqlstate text;
begin
  set role pgque_test_writer;
  begin
    perform pgque.get_batch_cursor(1::bigint, 'probe_cursor_w4', 0, 'true');
    raise exception 'expected insufficient_privilege calling get_batch_cursor/4 as pgque_writer, got success';
  exception
    when insufficient_privilege then
      v_sqlstate := sqlstate;
  end;
  reset role;

  assert v_sqlstate = '42501',
    'expected sqlstate 42501 (insufficient_privilege) for writer/4, got ' || coalesce(v_sqlstate, 'NULL');

  raise notice 'PASS: security_get_batch_cursor/B2 - pgque_writer blocked from get_batch_cursor/4 (extra_where overload)';
end $$;

-- =========================================================================
-- Test C: pgque_admin can still call get_batch_cursor (positive path)
-- =========================================================================
-- Build a queue + consumer + batch so we have a real batch id to point at.
-- This must run as superuser/owner so we have permission to set it up.

select pgque.create_queue('issue108_q');
select pgque.register_consumer('issue108_q', 'issue108_c');
select pgque.insert_event('issue108_q', 'real', 'real-data');
select pgque.ticker('issue108_q');

do $$
declare
  v_batch_id bigint;
  v_count_admin int := 0;
  v_forged_seen int := 0;
begin
  -- Switch to admin member to claim the batch.
  set role pgque_test_admin;

  v_batch_id := pgque.next_batch('issue108_q', 'issue108_c');

  if v_batch_id is null then
    reset role;
    raise exception 'next_batch returned NULL; cannot continue test C';
  end if;

  -- 3-arg overload should work for admin. The function returns the first
  -- quick_limit rows directly as setof record, so we can just count.
  select count(*) into v_count_admin
    from pgque.get_batch_cursor(v_batch_id, 'issue108_admin_c3', 100);

  -- Close the cursor we just opened so the name is reusable in this xact.
  execute 'close issue108_admin_c3';

  assert v_count_admin >= 1,
    'pgque_admin/3 returned no rows from batch ' || v_batch_id::text;

  raise notice 'PASS: security_get_batch_cursor/C1 - pgque_admin can call get_batch_cursor/3 (rows=%)', v_count_admin;

  -- 4-arg overload (the dangerous one): admin can call it.
  -- Demonstrate the threat model: UNION ALL forgery still produces forged
  -- rows *under admin* — we are NOT sanitizing predicates; we are revoking
  -- access so untrusted callers cannot reach this entry point at all.
  begin
    select count(*) filter (where ev_type = 'forged-by-admin')
      into v_forged_seen
      from pgque.get_batch_cursor(
        v_batch_id,
        'issue108_admin_c4',
        100,
        $extra$false UNION ALL
          SELECT 999999::bigint, now(), '0'::xid8, 0::int4,
                 'forged-by-admin'::text, current_database()::text,
                 NULL::text, NULL::text, NULL::text, NULL::text$extra$);
    execute 'close issue108_admin_c4';
  exception when others then
    -- Probe is informational; only the reader/writer revoke is the hard fix.
    raise notice 'NOTICE: union-all probe failed under admin (column shape may have changed): %', sqlerrm;
    v_forged_seen := -1;
  end;

  reset role;

  if v_forged_seen > 0 then
    raise notice 'THREAT-MODEL: union-all forgery is reachable inside pgque_admin (forged_rows=%) — extra_where is RAW SQL; admin must treat this entry point as trusted-SQL only', v_forged_seen;
  elsif v_forged_seen = 0 then
    raise notice 'THREAT-MODEL: union-all probe under admin returned 0 forged rows — fix relies on grant revoke, not predicate sanitization';
  else
    raise notice 'THREAT-MODEL: union-all probe under admin did not execute (column shape mismatch). Fix relies on grant revoke at the boundary.';
  end if;
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================

select pgque.unregister_consumer('issue108_q', 'issue108_c');
select pgque.drop_queue('issue108_q');

revoke pgque_reader from pgque_test_reader;
revoke pgque_writer from pgque_test_writer;
revoke pgque_admin  from pgque_test_admin;
drop role if exists pgque_test_reader;
drop role if exists pgque_test_writer;
drop role if exists pgque_test_admin;

\echo 'PASS: test_security_get_batch_cursor'
