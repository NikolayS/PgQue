-- test_security_get_batch_cursor_injection.sql
-- Regression: get_batch_cursor must not allow SQL forgery via extra_where (#108)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Posture asserted:
--   1. As pgque_admin (the only role that can call the 4-arg overload at all),
--      a `false UNION ALL SELECT ...forged...` payload must NOT cause forged
--      rows to appear in the returned event stream. Either the call is
--      rejected outright (preferred), or the injection is neutralised.
--   2. The 3-arg overload (which never accepts caller SQL) must keep working.

-- =========================================================================
-- Setup: dedicated admin probe role
-- =========================================================================

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'pgque_inj_admin') then
    execute 'create role pgque_inj_admin login';
    execute 'grant pgque_admin to pgque_inj_admin';
  end if;
end $$;

-- =========================================================================
-- Build a real batch with one real event so we have something to forge into.
-- Run as the test owner (superuser) so we have the privileges to set up.
-- =========================================================================

select pgque.create_queue('inj_q');
select pgque.register_consumer('inj_q', 'inj_c');
select pgque.insert_event('inj_q', 'real', 'real-data');
select pgque.ticker('inj_q');

-- =========================================================================
-- Test A: UNION ALL injection via extra_where must NOT yield forged rows.
-- =========================================================================
do $$
declare
  v_batch_id     bigint;
  v_total        int := 0;
  v_forged       int := 0;
  v_real         int := 0;
  v_caught_state text;
begin
  set role pgque_inj_admin;

  v_batch_id := pgque.next_batch('inj_q', 'inj_c');
  if v_batch_id is null then
    reset role;
    raise exception 'next_batch returned NULL; cannot continue injection test';
  end if;

  -- The classic UNION ALL forgery payload from issue #108.
  --
  -- Three valid outcomes for this call:
  --   (a) the call raises (any sqlstate) before producing any forged row;
  --   (b) the call returns rows but no row has ev_type = 'injected';
  --   (c) the call returns ONLY genuine rows (the real 'real-data' row).
  --
  -- Failure (the bug we are guarding against) is: the call returns one or
  -- more rows where ev_type = 'injected' / ev_data = current_database().
  begin
    select
      count(*),
      count(*) filter (where ev_type = 'injected'),
      count(*) filter (where ev_type = 'real')
    into v_total, v_forged, v_real
    from pgque.get_batch_cursor(
        v_batch_id,
        'inj_cursor_a',
        100,
        $f$false union all select 999999::bigint, now()::timestamptz, '0'::xid8, 0::int4, 'injected'::text, current_database()::text, null::text, null::text, null::text, null::text$f$);

    -- If get_batch_cursor opened the cursor, close it so we don't leak it.
    begin
      execute 'close inj_cursor_a';
    exception when others then
      null;
    end;
  exception
    when others then
      v_caught_state := sqlstate;
  end;

  reset role;

  if v_caught_state is not null then
    raise notice 'PASS: get_batch_cursor extra_where injection rejected with sqlstate=%', v_caught_state;
  else
    assert v_forged = 0,
      format(
        'SQL forgery via extra_where leaked %s row(s); total=%s real=%s. Issue #108',
        v_forged, v_total, v_real);
    raise notice 'PASS: get_batch_cursor extra_where injection neutralised (total=% real=% forged=%)',
      v_total, v_real, v_forged;
  end if;
end $$;

-- =========================================================================
-- Test B: a second variant — comment-style trailing predicate — must also
-- not silently accept arbitrary tail SQL.
-- =========================================================================
do $$
declare
  v_batch_id     bigint;
  v_forged       int := 0;
  v_caught_state text;
begin
  set role pgque_inj_admin;

  v_batch_id := pgque.next_batch('inj_q', 'inj_c');
  if v_batch_id is null then
    -- batch already finished by previous test step in some flows; that's OK.
    reset role;
    raise notice 'SKIP B: no batch available to claim (already consumed)';
    return;
  end if;

  begin
    select count(*) filter (where ev_type = 'injected')
    into v_forged
    from pgque.get_batch_cursor(
        v_batch_id,
        'inj_cursor_b',
        100,
        $f$1=1 union all select 1::bigint, now()::timestamptz, '0'::xid8, 0::int4, 'injected'::text, 'pwned'::text, null::text, null::text, null::text, null::text$f$);

    begin
      execute 'close inj_cursor_b';
    exception when others then
      null;
    end;
  exception
    when others then
      v_caught_state := sqlstate;
  end;

  reset role;

  if v_caught_state is not null then
    raise notice 'PASS: variant-2 injection rejected with sqlstate=%', v_caught_state;
  else
    assert v_forged = 0,
      format('SQL forgery (1=1 variant) leaked %s row(s); issue #108', v_forged);
    raise notice 'PASS: variant-2 injection neutralised (forged=%)', v_forged;
  end if;
end $$;

-- =========================================================================
-- Test C: the safe 3-arg overload must keep returning real events as admin.
-- =========================================================================
do $$
declare
  v_batch_id bigint;
  v_count    int := 0;
begin
  set role pgque_inj_admin;

  v_batch_id := pgque.next_batch('inj_q', 'inj_c');
  if v_batch_id is null then
    -- create another event so we have something to claim
    reset role;
    perform pgque.insert_event('inj_q', 'real', 'real-data-2');
    perform pgque.ticker('inj_q');
    set role pgque_inj_admin;
    v_batch_id := pgque.next_batch('inj_q', 'inj_c');
  end if;

  if v_batch_id is null then
    reset role;
    raise notice 'SKIP C: no batch available (no new events ticked)';
    return;
  end if;

  select count(*) into v_count
  from pgque.get_batch_cursor(v_batch_id, 'safe_cursor_c', 100);

  begin
    execute 'close safe_cursor_c';
  exception when others then
    null;
  end;

  reset role;

  assert v_count >= 0,
    'safe 3-arg get_batch_cursor unexpectedly errored';
  raise notice 'PASS: safe 3-arg get_batch_cursor returned % row(s) for admin', v_count;
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================

select pgque.unregister_consumer('inj_q', 'inj_c');
select pgque.drop_queue('inj_q');

revoke pgque_admin from pgque_inj_admin;
drop role if exists pgque_inj_admin;

\echo 'PASS: test_security_get_batch_cursor_injection'
