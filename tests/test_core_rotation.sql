-- test_core_rotation.sql -- Table rotation advances queue_cur_table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Verifies that maint_rotate_tables_step1() and step2() work correctly
-- when called in SEPARATE transactions (the PgQ design requirement).
--
-- Each DO block is a separate transaction. Step1 and step2 are in
-- different blocks to simulate the pg_cron scheduling pattern.

-- Setup
do $$
begin
  perform pgque.create_queue('test_rotation');
  perform pgque.set_queue_config('test_rotation', 'rotation_period', '1 second');
  perform pgque.ticker('test_rotation');
  perform pgque.insert_event('test_rotation', 'test', 'data');
  perform pgque.ticker('test_rotation');
  raise notice 'PASS: rotation test setup complete';
end;
$$;

-- Wait for rotation period (must be outside a TX for timestamps to advance)
select pg_sleep(2);

-- Rotation 1: step1
do $$
declare
  v_result integer;
begin
  select pgque.maint_rotate_tables_step1('test_rotation') into v_result;
  raise notice 'step1 returned %, queue_cur_table should now be 1', v_result;
end;
$$;

-- Rotation 1: step2 (SEPARATE TX — this is the critical design requirement)
do $$
declare
  v_cur integer;
begin
  perform pgque.maint_rotate_tables_step2();

  select queue_cur_table into v_cur
  from pgque.queue where queue_name = 'test_rotation';

  if v_cur != 1 then
    raise exception 'FAIL: expected queue_cur_table = 1, got %', v_cur;
  end if;
  raise notice 'PASS: rotation 1 complete (cur_table = %)', v_cur;
end;
$$;

-- Wait again
select pg_sleep(2);

-- Rotation 2: step1
do $$ begin perform pgque.maint_rotate_tables_step1('test_rotation'); end; $$;

-- Rotation 2: step2
do $$
declare v_cur integer;
begin
  perform pgque.maint_rotate_tables_step2();
  select queue_cur_table into v_cur from pgque.queue where queue_name = 'test_rotation';
  if v_cur != 2 then raise exception 'FAIL: expected 2, got %', v_cur; end if;
  raise notice 'PASS: rotation 2 complete (cur_table = %)', v_cur;
end;
$$;

-- Wait again
select pg_sleep(2);

-- Rotation 3: step1 (wraps 2 → 0)
do $$ begin perform pgque.maint_rotate_tables_step1('test_rotation'); end; $$;

-- Rotation 3: step2
do $$
declare v_cur integer;
begin
  perform pgque.maint_rotate_tables_step2();
  select queue_cur_table into v_cur from pgque.queue where queue_name = 'test_rotation';
  if v_cur != 0 then raise exception 'FAIL: expected 0 (wrap), got %', v_cur; end if;
  raise notice 'PASS: rotation 3 complete (cur_table = %, wrapped 2→0)', v_cur;
end;
$$;

-- Cleanup
do $$
begin
  perform pgque.drop_queue('test_rotation');
  raise notice 'PASS: all rotation tests passed (0→1→2→0)';
end;
$$;
