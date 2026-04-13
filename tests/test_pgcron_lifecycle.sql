-- test_pgcron_lifecycle.sql -- Verify pgque.start() and pgque.stop() with pg_cron
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- These tests exercise the pg_cron lifecycle integration.
-- Tests auto-skip when pg_cron is not available.

-- Test 1: start() sets job IDs in config
do $$
declare
  v_ticker_id bigint;
  v_maint_id bigint;
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pgque.start();

  select ticker_job_id, maint_job_id into v_ticker_id, v_maint_id
  from pgque.config;

  assert v_ticker_id is not null, 'ticker_job_id should be set after start()';
  assert v_maint_id is not null, 'maint_job_id should be set after start()';

  -- Clean up
  perform pgque.stop();

  raise notice 'PASS: start() sets job IDs (ticker=%, maint=%)', v_ticker_id, v_maint_id;
end $$;

-- Test 2: start() is idempotent
do $$
declare
  v_ticker_id1 bigint;
  v_ticker_id2 bigint;
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pgque.start();
  select ticker_job_id into v_ticker_id1 from pgque.config;

  perform pgque.start();
  select ticker_job_id into v_ticker_id2 from pgque.config;

  assert v_ticker_id2 is not null, 'should still have ticker job after second start()';

  -- Clean up
  perform pgque.stop();

  raise notice 'PASS: start() is idempotent';
end $$;

-- Test 3: stop() clears job IDs
do $$
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pgque.start();
  perform pgque.stop();

  assert (select ticker_job_id from pgque.config) is null,
    'ticker_job_id should be NULL after stop()';
  assert (select maint_job_id from pgque.config) is null,
    'maint_job_id should be NULL after stop()';

  raise notice 'PASS: stop() clears job IDs';
end $$;

-- Test 4: stop() is idempotent (calling twice does not error)
do $$
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pgque.start();
  perform pgque.stop();
  perform pgque.stop();

  raise notice 'PASS: stop() is idempotent';
end $$;

-- Test 5: Without pg_cron, start() raises informative error
do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron IS installed (cannot test without-pgcron path)';
    return;
  end if;

  begin
    perform pgque.start();
    assert false, 'start() should raise error without pg_cron';
  exception when raise_exception then
    assert sqlerrm like '%pg_cron%',
      'error should mention pg_cron, got: ' || sqlerrm;
    raise notice 'PASS: start() raises informative error without pg_cron';
  end;
end $$;

-- Test 6: Without pg_cron, stop() does not error (graceful no-op)
do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron IS installed (cannot test without-pgcron path)';
    return;
  end if;

  -- Should not raise: just clears any stale job IDs
  perform pgque.stop();

  assert (select ticker_job_id from pgque.config) is null,
    'ticker_job_id should be NULL after stop() without pg_cron';
  assert (select maint_job_id from pgque.config) is null,
    'maint_job_id should be NULL after stop() without pg_cron';

  raise notice 'PASS: stop() is graceful no-op without pg_cron';
end $$;
