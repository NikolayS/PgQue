-- test_pgcron_lifecycle.sql -- Verify pg_current.start() and pg_current.stop() with pg_cron
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- These tests exercise the pg_cron lifecycle integration.
-- Tests auto-skip when pg_cron is not available.

-- Test 1: start() sets job IDs in config and schedules all four jobs
do $$
declare
  v_ticker_id bigint;
  v_maint_id bigint;
  v_job_count int;
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pg_current.start();

  select ticker_job_id, maint_job_id into v_ticker_id, v_maint_id
  from pg_current.config;

  assert v_ticker_id is not null, 'ticker_job_id should be set after start()';
  assert v_maint_id is not null, 'maint_job_id should be set after start()';

  -- All four named jobs should exist in cron.job after start().
  select count(*) into v_job_count
  from cron.job
  where jobname in ('pg_current_ticker', 'pg_current_retry_events', 'pg_current_maint', 'pg_current_rotate_step2');
  assert v_job_count = 4,
    'expected 4 pg_current_* jobs in cron.job, found ' || v_job_count;

  -- Clean up
  perform pg_current.stop();

  raise notice 'PASS: start() schedules four jobs (ticker, retry_events, maint, rotate_step2)';
end $$;

-- Test 1b: stop() unschedules every pg_current_* job (ticker, retry_events, maint, rotate_step2)
do $$
declare
  v_job_count int;
begin
  if not exists (select from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pg_current.start();
  perform pg_current.stop();

  select count(*) into v_job_count
  from cron.job
  where jobname in ('pg_current_ticker', 'pg_current_retry_events', 'pg_current_maint', 'pg_current_rotate_step2');
  assert v_job_count = 0,
    'expected 0 pg_current_* jobs in cron.job after stop(), found ' || v_job_count;

  raise notice 'PASS: stop() unschedules all four pg_current_* jobs';
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

  perform pg_current.start();
  select ticker_job_id into v_ticker_id1 from pg_current.config;

  perform pg_current.start();
  select ticker_job_id into v_ticker_id2 from pg_current.config;

  assert v_ticker_id2 is not null, 'should still have ticker job after second start()';

  -- Clean up
  perform pg_current.stop();

  raise notice 'PASS: start() is idempotent';
end $$;

-- Test 3: stop() clears job IDs
do $$
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed';
    return;
  end if;

  perform pg_current.start();
  perform pg_current.stop();

  assert (select ticker_job_id from pg_current.config) is null,
    'ticker_job_id should be NULL after stop()';
  assert (select maint_job_id from pg_current.config) is null,
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

  perform pg_current.start();
  perform pg_current.stop();
  perform pg_current.stop();

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
    perform pg_current.start();
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
  perform pg_current.stop();

  assert (select ticker_job_id from pg_current.config) is null,
    'ticker_job_id should be NULL after stop() without pg_cron';
  assert (select maint_job_id from pg_current.config) is null,
    'maint_job_id should be NULL after stop() without pg_cron';

  raise notice 'PASS: stop() is graceful no-op without pg_cron';
end $$;
