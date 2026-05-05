\set ON_ERROR_STOP on

-- US-8: Install on managed PostgreSQL (simulated)
-- As an operator, I want pgque to install and run correctly on a managed
-- PostgreSQL service (no superuser, no CREATE EXTENSION).
-- This test verifies the core workflow without pg_cron.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Verify: pgque.status() returns rows
do $$
declare
  v_row record;
  v_count int := 0;
  v_found_pg bool := false;
  v_found_pgque bool := false;
  v_found_queues bool := false;
begin
  for v_row in select * from pgque.status()
  loop
    v_count := v_count + 1;
    if v_row.component = 'postgresql' then
      v_found_pg := true;
      assert v_row.status = 'info', 'postgresql status should be info';
      assert v_row.detail is not null, 'postgresql detail should not be null';
    end if;
    if v_row.component = 'pgque' then
      v_found_pgque := true;
      assert v_row.status = 'info', 'pgque status should be info';
      assert v_row.detail like '%dev%' or v_row.detail like '%0%',
        'pgque version should contain dev or version number';
    end if;
    if v_row.component = 'queues' then
      v_found_queues := true;
    end if;
  end loop;

  assert v_count >= 3, 'status() should return at least 3 rows, got ' || v_count;
  assert v_found_pg, 'status() should include postgresql component';
  assert v_found_pgque, 'status() should include pgque component';
  assert v_found_queues, 'status() should include queues component';
  raise notice 'PASS: US-8 pgque.status() returns correct components';
end $$;

-- Verify: full create + send + tick + receive + ack cycle
do $$ begin
  perform pgque.create_queue('us8_managed');
  perform pgque.subscribe('us8_managed', 'app');
end $$;

do $$ begin
  perform pgque.send('us8_managed', 'test.event',
    '{"source":"managed_pg"}'::jsonb);
end $$;

do $$ begin
  perform pgque.prime_tick('us8_managed');
  perform pgque.ticker();
  perform pgque.prime_tick('us8_managed');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from pgque.receive('us8_managed', 'app', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert v_msg.type = 'test.event',
      'event type should match, got ' || coalesce(v_msg.type, 'NULL');
    assert v_msg.payload::jsonb = '{"source":"managed_pg"}'::jsonb,
      'payload should match, got ' || coalesce(v_msg.payload, 'NULL');
  end loop;

  assert v_count = 1, 'should receive exactly 1 event, got ' || v_count;
  perform pgque.ack(v_batch_id);
  raise notice 'PASS: US-8 full send/tick/receive/ack cycle works';
end $$;

-- Verify: pgque.stop() does not error (even without pg_cron)
-- On managed PG without pg_cron, stop() should handle gracefully
do $$
begin
  -- stop() clears job IDs from config; without pg_cron it skips unschedule
  perform pgque.stop();
  raise notice 'PASS: US-8 pgque.stop() completed without error';
end $$;

-- Verify: version() returns a valid string
do $$
declare
  v_ver text;
begin
  v_ver := pgque.version();
  assert v_ver is not null and length(v_ver) > 0,
    'version() should return non-empty string, got ' || coalesce(v_ver, 'NULL');
  raise notice 'PASS: US-8 pgque.version() = %', v_ver;
end $$;

-- Teardown
do $$ begin
  perform pgque.unsubscribe('us8_managed', 'app');
  perform pgque.drop_queue('us8_managed');
end $$;

\echo 'US-8: PASSED'
