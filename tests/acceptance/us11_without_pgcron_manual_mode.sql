\set ON_ERROR_STOP on

-- US-11: Manual mode without pg_cron
-- On a fresh database without pg_cron, PgQue should install and basic
-- manual operation should work: create queue, send, ticker(), maint(), receive.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Guard: this acceptance test is specifically for environments without pg_cron.
do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron is installed in this database';
    return;
  end if;
end $$;

-- start() should fail with an informative message, because it schedules cron jobs.
do $$
begin
  begin
    perform pgque.start();
    assert false, 'pgque.start() should fail without pg_cron';
  exception when raise_exception then
    assert sqlerrm like '%PgQue itself works without pg_cron%',
      'unexpected error: ' || sqlerrm;
  end;
  raise notice 'PASS: start() explains manual mode without pg_cron';
end $$;

-- Manual mode should still work.
-- Separate transactions matter here because PgQ batching depends on snapshots.
do $$
begin
  perform pgque.create_queue('us11_manual');
  perform pgque.subscribe('us11_manual', 'worker');
end $$;

do $$
begin
  perform pgque.send('us11_manual', 'demo.event', '{"ok":true}'::jsonb);
end $$;

do $$
begin
  perform pgque.force_next_tick('us11_manual');
  perform pgque.ticker();
  perform pgque.maint();
end $$;

do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from pgque.receive('us11_manual', 'worker', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert v_msg.type = 'demo.event',
      'unexpected event type: ' || coalesce(v_msg.type, 'NULL');
    assert v_msg.payload::jsonb = '{"ok": true}'::jsonb,
      'unexpected payload: ' || coalesce(v_msg.payload, 'NULL');
  end loop;

  assert v_count = 1, 'expected 1 message, got ' || v_count;
  perform pgque.ack(v_batch_id);
  raise notice 'PASS: manual send/ticker/maint/receive/ack works without pg_cron';
end $$;

-- status should mention manual scheduling path.
do $$
declare
  v_found bool := false;
begin
  select true into v_found
  from pgque.status()
  where component = 'pg_cron'
    and status = 'unavailable'
    and detail like '%pgque.ticker()%pgque.maint()%';

  assert coalesce(v_found, false), 'status() should describe manual scheduler mode';
  raise notice 'PASS: status() documents manual scheduler mode';
end $$;

-- Teardown.
do $$
begin
  perform pgque.unsubscribe('us11_manual', 'worker');
  perform pgque.drop_queue('us11_manual');
end $$;

\echo 'US-11: PASSED'
