\set ON_ERROR_STOP on

-- test_upgrade_v0_1_assertions.sql -- Verify v0.1.0 state after HEAD reinstall
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Run after tests/test_upgrade_v0_1_fixture.sql and a HEAD sql/pgque.sql reinstall.

do $$
declare
  v_version text;
  v_scheduler text;
  v_tick_period_ms int;
begin
  v_version := pgque.version();
  assert v_version = '0.2.0-rc.1',
    'pgque.version() should report HEAD version, got ' || v_version;

  assert exists (select 1 from pgque.queue where queue_name = 'upgrade_v01_q'),
    'pre-upgrade queue should survive';
  assert exists (select 1 from pgque.consumer where co_name = 'upgrade_v01_c'),
    'pre-upgrade consumer should survive';
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'upgrade_v01_q'
      and c.co_name = 'upgrade_v01_c'
  ), 'pre-upgrade subscription should survive';
  assert exists (select 1 from pgque.retry_queue),
    'pre-upgrade retry row should survive';
  assert exists (select 1 from pgque.dead_letter),
    'pre-upgrade DLQ row should survive';

  select scheduler, tick_period_ms
  into v_scheduler, v_tick_period_ms
  from pgque.config
  where singleton;

  assert v_tick_period_ms = 100,
    'pgque.config.tick_period_ms should default to 100, got ' || coalesce(v_tick_period_ms::text, 'NULL');
  assert v_scheduler is null or v_scheduler in ('pg_cron', 'pg_timetable'),
    'pgque.config.scheduler should be null or known scheduler, got ' || coalesce(v_scheduler, 'NULL');

  assert to_regprocedure('pgque.receive_coop(text,text,text,integer,interval)') is not null,
    'pgque.receive_coop(text,text,text,integer,interval) should exist after upgrade';
  assert to_regprocedure('pgque.force_next_tick(text)') is not null,
    'pgque.force_next_tick(text) should exist after upgrade';
  assert to_regprocedure('pgque.send_batch(text,jsonb[])') is not null,
    'pgque.send_batch(text,jsonb[]) should exist after upgrade';

  perform pgque.send(queue_name := 'upgrade_v01_q', payload := '{"named":"jsonb-default"}'::jsonb);
  perform pgque.send(queue_name := 'upgrade_v01_q', payload := 'named-text-default');
  perform pgque.send(queue_name := 'upgrade_v01_q', type_name := 'named.jsonb', payload := '{"named":"jsonb-explicit"}'::jsonb);
  perform pgque.send(queue_name := 'upgrade_v01_q', type_name := 'named.text', payload := 'named-text-explicit');
  perform pgque.send_batch(queue_name := 'upgrade_v01_q', payloads := array['{"named":"batch-jsonb-default"}'::jsonb]);
  perform pgque.send_batch(queue_name := 'upgrade_v01_q', type_name := 'named.jsonb.batch', payloads := array['{"named":"batch-jsonb-explicit"}'::jsonb]);
  perform pgque.send_batch(queue_name := 'upgrade_v01_q', type_name := 'named.text.batch', payloads := array['named-batch-text']);
  perform pgque.subscribe(queue := 'upgrade_v01_q', consumer := 'upgrade_v01_named_c');
  perform pgque.unsubscribe(queue := 'upgrade_v01_q', consumer := 'upgrade_v01_named_c');

  raise notice 'PASS: upgraded schema, pre-existing state, and named-argument wrappers verified';
end $$;

-- Recreated v0.1.0 wrappers must preserve their pre-upgrade owner.
do $$
declare
  f text;
  v_owner name;
begin
  foreach f in array array[
    'pgque.send(text,jsonb)',
    'pgque.send(text,text)',
    'pgque.send(text,text,jsonb)',
    'pgque.send(text,text,text)',
    'pgque.send_batch(text,text,jsonb[])',
    'pgque.send_batch(text,text,text[])',
    'pgque.subscribe(text,text)',
    'pgque.unsubscribe(text,text)'
  ] loop
    select r.rolname
    into v_owner
    from pg_proc as p
    join pg_roles as r on r.oid = p.proowner
    where p.oid = f::regprocedure::oid;

    assert v_owner = 'pgque_v01_wrapper_owner',
      format('wrapper %s should preserve owner pgque_v01_wrapper_owner, got %s', f, coalesce(v_owner, 'NULL'));
  end loop;

  raise notice 'PASS: recreated v0.1.0 wrapper owners preserved';
end $$;

-- New publishing/consuming still works on the upgraded queue.
do $$
begin
  perform pgque.send(queue_name := 'upgrade_v01_q', type_name := 'post.upgrade', payload := '{"ok":true}'::jsonb);
end $$;

do $$
begin
  perform pgque.force_next_tick('upgrade_v01_q');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('upgrade_v01_q', 'upgrade_v01_c', 100)
  loop
    if v_msg.type = 'post.upgrade' then
      v_count := v_count + 1;
      v_batch_id := v_msg.batch_id;
      assert v_msg.payload::jsonb = '{"ok": true}'::jsonb,
        'post-upgrade payload should round-trip';
    end if;
  end loop;

  assert v_count = 1,
    'post-upgrade receive should return exactly one new message, got ' || v_count;
  assert v_batch_id is not null,
    'post-upgrade receive should expose a batch id';

  perform pgque.ack(v_batch_id);
  raise notice 'PASS: upgraded queue can send, receive, and ack new messages';
end $$;
