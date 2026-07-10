\set ON_ERROR_STOP on

-- Verify a v0.2.0 ordinary consumer containing # remains usable after upgrade.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

set role pgque_reader;

do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in
    select *
    from pgque.receive('upgrade_v02_hash_q', 'team#1', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert v_msg.type = 'upgrade.pending',
      'legacy # consumer should receive the pending pre-upgrade event';
    assert v_msg.payload::jsonb = '{"phase":"before-upgrade"}'::jsonb,
      'pending pre-upgrade payload should survive';
  end loop;

  assert v_count = 1,
    format('legacy # consumer should receive one pending event, got %s', v_count);
  assert pgque.ack(v_batch_id) = 1,
    'plain ack should remain usable for a legacy # consumer';
end $$;

reset role;

do $$
declare
  v_raised boolean := false;
begin
  begin
    perform pgque.subscribe_slot('upgrade_v02_hash_q', 'workers', 0, 2);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%already registered as an ordinary consumer%',
      format('slot-name collision should identify the ordinary consumer, got: %s', sqlerrm);
  end;

  assert v_raised,
    'an ordinary consumer matching a generated slot name must not be reclassified';
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'upgrade_v02_hash_q'
      and c.co_name = 'workers#0/2'
  ), 'slot-name collision must preserve the ordinary subscription';
  assert not exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'upgrade_v02_hash_q'
      and pc.co_name = 'workers'
  ), 'slot-name collision must roll back the partial partition consumer';
  assert not exists (
    select 1
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'upgrade_v02_hash_q'
      and ps.co_name = 'workers'
      and ps.slot = 0
  ), 'slot-name collision must not materialize a partition slot';

  perform pgque.subscribe_slot('upgrade_v02_hash_q', 'materialized', 0, 2);
  perform pgque.subscribe_slot('upgrade_v02_hash_q', 'materialized', 0, 2);
  assert (
    select count(*) = 1
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'upgrade_v02_hash_q'
      and ps.co_name = 'materialized'
      and ps.slot = 0
  ), 're-subscribing a materialized slot must remain idempotent';
  assert (
    select count(*) = 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'upgrade_v02_hash_q'
      and c.co_name = 'materialized#0/2'
  ), 'idempotent slot re-subscribe must keep one engine subscription';
  perform pgque.unsubscribe_slot('upgrade_v02_hash_q', 'materialized', 0);

  raise notice 'PASS: exact generated slot-name collision remains ordinary';
end $$;

do $$
begin
  perform pgque.send(
    'upgrade_v02_hash_q',
    'upgrade.nack',
    '{"phase":"after-upgrade"}'::jsonb
  );
end $$;

do $$
begin
  perform pgque.force_next_tick('upgrade_v02_hash_q');
  perform pgque.ticker();
end $$;

set role pgque_reader;

do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in
    select *
    from pgque.receive('upgrade_v02_hash_q', 'team#1', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert pgque.nack(v_msg.batch_id, v_msg, interval '0 seconds', 'upgrade check') = 1,
      'plain nack should remain usable for a legacy # consumer';
  end loop;

  assert v_count = 1,
    format('legacy # consumer should receive one post-upgrade event, got %s', v_count);
  assert pgque.ack(v_batch_id) = 1,
    'batch containing the nacked event should be finishable';
  assert exists (
    select 1
    from pgque.dead_letter as dl
    join pgque.queue as q on q.queue_id = dl.dl_queue_id
    join pgque.consumer as c on c.co_id = dl.dl_consumer_id
    where q.queue_name = 'upgrade_v02_hash_q'
      and c.co_name = 'team#1'
      and dl.ev_type = 'upgrade.nack'
  ), 'legacy # consumer nack should create a dead-letter row';
end $$;

do $$
declare
  v_raised boolean := false;
begin
  begin
    perform pgque.subscribe('upgrade_v02_hash_q', 'new#plain');
  exception when others then
    v_raised := true;
    assert sqlerrm like '%reserved character #%',
      format('new # consumer rejection should name the reserved character, got: %s', sqlerrm);
  end;

  assert v_raised, 'new plain consumers containing # should remain rejected';
  assert not exists (
    select 1
    from pgque.consumer
    where co_name = 'new#plain'
  ), 'rejected new # consumer should leave no catalog row';

  raise notice 'PASS: legacy # consumer receive/ack/nack survived v0.2.0 upgrade';
end $$;

reset role;
