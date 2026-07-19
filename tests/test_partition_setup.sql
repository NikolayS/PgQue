\set ON_ERROR_STOP on

-- Regression coverage for atomic partitioned-consumer setup.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- A partial alpha-style setup must be visible as incomplete, not caught up.
do $$
declare
  v_key text;
begin
  perform pgque.create_queue('partition_setup_partial');
  perform pgque.subscribe_slot('partition_setup_partial', 'workers', 0, 2);

  select format('partial-key-%s', g) into v_key
  from generate_series(1, 10000) as g
  where (pg_catalog.hashtextextended(format('partial-key-%s', g), 0) % 2 + 2) % 2 = 1
  limit 1;
  assert v_key is not null, 'partial setup test needs a key for slot 1';
  perform pgque.send('partition_setup_partial', 'partial.event', '{}'::jsonb, v_key);
end $$;

do $$
begin
  perform pgque.force_next_tick('partition_setup_partial');
  perform pgque.ticker();
end $$;

do $$
begin
  assert (
    select count(*) = 2
    from pgque.partition_slot_status
    where queue_name = 'partition_setup_partial'
      and consumer = 'workers'
  ), 'partial setup must expose every expected slot';
  assert (
    select subscribed and last_tick is not null and pending_events > 0
    from pgque.partition_slot_status
    where queue_name = 'partition_setup_partial'
      and consumer = 'workers'
      and slot = 0
  ), 'the materialized slot must report subscribed with known lag';
  assert (
    select not subscribed and last_tick is null and pending_events is null
    from pgque.partition_slot_status
    where queue_name = 'partition_setup_partial'
      and consumer = 'workers'
      and slot = 1
  ), 'the missing slot must report subscribed=false and unknown lag';
end $$;

-- Completing an already-late setup through the atomic API would imply safety.
do $$
declare
  v_raised boolean := false;
begin
  begin
    perform pgque.subscribe_partitioned('partition_setup_partial', 'workers', 2);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%incomplete%',
      format('partial setup rejection must identify incomplete state, got: %s', sqlerrm);
  end;
  assert v_raised, 'atomic setup must reject a pre-existing partial consumer';
  assert (
    select not subscribed and pending_events is null
    from pgque.partition_slot_status
    where queue_name = 'partition_setup_partial'
      and consumer = 'workers'
      and slot = 1
  ), 'rejected atomic setup must leave the missing slot unmistakably incomplete';

  perform pgque.unsubscribe_slot('partition_setup_partial', 'workers', 0);
  perform pgque.drop_queue('partition_setup_partial');
  raise notice 'PASS: partial partition setup is explicit and rejected by atomic setup';
end $$;

-- Atomic setup creates every slot at one cursor before any later event exists.
do $$
begin
  perform pgque.create_queue('partition_setup_atomic');
end $$;

set role pgque_reader;
select pgque.subscribe_partitioned('partition_setup_atomic', 'workers', 3);
reset role;

do $$
declare
  v_key text;
  v_raised boolean := false;
  v_slot int;
begin
  assert (
    select count(*) = 3 and bool_and(subscribed)
    from pgque.partition_slot_status
    where queue_name = 'partition_setup_atomic'
      and consumer = 'workers'
  ), 'atomic setup must materialize all slots and subscriptions';
  assert (
    select count(distinct s.sub_last_tick) = 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'partition_setup_atomic'
      and c.co_name like 'workers#%/3'
  ), 'atomic setup must start every slot from one shared tick';

  begin
    perform pgque.subscribe_partitioned(
      'partition_setup_atomic', 'workers', 4);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%pinned to n=3%',
      format('changed slot count got unexpected error: %s', sqlerrm);
  end;
  assert v_raised, 'atomic setup must reject a changed slot count';

  for v_slot in 0..2 loop
    select format('atomic-key-%s', g) into v_key
    from generate_series(1, 10000) as g
    where (pg_catalog.hashtextextended(format('atomic-key-%s', g), 0) % 3 + 3) % 3 = v_slot
    limit 1;
    assert v_key is not null, format('atomic setup test needs a key for slot %s', v_slot);
    perform pgque.send('partition_setup_atomic', 'atomic.event', '{}'::jsonb, v_key);
  end loop;
end $$;

do $$
begin
  perform pgque.force_next_tick('partition_setup_atomic');
  perform pgque.ticker();
end $$;

/*
 * Idempotent setup after events are visible must not reposition the existing
 * subscriptions to the latest tick and skip those events.
 */
select pgque.subscribe_partitioned('partition_setup_atomic', 'workers', 3);

create temporary table partition_setup_received (
  slot int not null,
  msg_id bigint not null,
  partition_key text not null
);

do $$
declare
  v_msg pgque.message;
  v_slot int;
begin
  for v_slot in 0..2 loop
    assert pgque.claim_slot(
      'partition_setup_atomic', 'workers', v_slot, 'setup-worker') is not null,
      format('atomic setup slot %s must be claimable', v_slot);
    for v_msg in
      select *
      from pgque.receive_partitioned(
        'partition_setup_atomic', 'workers', v_slot, 3, 'setup-worker', 100)
    loop
      insert into partition_setup_received (slot, msg_id, partition_key)
      values (v_slot, v_msg.msg_id, v_msg.extra1);
    end loop;
    assert pgque.ack_partitioned(
      'partition_setup_atomic', 'workers', v_slot, 3, 'setup-worker') = 1,
      format('atomic setup slot %s batch must be finishable', v_slot);
    perform pgque.release_slot(
      'partition_setup_atomic', 'workers', v_slot, 'setup-worker');
  end loop;
end $$;

do $$
declare
  v_slot int;
begin
  assert (
    select count(*) = 3 and count(distinct slot) = 3
    from partition_setup_received
  ), 'events produced after atomic setup must remain reachable on every slot';
  for v_slot in 0..2 loop
    assert not exists (
      select 1
      from partition_setup_received
      where slot <> v_slot
        and (pg_catalog.hashtextextended(partition_key, 0) % 3 + 3) % 3 = v_slot
    ), format('slot %s events must not be delivered by another slot', v_slot);
  end loop;
  perform pgque.unsubscribe_partitioned('partition_setup_atomic', 'workers');
  perform pgque.drop_queue('partition_setup_atomic');
  raise notice 'PASS: atomic setup preserves all post-setup slot histories';
end $$;

-- A failure after one generated slot is created must roll back the whole setup.
do $$
begin
  perform pgque.create_queue('partition_setup_rollback');
  perform pgque.register_consumer(
    'partition_setup_rollback', 'workers#1/3');
  perform pgque.send(
    'partition_setup_rollback', 'collision.event', '{}'::jsonb, 'collision');
end $$;

do $$
begin
  perform pgque.force_next_tick('partition_setup_rollback');
  perform pgque.ticker();
end $$;

do $$
declare
  v_before bigint;
  v_after bigint;
  v_raised boolean := false;
begin
  select s.sub_last_tick into v_before
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'partition_setup_rollback'
    and c.co_name = 'workers#1/3';

  begin
    perform pgque.subscribe_partitioned(
      'partition_setup_rollback', 'workers', 3);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%already registered%',
      format('atomic rollback test got unexpected error: %s', sqlerrm);
  end;
  assert v_raised, 'a generated-name collision must abort atomic setup';
  assert not exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'partition_setup_rollback'
      and pc.co_name = 'workers'
  ), 'failed atomic setup must roll back partition_consumer';
  assert not exists (
    select 1
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'partition_setup_rollback'
      and ps.co_name = 'workers'
  ), 'failed atomic setup must roll back every lease row';
  assert not exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'partition_setup_rollback'
      and c.co_name in ('workers#0/3', 'workers#2/3')
  ), 'failed atomic setup must roll back generated subscriptions';

  select s.sub_last_tick into v_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'partition_setup_rollback'
    and c.co_name = 'workers#1/3';
  assert v_after = v_before,
    'failed atomic setup must not reposition the colliding subscription';

  perform pgque.unregister_consumer(
    'partition_setup_rollback', 'workers#1/3');
  perform pgque.drop_queue('partition_setup_rollback');
  raise notice 'PASS: failed atomic setup rolls back all partial state';
end $$;

/*
 * Whole-consumer teardown: unsubscribe_partitioned is the inverse of
 * subscribe_partitioned. A complete consumer tears down atomically, and a
 * fresh setup with a different slot count works afterward.
 */
do $$
begin
  perform pgque.create_queue('partition_teardown');
  perform pgque.subscribe_partitioned('partition_teardown', 'workers', 3);
end $$;

set role pgque_reader;
select pgque.unsubscribe_partitioned('partition_teardown', 'workers');
reset role;

do $$
begin
  assert not exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'partition_teardown'
      and pc.co_name = 'workers'
  ), 'teardown must remove the pinned-N row';
  assert not exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'partition_teardown'
      and c.co_name like 'workers#%/3'
  ), 'teardown must remove every slot subscription';
  assert not exists (
    select 1
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'partition_teardown'
      and ps.co_name = 'workers'
  ), 'teardown must remove every lease row';

  perform pgque.subscribe_partitioned('partition_teardown', 'workers', 2);
  assert (
    select count(*) = 2 and bool_and(subscribed)
    from pgque.partition_slot_status
    where queue_name = 'partition_teardown'
      and consumer = 'workers'
  ), 'teardown must allow a fresh setup with a new slot count';
end $$;

/*
 * Partial state (a slot already missing) must still tear down cleanly:
 * teardown is the escape hatch the incomplete-setup error points to. An
 * absent consumer is a no-op, never an error.
 */
do $$
begin
  -- Warns: this leaves the complete consumer with incomplete setup.
  perform pgque.unsubscribe_slot('partition_teardown', 'workers', 1);
  perform pgque.unsubscribe_partitioned('partition_teardown', 'workers');
  assert not exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'partition_teardown'
      and pc.co_name = 'workers'
  ), 'partial teardown must remove the pinned-N row';
  assert not exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'partition_teardown'
      and c.co_name like 'workers#%/2'
  ), 'partial teardown must remove the surviving slot subscriptions';

  -- Absent consumer (just torn down, and one that never existed): no-op.
  perform pgque.unsubscribe_partitioned('partition_teardown', 'workers');
  perform pgque.unsubscribe_partitioned('partition_teardown', 'no_such');

  perform pgque.drop_queue('partition_teardown');
  raise notice 'PASS: whole-consumer teardown is atomic, partial-safe, idempotent';
end $$;
