\set ON_ERROR_STOP on

-- Bound partition setup and status expansion at the supported slot ceiling.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  perform pgque.create_queue('partition_limit_boundary');
  perform pgque.subscribe_partitioned(
    'partition_limit_boundary', 'workers', 256);

  assert (
    select count(*) = 256 and bool_and(subscribed)
    from pgque.partition_slot_status
    where queue_name = 'partition_limit_boundary'
      and consumer = 'workers'
  ), 'the maximum supported slot count must materialize completely';

  perform pgque.drop_queue('partition_limit_boundary', true);
end $$;

do $$
declare
  v_raised boolean := false;
begin
  perform pgque.create_queue('partition_limit_atomic_reject');

  begin
    perform pgque.subscribe_partitioned(
      'partition_limit_atomic_reject', 'workers', 257);
    raise exception 'expected subscribe_partitioned slot-limit rejection';
  exception
    when others then
      if sqlerrm = 'expected subscribe_partitioned slot-limit rejection' then
        raise;
      end if;
      v_raised := true;
      assert sqlstate = 'P0001'
        and sqlerrm = 'slot count n must be between 1 and 256, got 257',
        format('unexpected subscribe_partitioned error [%s] %s',
          sqlstate, sqlerrm);
  end;

  assert v_raised, 'subscribe_partitioned must reject max+1';
  assert not exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'partition_limit_atomic_reject'
      and pc.co_name = 'workers'
  ), 'rejected atomic setup must not leave partition metadata';
  assert not exists (
    select 1
    from pgque.consumer
    where co_name like 'workers#%/257'
  ), 'rejected atomic setup must not leave generated consumers';

  perform pgque.drop_queue('partition_limit_atomic_reject', true);
end $$;

do $$
declare
  v_raised boolean := false;
begin
  perform pgque.create_queue('partition_limit_repair_reject');

  begin
    perform pgque.subscribe_slot(
      'partition_limit_repair_reject', 'workers', 0, 257);
    raise exception 'expected subscribe_slot slot-limit rejection';
  exception
    when others then
      if sqlerrm = 'expected subscribe_slot slot-limit rejection' then
        raise;
      end if;
      v_raised := true;
      assert sqlstate = 'P0001'
        and sqlerrm = 'slot count n must be between 1 and 256, got 257',
        format('unexpected subscribe_slot error [%s] %s', sqlstate, sqlerrm);
  end;

  assert v_raised, 'subscribe_slot must reject max+1';
  assert not exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'partition_limit_repair_reject'
      and pc.co_name = 'workers'
  ), 'rejected repair setup must not leave partition metadata';

  perform pgque.drop_queue('partition_limit_repair_reject', true);
  raise notice 'PASS: partition slot count ceiling';
end $$;
