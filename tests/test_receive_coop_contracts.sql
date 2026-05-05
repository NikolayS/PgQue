\set ON_ERROR_STOP on

-- Test: pgque.receive_coop() must mirror pgque.receive() contracts.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Two regressions covered:
--
-- 1. Empty-tick-window wedge: when next_batch_custom() allocates a batch
--    over a tick window with zero events visible to the member,
--    receive_coop() returns 0 rows but leaves sub_batch set on the member.
--    Subsequent receive_coop() calls short-circuit on the same empty batch
--    and never advance.  Mirror of receive()'s #103 fix.
--
-- 2. Validation parity: receive() rejects i_max_return < 1; receive_coop()
--    must too, otherwise <=0 silently returns the entire batch.

-- 1. Empty-tick-window wedge
do $$
begin
  perform pgque.create_queue('coop_empty_batch');
  perform pgque.register_subconsumer('coop_empty_batch', 'main_c', 'w1');
end $$;

-- Force a tick that captures zero events.
do $$
begin
  perform pgque.force_next_tick('coop_empty_batch');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_sub   record;
begin
  for v_msg in select * from pgque.receive_coop('coop_empty_batch', 'main_c', 'w1', 100)
  loop
    v_count := v_count + 1;
  end loop;

  assert v_count = 0,
    'empty tick window should yield 0 messages, got ' || v_count;

  select s.* into v_sub
  from pgque.subscription s
  join pgque.queue q on q.queue_id = s.sub_queue
  join pgque.consumer c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_empty_batch'
    and c.co_name = 'main_c.w1';

  assert v_sub.sub_batch is null,
    'receive_coop() left sub_batch set after returning 0 rows — '
    || 'subconsumer is wedged on empty batch '
    || coalesce(v_sub.sub_batch::text, 'NULL');
end $$;

-- After a real event arrives, the next receive_coop() must see it.
select pgque.send('coop_empty_batch', 'late', 'arrival');
select pgque.force_next_tick('coop_empty_batch');
select pgque.ticker();

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive_coop('coop_empty_batch', 'main_c', 'w1', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;

  assert v_count = 1,
    'second receive_coop() should return the late event, got '
    || v_count || ' rows (0 = wedged on empty batch)';
  assert v_msg.type = 'late',
    'unexpected type: ' || coalesce(v_msg.type, 'NULL');

  perform pgque.ack(v_batch);
end $$;

do $$
begin
  perform pgque.unregister_subconsumer('coop_empty_batch', 'main_c', 'w1');
  perform pgque.unregister_consumer('coop_empty_batch', 'main_c');
  perform pgque.drop_queue('coop_empty_batch');
end $$;

-- 2. i_max_return < 1 must raise (parity with pgque.receive)
do $$
begin
  perform pgque.create_queue('coop_max_return');
  perform pgque.register_subconsumer('coop_max_return', 'main_c', 'w1');
end $$;

do $$
begin
  begin
    perform pgque.receive_coop('coop_max_return', 'main_c', 'w1', 0);
    raise exception 'expected receive_coop to reject max_return = 0';
  exception when others then
    if sqlerrm = 'expected receive_coop to reject max_return = 0' then raise; end if;
  end;

  begin
    perform pgque.receive_coop('coop_max_return', 'main_c', 'w1', -5);
    raise exception 'expected receive_coop to reject negative max_return';
  exception when others then
    if sqlerrm = 'expected receive_coop to reject negative max_return' then raise; end if;
  end;
end $$;

do $$
begin
  perform pgque.unregister_subconsumer('coop_max_return', 'main_c', 'w1');
  perform pgque.unregister_consumer('coop_max_return', 'main_c');
  perform pgque.drop_queue('coop_max_return');
  raise notice 'PASS: receive_coop() empty-batch + max_return contracts';
end $$;
