\set ON_ERROR_STOP on

-- Test receive/ack/nack API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ requires insert, ticker, and receive to be in separate transactions
-- (snapshot visibility). Each DO block is a separate transaction.

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('test_recv');
  perform pgque.register_consumer('test_recv', 'c1');
end $$;

-- Step 2: insert event (separate transaction)
do $$
begin
  perform pgque.insert_event('test_recv', 'test.type', '{"key":"val"}');
end $$;

-- Step 3: ticker (separate transaction to capture the insert)
do $$
begin
  perform pgque.ticker();
end $$;

-- Step 4: receive and verify
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_recv', 'c1', 10)
  loop
    v_count := v_count + 1;
    assert v_msg.type = 'test.type', 'type should be test.type';
    assert v_msg.payload = '{"key":"val"}', 'payload should match';
    assert v_msg.batch_id is not null, 'batch_id should be set';
  end loop;

  assert v_count = 1, 'should receive exactly 1 message, got ' || v_count;

  -- Ack the batch
  perform pgque.ack(v_msg.batch_id);
end $$;

-- Step 5: verify no more messages after ack
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_recv', 'c1', 10)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'should have no more messages after ack';
end $$;

-- Step 6: partial receive still acks the whole underlying batch
do $$
begin
  perform pgque.create_queue('test_recv_partial');
  perform pgque.register_consumer('test_recv_partial', 'c1');
end $$;

do $$
begin
  perform pgque.insert_event('test_recv_partial', 'test.type', '{"n":1}');
  perform pgque.insert_event('test_recv_partial', 'test.type', '{"n":2}');
  perform pgque.insert_event('test_recv_partial', 'test.type', '{"n":3}');
end $$;

do $$
begin
  perform pgque.force_tick('test_recv_partial');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from pgque.receive('test_recv_partial', 'c1', 1)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 1, 'receive(..., 1) should return exactly 1 row';
  assert v_batch_id is not null, 'batch_id should be set for partial receive';

  perform pgque.ack(v_batch_id);
end $$;

do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_recv_partial', 'c1', 10)
  loop
    v_count := v_count + 1;
  end loop;

  assert v_count = 0,
    'ack(batch_id) should finish the whole batch, even if receive(..., 1) returned one row';
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_recv', 'c1');
  perform pgque.drop_queue('test_recv');
  perform pgque.unregister_consumer('test_recv_partial', 'c1');
  perform pgque.drop_queue('test_recv_partial');
  raise notice 'PASS: receive + ack semantics';
end $$;
