\set ON_ERROR_STOP on

-- Test receive/ack/nack API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Test 1: receive() returns messages
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  perform pgque.create_queue('test_recv');
  perform pgque.register_consumer('test_recv', 'c1');

  -- Insert event using raw API (send() may not exist yet)
  perform pgque.insert_event('test_recv', 'test.type', '{"key":"val"}');
  perform pgque.ticker();

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

  -- Next receive should be empty
  v_count := 0;
  for v_msg in select * from pgque.receive('test_recv', 'c1', 10)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'should have no more messages after ack';

  perform pgque.unregister_consumer('test_recv', 'c1');
  perform pgque.drop_queue('test_recv');
  raise notice 'PASS: receive + ack';
end $$;
