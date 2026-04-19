\set ON_ERROR_STOP on

-- US-1: Basic produce/consume cycle
-- As a developer, I want to send a JSON message and receive it
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Setup
do $$ begin
  perform logres.create_queue('us1_orders');
  perform logres.subscribe('us1_orders', 'app');
end $$;

-- Action: send a message using the modern API
do $$ begin
  perform logres.send('us1_orders', '{"id":1}'::jsonb);
end $$;

-- Tick (force_tick bypasses throttle; separate transaction for snapshot visibility)
do $$ begin
  perform logres.force_tick('us1_orders');
  perform logres.ticker();
end $$;

-- Verify: receive returns exactly 1 message with correct fields
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us1_orders', 'app', 10)
  loop
    v_count := v_count + 1;
    assert v_msg.payload::jsonb = '{"id":1}'::jsonb, 'payload should match, got ' || coalesce(v_msg.payload, 'NULL');
    assert v_msg.type = 'default', 'type should be default, got ' || coalesce(v_msg.type, 'NULL');
    assert v_msg.batch_id is not null, 'batch_id should be set';
    assert v_msg.msg_id is not null, 'msg_id should be set';
    assert v_msg.created_at is not null, 'created_at should be set';
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 1, 'should receive exactly 1 message, got ' || v_count;

  -- Ack the batch
  perform logres.ack(v_batch_id);

  raise notice 'PASS: US-1 send + receive + ack';
end $$;

-- Verify: subsequent receive is empty (batch was acked)
do $$
declare
  v_count int := 0;
  v_msg logres.message;
begin
  for v_msg in select * from logres.receive('us1_orders', 'app', 10)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'subsequent receive should be empty, got ' || v_count;
  raise notice 'PASS: US-1 subsequent receive empty';
end $$;

-- Teardown
do $$ begin
  perform logres.unsubscribe('us1_orders', 'app');
  perform logres.drop_queue('us1_orders');
end $$;

\echo 'US-1: PASSED'
