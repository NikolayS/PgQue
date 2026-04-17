\set ON_ERROR_STOP on

-- Test pgque.send() and related functions
-- These tests use the modern API layer

-- Test 1: send() returns event ID
do $$
declare
  v_eid bigint;
begin
  perform pgque.create_queue('test_send');
  perform pgque.subscribe('test_send', 'c1');

  v_eid := pgque.send('test_send', '{"key": "value"}'::jsonb);
  assert v_eid is not null, 'send() should return event id';

  raise notice 'PASS: send() returns event id %', v_eid;

  -- Cleanup will happen at end
end $$;

-- Test 2: send() with explicit type
do $$
declare
  v_eid bigint;
begin
  v_eid := pgque.send('test_send', 'order.created', '{"id": 1}'::jsonb);
  assert v_eid is not null, 'send(queue, type, payload) should return event id';
  raise notice 'PASS: send() with type returns event id %', v_eid;
end $$;

-- Test 3: send_batch() returns array of IDs
do $$
declare
  v_ids bigint[];
begin
  v_ids := pgque.send_batch('test_send', 'batch.test', array[
    '{"n":1}'::jsonb,
    '{"n":2}'::jsonb,
    '{"n":3}'::jsonb
  ]);
  assert array_length(v_ids, 1) = 3, 'send_batch should return 3 ids, got ' || coalesce(array_length(v_ids, 1)::text, 'NULL');
  raise notice 'PASS: send_batch() returns 3 ids';
end $$;

-- Test 3a: send(text) fast path, default type
do $$
declare
  v_eid bigint;
begin
  v_eid := pgque.send('test_send', '{"raw":"text"}'::text);
  assert v_eid is not null, 'send(queue, text) should return event id';
  raise notice 'PASS: send(queue, text) returns event id %', v_eid;
end $$;

-- Test 3b: send(type, text) fast path, explicit type
do $$
declare
  v_eid bigint;
begin
  v_eid := pgque.send('test_send', 'raw.binary', E'\\x01\\x02\\x03 not-json');
  assert v_eid is not null, 'send(queue, type, text) should return event id';
  raise notice 'PASS: send(queue, type, text) accepts non-JSON payload';
end $$;

-- Test 3c: send_batch(text[]) fast path
do $$
declare
  v_ids bigint[];
begin
  v_ids := pgque.send_batch('test_send', 'batch.text', array[
    'opaque-1',
    'opaque-2',
    'opaque-3'
  ]::text[]);
  assert array_length(v_ids, 1) = 3,
    'send_batch(text[]) should return 3 ids, got '
    || coalesce(array_length(v_ids, 1)::text, 'NULL');
  raise notice 'PASS: send_batch(text[]) returns 3 ids';
end $$;

-- Test 3d: send_batch() on empty input returns empty array, not NULL
do $$
declare
  v_ids bigint[];
begin
  v_ids := pgque.send_batch('test_send', 'batch.empty', array[]::jsonb[]);
  assert v_ids is not null, 'send_batch(jsonb[]) on empty input must not return NULL';
  assert cardinality(v_ids) = 0,
    'send_batch(jsonb[]) on empty input must return empty array, got '
    || cardinality(v_ids)::text;

  v_ids := pgque.send_batch('test_send', 'batch.empty', array[]::text[]);
  assert v_ids is not null, 'send_batch(text[]) on empty input must not return NULL';
  assert cardinality(v_ids) = 0,
    'send_batch(text[]) on empty input must return empty array, got '
    || cardinality(v_ids)::text;

  raise notice 'PASS: send_batch() on empty input returns empty array';
end $$;

-- Test 4: subscribe/unsubscribe
do $$
declare
  v_count int;
begin
  perform pgque.subscribe('test_send', 'c2');

  select count(*) into v_count from pgque.get_consumer_info('test_send');
  assert v_count = 2, 'should have 2 consumers (c1 + c2), got ' || v_count;

  perform pgque.unsubscribe('test_send', 'c2');

  select count(*) into v_count from pgque.get_consumer_info('test_send');
  assert v_count = 1, 'should have 1 consumer after unsubscribe, got ' || v_count;

  raise notice 'PASS: subscribe/unsubscribe';
end $$;

-- Cleanup
do $$
begin
  perform pgque.unsubscribe('test_send', 'c1');
  perform pgque.drop_queue('test_send');
  raise notice 'PASS: cleanup complete';
end $$;
