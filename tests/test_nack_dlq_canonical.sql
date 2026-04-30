\set ON_ERROR_STOP on

-- test_nack_dlq_canonical.sql
-- Red/green regression tests for:
--   #98  -- nack() DLQ path trusts caller-supplied pgque.message (forge)
--   #104 -- repeated nack() creates duplicate DLQ rows
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- ========================================
-- Test 1 (#98): forged msg_id must be rejected
-- ========================================
-- nack() must verify the msg_id actually belongs to the active batch.
-- Previously it trusted caller-supplied composite and inserted fake DLQ rows.

-- Setup
do $$
begin
  perform pgque.create_queue('test_nack_forge');
  perform pgque.set_queue_config('test_nack_forge', 'max_retries', '0');
  perform pgque.register_consumer('test_nack_forge', 'c');
end $$;

do $$
begin
  perform pgque.insert_event('test_nack_forge', 'realtype', 'realpayload');
end $$;

do $$
begin
  perform pgque.ticker();
end $$;

-- Receive to get an active batch
do $$
declare
  v_msg   pgque.message;
  v_batch bigint;
  v_ok    boolean := false;
begin
  select * into v_msg from pgque.receive('test_nack_forge', 'c', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive a message';

  v_batch := v_msg.batch_id;

  -- Attempt to nack with a forged msg_id that is not in the batch.
  -- This MUST raise an exception after the fix. Before the fix it inserts a
  -- fake DLQ row silently.
  begin
    perform pgque.nack(
      v_batch,
      row(
        999999,           -- forged msg_id (not in this batch)
        v_batch,
        'forgedtype',
        'forgedpayload',
        999,              -- forged retry_count
        now(),
        null, null, null, null
      )::pgque.message,
      interval '0 seconds',
      'forged'
    );
    -- If we get here, the forge was NOT blocked -> test failure
    assert false, 'FAIL #98: nack() should have rejected forged msg_id 999999';
  exception when others then
    -- Expected: function raised an error
    v_ok := true;
  end;

  assert v_ok, 'FAIL #98: nack() did not reject forged msg_id';

  -- DLQ must be empty -- no forged row inserted
  assert (
    select count(*) = 0 from pgque.dead_letter
    where dl_queue_id = (
      select queue_id from pgque.queue where queue_name = 'test_nack_forge'
    )
  ), 'FAIL #98: forged DLQ row was inserted';

  -- Clean up batch
  perform pgque.ack(v_batch);
  raise notice 'PASS #98: nack() rejected forged msg_id and DLQ is clean';
end $$;

-- Cleanup test 1
do $$
begin
  perform pgque.unregister_consumer('test_nack_forge', 'c');
  perform pgque.drop_queue('test_nack_forge');
end $$;

-- ========================================
-- Test 2 (#104): repeated nack() must not create duplicate DLQ rows
-- ========================================
-- With max_retries=0, two consecutive nack() calls for the same message
-- must result in exactly one DLQ row, not two.

-- Setup
do $$
begin
  perform pgque.create_queue('test_nack_dup');
  perform pgque.set_queue_config('test_nack_dup', 'max_retries', '0');
  perform pgque.register_consumer('test_nack_dup', 'c');
end $$;

do $$
begin
  perform pgque.insert_event('test_nack_dup', 'mytype', 'mypayload');
end $$;

do $$
begin
  perform pgque.ticker();
end $$;

-- Receive, then call nack() twice before ack()
do $$
declare
  v_msg       pgque.message;
  v_dlq_count bigint;
begin
  select * into v_msg from pgque.receive('test_nack_dup', 'c', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive a message';

  -- First nack: msg is DLQ-bound (max_retries=0, retry_count=0)
  perform pgque.nack(v_msg.batch_id, v_msg, interval '0 seconds', 'dead1');

  -- Second nack for same message, same batch: must be idempotent
  perform pgque.nack(v_msg.batch_id, v_msg, interval '0 seconds', 'dead2');

  perform pgque.ack(v_msg.batch_id);

  select count(*) into v_dlq_count
  from pgque.dead_letter
  where dl_queue_id = (
    select queue_id from pgque.queue where queue_name = 'test_nack_dup'
  );

  assert v_dlq_count = 1,
    format('FAIL #104: expected 1 DLQ row, got %s', v_dlq_count);

  raise notice 'PASS #104: repeated nack() is idempotent (1 DLQ row)';
end $$;

-- Cleanup test 2
do $$
begin
  perform pgque.unregister_consumer('test_nack_dup', 'c');
  perform pgque.drop_queue('test_nack_dup');
end $$;
