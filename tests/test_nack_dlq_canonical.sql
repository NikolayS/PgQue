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
  exception when raise_exception then
    -- Narrow match: must be the specific msg_id-not-found error, not some
    -- other failure (e.g., a regression in get_batch_events).
    assert sqlerrm like 'msg_id % not found in batch %',
      format('FAIL #98: unexpected error message: %s', sqlerrm);
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
-- Test 1b: valid msg_id with forged payload/type/retry_count
-- ========================================
-- nack() must use canonical row values (realtype / realpayload / ev_retry=0)
-- even when the caller supplies forged type/payload/retry_count in the
-- composite. This is the primary #98 attack vector.

-- Setup
do $$
begin
  perform pgque.create_queue('test_nack_forge_payload');
  perform pgque.set_queue_config('test_nack_forge_payload', 'max_retries', '0');
  perform pgque.register_consumer('test_nack_forge_payload', 'c');
end $$;

do $$
begin
  perform pgque.insert_event('test_nack_forge_payload', 'realtype', 'realpayload');
end $$;

do $$
begin
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg     pgque.message;
  v_forged  pgque.message;
  v_dl      pgque.dead_letter;
begin
  select * into v_msg from pgque.receive('test_nack_forge_payload', 'c', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive a message';

  -- Build a forged composite: real msg_id but fake type/payload/retry_count
  v_forged := row(
    v_msg.msg_id,        -- real msg_id
    v_msg.batch_id,
    'forgetype',         -- forged type
    'forgepayload',      -- forged payload
    999,                 -- forged retry_count (would skip DLQ routing if trusted)
    now(),
    'fe1', 'fe2', 'fe3', 'fe4'  -- forged extras
  )::pgque.message;

  -- nack() must accept (real msg_id), but write canonical values to DLQ
  perform pgque.nack(v_msg.batch_id, v_forged, interval '0 seconds', 'test1b');
  perform pgque.ack(v_msg.batch_id);

  -- Verify DLQ row contains canonical values, not forged ones
  select * into v_dl
  from pgque.dead_letter
  where dl_queue_id = (
    select queue_id from pgque.queue where queue_name = 'test_nack_forge_payload'
  );

  assert v_dl.ev_id = v_msg.msg_id,
    format('FAIL 1b: expected ev_id=%s, got %s', v_msg.msg_id, v_dl.ev_id);
  assert v_dl.ev_type = 'realtype',
    format('FAIL 1b: expected ev_type=realtype, got %s', v_dl.ev_type);
  assert v_dl.ev_data = 'realpayload',
    format('FAIL 1b: expected ev_data=realpayload, got %s', v_dl.ev_data);
  assert coalesce(v_dl.ev_retry, 0) = 0,
    format('FAIL 1b: expected ev_retry=0, got %s', v_dl.ev_retry);
  assert v_dl.ev_extra1 is null,
    format('FAIL 1b: expected ev_extra1=null (canonical), got %s', v_dl.ev_extra1);

  raise notice 'PASS 1b: canonical values in DLQ, forged payload/type ignored';
end $$;

-- Cleanup test 1b
do $$
begin
  perform pgque.unregister_consumer('test_nack_forge_payload', 'c');
  perform pgque.drop_queue('test_nack_forge_payload');
end $$;

-- ========================================
-- Test 1c: NULL msg_id must error gracefully
-- ========================================
-- When i_msg.msg_id is NULL the where ev_id = NULL yields no rows, so
-- nack() raises 'msg_id <NULL> not found in batch %'. Verify it errors
-- cleanly and does not insert a DLQ row.

-- Setup
do $$
begin
  perform pgque.create_queue('test_nack_null_id');
  perform pgque.set_queue_config('test_nack_null_id', 'max_retries', '0');
  perform pgque.register_consumer('test_nack_null_id', 'c');
end $$;

do $$
begin
  perform pgque.insert_event('test_nack_null_id', 'mytype', 'mypayload');
end $$;

do $$
begin
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_null  pgque.message;
  v_ok    boolean := false;
begin
  select * into v_msg from pgque.receive('test_nack_null_id', 'c', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive a message';

  -- Build composite with NULL msg_id
  v_null := row(
    null,              -- NULL msg_id
    v_msg.batch_id,
    'mytype', 'mypayload', 0, now(), null, null, null, null
  )::pgque.message;

  begin
    perform pgque.nack(v_msg.batch_id, v_null, interval '0 seconds', 'null-id');
    assert false, 'FAIL 1c: nack() should have raised for NULL msg_id';
  exception when raise_exception then
    assert sqlerrm like 'msg_id % not found in batch %',
      format('FAIL 1c: unexpected error: %s', sqlerrm);
    v_ok := true;
  end;

  assert v_ok, 'FAIL 1c: nack() did not raise for NULL msg_id';

  -- DLQ must be empty
  assert (
    select count(*) = 0 from pgque.dead_letter
    where dl_queue_id = (
      select queue_id from pgque.queue where queue_name = 'test_nack_null_id'
    )
  ), 'FAIL 1c: DLQ row inserted for NULL msg_id';

  -- Clean up batch
  perform pgque.ack(v_msg.batch_id);
  raise notice 'PASS 1c: NULL msg_id raised gracefully, DLQ is clean';
end $$;

-- Cleanup test 1c
do $$
begin
  perform pgque.unregister_consumer('test_nack_null_id', 'c');
  perform pgque.drop_queue('test_nack_null_id');
end $$;

-- ========================================
-- Test 2 (#104): repeated nack() must not create duplicate DLQ rows
-- Note: this covers serial-only double-nack. The concurrent case
-- (two sessions both pass get_batch_events, both call event_dead) is
-- reasoned safe via the unique index + ON CONFLICT DO NOTHING on
-- (dl_queue_id, dl_consumer_id, ev_id); concurrent testing deferred.
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
