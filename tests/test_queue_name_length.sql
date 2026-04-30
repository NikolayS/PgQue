-- test_queue_name_length.sql -- Queue name length validation (issue #109)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- pg_notify channel names are limited to 63 bytes (PostgreSQL identifier limit).
-- PgQue prefixes them with 'pgque_' (6 bytes), leaving 57 bytes for the queue name.
-- Names > 57 bytes must be rejected with a clear error before the notify fires.

do $$
declare
  v_ok bool;
begin
  -- 57-byte name should succeed: length('pgque_' || name) = 63
  perform pgque.create_queue(repeat('q', 57));
  assert exists (
    select 1 from pgque.get_queue_info()
    where queue_name = repeat('q', 57)
  ), '57-byte queue name should be created successfully';
  perform pgque.drop_queue(repeat('q', 57));

  raise notice 'PASS: 57-byte queue name accepted';
end $$;

do $$
declare
  v_caught bool := false;
  v_msg    text;
begin
  -- 58-byte name must be rejected: length('pgque_' || name) = 64 > 63
  begin
    perform pgque.create_queue(repeat('q', 58));
  exception when others then
    v_caught := true;
    v_msg := sqlerrm;
  end;

  assert v_caught,
    '58-byte queue name should raise an error, got none';
  assert v_msg like '%queue name too long%' or v_msg like '%57%',
    'error message should mention queue name length limit, got: ' || v_msg;

  raise notice 'PASS: 58-byte queue name rejected with: %', v_msg;
end $$;

do $$
declare
  v_caught bool := false;
begin
  -- 63-byte name must also be rejected
  begin
    perform pgque.create_queue(repeat('x', 63));
  exception when others then
    v_caught := true;
  end;

  assert v_caught, '63-byte queue name should raise an error';

  raise notice 'PASS: 63-byte queue name rejected';
end $$;

-- Multi-byte UTF-8 boundary: octet_length vs length differ for non-ASCII chars.
-- 19 × 3-byte char (日) = 57 bytes → accept (locks in octet_length semantics).
do $$
begin
  perform pgque.create_queue(repeat('日', 19));
  assert exists (
    select 1 from pgque.get_queue_info()
    where queue_name = repeat('日', 19)
  ), '57-byte UTF-8 queue name (19 × 3-byte char) should be created successfully';
  perform pgque.drop_queue(repeat('日', 19));

  raise notice 'PASS: 57-byte UTF-8 queue name (19 × 3-byte char) accepted';
end $$;

-- 20 × 3-byte char (日) = 60 bytes → reject.
do $$
declare
  v_caught bool := false;
begin
  begin
    perform pgque.create_queue(repeat('日', 20));
  exception when others then
    v_caught := true;
  end;

  assert v_caught, '60-byte UTF-8 queue name (20 × 3-byte char) should raise an error';

  raise notice 'PASS: 60-byte UTF-8 queue name (20 × 3-byte char) rejected';
end $$;

-- No partial state: a rejected create_queue must not insert into pgque.queue.
do $$
declare
  v_caught bool := false;
begin
  begin
    perform pgque.create_queue(repeat('q', 58));
  exception when others then
    v_caught := true;
  end;

  assert v_caught, '58-byte queue name should raise an error';
  assert not exists (
    select 1 from pgque.queue where queue_name = repeat('q', 58)
  ), 'rejected queue must not leave partial state in pgque.queue';

  raise notice 'PASS: rejected create_queue leaves no partial state';
end $$;
