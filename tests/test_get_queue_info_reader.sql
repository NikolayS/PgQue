-- test_get_queue_info_reader.sql -- pgque_reader can call get_queue_info
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- get_queue_info() and get_queue_info(text) are granted to pgque_reader and
-- documented as reader-usable monitoring functions. They internally call
-- pgque.seq_getval(text), whose ACL is admin-only, so a SECURITY INVOKER
-- get_queue_info fails at runtime for a reader with:
--   ERROR: permission denied for function seq_getval
-- This pins the fix: get_queue_info must be callable by pgque_reader, the
-- exact role it is granted to. See
-- https://github.com/NikolayS/pgque/issues/265

\set ON_ERROR_STOP on

-- Idempotent preamble.
do $$ begin
  if exists (select 1 from pgque.queue where queue_name = 'gqi_reader_q') then
    perform pgque.drop_queue('gqi_reader_q', true);
  end if;
end $$;

select pgque.create_queue('gqi_reader_q');

-- A tick must exist so the function exercises the seq_getval() code path.
select pgque.subscribe('gqi_reader_q', 'gqi_consumer');
select pgque.send('gqi_reader_q', 'gqi.msg', '{"n":1}'::jsonb);
select pgque.ticker();

-- Exercise both overloads under the role they are granted to.
set role pgque_reader;

do $$
declare
  v_count integer;
  v_name  text;
  v_ev    bigint;
begin
  -- get_queue_info(text): the overload that calls seq_getval().
  select queue_name, ev_new
    into v_name, v_ev
    from pgque.get_queue_info('gqi_reader_q');
  assert v_name = 'gqi_reader_q', 'get_queue_info(text) must return the queue row';
  assert v_ev is not null, 'get_queue_info(text) ev_new must be populated';
  raise notice 'PASS: pgque_reader can call get_queue_info(text), ev_new=%', v_ev;

  -- get_queue_info(): the zero-arg overload fans out to the text overload.
  select count(*) into v_count from pgque.get_queue_info();
  assert v_count >= 1, 'get_queue_info() must return at least the one queue';
  raise notice 'PASS: pgque_reader can call get_queue_info(), rows=%', v_count;
end $$;

reset role;

-- Cleanup.
select pgque.drop_queue('gqi_reader_q', true);

\echo 'PASS: test_get_queue_info_reader'
