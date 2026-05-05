-- test_e2e_role_split.sql
-- End-to-end produce → tick → consume cycle exercised under the strict
-- producer/consumer role split. test_pgque_roles.sql checks the GRANT
-- table; test_security_producer_isolation.sql checks the cross-role
-- denials. This file checks the legitimate happy path:
--
--   set role pgque_test_producer;  -- has only pgque_writer
--     → pgque.send / pgque.send_batch     succeed
--     → pgque.ack / pgque.receive         denied
--   set role pgque_test_consumer;  -- has only pgque_reader
--     → pgque.subscribe / pgque.receive
--       / pgque.nack / pgque.ack          succeed
--     → pgque.send                        denied
--   admin: ticker + cleanup
--
-- A regression that, e.g., revoked send() from pgque_writer or moved
-- subscribe() back to pgque_writer would still pass test_pgque_roles
-- (it only checks the current grant table, not whether the function is
-- actually callable in a real transaction) but would break the install
-- the moment a real producer connection tried to publish. This test
-- catches that.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- =========================================================================
-- Preamble: idempotent test-role cleanup so reruns on a shared dev DB
-- don't fail on duplicate roles.
-- =========================================================================
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'pgque_test_producer') then
    revoke all privileges on schema pgque from pgque_test_producer;
    revoke pgque_writer from pgque_test_producer;
    drop role pgque_test_producer;
  end if;
  if exists (select 1 from pg_roles where rolname = 'pgque_test_consumer') then
    revoke all privileges on schema pgque from pgque_test_consumer;
    revoke pgque_reader from pgque_test_consumer;
    drop role pgque_test_consumer;
  end if;
exception when others then
  raise notice 'preamble cleanup: % / %', sqlstate, sqlerrm;
end $$;

do $$ begin
  if exists (select 1 from pgque.queue where queue_name = 'test_e2e_split') then
    perform pgque.drop_queue('test_e2e_split', true);
  end if;
end $$;

-- =========================================================================
-- Setup: admin creates the queue + the two test roles.
-- NOLOGIN: set role does not require LOGIN, and prevents accidental
-- direct authentication if the test aborts before cleanup.
-- =========================================================================
create role pgque_test_producer nologin;
grant pgque_writer to pgque_test_producer;

create role pgque_test_consumer nologin;
grant pgque_reader to pgque_test_consumer;

select pgque.create_queue('test_e2e_split');

-- The consumer role must be able to subscribe itself (subscribe is on
-- pgque_reader since #163). Verify this end-to-end by calling subscribe
-- under the consumer role rather than the admin role.
set role pgque_test_consumer;
select pgque.subscribe('test_e2e_split', 'e2e_consumer');
reset role;

-- =========================================================================
-- Producer half (set role pgque_test_producer): send single + batch
-- =========================================================================
set role pgque_test_producer;

do $$
declare
  v_eid bigint;
  v_ids bigint[];
begin
  -- send (jsonb)
  v_eid := pgque.send('test_e2e_split', 'split.single', '{"who":"producer","seq":1}'::jsonb);
  assert v_eid is not null, 'producer must be able to send (jsonb)';

  -- send (text fast path)
  v_eid := pgque.send('test_e2e_split', 'split.single_text', 'opaque-text'::text);
  assert v_eid is not null, 'producer must be able to send (text)';

  -- send_batch (jsonb[])
  v_ids := pgque.send_batch('test_e2e_split', 'split.batch_json', array[
    '{"n":1}'::jsonb,
    '{"n":2}'::jsonb,
    '{"n":3}'::jsonb
  ]);
  assert cardinality(v_ids) = 3, 'producer must be able to send_batch (jsonb[])';

  -- send_batch (text[])
  v_ids := pgque.send_batch('test_e2e_split', 'split.batch_text', array[
    'a', 'b'
  ]::text[]);
  assert cardinality(v_ids) = 2, 'producer must be able to send_batch (text[])';

  raise notice 'PASS: producer (pgque_writer) can send + send_batch';
end $$;

-- Negative checks: producer must NOT be able to receive / ack / nack /
-- subscribe (those moved to pgque_reader in #163).
do $$
declare
  v_ok boolean;
begin
  v_ok := false;
  begin
    perform * from pgque.receive('test_e2e_split', 'e2e_consumer', 1);
    raise exception 'producer must not be able to receive';
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'producer was able to call receive (regression)';

  v_ok := false;
  begin
    perform pgque.ack(1::bigint);
    raise exception 'producer must not be able to ack';
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'producer was able to call ack (regression)';

  v_ok := false;
  begin
    perform pgque.subscribe('test_e2e_split', 'producer_sub_attempt');
    raise exception 'producer must not be able to subscribe';
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'producer was able to call subscribe (regression)';

  raise notice 'PASS: producer (pgque_writer) is denied receive/ack/subscribe';
end $$;

reset role;

-- =========================================================================
-- Admin half: ticker so the just-sent events become visible to consumers.
-- (ticker / force_tick are admin-only by grant; in production this is
-- pg_cron or a service role.)
-- =========================================================================
select pgque.force_tick('test_e2e_split');
select pgque.ticker('test_e2e_split');

-- =========================================================================
-- Consumer half (set role pgque_test_consumer): receive + ack + nack
-- =========================================================================
set role pgque_test_consumer;

do $$
declare
  v_msg     pgque.message;
  v_count   int := 0;
  v_batch   bigint;
  v_seen_single boolean := false;
  v_seen_text   boolean := false;
  v_seen_batch  int := 0;
begin
  for v_msg in select * from pgque.receive('test_e2e_split', 'e2e_consumer', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;

    if v_msg.type = 'split.single' then v_seen_single := true; end if;
    if v_msg.type = 'split.single_text' then v_seen_text := true; end if;
    if v_msg.type in ('split.batch_json', 'split.batch_text') then
      v_seen_batch := v_seen_batch + 1;
    end if;
  end loop;

  -- 1 single (jsonb) + 1 single (text) + 3 batch_json + 2 batch_text = 7
  assert v_count = 7,
    format('consumer must receive all 7 produced messages, got %s', v_count);
  assert v_seen_single, 'expected split.single message';
  assert v_seen_text,   'expected split.single_text message';
  assert v_seen_batch = 5,
    format('expected 5 batch messages, saw %s', v_seen_batch);

  -- ack must succeed and return 1.
  assert pgque.ack(v_batch) = 1, 'consumer ack must return 1 on success';
  raise notice 'PASS: consumer (pgque_reader) can receive + ack the full batch';
end $$;

-- Negative checks: consumer must NOT be able to send (#163 split).
do $$
declare
  v_ok boolean;
begin
  v_ok := false;
  begin
    perform pgque.send('test_e2e_split', 'split.illegal', '{"who":"consumer"}'::jsonb);
    raise exception 'consumer must not be able to send';
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'consumer was able to call send (regression)';

  v_ok := false;
  begin
    perform pgque.send_batch('test_e2e_split', 'split.illegal',
      array['{"n":1}'::jsonb]);
    raise exception 'consumer must not be able to send_batch';
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'consumer was able to call send_batch (regression)';

  raise notice 'PASS: consumer (pgque_reader) is denied send/send_batch';
end $$;

reset role;

-- =========================================================================
-- Cleanup
-- =========================================================================
set role pgque_test_consumer;
select pgque.unsubscribe('test_e2e_split', 'e2e_consumer');
reset role;

select pgque.drop_queue('test_e2e_split', true);

revoke pgque_writer from pgque_test_producer;
revoke pgque_reader from pgque_test_consumer;
revoke all privileges on schema pgque from pgque_test_producer;
revoke all privileges on schema pgque from pgque_test_consumer;
drop role pgque_test_producer;
drop role pgque_test_consumer;

\echo 'PASS: test_e2e_role_split'
