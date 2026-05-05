-- test_e2e_role_split.sql -- e2e produce → tick → consume under role split
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Existing role tests check the GRANT table (test_pgque_roles.sql) and
-- cross-role denials (test_security_producer_isolation.sql). This file
-- pins the legitimate happy path: a real `set role` produce → tick →
-- consume cycle under the strict pgque_writer / pgque_reader split.

\set ON_ERROR_STOP on

-- Idempotent preamble: clean up any leftovers from a prior aborted run.
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'pgque_test_producer') then
    revoke pgque_writer from pgque_test_producer;
    drop role pgque_test_producer;
  end if;
  if exists (select 1 from pg_roles where rolname = 'pgque_test_consumer') then
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

-- NOLOGIN: `set role` does not require LOGIN; a NOLOGIN role can't be
-- authenticated against directly even if the test aborts before cleanup.
create role pgque_test_producer nologin;
grant pgque_writer to pgque_test_producer;

create role pgque_test_consumer nologin;
grant pgque_reader to pgque_test_consumer;

select pgque.create_queue('test_e2e_split');

-- Subscribe under the consumer role itself (subscribe is on pgque_reader
-- per the producer/consumer split).
set role pgque_test_consumer;
select pgque.subscribe('test_e2e_split', 'e2e_consumer');
reset role;

-- Producer: send + send_batch.
set role pgque_test_producer;

do $$
declare
  v_ids bigint[];
begin
  perform pgque.send('test_e2e_split', 'split.single', '{"who":"producer","seq":1}'::jsonb);
  perform pgque.send('test_e2e_split', 'split.single_text', 'opaque-text'::text);

  v_ids := pgque.send_batch('test_e2e_split', 'split.batch_json',
    array['{"n":1}'::jsonb, '{"n":2}'::jsonb, '{"n":3}'::jsonb]);
  assert cardinality(v_ids) = 3, 'send_batch (jsonb[]) must return 3 ids';

  v_ids := pgque.send_batch('test_e2e_split', 'split.batch_text',
    array['a', 'b']::text[]);
  assert cardinality(v_ids) = 2, 'send_batch (text[]) must return 2 ids';

  raise notice 'PASS: producer (pgque_writer) can send + send_batch';
end $$;

-- Producer must NOT be able to receive / ack / subscribe.
do $$
declare
  v_ok boolean;
begin
  v_ok := false;
  begin
    perform * from pgque.receive('test_e2e_split', 'e2e_consumer', 1);
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'producer was able to call receive (regression)';

  v_ok := false;
  begin
    perform pgque.ack(1::bigint);
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'producer was able to call ack (regression)';

  v_ok := false;
  begin
    perform pgque.subscribe('test_e2e_split', 'producer_sub_attempt');
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'producer was able to call subscribe (regression)';

  raise notice 'PASS: producer (pgque_writer) is denied receive/ack/subscribe';
end $$;

reset role;

-- Admin: ticker so the events become visible. ticker / force_next_tick are
-- admin-only; in production this is pg_cron or a dedicated service role.
select pgque.force_next_tick('test_e2e_split');
select pgque.ticker('test_e2e_split');

-- Consumer: receive + ack.
set role pgque_test_consumer;

do $$
declare
  v_msg         pgque.message;
  v_count       int := 0;
  v_batch       bigint;
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

  assert v_count = 7, format('consumer must receive 7 produced messages, got %s', v_count);
  assert v_seen_single, 'expected split.single message';
  assert v_seen_text,   'expected split.single_text message';
  assert v_seen_batch = 5, format('expected 5 batch messages, saw %s', v_seen_batch);

  assert pgque.ack(v_batch) = 1, 'consumer ack must return 1 on success';
  raise notice 'PASS: consumer (pgque_reader) can receive + ack the full batch';
end $$;

-- Consumer must NOT be able to send.
do $$
declare
  v_ok boolean;
begin
  v_ok := false;
  begin
    perform pgque.send('test_e2e_split', 'split.illegal', '{"who":"consumer"}'::jsonb);
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'consumer was able to call send (regression)';

  v_ok := false;
  begin
    perform pgque.send_batch('test_e2e_split', 'split.illegal',
      array['{"n":1}'::jsonb]);
  exception when insufficient_privilege then v_ok := true; end;
  assert v_ok, 'consumer was able to call send_batch (regression)';

  raise notice 'PASS: consumer (pgque_reader) is denied send/send_batch';
end $$;

reset role;

set role pgque_test_consumer;
select pgque.unsubscribe('test_e2e_split', 'e2e_consumer');
reset role;

select pgque.drop_queue('test_e2e_split', true);
drop role pgque_test_producer;
drop role pgque_test_consumer;

\echo 'PASS: test_e2e_role_split'
