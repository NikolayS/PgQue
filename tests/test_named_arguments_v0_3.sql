\set ON_ERROR_STOP on

-- Stable named-argument contract for public partition/idempotency APIs.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create temporary table named_arg_contract (
  signature regprocedure primary key,
  expected_names text[] not null
);

insert into named_arg_contract (signature, expected_names)
values
  ('pgque.send(text,text,text,text)'::regprocedure,
    array['queue_name', 'type_name', 'payload', 'partition_key']),
  ('pgque.send(text,text,jsonb,text)'::regprocedure,
    array['queue_name', 'type_name', 'payload', 'partition_key']),
  ('pgque.subscribe_partitioned(text,text,integer)'::regprocedure,
    array['queue_name', 'consumer', 'n']),
  ('pgque.subscribe_slot(text,text,integer,integer)'::regprocedure,
    array['queue_name', 'consumer', 'slot', 'n']),
  ('pgque.unsubscribe_slot(text,text,integer)'::regprocedure,
    array['queue_name', 'consumer', 'slot']),
  ('pgque.claim_slot(text,text,integer,text,interval)'::regprocedure,
    array['queue_name', 'consumer', 'slot', 'worker', 'ttl']),
  ('pgque.release_slot(text,text,integer,text)'::regprocedure,
    array['queue_name', 'consumer', 'slot', 'worker']),
  ('pgque.receive_partitioned(text,text,integer,integer,text,integer)'::regprocedure,
    array['queue_name', 'consumer', 'slot', 'n', 'worker', 'max_return']),
  ('pgque.ack_partitioned(text,text,integer,integer,text)'::regprocedure,
    array['queue_name', 'consumer', 'slot', 'n', 'worker']),
  ('pgque.nack_partitioned(text,text,integer,integer,text,pgque.message,interval,text)'::regprocedure,
    array['queue_name', 'consumer', 'slot', 'n', 'worker', 'msg', 'retry_after', 'reason']),
  ('pgque.send_idem(text,text,text,text,interval,text)'::regprocedure,
    array['queue_name', 'type_name', 'payload', 'idem_key', 'ttl', 'partition_key']),
  ('pgque.send_idem(text,text,jsonb,text,interval,text)'::regprocedure,
    array['queue_name', 'type_name', 'payload', 'idem_key', 'ttl', 'partition_key']),
  ('pgque.maint_idem(text)'::regprocedure,
    array['queue_name']);

do $$
declare
  v_actual text[];
  v_contract record;
begin
  for v_contract in
    select signature, expected_names
    from named_arg_contract
    order by signature::text
  loop
    select p.proargnames[1:p.pronargs] into v_actual
    from pg_proc as p
    where p.oid = v_contract.signature;
    assert v_actual = v_contract.expected_names,
      format('%s input names: expected %s, got %s',
        v_contract.signature, v_contract.expected_names, v_actual);
  end loop;
end $$;

do $$
begin
  perform pgque.create_queue('named_v03_q');
  perform pgque.subscribe_partitioned(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    n => 2);
  perform pgque.subscribe_slot(
    queue_name => 'named_v03_q',
    consumer => 'repair',
    slot => 0,
    n => 1);
  perform pgque.unsubscribe_slot(
    queue_name => 'named_v03_q',
    consumer => 'repair',
    slot => 0);
end $$;

do $$
declare
  v_key_0 text;
  v_key_1 text;
  v_result record;
begin
  select format('named-key-%s', g) into v_key_0
  from generate_series(1, 10000) as g
  where (pg_catalog.hashtextextended(format('named-key-%s', g), 0) % 2 + 2) % 2 = 0
  limit 1;
  select format('named-key-%s', g) into v_key_1
  from generate_series(1, 10000) as g
  where (pg_catalog.hashtextextended(format('named-key-%s', g), 0) % 2 + 2) % 2 = 1
  limit 1;

  perform pgque.send(
    queue_name => 'named_v03_q',
    type_name => 'named.jsonb',
    payload => '{"source":"jsonb"}'::jsonb,
    partition_key => v_key_0);
  perform pgque.send(
    queue_name => 'named_v03_q',
    type_name => 'named.text',
    payload => '{"source":"text"}'::text,
    partition_key => v_key_1);

  select * into v_result
  from pgque.send_idem(
    queue_name => 'named_v03_q',
    type_name => 'named.idem-text',
    payload => '{"source":"idem-text"}'::text,
    idem_key => 'named-idem-text',
    ttl => interval '1 hour',
    partition_key => v_key_0);
  assert not v_result.deduped,
    'named text send_idem call must insert on first use';

  select * into v_result
  from pgque.send_idem(
    queue_name => 'named_v03_q',
    type_name => 'named.idem-jsonb',
    payload => '{"source":"idem-jsonb"}'::jsonb,
    idem_key => 'named-idem-jsonb',
    partition_key => v_key_1);
  assert not v_result.deduped,
    'named jsonb send_idem call must resolve with default ttl';

  select * into v_result
  from pgque.send_idem(
    queue_name => 'named_v03_q',
    type_name => 'named.idem-defaults',
    payload => '{"source":"idem-defaults"}'::text,
    idem_key => 'named-idem-defaults');
  assert not v_result.deduped,
    'named send_idem call must resolve with default ttl and partition_key';

  assert pgque.maint_idem(queue_name => 'named_v03_q') = 0,
    'named maint_idem call must resolve';
end $$;

do $$
begin
  perform pgque.force_next_tick('named_v03_q');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  assert pgque.claim_slot(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 0,
    worker => 'named-worker') is not null,
    'named claim_slot call must resolve with default ttl';

  select * into v_msg
  from pgque.receive_partitioned(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 0,
    n => 2,
    worker => 'named-worker');
  assert v_msg.msg_id is not null,
    'named receive_partitioned call must resolve with default max_return';
  assert pgque.nack_partitioned(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 0,
    n => 2,
    worker => 'named-worker',
    msg => v_msg) = 1,
    'named nack_partitioned call must resolve with retry defaults';
  assert pgque.ack_partitioned(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 0,
    n => 2,
    worker => 'named-worker') = 1,
    'named ack_partitioned call must resolve';
  assert pgque.release_slot(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 0,
    worker => 'named-worker'),
    'named release_slot call must resolve';

  assert pgque.claim_slot(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 1,
    worker => 'named-worker',
    ttl => interval '30 seconds') is not null,
    'named claim_slot call must accept explicit ttl';
  select * into v_msg
  from pgque.receive_partitioned(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 1,
    n => 2,
    worker => 'named-worker',
    max_return => 100);
  assert v_msg.msg_id is not null,
    'named receive_partitioned call must accept explicit max_return';
  assert pgque.ack_partitioned(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 1,
    n => 2,
    worker => 'named-worker') = 1,
    'named ack_partitioned call must finish slot 1';
  perform pgque.release_slot(
    queue_name => 'named_v03_q',
    consumer => 'workers',
    slot => 1,
    worker => 'named-worker');

  perform pgque.unsubscribe_slot(
    queue_name => 'named_v03_q', consumer => 'workers', slot => 0);
  perform pgque.unsubscribe_slot(
    queue_name => 'named_v03_q', consumer => 'workers', slot => 1);
  perform pgque.drop_queue('named_v03_q');
  raise notice 'PASS: stable named arguments for all 0.3 public APIs';
end $$;
