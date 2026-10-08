\set ON_ERROR_STOP on

-- Verify alpha function recreation preserves state, owners, and grants.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_function record;
  v_new_oid oid;
  v_new_owner name;
begin
  for v_function in
    select *
    from public.upgrade_v03_named_functions
    order by signature
  loop
    v_new_oid := to_regprocedure(v_function.signature)::oid;
    select pg_get_userbyid(p.proowner) into v_new_owner
    from pg_proc as p
    where p.oid = v_new_oid;

    assert v_new_oid <> v_function.function_oid,
      format('%s must be dropped and recreated to rename inputs', v_function.signature);
    assert v_new_owner = v_function.owner_name,
      format('%s owner changed from %s to %s',
        v_function.signature, v_function.owner_name, v_new_owner);
    assert has_function_privilege(
      'pgque_reader', v_new_oid, 'execute') = v_function.reader_execute,
      format('%s pgque_reader grant changed', v_function.signature);
    assert has_function_privilege(
      'pgque_writer', v_new_oid, 'execute') = v_function.writer_execute,
      format('%s pgque_writer grant changed', v_function.signature);
    assert has_function_privilege(
      'pgque_admin', v_new_oid, 'execute') = v_function.admin_execute,
      format('%s pgque_admin grant changed', v_function.signature);
  end loop;
end $$;

do $$
declare
  v_msg pgque.message;
  v_seen_idem boolean := false;
  v_seen_partition boolean := false;
  v_state public.upgrade_v03_named_state%rowtype;
begin
  select * into v_state
  from public.upgrade_v03_named_state;

  assert pgque.claim_slot(
    queue_name => 'upgrade_v03_named_q',
    consumer => 'workers',
    slot => 0,
    worker => 'upgrade-worker') is not null,
    'upgraded named claim_slot must resolve';
  for v_msg in
    select *
    from pgque.receive_partitioned(
      queue_name => 'upgrade_v03_named_q',
      consumer => 'workers',
      slot => 0,
      n => 2,
      worker => 'upgrade-worker')
  loop
    v_seen_partition := v_seen_partition
      or v_msg.msg_id = v_state.partition_event_id;
    v_seen_idem := v_seen_idem
      or v_msg.msg_id = v_state.idem_event_id;
  end loop;
  assert v_seen_partition and v_seen_idem,
    'alpha partition and idempotency events must survive function recreation';
  assert pgque.ack_partitioned(
    queue_name => 'upgrade_v03_named_q',
    consumer => 'workers',
    slot => 0,
    n => 2,
    worker => 'upgrade-worker') = 1,
    'upgraded named ack_partitioned must resolve';
  perform pgque.release_slot(
    queue_name => 'upgrade_v03_named_q',
    consumer => 'workers',
    slot => 0,
    worker => 'upgrade-worker');
end $$;

do $$
declare
  v_result record;
  v_state public.upgrade_v03_named_state%rowtype;
begin
  select * into v_state
  from public.upgrade_v03_named_state;
  select * into v_result
  from pgque.send_idem(
    queue_name => 'upgrade_v03_named_q',
    type_name => 'upgrade.idem',
    payload => '{}'::text,
    idem_key => 'upgrade-idem-key',
    ttl => interval '1 hour',
    partition_key => v_state.partition_key);
  assert v_result.deduped and v_result.event_id = v_state.idem_event_id,
    'alpha idempotency claim must survive named-argument function recreation';

  perform pgque.subscribe_partitioned(
    queue_name => 'upgrade_v03_named_q',
    consumer => 'atomic-workers',
    n => 2);
  assert (
    select count(*) = 2 and bool_and(subscribed)
    from pgque.partition_slot_status
    where queue_name = 'upgrade_v03_named_q'
      and consumer = 'atomic-workers'
  ), 'new subscribe_partitioned named arguments must resolve after alpha upgrade';

  perform pgque.unsubscribe_slot(
    queue_name => 'upgrade_v03_named_q', consumer => 'atomic-workers', slot => 0);
  perform pgque.unsubscribe_slot(
    queue_name => 'upgrade_v03_named_q', consumer => 'atomic-workers', slot => 1);
  perform pgque.unsubscribe_slot(
    queue_name => 'upgrade_v03_named_q', consumer => 'workers', slot => 0);
  perform pgque.unsubscribe_slot(
    queue_name => 'upgrade_v03_named_q', consumer => 'workers', slot => 1);
  perform pgque.drop_queue('upgrade_v03_named_q');

  drop table public.upgrade_v03_named_state;
  drop table public.upgrade_v03_named_functions;
  raise notice 'PASS: alpha named-argument upgrade preserved state, owners, and grants';
end $$;
