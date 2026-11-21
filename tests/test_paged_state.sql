-- test_paged_state.sql -- durable paging state and private helper contracts
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

do $$
begin
  perform pgque.create_queue('paged_state_q');
  perform pgque.register_consumer('paged_state_q', 'normal');
  perform pgque.register_consumer('paged_state_q', 'victim');
  perform pgque.register_consumer('paged_state_q', 'destination');
end $$;

-- Schema contract: the paging row belongs to one concrete subscription and
-- application roles cannot mutate it directly.
do $$
declare
  v_fk_delete_action "char";
begin
  assert to_regclass('pgque.page_state') is not null,
    'page_state table must exist';

  select c.confdeltype
  into v_fk_delete_action
  from pg_constraint as c
  where
    c.conrelid = 'pgque.page_state'::regclass
    and c.contype = 'f';

  assert v_fk_delete_action = 'c',
    'page_state subscription foreign key must use ON DELETE CASCADE';
  assert not has_table_privilege('pgque_reader', 'pgque.page_state', 'INSERT'),
    'pgque_reader must not insert page_state';
  assert not has_table_privilege('pgque_writer', 'pgque.page_state', 'UPDATE'),
    'pgque_writer must not update page_state';
  assert not has_table_privilege('pgque_admin', 'pgque.page_state', 'DELETE'),
    'pgque_admin must not delete page_state';
end $$;

-- _assert_unpaged locks/resolves the active subscription by batch id and
-- rejects paging state even between pages (pending_token is null).
do $$
declare
  v_queue_id int4;
  v_consumer_id int4;
  v_caught boolean := false;
begin
  select s.sub_queue, s.sub_consumer
  into v_queue_id, v_consumer_id
  from pgque.subscription as s
  inner join pgque.queue as q on q.queue_id = s.sub_queue
  inner join pgque.consumer as c on c.co_id = s.sub_consumer
  where
    q.queue_name = 'paged_state_q'
    and c.co_name = 'normal';

  update pgque.subscription
  set
    sub_batch = 81001,
    sub_next_tick = sub_last_tick
  where
    sub_queue = v_queue_id
    and sub_consumer = v_consumer_id;

  insert into pgque.page_state (
    queue_id,
    consumer_id,
    active_batch_id,
    mode,
    acked_page_number
  )
  values (
    v_queue_id,
    v_consumer_id,
    81001,
    'normal',
    1
  );

  begin
    perform pgque._assert_unpaged(81001);
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught,
    '_assert_unpaged must reject active paging state between pages';

  perform pgque._assert_unpaged(81999);
end $$;

-- Clearing active state preserves the retained acknowledgement receipt.
do $$
declare
  v_state pgque.page_state%rowtype;
begin
  update pgque.page_state
  set
    prev_tick_id = 10,
    next_tick_id = 11,
    acked_event_id = 42,
    pending_token = '00000000-0000-0000-0000-000000000011',
    pending_last_event_id = 43,
    pending_page_size = 2,
    pending_final = false,
    pending_worker = 'worker-a',
    pending_lease_ttl = interval '60 seconds',
    pending_lease_until = clock_timestamp() + interval '60 seconds',
    partition_co_name = 'partition-consumer',
    partition_slot = 2,
    partition_n = 4,
    partition_epoch = 7,
    last_ack_token = '00000000-0000-0000-0000-000000000012',
    last_ack_worker = 'worker-a',
    last_ack_request = '[{"msg_id":"42"}]'::jsonb,
    last_ack_finished = false
  where active_batch_id = 81001;

  select queue_id, consumer_id
  into v_state.queue_id, v_state.consumer_id
  from pgque.page_state
  where active_batch_id = 81001;

  perform pgque._clear_paged_active(v_state.queue_id, v_state.consumer_id);

  select *
  into strict v_state
  from pgque.page_state
  where
    queue_id = v_state.queue_id
    and consumer_id = v_state.consumer_id;

  assert v_state.active_batch_id is null
    and v_state.prev_tick_id is null
    and v_state.next_tick_id is null
    and v_state.mode is null
    and v_state.acked_event_id is null
    and v_state.acked_page_number = 0
    and v_state.pending_token is null
    and v_state.pending_last_event_id is null
    and v_state.pending_page_size is null
    and v_state.pending_final is null
    and v_state.pending_worker is null
    and v_state.pending_lease_ttl is null
    and v_state.pending_lease_until is null
    and v_state.partition_co_name is null
    and v_state.partition_slot is null
    and v_state.partition_n is null
    and v_state.partition_epoch is null,
    '_clear_paged_active must reset every active field';
  assert v_state.last_ack_token = '00000000-0000-0000-0000-000000000012'
    and v_state.last_ack_worker = 'worker-a'
    and v_state.last_ack_request = '[{"msg_id":"42"}]'::jsonb
    and v_state.last_ack_finished = false,
    '_clear_paged_active must preserve the acknowledgement receipt';
end $$;

-- Cooperative transfer copies only active progress, gives the destination a
-- fresh batch id, preserves both receipts, and leaves the outstanding page
-- boundary available for reissue under a fresh token.
do $$
declare
  v_queue_id int4;
  v_victim_id int4;
  v_destination_id int4;
  v_victim pgque.page_state%rowtype;
  v_destination pgque.page_state%rowtype;
begin
  select q.queue_id into v_queue_id
  from pgque.queue as q
  where q.queue_name = 'paged_state_q';

  select c.co_id into v_victim_id
  from pgque.consumer as c
  where c.co_name = 'victim';

  select c.co_id into v_destination_id
  from pgque.consumer as c
  where c.co_name = 'destination';

  insert into pgque.page_state (
    queue_id, consumer_id, active_batch_id, prev_tick_id, next_tick_id,
    mode, acked_event_id, acked_page_number, pending_token,
    pending_last_event_id, pending_page_size, pending_final, pending_worker,
    pending_lease_ttl, pending_lease_until, last_ack_token,
    last_ack_worker, last_ack_request, last_ack_finished
  )
  values (
    v_queue_id, v_victim_id, 82001, 20, 21,
    'coop', 100, 3, '00000000-0000-0000-0000-000000000021',
    105, 5, true, 'dead-worker', interval '90 seconds',
    clock_timestamp() - interval '1 second',
    '00000000-0000-0000-0000-000000000022',
    'victim-worker', '[]'::jsonb, false
  );

  insert into pgque.page_state (
    queue_id, consumer_id, last_ack_token, last_ack_worker,
    last_ack_request, last_ack_finished
  )
  values (
    v_queue_id, v_destination_id,
    '00000000-0000-0000-0000-000000000023',
    'destination-worker', '[{"msg_id":"9"}]'::jsonb, true
  );

  perform pgque._transfer_paged_active(
    v_queue_id,
    v_victim_id,
    v_destination_id,
    82002
  );

  select * into strict v_victim
  from pgque.page_state
  where queue_id = v_queue_id and consumer_id = v_victim_id;

  select * into strict v_destination
  from pgque.page_state
  where queue_id = v_queue_id and consumer_id = v_destination_id;

  assert v_victim.active_batch_id is null
    and v_victim.acked_page_number = 0
    and v_victim.pending_last_event_id is null,
    'transfer must clear victim active state';
  assert v_victim.last_ack_token = '00000000-0000-0000-0000-000000000022'
    and v_victim.last_ack_worker = 'victim-worker',
    'transfer must preserve victim receipt';

  assert v_destination.active_batch_id = 82002
    and v_destination.prev_tick_id = 20
    and v_destination.next_tick_id = 21
    and v_destination.mode = 'coop'
    and v_destination.acked_event_id = 100
    and v_destination.acked_page_number = 3,
    'transfer must copy active batch progress with the new batch id';
  assert v_destination.pending_token is null
    and v_destination.pending_worker is null
    and v_destination.pending_lease_until is null,
    'transfer must invalidate pending ownership';
  assert v_destination.pending_last_event_id = 105
    and v_destination.pending_page_size = 5
    and v_destination.pending_final = true
    and v_destination.pending_lease_ttl = interval '90 seconds',
    'transfer must preserve the outstanding page boundary for reissue';
  assert v_destination.last_ack_token = '00000000-0000-0000-0000-000000000023'
    and v_destination.last_ack_worker = 'destination-worker'
    and v_destination.last_ack_finished = true,
    'transfer must preserve destination receipt';
end $$;

-- Subscription deletion owns lifecycle cleanup through the composite FK.
do $$
declare
  v_queue_id int4;
  v_consumer_id int4;
begin
  select s.sub_queue, s.sub_consumer
  into v_queue_id, v_consumer_id
  from pgque.subscription as s
  inner join pgque.queue as q on q.queue_id = s.sub_queue
  inner join pgque.consumer as c on c.co_id = s.sub_consumer
  where
    q.queue_name = 'paged_state_q'
    and c.co_name = 'destination';

  delete from pgque.subscription
  where
    sub_queue = v_queue_id
    and sub_consumer = v_consumer_id;

  assert not exists (
    select 1
    from pgque.page_state
    where queue_id = v_queue_id and consumer_id = v_consumer_id
  ), 'subscription delete must cascade to page_state';
end $$;

do $$
begin
  assert not has_function_privilege(
    'pgque_reader', 'pgque._assert_unpaged(bigint)', 'EXECUTE'),
    'pgque_reader must not execute _assert_unpaged';
  assert not has_function_privilege(
    'pgque_admin', 'pgque._clear_paged_active(integer,integer)', 'EXECUTE'),
    'pgque_admin must not execute _clear_paged_active';
  assert not has_function_privilege(
    'pgque_writer',
    'pgque._transfer_paged_active(integer,integer,integer,bigint)',
    'EXECUTE'),
    'pgque_writer must not execute _transfer_paged_active';
end $$;

select pgque.drop_queue('paged_state_q', true);

\echo 'PASS: test_paged_state'
