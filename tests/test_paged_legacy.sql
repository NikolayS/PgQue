-- test_paged_legacy.sql -- Legacy mutation guards for active paged batches
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

do $$
begin
  perform pgque.create_queue('paged_legacy_q');
  perform pgque.register_consumer('paged_legacy_q', 'c1');
  perform pgque.insert_event('paged_legacy_q', 'legacy.guard', 'payload');
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_legacy_q');
  perform pgque.ticker('paged_legacy_q');
end $$;

do $$
declare
  v_batch_id bigint;
  v_consumer_id int4;
  v_event_id bigint;
  v_queue_id int4;
  v_tick_id bigint;
  v_caught boolean;
begin
  v_batch_id := pgque.next_batch('paged_legacy_q', 'c1');
  assert v_batch_id is not null, 'expected an active batch';

  select s.sub_queue, s.sub_consumer, s.sub_last_tick
  into v_queue_id, v_consumer_id, v_tick_id
  from pgque.subscription as s
  where s.sub_batch = v_batch_id;

  select ev_id into strict v_event_id
  from pgque.get_batch_events(v_batch_id);

  insert into pgque.page_state (
    queue_id, consumer_id, active_batch_id, mode, acked_page_number)
  values (v_queue_id, v_consumer_id, v_batch_id, 'normal', 0);

  -- The private core remains available to ack_page after page membership and
  -- ownership validation, without weakening any public wrapper.
  assert pgque._event_retry_core(
    v_batch_id, v_event_id, current_timestamp + interval '1 minute') = 1,
    'private retry core should preserve event_retry success semantics';
  assert pgque._nack_paged_event(
    v_batch_id,
    row(
      v_event_id, v_batch_id, null, null, null,
      null, null, null, null, null
    )::pgque.message,
    interval '1 minute',
    'paged failure') = 1,
    'paged nack helper should resolve the canonical event with bounded SQL';

  v_caught := false;
  begin
    perform pgque.event_retry(v_batch_id, v_event_id, current_timestamp);
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught, 'timestamptz event_retry must reject a paged batch';

  v_caught := false;
  begin
    perform pgque.event_retry(v_batch_id, v_event_id, 0);
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught, 'integer event_retry must reject a paged batch';

  v_caught := false;
  begin
    perform pgque.batch_retry(v_batch_id, 0);
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught, 'batch_retry must reject a paged batch';

  v_caught := false;
  begin
    perform pgque.register_consumer_at('paged_legacy_q', 'c1', v_tick_id);
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught, 'register_consumer_at cursor reset must reject a paged batch';

  -- Null tick is the existing no-op registration path, not a cursor move.
  assert pgque.register_consumer_at('paged_legacy_q', 'c1', null) = 0,
    'no-op register_consumer_at should remain allowed';

  v_caught := false;
  begin
    perform pgque.unregister_consumer('paged_legacy_q', 'c1');
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught, 'unregister_consumer must reject a paged batch';

  assert exists (
    select 1
    from pgque.subscription as s
    where s.sub_batch = v_batch_id
  ), 'rejected legacy mutations must preserve the subscription';

  delete from pgque.retry_queue
  where ev_queue = v_queue_id and ev_id = v_event_id;
  perform pgque._clear_paged_active(v_queue_id, v_consumer_id);
  perform pgque.finish_batch(v_batch_id);
end $$;

do $$
begin
  assert not has_function_privilege(
    'pgque_reader',
    'pgque._event_retry_core(bigint,bigint,timestamptz)',
    'EXECUTE'),
    'pgque_reader must not execute the private retry core';
  assert not has_function_privilege(
    'pgque_admin',
    'pgque._nack_paged_event(bigint,pgque.message,interval,text)',
    'EXECUTE'),
    'pgque_admin must not execute the private paged nack helper';

  perform pgque.unregister_consumer('paged_legacy_q', 'c1');
  perform pgque.drop_queue('paged_legacy_q');
end $$;

/* A cooperative main cursor reset must inspect active member pages. */
do $$
begin
  perform pgque.create_queue('paged_legacy_coop_q');
  perform pgque.register_subconsumer(
    'paged_legacy_coop_q', 'main_c', 'w1');
end $$;

do $$
declare
  v_batch_id bigint := 91001;
  v_main_tick bigint;
  v_member_id int4;
  v_queue_id int4;
  v_caught boolean := false;
begin
  select s.sub_queue, s.sub_consumer
  into strict v_queue_id, v_member_id
  from pgque.subscription as s
  inner join pgque.consumer as c on c.co_id = s.sub_consumer
  inner join pgque.queue as q on q.queue_id = s.sub_queue
  where
    q.queue_name = 'paged_legacy_coop_q'
    and c.co_name = 'main_c.w1';

  select s.sub_last_tick into strict v_main_tick
  from pgque.subscription as s
  inner join pgque.consumer as c on c.co_id = s.sub_consumer
  where
    s.sub_queue = v_queue_id
    and c.co_name = 'main_c';

  update pgque.subscription
  set
    sub_last_tick = v_main_tick,
    sub_next_tick = v_main_tick,
    sub_batch = v_batch_id
  where
    sub_queue = v_queue_id
    and sub_consumer = v_member_id;

  insert into pgque.page_state (
    queue_id, consumer_id, active_batch_id, mode, acked_page_number)
  values (v_queue_id, v_member_id, v_batch_id, 'coop', 0);

  begin
    perform pgque.register_consumer_at(
      'paged_legacy_coop_q', 'main_c', v_main_tick);
  exception when sqlstate '55000' then
    v_caught := true;
  end;
  assert v_caught,
    'cooperative main reset must reject an active member page';
  assert exists (
    select 1
    from pgque.subscription
    where
      sub_queue = v_queue_id
      and sub_consumer = v_member_id
      and sub_batch = v_batch_id
  ), 'rejected main reset must preserve the active member batch';

  perform pgque._clear_paged_active(v_queue_id, v_member_id);
  perform pgque._clear_member_cursor(v_queue_id, v_member_id);
end $$;

do $$
begin
  perform pgque.unregister_subconsumer(
    'paged_legacy_coop_q', 'main_c', 'w1');
  perform pgque.unregister_consumer('paged_legacy_coop_q', 'main_c');
  perform pgque.drop_queue('paged_legacy_coop_q');
end $$;

\echo 'PASS: test_paged_legacy'
