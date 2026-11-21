-- test_paged_history.sql -- Paged delivery across rotation and retry history
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

create temporary table _paged_history_expected (
  scenario text not null,
  ordinal int4 not null,
  msg_id bigint not null,
  type text not null,
  payload text not null,
  extra1 text,
  primary key (scenario, ordinal)
);

/*
 * Keep a logical batch partially acknowledged while the queue rotates to its
 * next event table. Subsequent pages must still resolve the original snapshot
 * and return the exact remaining identities in order.
 */
do $$
begin
  perform pgque.create_queue('paged_history_rotation');
  perform pgque.set_queue_config(
    'paged_history_rotation', 'rotation_period', '1 second');
  update pgque.queue
  set queue_switch_time = clock_timestamp() - interval '2 seconds'
  where queue_name = 'paged_history_rotation';
end $$;

-- Establish an initial committed rotation boundary before subscribing. The
-- batch below is then new enough that a second rotation may proceed while its
-- page checkpoint keeps the event-table history pinned.
do $$
begin
  perform pgque.maint_rotate_tables_step1('paged_history_rotation');
end $$;
do $$
declare
  v_cur_table int4;
begin
  perform pgque.maint_rotate_tables_step2();
  select queue_cur_table into strict v_cur_table
  from pgque.queue
  where queue_name = 'paged_history_rotation';
  assert v_cur_table = 1, 'history fixture must establish event table 1';
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_history_rotation');
  perform pgque.ticker('paged_history_rotation');
end $$;

do $$
declare
  v_id bigint;
begin
  perform pgque.subscribe('paged_history_rotation', 'c1');

  v_id := pgque.send(
    'paged_history_rotation', 'rotation.one', 'payload-one', 'tenant-a');
  insert into _paged_history_expected
  values ('rotation', 1, v_id, 'rotation.one', 'payload-one', 'tenant-a');

  v_id := pgque.send(
    'paged_history_rotation', 'rotation.two', 'payload-two', 'tenant-b');
  insert into _paged_history_expected
  values ('rotation', 2, v_id, 'rotation.two', 'payload-two', 'tenant-b');

  v_id := pgque.send(
    'paged_history_rotation', 'rotation.three', 'payload-three', 'tenant-c');
  insert into _paged_history_expected
  values ('rotation', 3, v_id, 'rotation.three', 'payload-three', 'tenant-c');
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_history_rotation');
  perform pgque.ticker('paged_history_rotation');
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_message pgque.message;
  v_expected _paged_history_expected%rowtype;
  v_ack record;
begin
  v_page := pgque.receive_page(
    'paged_history_rotation', 'c1', 'worker-rotation', 1, interval '1 minute');
  assert v_page.status = 'page' and v_page.page_number = 1
    and not v_page.is_last and cardinality(v_page.messages) = 1,
    'rotation fixture must issue a nonterminal first page';
  v_message := (v_page.messages)[1];
  select * into strict v_expected
  from _paged_history_expected
  where scenario = 'rotation' and ordinal = 1;
  assert v_message.msg_id = v_expected.msg_id
    and v_message.type = v_expected.type
    and v_message.payload = v_expected.payload
    and v_message.extra1 = v_expected.extra1,
    'first page identity must match the original event';

  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-rotation');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'first page must checkpoint without finishing the logical batch';
end $$;

-- Rotation steps intentionally run in separate transactions.
do $$
begin
  update pgque.queue
  set queue_switch_time = clock_timestamp() - interval '2 seconds'
  where queue_name = 'paged_history_rotation';
  perform pgque.maint_rotate_tables_step1('paged_history_rotation');
end $$;
do $$
declare
  v_cur_table int4;
begin
  perform pgque.maint_rotate_tables_step2();
  select queue_cur_table into strict v_cur_table
  from pgque.queue
  where queue_name = 'paged_history_rotation';
  assert v_cur_table = 2,
    'partial paging must allow safe rotation from event table 1 to 2';
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_message pgque.message;
  v_expected _paged_history_expected%rowtype;
  v_ack record;
  v_ordinal int4;
begin
  for v_ordinal in 2..3 loop
    v_page := pgque.receive_page(
      'paged_history_rotation', 'c1', 'worker-rotation', 1, interval '1 minute');
    assert v_page.status = 'page'
      and v_page.page_number = v_ordinal
      and cardinality(v_page.messages) = 1,
      format('rotation page %s must contain exactly one event', v_ordinal);
    assert v_page.is_last = (v_ordinal = 3),
      format('rotation page %s terminal flag is wrong', v_ordinal);

    v_message := (v_page.messages)[1];
    select * into strict v_expected
    from _paged_history_expected
    where scenario = 'rotation' and ordinal = v_ordinal;
    assert v_message.msg_id = v_expected.msg_id
      and v_message.type = v_expected.type
      and v_message.payload = v_expected.payload
      and v_message.extra1 = v_expected.extra1,
      format('rotation page %s identity changed across table rotation', v_ordinal);

    select * into v_ack
    from pgque.ack_page(v_page.page_token, 'worker-rotation');
    assert v_ack.batch_finished = (v_ordinal = 3),
      format('rotation page %s finish state is wrong', v_ordinal);
  end loop;
end $$;

/*
 * Route one exact event from a valid page to retry, commit maintenance, and
 * verify that its reused event ID is delivered once with canonical identity.
 * A normal retry must not trigger the duplicate-ID ambiguity guard.
 */
do $$
declare
  v_id bigint;
begin
  perform pgque.create_queue('paged_history_retry');
  perform pgque.subscribe('paged_history_retry', 'c1');

  v_id := pgque.send(
    'paged_history_retry', 'retry.ok-one', 'payload-ok-one', 'tenant-ok-1');
  insert into _paged_history_expected
  values ('retry', 1, v_id, 'retry.ok-one', 'payload-ok-one', 'tenant-ok-1');

  v_id := pgque.send(
    'paged_history_retry', 'retry.again', 'payload-retry', 'tenant-retry');
  insert into _paged_history_expected
  values ('retry', 2, v_id, 'retry.again', 'payload-retry', 'tenant-retry');

  v_id := pgque.send(
    'paged_history_retry', 'retry.ok-three', 'payload-ok-three', 'tenant-ok-3');
  insert into _paged_history_expected
  values ('retry', 3, v_id, 'retry.ok-three', 'payload-ok-three', 'tenant-ok-3');
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_history_retry');
  perform pgque.ticker('paged_history_retry');
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_message pgque.message;
  v_expected _paged_history_expected%rowtype;
  v_failures jsonb;
  v_ack record;
  v_ordinal int4 := 0;
begin
  v_page := pgque.receive_page(
    'paged_history_retry', 'c1', 'worker-retry-history', 3, interval '1 minute');
  assert v_page.status = 'page' and v_page.is_last
    and cardinality(v_page.messages) = 3,
    'retry fixture must issue one terminal three-event page';

  foreach v_message in array v_page.messages loop
    v_ordinal := v_ordinal + 1;
    select * into strict v_expected
    from _paged_history_expected
    where scenario = 'retry' and ordinal = v_ordinal;
    assert v_message.msg_id = v_expected.msg_id
      and v_message.type = v_expected.type
      and v_message.payload = v_expected.payload
      and v_message.extra1 = v_expected.extra1,
      format('original retry fixture identity %s is wrong', v_ordinal);
  end loop;

  select jsonb_build_array(jsonb_build_object(
    'msg_id', msg_id::text,
    'retry_after_seconds', 0,
    'reason', 'history retry'))
  into strict v_failures
  from _paged_history_expected
  where scenario = 'retry' and ordinal = 2;

  select * into v_ack
  from pgque.ack_page(
    v_page.page_token, 'worker-retry-history', v_failures);
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'failure routing and terminal page ack must commit atomically';
end $$;

do $$
begin
  perform pgque.maint_retry_events();
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_history_retry');
  perform pgque.ticker('paged_history_retry');
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_message pgque.message;
  v_expected _paged_history_expected%rowtype;
  v_ack record;
begin
  -- A valid retry reuses ev_id, but only one occurrence belongs to this new
  -- snapshot window; receive_page must not raise the duplicate-ID guard.
  v_page := pgque.receive_page(
    'paged_history_retry', 'c1', 'worker-retry-history', 1, interval '1 minute');
  assert v_page.status = 'page' and v_page.is_last
    and cardinality(v_page.messages) = 1,
    'retry maintenance must redeliver exactly one event';

  v_message := (v_page.messages)[1];
  select * into strict v_expected
  from _paged_history_expected
  where scenario = 'retry' and ordinal = 2;
  assert v_message.msg_id = v_expected.msg_id
    and v_message.type = v_expected.type
    and v_message.payload = v_expected.payload
    and v_message.extra1 = v_expected.extra1
    and v_message.retry_count = 1,
    'retry redelivery must preserve canonical identity and increment retry count';

  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-retry-history');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'retried event page must finish normally';
end $$;

do $$
begin
  perform pgque.unsubscribe('paged_history_rotation', 'c1');
  perform pgque.unsubscribe('paged_history_retry', 'c1');
  perform pgque.drop_queue('paged_history_rotation');
  perform pgque.drop_queue('paged_history_retry');
end $$;

drop table _paged_history_expected;

\echo 'PASS: test_paged_history'
