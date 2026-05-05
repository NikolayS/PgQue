\set ON_ERROR_STOP on

-- Test: ultrareview regressions for cooperative consumers.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- bug_003: cooperative next_batch_custom must support min_count/min_interval
-- without ambiguous OUT/local column names.
do $$
begin
  perform pgque.create_queue('coop_custom_tick');
  perform pgque.register_subconsumer('coop_custom_tick', 'main_c', 'w1');
end $$;

select pgque.send('coop_custom_tick', 't', 'custom-min-count');
select pgque.force_tick('coop_custom_tick');
select pgque.ticker('coop_custom_tick');

do $$
declare
  v_batch_id bigint;
  v_prev_tick bigint;
  v_next_tick bigint;
begin
  select batch_id, prev_tick_id, next_tick_id
  into v_batch_id, v_prev_tick, v_next_tick
  from pgque.next_batch_custom(
    'coop_custom_tick', 'main_c', 'w1', null, 1, null, null
  );

  assert v_batch_id is not null,
    'cooperative next_batch_custom(min_count) should allocate a batch';
  assert v_prev_tick is not null and v_next_tick is not null,
    'cooperative next_batch_custom(min_count) should return tick window';

  perform pgque.ack(v_batch_id);
end $$;

-- bug_001: forced unregister DLQ path must preserve original ev_txid like nack().
do $$
begin
  perform pgque.create_queue('coop_force_txid');
  update pgque.queue
  set queue_max_retries = 0
  where queue_name = 'coop_force_txid';
  perform pgque.register_subconsumer('coop_force_txid', 'main_c', 'w1');
end $$;

select pgque.send('coop_force_txid', 't', 'dlq-with-txid');
select pgque.force_tick('coop_force_txid');
select pgque.ticker('coop_force_txid');

do $$
declare
  v_msg pgque.message;
  v_original_txid xid8;
  v_dlq_txid xid8;
begin
  select *
  into v_msg
  from pgque.receive_coop('coop_force_txid', 'main_c', 'w1', 10)
  limit 1;

  select ev_txid::text::xid8
  into v_original_txid
  from pgque.get_batch_events(v_msg.batch_id)
  where ev_id = v_msg.msg_id;

  assert v_original_txid is not null,
    'test setup should see original ev_txid from active batch';

  assert pgque.unregister_subconsumer('coop_force_txid', 'main_c', 'w1', 1) = 1,
    'forced unregister should succeed';

  select dl.ev_txid
  into v_dlq_txid
  from pgque.dead_letter as dl
  join pgque.queue as q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'coop_force_txid'
    and dl.ev_id = v_msg.msg_id;

  assert v_dlq_txid = v_original_txid,
    'forced unregister DLQ should preserve ev_txid; got '
    || coalesce(v_dlq_txid::text, 'null')
    || ', expected ' || v_original_txid::text;
end $$;

-- REV high: normal consumers must not be silently converted to coop mains.
do $$
begin
  perform pgque.create_queue('coop_convert_guard');
  perform pgque.register_consumer('coop_convert_guard', 'normal_idle');

  begin
    perform pgque.register_subconsumer('coop_convert_guard', 'normal_idle', 'w1');
    raise exception 'expected normal consumer conversion rejection';
  exception when others then
    if sqlerrm = 'expected normal consumer conversion rejection' then raise; end if;
  end;

  assert pgque.register_subconsumer('coop_convert_guard', 'normal_idle', 'w1', true) = 1,
    'explicit normal-to-coop conversion should succeed';
end $$;

-- REV high: legacy unregister_consumer(main) must not cascade-delete members.
do $$
begin
  perform pgque.create_queue('coop_no_main_cascade');
  perform pgque.register_subconsumer('coop_no_main_cascade', 'cascade_main', 'w1');
  perform pgque.register_subconsumer('coop_no_main_cascade', 'cascade_main', 'w2');

  begin
    perform pgque.unregister_consumer('coop_no_main_cascade', 'cascade_main');
    raise exception 'expected cooperative main unregister rejection';
  exception when others then
    if sqlerrm = 'expected cooperative main unregister rejection' then raise; end if;
  end;

  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_no_main_cascade'
      and c.co_name = 'cascade_main.w1'
      and s.sub_role = 'coop_member'
  ), 'rejected main unregister must preserve member w1';
end $$;

-- bug_002: legacy unregister_consumer(member) must remove orphan consumer row.
do $$
declare
  v_member_consumer_id int4;
  v_still_exists boolean;
begin
  perform pgque.create_queue('coop_legacy_unreg');
  perform pgque.register_subconsumer('coop_legacy_unreg', 'legacy_main', 'w1');
  perform pgque.register_subconsumer('coop_legacy_unreg', 'legacy_main', 'w2');

  select co_id
  into v_member_consumer_id
  from pgque.consumer
  where co_name = 'legacy_main.w1';

  assert v_member_consumer_id is not null,
    'test setup should create dotted member consumer row';

  assert pgque.unregister_consumer('coop_legacy_unreg', 'legacy_main.w1') = 1,
    'legacy unregister_consumer(member) should remove one subconsumer';

  select exists (
    select 1
    from pgque.consumer
    where co_id = v_member_consumer_id
  )
  into v_still_exists;

  assert not v_still_exists,
    'legacy unregister_consumer(member) should delete orphan dotted consumer row';

  assert pgque.unregister_consumer('coop_legacy_unreg', 'legacy_main.w2') = 1,
    'legacy unregister_consumer(last member) should remove final subconsumer';

  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_legacy_unreg'
      and c.co_name = 'legacy_main'
      and s.sub_role = 'normal'
  ), 'legacy unregister_consumer(last member) should revert main to normal';
end $$;

-- merged_bug_001: forced unregister DLQ row must survive the function call.
-- event_dead() looks up dl_consumer_id via subscription.sub_batch, which
-- resolves to the coop_member's co_id. unregister_subconsumer then deletes
-- the member subscription and (when the member has no other subscriptions)
-- the pgque.consumer row. dead_letter.dl_consumer_id has on delete cascade,
-- so without a fix the freshly inserted DLQ row is wiped before commit.
-- Use unique consumer/subconsumer names so the member co_id is not shared
-- with sibling tests in this file -- sharing accidentally averts the cascade.
do $$
begin
  perform pgque.create_queue('coop_dlq_survive');
  update pgque.queue
  set queue_max_retries = 0
  where queue_name = 'coop_dlq_survive';
  perform pgque.register_subconsumer('coop_dlq_survive', 'dlq_survive_main', 'dlq_survive_w1');
end $$;

select pgque.send('coop_dlq_survive', 't', 'must-not-vanish');
select pgque.force_tick('coop_dlq_survive');
select pgque.ticker('coop_dlq_survive');

do $$
declare
  v_msg pgque.message;
  v_dlq_count int;
begin
  select *
  into v_msg
  from pgque.receive_coop('coop_dlq_survive', 'dlq_survive_main', 'dlq_survive_w1', 10)
  limit 1;

  assert v_msg.msg_id is not null,
    'test setup should yield a message';

  assert pgque.unregister_subconsumer('coop_dlq_survive', 'dlq_survive_main', 'dlq_survive_w1', 1) = 1,
    'forced unregister should succeed';

  select count(*)
  into v_dlq_count
  from pgque.dead_letter as dl
  join pgque.queue as q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'coop_dlq_survive'
    and dl.ev_id = v_msg.msg_id;

  assert v_dlq_count = 1,
    format('forced unregister DLQ row was lost to consumer-row cascade; expected 1, got %s', v_dlq_count);
end $$;

-- finish_batch: defensive guard against running on a coop_main row with a
-- non-null sub_batch (a state that should never arise in normal operation).
-- Force the corrupt state via direct UPDATE and verify finish_batch raises.
do $$
declare
  v_main_sub_id int4;
  v_main_co_id int4;
  v_queue_id int4;
  v_corrupt_batch bigint;
  v_caught text := null;
begin
  perform pgque.create_queue('coop_finish_guard');
  perform pgque.register_subconsumer('coop_finish_guard', 'finish_main', 'finish_w1');

  select s.sub_id, s.sub_consumer, s.sub_queue
  into v_main_sub_id, v_main_co_id, v_queue_id
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_finish_guard'
    and c.co_name = 'finish_main';
  assert v_main_sub_id is not null, 'test setup must locate coop_main row';

  v_corrupt_batch := nextval('pgque.batch_id_seq');
  update pgque.subscription
  set sub_batch = v_corrupt_batch
  where sub_queue = v_queue_id and sub_consumer = v_main_co_id;

  begin
    perform pgque.finish_batch(v_corrupt_batch);
    raise exception 'expected coop_main finish guard to fire';
  exception when others then
    if sqlerrm = 'expected coop_main finish guard to fire' then raise; end if;
    v_caught := sqlerrm;
  end;
  assert v_caught is not null and v_caught like 'cannot finish cooperative main consumer batch%',
    format('finish_batch should reject a coop_main batch; got %L', v_caught);

  -- Restore so unregister_subconsumer can clean up.
  update pgque.subscription
  set sub_batch = null
  where sub_queue = v_queue_id and sub_consumer = v_main_co_id;
end $$;

-- subscribe_subconsumer / unsubscribe_subconsumer are documented public-API
-- aliases for register_subconsumer / unregister_subconsumer. Make sure they
-- still resolve and round-trip so a future signature/grant drift on the
-- aliases is not silently invisible because no test calls them.
do $$
begin
  perform pgque.create_queue('coop_alias');
  assert pgque.subscribe_subconsumer('coop_alias', 'alias_main', 'alias_w1') = 1,
    'subscribe_subconsumer should register a fresh subconsumer';
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_alias'
      and c.co_name = 'alias_main.alias_w1'
      and s.sub_role = 'coop_member'
  ), 'subscribe_subconsumer must produce a coop_member row';

  assert pgque.unsubscribe_subconsumer('coop_alias', 'alias_main', 'alias_w1') = 1,
    'unsubscribe_subconsumer should remove the subconsumer';
  assert not exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_alias'
      and c.co_name = 'alias_main.alias_w1'
  ), 'unsubscribe_subconsumer must drop the subscription row';
end $$;

do $$
begin
  raise notice 'PASS: cooperative ultrareview regressions';
end $$;
