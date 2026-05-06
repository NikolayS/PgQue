\set ON_ERROR_STOP on

-- SQL-core regression tests for experimental cooperative consumers.

-- 1, 2, 12, 18, 19, 21, 22: registration, auto-create, normal isolation,
-- mixed normal/cooperative rejection, dotted names, heartbeat, migration default.
do $$
declare
  v_before timestamptz;
  v_after timestamptz;
begin
  perform pgque.create_queue('coop_meta');
  perform pgque.register_consumer('coop_meta', 'normal_c');

  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_meta'
      and c.co_name = 'normal_c'
      and s.sub_role = 'normal'
  ), 'existing/default subscription role must be normal';

  assert pgque.register_subconsumer('coop_meta', 'main_c', 'w1') = 1,
    'first subconsumer registration should create rows';
  assert pgque.register_subconsumer('coop_meta', 'main_c', 'w1') = 0,
    'register_subconsumer should be idempotent';

  begin
    perform pgque.register_subconsumer('coop_meta', 'bad.main', 'w1');
    raise exception 'expected dotted consumer rejection';
  exception when others then
    if sqlerrm = 'expected dotted consumer rejection' then raise; end if;
  end;

  begin
    perform pgque.register_subconsumer('coop_meta', 'main_c', 'bad.w1');
    raise exception 'expected dotted subconsumer rejection';
  exception when others then
    if sqlerrm = 'expected dotted subconsumer rejection' then raise; end if;
  end;

  select s.sub_active into v_before
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_meta'
    and c.co_name = 'main_c.w1';

  perform pg_sleep(0.01);
  assert pgque.touch_subconsumer('coop_meta', 'main_c', 'w1') = 1,
    'touch_subconsumer should update existing idle coop_member';
  assert pgque.touch_subconsumer('coop_meta', 'main_c', 'missing') = 0,
    'touch_subconsumer must not create missing subconsumer';

  select s.sub_active into v_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_meta'
    and c.co_name = 'main_c.w1';
  assert v_after > v_before, 'touch_subconsumer should refresh sub_active';

  begin
    perform pgque.receive('coop_meta', 'main_c', 10);
    raise exception 'expected normal receive rejection for coop_main';
  exception when others then
    if sqlerrm = 'expected normal receive rejection for coop_main' then raise; end if;
  end;

  assert (select count(*) from pgque.receive('coop_meta', 'normal_c', 10)) = 0,
    'other normal consumers must be unaffected';
end $$;

-- Auto-created cooperative receive should create rows and start from current tick.
do $$
begin
  perform pgque.create_queue('coop_auto');
end $$;
select count(*) as auto_empty from pgque.receive_coop('coop_auto', 'main_c', 'w1', 10);
do $$
begin
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_auto'
      and c.co_name = 'main_c'
      and s.sub_role = 'coop_main'
  ), 'receive_coop should auto-create coop_main';
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'coop_auto'
      and c.co_name = 'main_c.w1'
      and s.sub_role = 'coop_member'
  ), 'receive_coop should auto-create coop_member';
end $$;

-- 3, 4, 5: split batches, active subconsumer receives same batch, ack clears member cursor.
do $$
begin
  perform pgque.create_queue('coop_split');
  perform pgque.register_subconsumer('coop_split', 'main_c', 'w1');
  perform pgque.register_subconsumer('coop_split', 'main_c', 'w2');
end $$;
select pgque.send('coop_split', 't', 'one');
select pgque.force_tick('coop_split');
select pgque.ticker('coop_split');
select pgque.send('coop_split', 't', 'two');
select pgque.force_tick('coop_split');
select pgque.ticker('coop_split');
do $$
declare
  m1 pgque.message;
  m1_repeat pgque.message;
  m2 pgque.message;
  v_member record;
  v_main_before record;
  v_main_after record;
begin
  /*
   * Snapshot the coop_main row before any member allocates a batch, so we
   * can prove the main cursor advanced through the member's allocation.
   */
  select s.* into v_main_before
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_split'
    and c.co_name = 'main_c';

  select * into m1 from pgque.receive_coop('coop_split', 'main_c', 'w1', 10) limit 1;
  select * into m1_repeat from pgque.receive_coop('coop_split', 'main_c', 'w1', 10) limit 1;
  select * into m2 from pgque.receive_coop('coop_split', 'main_c', 'w2', 10) limit 1;

  assert m1.batch_id is not null, 'w1 should receive first batch';
  assert m1_repeat.batch_id = m1.batch_id,
    'repeated receive by active subconsumer should return same batch';
  assert m2.batch_id is not null and m2.batch_id <> m1.batch_id,
    'two subconsumers should split distinct batches';
  assert m1.payload = 'one' and m2.payload = 'two',
    'split batches should preserve cursor order';

  perform pgque.ack(m1.batch_id);

  select s.* into v_member
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_split'
    and c.co_name = 'main_c.w1';
  assert v_member.sub_batch is null, 'ack should clear coop_member batch';
  assert v_member.sub_last_tick is null, 'ack should not advance coop_member cursor';

  /*
   * coop_main invariants after a member's ack:
   *   - sub_batch must NEVER be non-null on a coop_main row;
   *   - sub_next_tick must be null on a coop_main row (members own the
   *     active batch, the main row carries only the group cursor);
   *   - sub_last_tick advanced when w1's batch was allocated (the cooperative
   *     mechanic moves the main cursor on member allocation, not on member
   *     ack), so it must be strictly greater than the pre-receive snapshot.
   */
  select s.* into v_main_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_split'
    and c.co_name = 'main_c';
  assert v_main_after.sub_batch is null,
    'coop_main must never have sub_batch set';
  assert v_main_after.sub_next_tick is null,
    'coop_main must never have sub_next_tick set';
  assert v_main_after.sub_last_tick > coalesce(v_main_before.sub_last_tick, 0),
    format('coop_main sub_last_tick should advance from %s, got %s',
           coalesce(v_main_before.sub_last_tick, 0), v_main_after.sub_last_tick);

  perform pgque.ack(m2.batch_id);
end $$;

-- 6, 15, 16: stale takeover uses fresh token and invalidates late ack/nack old token.
do $$
begin
  perform pgque.create_queue('coop_stale');
  perform pgque.register_subconsumer('coop_stale', 'main_c', 'w1');
  perform pgque.register_subconsumer('coop_stale', 'main_c', 'w2');
end $$;
select pgque.send('coop_stale', 't', 'stale-one');
select pgque.force_tick('coop_stale');
select pgque.ticker('coop_stale');
do $$
declare
  old_msg pgque.message;
  new_msg pgque.message;
  old_batch bigint;
  new_batch bigint;
  still_active bigint;
  v_late_nack_err text;
begin
  select * into old_msg from pgque.receive_coop('coop_stale', 'main_c', 'w1', 10) limit 1;
  old_batch := old_msg.batch_id;

  update pgque.subscription as s
     set sub_active = now() - interval '10 minutes'
    from pgque.queue as q
    cross join pgque.consumer as c
   where q.queue_name = 'coop_stale'
     and c.co_name = 'main_c.w1'
     and s.sub_queue = q.queue_id
     and s.sub_consumer = c.co_id;

  select * into new_msg
  from pgque.receive_coop('coop_stale', 'main_c', 'w2', 10, interval '1 minute')
  limit 1;
  new_batch := new_msg.batch_id;

  assert new_batch is not null and new_batch <> old_batch,
    'stale takeover must allocate a fresh batch_id';
  assert new_msg.msg_id = old_msg.msg_id,
    'stale takeover should move the same message window';

  assert pgque.ack(old_batch) = 0,
    'late ack(old_batch) after stale takeover must not finish new owner';

  select s.sub_batch into still_active
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_stale'
    and c.co_name = 'main_c.w2';
  assert still_active = new_batch,
    'late ack(old_batch) must leave new owner active';

  v_late_nack_err := null;
  begin
    perform pgque.nack(old_batch, new_msg, interval '1 second', 'late nack');
    raise exception 'expected late nack rejection';
  exception when others then
    if sqlerrm = 'expected late nack rejection' then raise; end if;
    v_late_nack_err := sqlerrm;
  end;
  assert v_late_nack_err is not null and v_late_nack_err like 'batch not found:%',
    format('late nack(old_batch) must raise "batch not found"; got %L', v_late_nack_err);

  -- Late nack must not clobber the new owner's active batch state.
  select s.sub_batch into still_active
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_stale'
    and c.co_name = 'main_c.w2';
  assert still_active = new_batch,
    format('late nack(old_batch) clobbered new owner sub_batch (%s -> %s)', new_batch, still_active);

  assert pgque.ack(new_batch) = 1,
    'new owner batch should remain ackable';
end $$;

-- 7, 8, 17: forced unregister retries active messages and invalidates old token.
do $$
begin
  perform pgque.create_queue('coop_force');
  perform pgque.register_subconsumer('coop_force', 'main_c', 'w1');
end $$;
select pgque.send('coop_force', 't', 'retry-me');
select pgque.force_tick('coop_force');
select pgque.ticker('coop_force');
do $$
declare
  m pgque.message;
  old_batch bigint;
  v_late_nack_err text;
begin
  select * into m from pgque.receive_coop('coop_force', 'main_c', 'w1', 10) limit 1;
  old_batch := m.batch_id;

  begin
    perform pgque.unregister_subconsumer('coop_force', 'main_c', 'w1', 0);
    raise exception 'expected active unregister rejection';
  exception when others then
    if sqlerrm = 'expected active unregister rejection' then raise; end if;
  end;

  assert pgque.unregister_subconsumer('coop_force', 'main_c', 'w1', 1) = 1,
    'forced unregister should remove active subconsumer';
  assert exists (select 1 from pgque.retry_queue as rq where rq.ev_id = m.msg_id),
    'forced unregister should route active message to retry queue';
  assert pgque.ack(old_batch) = 0,
    'late ack after forced unregister must not affect anything';

  v_late_nack_err := null;
  begin
    perform pgque.nack(old_batch, m, interval '1 second', 'late nack');
    raise exception 'expected late nack after unregister rejection';
  exception when others then
    if sqlerrm = 'expected late nack after unregister rejection' then raise; end if;
    v_late_nack_err := sqlerrm;
  end;
  assert v_late_nack_err is not null and v_late_nack_err like 'batch not found:%',
    format('late nack after forced unregister must raise "batch not found"; got %L', v_late_nack_err);
end $$;

-- 9: retry/DLQ routing failure during forced unregister leaves state intact.
create or replace function pgque._test_fail_dead_letter()
returns trigger as $$
begin
  raise exception 'forced test dead_letter failure';
end;
$$ language plpgsql;

create trigger coop_test_fail_dead_letter
before insert on pgque.dead_letter
for each row execute function pgque._test_fail_dead_letter();

do $$
begin
  perform pgque.create_queue('coop_force_fail');
  update pgque.queue set queue_max_retries = 0 where queue_name = 'coop_force_fail';
  perform pgque.register_subconsumer('coop_force_fail', 'main_c', 'w1');
end $$;
select pgque.send('coop_force_fail', 't', 'dlq-fail');
select pgque.force_tick('coop_force_fail');
select pgque.ticker('coop_force_fail');
do $$
declare
  m pgque.message;
  active_batch bigint;
begin
  select * into m from pgque.receive_coop('coop_force_fail', 'main_c', 'w1', 10) limit 1;

  begin
    perform pgque.unregister_subconsumer('coop_force_fail', 'main_c', 'w1', 1);
    raise exception 'expected forced unregister failure';
  exception when others then
    if sqlerrm = 'expected forced unregister failure' then raise; end if;
  end;

  select s.sub_batch into active_batch
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_force_fail'
    and c.co_name = 'main_c.w1';
  assert active_batch = m.batch_id,
    'failed forced unregister should leave active batch intact';
end $$;

drop trigger coop_test_fail_dead_letter on pgque.dead_letter;
drop function pgque._test_fail_dead_letter();

-- 10, 11, 13, 18, 21, 23: main unregister safety, cooperative nack owner,
-- unchanged normal behavior, idle stale ignore, active-normal conversion rejection.
do $$
begin
  perform pgque.create_queue('coop_misc');
  perform pgque.register_consumer('coop_misc', 'normal_active');
end $$;
select pgque.send('coop_misc', 't', 'n1');
select pgque.force_tick('coop_misc');
select pgque.ticker('coop_misc');
do $$
declare
  normal_msg pgque.message;
begin
  select * into normal_msg from pgque.receive('coop_misc', 'normal_active', 10) limit 1;
  /*
   * Two distinct guards on register_subconsumer fire on a normal consumer:
   *
   *   1. Without convert_normal: 'explicit conversion is required' — the
   *      caller didn't opt in to converting an existing normal consumer.
   *   2. With convert_normal := true AND sub_batch is not null: 'cannot
   *      convert active normal consumer ...' — opt-in is honored, but the
   *      consumer is mid-batch and conversion would orphan the active
   *      cursor token.
   *
   * Both branches are blueprint invariants; we test both here.
   */
  begin
    perform pgque.register_subconsumer('coop_misc', 'normal_active', 'w1');
    raise exception 'expected active normal conversion rejection';
  exception when others then
    if sqlerrm = 'expected active normal conversion rejection' then raise; end if;
    assert sqlerrm like 'consumer % on queue % is already a normal consumer%',
      format('without convert_normal, expected explicit-conversion-required error, got: %s', sqlerrm);
  end;

  begin
    perform pgque.register_subconsumer('coop_misc', 'normal_active', 'w1', i_convert_normal => true);
    raise exception 'expected active-batch conversion rejection';
  exception when others then
    if sqlerrm = 'expected active-batch conversion rejection' then raise; end if;
    assert sqlerrm like 'cannot convert active normal consumer%',
      format('with i_convert_normal := true on an active batch, expected active-batch rejection, got: %s', sqlerrm);
  end;

  perform pgque.ack(normal_msg.batch_id);
end $$;

do $$
begin
  perform pgque.register_subconsumer('coop_misc', 'main_c', 'w1');
  perform pgque.register_subconsumer('coop_misc', 'main_c', 'w2');
  perform pgque.register_subconsumer('coop_misc', 'main_c', 'w3');
end $$;
select pgque.send('coop_misc', 't', 'coop-nack');
select pgque.force_tick('coop_misc');
select pgque.ticker('coop_misc');
do $$
declare
  m pgque.message;
  shared_sub_id int4;
  no_batch bigint;
  v_w1_sub_active_before timestamptz;
  v_w1_sub_active_after timestamptz;
begin
  select * into m from pgque.receive_coop('coop_misc', 'main_c', 'w2', 10) limit 1;
  perform pgque.nack(m.batch_id, m, interval '1 second', 'test nack');

  select s.sub_id into shared_sub_id
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_misc'
    and c.co_name = 'main_c';
  assert exists (
    select 1 from pgque.retry_queue as rq
    where rq.ev_id = m.msg_id
      and rq.ev_owner = shared_sub_id
  ), 'cooperative nack should write retry state with shared sub_id';

  update pgque.subscription as s
     set sub_active = now() - interval '10 minutes'
    from pgque.queue as q
    cross join pgque.consumer as c
   where q.queue_name = 'coop_misc'
     and c.co_name = 'main_c.w1'
     and s.sub_queue = q.queue_id
     and s.sub_consumer = c.co_id
     and s.sub_batch is null
   returning s.sub_active into v_w1_sub_active_before;
  no_batch := pgque.next_batch('coop_misc', 'main_c', 'w3', interval '1 minute');
  assert no_batch is null,
    'stale takeover must ignore idle coop_member rows';

  /*
   * Idle members with stale sub_active must NOT be refreshed by a failed
   * stale-takeover scan. The impl skips the heartbeat update on an empty
   * tick window; if a regression added an unconditional sub_active = now()
   * write, w1's timestamp would jump forward and silently mask the bug.
   */
  select s.sub_active into v_w1_sub_active_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_misc'
    and c.co_name = 'main_c.w1';
  assert v_w1_sub_active_after = v_w1_sub_active_before,
    format('failed stale-takeover must not refresh idle member sub_active (before=%s, after=%s)',
           v_w1_sub_active_before, v_w1_sub_active_after);

  begin
    perform pgque.unregister_consumer('coop_misc', 'main_c');
    raise exception 'expected main unregister active sibling rejection';
  exception when others then
    if sqlerrm = 'expected main unregister active sibling rejection' then raise; end if;
  end;
end $$;

-- 11 (full cycle): nack must make the message redeliverable, not just write
-- a retry_queue row. retry_after = 0 lets maint_retry_events promote the row
-- immediately. Each phase runs as its own top-level statement so that
-- maint_retry_events, ticker, and the second receive_coop see one another's
-- effects (cf. snapshot visibility contract: same-xact maint+tick+receive
-- returns 0; committed maint+tick+receive delivers).
do $$
begin
  perform pgque.create_queue('coop_redeliver');
  perform pgque.register_subconsumer('coop_redeliver', 'main_c', 'w1');
end $$;
select pgque.send('coop_redeliver', 't', 'redeliver-me');
select pgque.force_tick('coop_redeliver');
select pgque.ticker('coop_redeliver');

-- Stash the first delivery so the redelivery DO block can compare against it.
create temp table coop_redeliver_first(msg_id bigint, batch_id bigint, retry_count int4);

do $$
declare
  m1 pgque.message;
begin
  select * into m1 from pgque.receive_coop('coop_redeliver', 'main_c', 'w1', 10) limit 1;
  assert m1.msg_id is not null,
    'redelivery test setup: first receive should yield the seeded message';
  assert coalesce(m1.retry_count, 0) = 0,
    'first delivery should have retry_count=0';

  perform pgque.nack(m1.batch_id, m1, interval '0 seconds', 'force redelivery');
  -- Close the now-empty active batch so the next receive_coop allocates fresh.
  perform pgque.ack(m1.batch_id);

  insert into coop_redeliver_first values (m1.msg_id, m1.batch_id, coalesce(m1.retry_count, 0));
end $$;

-- Promote the nacked row, advance the tick window (separate xacts).
do $$
declare v_promoted int;
begin
  v_promoted := pgque.maint_retry_events();
  assert v_promoted >= 1,
    format('maint_retry_events should promote the nacked row, got %s', v_promoted);
end $$;
select pgque.force_tick('coop_redeliver');
select pgque.ticker('coop_redeliver');

do $$
declare
  m2 pgque.message;
  first_msg_id bigint;
  first_retry int4;
begin
  select msg_id, retry_count into first_msg_id, first_retry from coop_redeliver_first;

  select * into m2 from pgque.receive_coop('coop_redeliver', 'main_c', 'w1', 10) limit 1;
  assert m2.msg_id is not null,
    'nack(retry_after=0) must make the message redeliverable';
  assert m2.msg_id = first_msg_id,
    format('redelivered msg_id should match original (got %s, expected %s)', m2.msg_id, first_msg_id);
  assert coalesce(m2.retry_count, 0) > first_retry,
    format('redelivered message should have incremented retry_count (got %s, was %s)', m2.retry_count, first_retry);

  perform pgque.ack(m2.batch_id);
end $$;
drop table coop_redeliver_first;

/*
 * REV-blocking + selected potential coverage. Each block is independent and
 * uses its own queue so failures point at one specific contract:
 *   B. unregister_subconsumer is idempotent (second call returns 0).
 *   C. unregister_subconsumer rejects unsupported batch_handling values.
 *   D. touch_subconsumer on an active (in-batch) member returns 1, refreshes
 *      sub_active, leaves sub_batch unchanged.
 *   E. Direct legacy next_batch(2-arg) and next_batch_custom(5-arg) calls on
 *      a coop_main with members raise the cooperative-form directive.
 *   F. Direct finish_batch on a coop_member returns 1 and clears the member
 *      cursor (mirrors the ack() path).
 *   G. A normal consumer and an active coop group on the same queue both
 *      receive the same event independently (fan-out is not suppressed).
 */

-- B: unregister_subconsumer idempotency
do $$
begin
  perform pgque.create_queue('coop_idempotent');
  perform pgque.register_subconsumer('coop_idempotent', 'main_c', 'w1');

  assert pgque.unregister_subconsumer('coop_idempotent', 'main_c', 'w1') = 1,
    'first unregister_subconsumer should return 1';
  assert pgque.unregister_subconsumer('coop_idempotent', 'main_c', 'w1') = 0,
    'second unregister_subconsumer must be idempotent (return 0)';
end $$;

-- C: invalid batch_handling raises explicit message
do $$
begin
  perform pgque.create_queue('coop_bh_invalid');
  perform pgque.register_subconsumer('coop_bh_invalid', 'main_c', 'w1');

  begin
    perform pgque.unregister_subconsumer('coop_bh_invalid', 'main_c', 'w1', 2);
    raise exception 'expected unsupported batch_handling rejection';
  exception when others then
    if sqlerrm = 'expected unsupported batch_handling rejection' then raise; end if;
    assert sqlerrm like 'unsupported batch_handling value%',
      format('unexpected batch_handling error message: %s', sqlerrm);
  end;
end $$;

-- D: touch_subconsumer on an active (in-batch) member
do $$
begin
  perform pgque.create_queue('coop_touch_active');
  perform pgque.register_subconsumer('coop_touch_active', 'main_c', 'w1');
end $$;
select pgque.send('coop_touch_active', 't', 'touch-payload');
select pgque.force_tick('coop_touch_active');
select pgque.ticker('coop_touch_active');
do $$
declare
  m pgque.message;
  v_before timestamptz;
  v_member_after record;
begin
  select * into m from pgque.receive_coop('coop_touch_active', 'main_c', 'w1', 10) limit 1;
  assert m.batch_id is not null, 'setup: w1 should hold an active batch';

  select s.sub_active into v_before
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_touch_active'
    and c.co_name = 'main_c.w1';

  perform pg_sleep(0.01);
  assert pgque.touch_subconsumer('coop_touch_active', 'main_c', 'w1') = 1,
    'touch_subconsumer on active member must return 1';

  select s.* into v_member_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_touch_active'
    and c.co_name = 'main_c.w1';
  assert v_member_after.sub_batch = m.batch_id,
    'touch_subconsumer must not clear or change the active sub_batch';
  assert v_member_after.sub_active > v_before,
    'touch_subconsumer must refresh sub_active even on active member';

  perform pgque.ack(m.batch_id);
end $$;

-- E: direct legacy next_batch / next_batch_custom rejection on coop_main
do $$
begin
  perform pgque.create_queue('coop_legacy_reject');
  perform pgque.register_subconsumer('coop_legacy_reject', 'main_c', 'w1');

  begin
    perform pgque.next_batch('coop_legacy_reject', 'main_c');
    raise exception 'expected legacy 2-arg next_batch rejection';
  exception when others then
    if sqlerrm = 'expected legacy 2-arg next_batch rejection' then raise; end if;
    assert sqlerrm like '%cooperative main consumer%',
      format('unexpected legacy 2-arg next_batch error: %s', sqlerrm);
  end;

  begin
    perform *
    from pgque.next_batch_custom(
      'coop_legacy_reject', 'main_c',
      null::interval, null::int4, null::interval
    );
    raise exception 'expected legacy 5-arg next_batch_custom rejection';
  exception when others then
    if sqlerrm = 'expected legacy 5-arg next_batch_custom rejection' then raise; end if;
    assert sqlerrm like '%cooperative main consumer%',
      format('unexpected legacy 5-arg next_batch_custom error: %s', sqlerrm);
  end;
end $$;

-- E2: legacy 5-arg next_batch_custom on a coop_member raises an explicit
-- subconsumer-form directive — not the misleading 'PgQ corruption' fallback
-- that the LEFT JOIN to pgque.tick produces (member rows have sub_last_tick
-- NULL by design, which trips the prev_tick_id sanity check downstream).
do $$
begin
  perform pgque.create_queue('coop_member_fallthrough');
  perform pgque.register_subconsumer('coop_member_fallthrough', 'main_c', 'w1');

  begin
    perform *
    from pgque.next_batch_custom(
      'coop_member_fallthrough', 'main_c.w1',
      null::interval, null::int4, null::interval
    );
    raise exception 'expected coop_member rejection on legacy next_batch_custom';
  exception when others then
    if sqlerrm = 'expected coop_member rejection on legacy next_batch_custom' then raise; end if;
    assert sqlerrm like '%cooperative subconsumer%',
      format('legacy next_batch_custom should reject coop_member with subconsumer-form directive, got: %s', sqlerrm);
  end;
end $$;

-- E3: 2-arg next_batch and receive on a dotted member name route through
-- next_batch_info -> next_batch_custom(5), so they must surface the same
-- coop_member rejection rather than the legacy 'PgQ corruption' fallback.
-- Asserts the rejection at the entry points users actually call (E2 only
-- exercises next_batch_custom directly).
do $$
begin
  perform pgque.create_queue('coop_member_2arg');
  perform pgque.register_subconsumer('coop_member_2arg', 'main_c', 'w1');

  begin
    perform pgque.next_batch('coop_member_2arg', 'main_c.w1');
    raise exception 'expected coop_member rejection on 2-arg next_batch';
  exception when others then
    if sqlerrm = 'expected coop_member rejection on 2-arg next_batch' then raise; end if;
    assert sqlerrm like '%cooperative subconsumer%',
      format('2-arg next_batch should reject coop_member with subconsumer-form directive, got: %s', sqlerrm);
  end;

  begin
    perform * from pgque.receive('coop_member_2arg', 'main_c.w1', 10);
    raise exception 'expected coop_member rejection on receive';
  exception when others then
    if sqlerrm = 'expected coop_member rejection on receive' then raise; end if;
    assert sqlerrm like '%cooperative subconsumer%',
      format('receive should reject coop_member with subconsumer-form directive, got: %s', sqlerrm);
  end;
end $$;

-- F: direct finish_batch on coop_member clears member cursor
do $$
begin
  perform pgque.create_queue('coop_finish_direct');
  perform pgque.register_subconsumer('coop_finish_direct', 'main_c', 'w1');
end $$;
select pgque.send('coop_finish_direct', 't', 'finish-direct-payload');
select pgque.force_tick('coop_finish_direct');
select pgque.ticker('coop_finish_direct');
do $$
declare
  m pgque.message;
  v_member_after record;
  v_finish_rc int;
begin
  select * into m from pgque.receive_coop('coop_finish_direct', 'main_c', 'w1', 10) limit 1;
  assert m.batch_id is not null, 'setup: w1 should hold an active batch';

  v_finish_rc := pgque.finish_batch(m.batch_id);
  assert v_finish_rc = 1,
    format('finish_batch on coop_member should return 1, got %s', v_finish_rc);

  select s.* into v_member_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_finish_direct'
    and c.co_name = 'main_c.w1';
  assert v_member_after.sub_batch is null,
    'direct finish_batch on coop_member must clear sub_batch';
  assert v_member_after.sub_last_tick is null,
    'direct finish_batch on coop_member must clear sub_last_tick';
end $$;

-- G: normal consumer + active coop group on the same queue both receive
do $$
begin
  perform pgque.create_queue('coop_fanout');
  perform pgque.register_consumer('coop_fanout', 'normal_c');
  perform pgque.register_subconsumer('coop_fanout', 'main_c', 'w1');
end $$;
select pgque.send('coop_fanout', 't', 'fanout-event');
select pgque.force_tick('coop_fanout');
select pgque.ticker('coop_fanout');
do $$
declare
  normal_rows int;
  coop_rows int;
begin
  select count(*) into normal_rows from pgque.receive('coop_fanout', 'normal_c', 10);
  assert normal_rows = 1,
    format('normal consumer must receive on a queue with active coop group, got %s', normal_rows);

  select count(*) into coop_rows from pgque.receive_coop('coop_fanout', 'main_c', 'w1', 10);
  assert coop_rows = 1,
    format('coop subconsumer must receive event independently, got %s', coop_rows);
end $$;

/*
 * H: full coop teardown DLQ retention contract.
 *
 * 817c084 routes coop DLQ writes to the coop_main's co_id. The fix protects
 * the DLQ row from member-cascade delete during unregister_subconsumer.
 * This test exercises the FULL teardown sequence the existing 817c084
 * regression in test_coop_ultrareview only partially covers:
 *
 *   Stage 1: unregister the last member -> coop_main demotes to 'normal'.
 *            DLQ row MUST survive (the contract 817c084 introduced).
 *   Stage 2: unregister_consumer the (now normal) main, which deletes the
 *            consumer row. The dl_consumer_id ON DELETE CASCADE then wipes
 *            the DLQ row -- documented behavior, see
 *            sql/pgque-additions/dlq.sql near the dl_consumer_id FK.
 *
 * Asserting both stages locks in the contract: cooperative DLQ rows
 * survive the cooperative-teardown path (member unregister), then follow
 * the same cascade-on-consumer-delete rule as normal DLQ rows.
 */
do $$
begin
  perform pgque.create_queue('coop_dlq_teardown');
  update pgque.queue
  set queue_max_retries = 0
  where queue_name = 'coop_dlq_teardown';
  /*
   * Use queue-unique consumer + subconsumer names. Other test blocks in
   * this file reuse 'main_c' as a shared consumer, which would prevent
   * the consumer row from being deleted in stage 2 (unregister_consumer
   * only deletes the consumer row when no other subscriptions reference
   * it). Without deletion, the dl_consumer_id ON DELETE CASCADE never
   * fires and the stage-2 assertion would fail.
   */
  perform pgque.register_subconsumer('coop_dlq_teardown', 'dlq_teardown_main_c', 'dlq_teardown_w1');
end $$;
select pgque.send('coop_dlq_teardown', 't', 'dlq-payload');
select pgque.force_tick('coop_dlq_teardown');
select pgque.ticker('coop_dlq_teardown');
do $$
declare
  m pgque.message;
  v_dlq_count int;
  v_main_role text;
begin
  select * into m from pgque.receive_coop('coop_dlq_teardown', 'dlq_teardown_main_c', 'dlq_teardown_w1', 10) limit 1;
  -- queue_max_retries = 0 routes the first nack straight to DLQ. nack()
  -- writes the dead_letter row but does not close the batch; ack() then
  -- clears the member's sub_batch so unregister can proceed via the idle
  -- path (no batch_handling = 1 needed).
  perform pgque.nack(m.batch_id, m, interval '1 second', 'force-dlq');
  perform pgque.ack(m.batch_id);

  select count(*) into v_dlq_count
  from pgque.dead_letter as dl
  join pgque.queue as q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'coop_dlq_teardown'
    and dl.ev_id = m.msg_id;
  assert v_dlq_count = 1,
    format('setup: DLQ row should exist after force-dlq nack, got %s', v_dlq_count);

  -- Stage 1: unregister the only member. Main should demote to 'normal' and
  -- the DLQ row must survive (anchored to persistent main co_id, not the
  -- ephemeral member co_id).
  assert pgque.unregister_subconsumer('coop_dlq_teardown', 'dlq_teardown_main_c', 'dlq_teardown_w1') = 1,
    'unregister of only member should succeed';
  select s.sub_role into v_main_role
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'coop_dlq_teardown'
    and c.co_name = 'dlq_teardown_main_c';
  assert v_main_role = 'normal',
    format('main should demote to normal after last member unregister, got %s', v_main_role);

  select count(*) into v_dlq_count
  from pgque.dead_letter as dl
  join pgque.queue as q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'coop_dlq_teardown'
    and dl.ev_id = m.msg_id;
  assert v_dlq_count = 1,
    format('DLQ row must survive last-member-unregister + main-demote sequence, got %s', v_dlq_count);

  -- Stage 2: unregister_consumer the (now normal) main. The cascade on
  -- dl_consumer_id wipes the DLQ row by documented design.
  assert pgque.unregister_consumer('coop_dlq_teardown', 'dlq_teardown_main_c') = 1,
    'unregister_consumer of demoted main should succeed';
  select count(*) into v_dlq_count
  from pgque.dead_letter as dl
  join pgque.queue as q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'coop_dlq_teardown'
    and dl.ev_id = m.msg_id;
  assert v_dlq_count = 0,
    format('DLQ row must cascade when its consumer is unregistered (per dl_consumer_id ON DELETE CASCADE), got %s', v_dlq_count);
end $$;

do $$ begin
  raise notice 'PASS: cooperative consumer SQL-core semantics';
end $$;
