\set ON_ERROR_STOP on

-- Regression: receive ceilings must fail closed instead of truncating a batch.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Plain receive: N+1 raises, rolls back batch allocation, and all events retry.
do $$
begin
  perform pgque.create_queue('recv_overflow');
  perform pgque.register_consumer('recv_overflow', 'c1');
  perform pgque.send('recv_overflow', 'ev', '{"n":1}'::text);
  perform pgque.send('recv_overflow', 'ev', '{"n":2}'::text);
  perform pgque.send('recv_overflow', 'ev', '{"n":3}'::text);
end $$;

select pgque.force_next_tick('recv_overflow');
select pgque.ticker();

do $$
declare
  v_raised boolean := false;
  v_count int := 0;
  v_batch_id bigint;
  v_payloads text[] := array[]::text[];
  v_msg pgque.message;
  v_before record;
  v_after record;
  v_hint text;
  v_state text;
begin
  select s.* into v_before
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'recv_overflow' and c.co_name = 'c1';

  begin
    perform * from pgque.receive('recv_overflow', 'c1', 2);
  exception
    when others then
      v_raised := true;
      get stacked diagnostics
        v_hint = pg_exception_hint,
        v_state = returned_sqlstate;
      assert v_state = '54000',
        'receive overflow SQLSTATE must be 54000, got ' || v_state;
      assert sqlerrm like '%batch exceeds max_return of 2%',
        'unexpected receive overflow error: ' || sqlerrm;
      assert v_hint like '%Do not acknowledge after this error%',
        'receive overflow must provide an actionable no-ack hint';
  end;
  assert v_raised, 'receive must raise when a batch contains N+1 events';

  select s.* into v_after
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'recv_overflow' and c.co_name = 'c1';
  assert v_after.sub_batch is not distinct from v_before.sub_batch,
    'overflow must roll back the active batch assignment';
  assert v_after.sub_last_tick is not distinct from v_before.sub_last_tick,
    'overflow must roll back the consumer cursor';
  assert v_after.sub_next_tick is not distinct from v_before.sub_next_tick,
    'overflow must roll back the next-tick boundary';

  for v_msg in select * from pgque.receive('recv_overflow', 'c1', 3)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    v_payloads := array_append(v_payloads, v_msg.payload);
  end loop;

  assert v_count = 3,
    format('receive retry must return all 3 events, got %s', v_count);
  assert v_payloads @> array['{"n":1}', '{"n":2}', '{"n":3}'],
    format('receive retry lost events: %s', v_payloads);
  perform pgque.ack(v_batch_id);
end $$;

-- An already allocated batch must also fail closed and remain retryable.
do $$
begin
  perform pgque.create_queue('recv_active');
  perform pgque.register_consumer('recv_active', 'c1');
  perform pgque.send('recv_active', 'ev', 'one');
  perform pgque.send('recv_active', 'ev', 'two');
  perform pgque.send('recv_active', 'ev', 'three');
end $$;

select pgque.force_next_tick('recv_active');
select pgque.ticker();

do $$
declare
  v_active_batch bigint;
  v_after_batch bigint;
  v_count int := 0;
  v_raised boolean := false;
  v_msg pgque.message;
begin
  select batch_id into v_active_batch
  from pgque.receive('recv_active', 'c1', 3)
  limit 1;
  assert v_active_batch is not null, 'expected an allocated batch';

  begin
    perform * from pgque.receive('recv_active', 'c1', 2);
  exception
    when others then
      v_raised := true;
  end;
  assert v_raised, 'overflow must also reject an already allocated batch';

  select s.sub_batch into v_after_batch
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'recv_active' and c.co_name = 'c1';
  assert v_after_batch = v_active_batch,
    'overflow must preserve the already allocated batch token';

  for v_msg in select * from pgque.receive('recv_active', 'c1', 3)
  loop
    v_count := v_count + 1;
    assert v_msg.batch_id = v_active_batch,
      'overflow retry must retain the existing batch token';
  end loop;
  assert v_count = 3,
    format('existing-batch retry must return all 3 events, got %s', v_count);
  perform pgque.ack(v_active_batch);
end $$;

-- Exactly N remains valid, including the largest int ceiling on an empty batch.
do $$
begin
  perform pgque.create_queue('recv_exact');
  perform pgque.register_consumer('recv_exact', 'c1');
  perform pgque.send('recv_exact', 'ev', 'one');
  perform pgque.send('recv_exact', 'ev', 'two');
end $$;

select pgque.force_next_tick('recv_exact');
select pgque.ticker();

do $$
declare
  v_count int := 0;
  v_batch_id bigint;
  v_msg pgque.message;
  v_active_batch bigint;
begin
  for v_msg in select * from pgque.receive('recv_exact', 'c1', 2)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;
  assert v_count = 2, format('exactly N events must succeed, got %s', v_count);
  perform pgque.ack(v_batch_id);

  v_count := 0;
  for v_msg in select * from pgque.receive('recv_exact', 'c1', 2147483647)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0,
    format('INT_MAX receive after ack must be empty, got %s', v_count);
  select s.sub_batch into v_active_batch
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'recv_exact' and c.co_name = 'c1';
  assert v_active_batch is null,
    'empty INT_MAX receive must not leave an active batch';
end $$;

-- A batch containing N-1 events also succeeds.
do $$
begin
  perform pgque.create_queue('recv_nminus1');
  perform pgque.register_consumer('recv_nminus1', 'c1');
  perform pgque.send('recv_nminus1', 'ev', 'one');
  perform pgque.send('recv_nminus1', 'ev', 'two');
end $$;

select pgque.force_next_tick('recv_nminus1');
select pgque.ticker();

do $$
declare
  v_count int := 0;
  v_batch_id bigint;
  v_msg pgque.message;
begin
  for v_msg in select * from pgque.receive('recv_nminus1', 'c1', 3)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;
  assert v_count = 2,
    format('N-1 events must succeed, got %s', v_count);
  perform pgque.ack(v_batch_id);
end $$;

-- Preserve the SQL NULL behavior: it means no explicit ceiling.
do $$
begin
  perform pgque.create_queue('recv_null');
  perform pgque.register_consumer('recv_null', 'c1');
  perform pgque.send('recv_null', 'ev', 'one');
  perform pgque.send('recv_null', 'ev', 'two');
  perform pgque.send('recv_null', 'ev', 'three');
end $$;

select pgque.force_next_tick('recv_null');
select pgque.ticker();

do $$
declare
  v_count int := 0;
  v_batch_id bigint;
  v_msg pgque.message;
begin
  for v_msg in select * from pgque.receive('recv_null', 'c1', null)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;
  assert v_count = 3,
    format('NULL max_return must preserve unbounded receive, got %s', v_count);
  perform pgque.ack(v_batch_id);
end $$;

-- Cooperative receive has the same fail-closed and retry contract.
do $$
begin
  perform pgque.create_queue('recv_coop_overflow');
  perform pgque.register_subconsumer('recv_coop_overflow', 'main_c', 'w1');
  perform pgque.send('recv_coop_overflow', 'ev', 'one');
  perform pgque.send('recv_coop_overflow', 'ev', 'two');
  perform pgque.send('recv_coop_overflow', 'ev', 'three');
end $$;

select pgque.force_next_tick('recv_coop_overflow');
select pgque.ticker();

do $$
declare
  v_raised boolean := false;
  v_count int := 0;
  v_batch_id bigint;
  v_payloads text[] := array[]::text[];
  v_msg pgque.message;
  v_hint text;
  v_state text;
begin
  begin
    perform * from pgque.receive_coop(
      'recv_coop_overflow', 'main_c', 'w1', 2
    );
  exception
    when others then
      v_raised := true;
      get stacked diagnostics
        v_hint = pg_exception_hint,
        v_state = returned_sqlstate;
      assert v_state = '54000',
        'receive_coop overflow SQLSTATE must be 54000, got ' || v_state;
      assert sqlerrm like '%batch exceeds max_return of 2%',
        'unexpected receive_coop overflow error: ' || sqlerrm;
      assert v_hint like '%Do not acknowledge after this error%',
        'receive_coop overflow must provide an actionable no-ack hint';
  end;
  assert v_raised, 'receive_coop must raise when a batch contains N+1 events';

  for v_msg in
    select * from pgque.receive_coop(
      'recv_coop_overflow', 'main_c', 'w1', 3
    )
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    v_payloads := array_append(v_payloads, v_msg.payload);
  end loop;
  assert v_count = 3,
    format('receive_coop retry must return all 3 events, got %s', v_count);
  assert v_payloads @> array['one', 'two', 'three'],
    format('receive_coop retry lost events: %s', v_payloads);
  perform pgque.ack(v_batch_id);
end $$;

-- A failed stale takeover must roll back ownership to the original member.
do $$
begin
  perform pgque.create_queue('recv_coop_takeover');
  perform pgque.register_subconsumer('recv_coop_takeover', 'main_c', 'w1');
  perform pgque.register_subconsumer('recv_coop_takeover', 'main_c', 'w2');
  perform pgque.send('recv_coop_takeover', 'ev', 'one');
  perform pgque.send('recv_coop_takeover', 'ev', 'two');
  perform pgque.send('recv_coop_takeover', 'ev', 'three');
end $$;

select pgque.force_next_tick('recv_coop_takeover');
select pgque.ticker();

do $$
declare
  v_old_batch bigint;
  v_w1_batch bigint;
  v_w2_batch bigint;
  v_new_batch bigint;
  v_count int := 0;
  v_raised boolean := false;
  v_payloads text[] := array[]::text[];
  v_msg pgque.message;
begin
  select batch_id into v_old_batch
  from pgque.receive_coop('recv_coop_takeover', 'main_c', 'w1', 3)
  limit 1;

  update pgque.subscription as s
  set sub_active = now() - interval '10 minutes'
  from pgque.queue as q
  cross join pgque.consumer as c
  where q.queue_name = 'recv_coop_takeover'
    and c.co_name = 'main_c.w1'
    and s.sub_queue = q.queue_id
    and s.sub_consumer = c.co_id;

  begin
    perform * from pgque.receive_coop(
      'recv_coop_takeover', 'main_c', 'w2', 2, interval '1 minute'
    );
  exception
    when others then
      v_raised := true;
  end;
  assert v_raised, 'overflow must abort a stale takeover';

  select s.sub_batch into v_w1_batch
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'recv_coop_takeover' and c.co_name = 'main_c.w1';
  select s.sub_batch into v_w2_batch
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  join pgque.consumer as c on c.co_id = s.sub_consumer
  where q.queue_name = 'recv_coop_takeover' and c.co_name = 'main_c.w2';
  assert v_w1_batch = v_old_batch,
    'failed takeover must preserve the stale owner batch';
  assert v_w2_batch is null,
    'failed takeover must not leave the new member owning a batch';

  for v_msg in
    select * from pgque.receive_coop(
      'recv_coop_takeover', 'main_c', 'w2', 3, interval '1 minute'
    )
  loop
    v_count := v_count + 1;
    v_new_batch := v_msg.batch_id;
    v_payloads := array_append(v_payloads, v_msg.payload);
  end loop;
  assert v_count = 3 and v_payloads @> array['one', 'two', 'three'],
    format('takeover retry lost events: count=%s payloads=%s', v_count, v_payloads);
  assert v_new_batch is not null and v_new_batch <> v_old_batch,
    'successful takeover retry must issue a fresh batch token';
  perform pgque.ack(v_new_batch);
end $$;

-- Partitioned receive must not truncate the filtered slot batch either.
do $$
begin
  perform pgque.create_queue('recv_part_overflow');
  perform pgque.subscribe_slot('recv_part_overflow', 'c1', 0, 1);
  perform pgque.send('recv_part_overflow', 'ev', 'one', 'key');
  perform pgque.send('recv_part_overflow', 'ev', 'two', 'key');
  perform pgque.send('recv_part_overflow', 'ev', 'three', 'key');
end $$;

select pgque.force_next_tick('recv_part_overflow');
select pgque.ticker();

do $$
declare
  v_raised boolean := false;
  v_count int := 0;
  v_msg pgque.message;
  v_hint text;
  v_state text;
begin
  perform pgque.claim_slot('recv_part_overflow', 'c1', 0, 'worker');
  begin
    perform * from pgque.receive_partitioned(
      'recv_part_overflow', 'c1', 0, 1, 'worker', 2
    );
  exception
    when others then
      v_raised := true;
      get stacked diagnostics
        v_hint = pg_exception_hint,
        v_state = returned_sqlstate;
      assert v_state = '54000',
        'receive_partitioned overflow SQLSTATE must be 54000, got ' || v_state;
      assert sqlerrm like '%batch exceeds max of 2%',
        'unexpected receive_partitioned overflow error: ' || sqlerrm;
      assert v_hint like '%Do not acknowledge after this error%',
        'receive_partitioned overflow must provide an actionable no-ack hint';
  end;
  assert v_raised,
    'receive_partitioned must raise when a filtered batch contains N+1 events';

  for v_msg in
    select * from pgque.receive_partitioned(
      'recv_part_overflow', 'c1', 0, 1, 'worker', 3
    )
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 3,
    format('receive_partitioned retry must return all 3 events, got %s', v_count);
  perform pgque.ack_partitioned('recv_part_overflow', 'c1', 0, 1, 'worker');

  perform * from pgque.receive_partitioned(
    'recv_part_overflow', 'c1', 0, 1, 'worker', 2147483647
  );
  perform pgque.release_slot('recv_part_overflow', 'c1', 0, 'worker');
end $$;

-- Partition overflow counts only rows matching the slot predicate.
do $$
begin
  perform pgque.create_queue('recv_part_filtered');
  perform pgque.subscribe_slot('recv_part_filtered', 'c1', 0, 2);
  perform pgque.send('recv_part_filtered', 'ev', 'match-one', 'tenant-a');
  perform pgque.send('recv_part_filtered', 'ev', 'other-one', 'tenant-b');
  perform pgque.send('recv_part_filtered', 'ev', 'match-two', 'tenant-a');
  perform pgque.send('recv_part_filtered', 'ev', 'other-two', 'tenant-b');
end $$;

select pgque.force_next_tick('recv_part_filtered');
select pgque.ticker();

do $$
declare
  v_count int := 0;
  v_msg pgque.message;
begin
  perform pgque.claim_slot('recv_part_filtered', 'c1', 0, 'worker');
  for v_msg in
    select * from pgque.receive_partitioned(
      'recv_part_filtered', 'c1', 0, 2, 'worker', 2
    )
  loop
    v_count := v_count + 1;
    assert v_msg.extra1 = 'tenant-a',
      'slot 0 must not count or return other-slot rows';
  end loop;
  assert v_count = 2,
    format('exactly 2 matching rows must succeed despite other-slot rows, got %s', v_count);
  perform pgque.ack_partitioned('recv_part_filtered', 'c1', 0, 2, 'worker');
end $$;

do $$
begin
  perform pgque.send('recv_part_filtered', 'ev', 'match-three', 'tenant-a');
  perform pgque.send('recv_part_filtered', 'ev', 'other-three', 'tenant-b');
  perform pgque.send('recv_part_filtered', 'ev', 'match-four', 'tenant-a');
  perform pgque.send('recv_part_filtered', 'ev', 'match-five', 'tenant-a');
end $$;

select pgque.force_next_tick('recv_part_filtered');
select pgque.ticker();

do $$
declare
  v_raised boolean := false;
  v_state text;
  v_count int := 0;
  v_msg pgque.message;
begin
  begin
    perform * from pgque.receive_partitioned(
      'recv_part_filtered', 'c1', 0, 2, 'worker', 2
    );
  exception
    when others then
      v_raised := true;
      get stacked diagnostics v_state = returned_sqlstate;
  end;
  assert v_raised and v_state = '54000',
    format('3 matching rows must overflow at max 2 with 54000, got %s', v_state);

  for v_msg in
    select * from pgque.receive_partitioned(
      'recv_part_filtered', 'c1', 0, 2, 'worker', 3
    )
  loop
    v_count := v_count + 1;
    assert v_msg.extra1 = 'tenant-a',
      'slot 0 retry must not return other-slot rows';
  end loop;
  assert v_count = 3,
    format('partition retry must return all 3 matching rows, got %s', v_count);
  perform pgque.ack_partitioned('recv_part_filtered', 'c1', 0, 2, 'worker');
  perform pgque.release_slot('recv_part_filtered', 'c1', 0, 'worker');
end $$;

do $$
begin
  perform pgque.unregister_consumer('recv_overflow', 'c1');
  perform pgque.drop_queue('recv_overflow');
  perform pgque.unregister_consumer('recv_active', 'c1');
  perform pgque.drop_queue('recv_active');
  perform pgque.unregister_consumer('recv_exact', 'c1');
  perform pgque.drop_queue('recv_exact');
  perform pgque.unregister_consumer('recv_nminus1', 'c1');
  perform pgque.drop_queue('recv_nminus1');
  perform pgque.unregister_consumer('recv_null', 'c1');
  perform pgque.drop_queue('recv_null');
  perform pgque.unregister_subconsumer('recv_coop_overflow', 'main_c', 'w1');
  perform pgque.unregister_consumer('recv_coop_overflow', 'main_c');
  perform pgque.drop_queue('recv_coop_overflow');
  perform pgque.unregister_subconsumer('recv_coop_takeover', 'main_c', 'w1');
  perform pgque.unregister_subconsumer('recv_coop_takeover', 'main_c', 'w2');
  perform pgque.unregister_consumer('recv_coop_takeover', 'main_c');
  perform pgque.drop_queue('recv_coop_takeover');
  perform pgque.unsubscribe_slot('recv_part_overflow', 'c1', 0);
  perform pgque.drop_queue('recv_part_overflow');
  perform pgque.unsubscribe_slot('recv_part_filtered', 'c1', 0);
  perform pgque.drop_queue('recv_part_filtered');
  raise notice 'PASS: receive ceilings fail closed and preserve complete batches';
end $$;
