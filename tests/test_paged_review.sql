\set ON_ERROR_STOP on

-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Review regressions for page ownership, partition filtering and legacy guards.

create or replace function pg_temp.expect_page_error(
  i_operation text,
  i_token uuid,
  i_worker text,
  i_expected_state text default 'PQP01'
)
returns void as $$
declare
  v_state text;
begin
  begin
    if i_operation = 'ack' then
      perform * from pgque.ack_page(i_token, i_worker);
    elsif i_operation = 'renew' then
      perform pgque.renew_page(i_token, i_worker);
    else
      raise exception 'unknown page operation: %', i_operation;
    end if;
    raise exception 'expected % from %', i_expected_state, i_operation;
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    if v_state <> i_expected_state then
      raise exception '% expected %, got %: %',
        i_operation, i_expected_state, v_state, sqlerrm;
    end if;
  end;
end;
$$ language plpgsql;

create or replace function pg_temp.expect_legacy_guard(i_sql text)
returns void as $$
declare
  v_state text;
begin
  begin
    execute i_sql;
    raise exception 'expected legacy guard: %', i_sql;
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    if v_state <> '55000' then
      raise exception 'legacy guard expected 55000, got % for %: %',
        v_state, i_sql, sqlerrm;
    end if;
  end;
end;
$$ language plpgsql;

/* Every mode rejects wrong/null workers and unknown tokens for ack and renew. */
do $$
begin
  perform pgque.create_queue('review_owner_normal');
  perform pgque.subscribe('review_owner_normal', 'c1');
  perform pgque.send('review_owner_normal', 'normal', 'one');

  perform pgque.create_queue('review_owner_coop');
  perform pgque.register_subconsumer('review_owner_coop', 'main_c', 'w1');
  perform pgque.send('review_owner_coop', 'coop', 'one');

  perform pgque.create_queue('review_owner_part');
  perform pgque.subscribe_slot('review_owner_part', 'part_c', 0, 1);
  perform pgque.send('review_owner_part', 'part', 'one', 'key');
end $$;

do $$
begin
  perform pgque.force_next_tick('review_owner_normal');
  perform pgque.force_next_tick('review_owner_coop');
  perform pgque.force_next_tick('review_owner_part');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
  v_ack record;
  v_forged constant uuid := 'ffffffff-ffff-4fff-8fff-ffffffffffff';
begin
  select * into v_page
  from pgque.receive_page(
    'review_owner_normal', 'c1', 'normal-owner', 1, interval '1 minute'
  );
  assert v_page.status = 'page',
    'normal ownership fixture must return a page, got ' || coalesce(v_page.status, 'null');
  perform pg_temp.expect_page_error('ack', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('renew', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('ack', v_page.page_token, null);
  perform pg_temp.expect_page_error('renew', v_page.page_token, null);
  perform pg_temp.expect_page_error('ack', v_forged, 'normal-owner');
  perform pg_temp.expect_page_error('renew', v_forged, 'normal-owner');
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'normal-owner');
  assert v_ack.status = 'acked', 'normal owner must ack its page';
  perform pg_temp.expect_page_error('ack', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('ack', v_page.page_token, null);

  select * into v_page
  from pgque.receive_page_coop(
    'review_owner_coop', 'main_c', 'w1', 'coop-owner',
    1, interval '1 minute', interval '1 minute'
  );
  assert v_page.status = 'page', 'cooperative ownership fixture must return a page';
  perform pg_temp.expect_page_error('ack', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('renew', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('ack', v_page.page_token, null);
  perform pg_temp.expect_page_error('renew', v_page.page_token, null);
  perform pg_temp.expect_page_error('ack', v_forged, 'coop-owner');
  perform pg_temp.expect_page_error('renew', v_forged, 'coop-owner');
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'coop-owner');
  assert v_ack.status = 'acked', 'cooperative owner must ack its page';
  perform pg_temp.expect_page_error('ack', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('ack', v_page.page_token, null);

  perform pgque.claim_slot(
    'review_owner_part', 'part_c', 0, 'part-owner', interval '1 minute'
  );
  select * into v_page
  from pgque.receive_page_partitioned(
    'review_owner_part', 'part_c', 0, 1, 'part-owner', 1
  );
  assert v_page.status = 'page', 'partition ownership fixture must return a page';
  perform pg_temp.expect_page_error('ack', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('renew', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('ack', v_page.page_token, null);
  perform pg_temp.expect_page_error('renew', v_page.page_token, null);
  perform pg_temp.expect_page_error('ack', v_forged, 'part-owner');
  perform pg_temp.expect_page_error('renew', v_forged, 'part-owner');
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'part-owner');
  assert v_ack.status = 'acked', 'partition owner must ack its page';
  perform pg_temp.expect_page_error('ack', v_page.page_token, 'wrong-owner');
  perform pg_temp.expect_page_error('ack', v_page.page_token, null);
end $$;

/* A same-worker ABA is fenced only by the partition epoch. */
do $$
begin
  perform pgque.create_queue('review_epoch_fence');
  perform pgque.subscribe_slot('review_epoch_fence', 'part_c', 0, 1);
  perform pgque.send('review_epoch_fence', 'part', 'one', 'key');
end $$;

do $$
begin
  perform pgque.force_next_tick('review_epoch_fence');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
  v_epoch_a bigint;
  v_epoch_b bigint;
  v_epoch_a_again bigint;
begin
  v_epoch_a := pgque.claim_slot(
    'review_epoch_fence', 'part_c', 0, 'worker-a', interval '1 minute'
  );
  select * into v_page
  from pgque.receive_page_partitioned(
    'review_epoch_fence', 'part_c', 0, 1, 'worker-a', 1
  );
  assert v_page.fence_epoch = v_epoch_a, 'page must record the original epoch';

  update pgque.partition_slot as ps
  set lease_until = clock_timestamp() - interval '1 second'
  from pgque.queue as q
  where q.queue_name = 'review_epoch_fence'
    and ps.queue_id = q.queue_id
    and ps.co_name = 'part_c'
    and ps.slot = 0;
  v_epoch_b := pgque.claim_slot(
    'review_epoch_fence', 'part_c', 0, 'worker-b', interval '1 minute'
  );
  assert v_epoch_b > v_epoch_a, 'slot takeover must advance the epoch';
  assert pgque.release_slot(
    'review_epoch_fence', 'part_c', 0, 'worker-b'
  ), 'intermediate owner must release the slot';
  v_epoch_a_again := pgque.claim_slot(
    'review_epoch_fence', 'part_c', 0, 'worker-a', interval '1 minute'
  );
  assert v_epoch_a_again > v_epoch_b,
    'same worker reclaim after an intermediate owner must advance the epoch';
  assert exists (
    select 1
    from pgque.page_state as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'review_epoch_fence'
      and ps.pending_token = v_page.page_token
      and ps.pending_worker = 'worker-a'
      and ps.partition_epoch = v_epoch_a
  ), 'test must retain worker-a old token and epoch without re-receive';

  perform pg_temp.expect_page_error('ack', v_page.page_token, 'worker-a');
  perform pg_temp.expect_page_error('renew', v_page.page_token, 'worker-a');
end $$;

/* n=3 proves actual hash membership, per-slot order, terminal and empty sets. */
do $$
declare
  v_slot int4;
  v_key text;
begin
  perform pgque.create_queue('review_hash_pages');
  for v_slot in 0..2 loop
    perform pgque.subscribe_slot('review_hash_pages', 'part_c', v_slot, 3);
    select candidate into strict v_key
    from (
      select 'slot-' || v_slot || '-' || n as candidate
      from generate_series(1, 1000) as n
    ) as candidates
    where (
      pg_catalog.hashtextextended(candidate, 0) % 3 + 3
    ) % 3 = v_slot
    order by candidate
    limit 1;
    perform pgque.send(
      'review_hash_pages', 'slot-' || v_slot, 'first-' || v_slot, v_key
    );
    perform pgque.send(
      'review_hash_pages', 'slot-' || v_slot, 'second-' || v_slot, v_key
    );
  end loop;
  perform pgque.send('review_hash_pages', 'null-slot', 'null-key', null);
end $$;

do $$
begin
  perform pgque.force_next_tick('review_hash_pages');
  perform pgque.ticker();
end $$;

do $$
declare
  v_slot int4;
  v_page record;
  v_ack record;
  v_seen bigint[];
  v_expected bigint[];
  v_message pgque.message;
begin
  for v_slot in 0..2 loop
    perform pgque.claim_slot(
      'review_hash_pages', 'part_c', v_slot,
      'hash-worker-' || v_slot, interval '1 minute'
    );
    select array_agg(ev_id order by ev_id) into v_expected
    from pgque.get_batch_events(
      pgque.next_batch(
        'review_hash_pages', pgque._slot_name('part_c', v_slot, 3)
      )
    ) as e
    where case
      when e.ev_extra1 is null then v_slot = 0
      else (
        pg_catalog.hashtextextended(e.ev_extra1, 0) % 3 + 3
      ) % 3 = v_slot
    end;
    v_seen := array[]::bigint[];
    loop
      select * into v_page
      from pgque.receive_page_partitioned(
        'review_hash_pages', 'part_c', v_slot, 3,
        'hash-worker-' || v_slot, 1
      );
      assert v_page.status = 'page', 'nonempty hash slot must return a page';
      foreach v_message in array v_page.messages loop
        assert case
          when v_message.extra1 is null then v_slot = 0
          else (
            pg_catalog.hashtextextended(v_message.extra1, 0) % 3 + 3
          ) % 3 = v_slot
        end, 'partition page returned an event from another hash slot';
        v_seen := array_append(v_seen, v_message.msg_id);
      end loop;
      select * into v_ack
      from pgque.ack_page(v_page.page_token, 'hash-worker-' || v_slot);
      exit when v_page.is_last;
      assert not v_ack.batch_finished,
        'nonterminal filtered page must not finish the batch';
    end loop;
    assert v_ack.batch_finished, 'terminal filtered page must finish the batch';
    assert v_seen = v_expected,
      'partition pages must preserve the filtered event order';
  end loop;
end $$;

do $$
begin
  perform pgque.create_queue('review_hash_empty');
  perform pgque.subscribe_slot('review_hash_empty', 'part_c', 2, 3);
  perform pgque.send('review_hash_empty', 'null-slot', 'only-slot-zero', null);
end $$;

do $$
begin
  perform pgque.force_next_tick('review_hash_empty');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
begin
  perform pgque.claim_slot(
    'review_hash_empty', 'part_c', 2, 'empty-worker', interval '1 minute'
  );
  select * into v_page
  from pgque.receive_page_partitioned(
    'review_hash_empty', 'part_c', 2, 3, 'empty-worker', 1
  );
  assert v_page.status = 'advanced',
    'a batch window empty after hash filtering must advance';
  assert cardinality(v_page.messages) = 0,
    'filtered-empty advance must not return messages';
end $$;

/* Real pages keep legacy receive/nack/allocation paths fenced. */
do $$
begin
  perform pgque.create_queue('review_legacy_normal');
  perform pgque.subscribe('review_legacy_normal', 'c1');
  perform pgque.send('review_legacy_normal', 'normal', 'one');
  perform pgque.send('review_legacy_normal', 'normal', 'two');

  perform pgque.create_queue('review_legacy_part');
  perform pgque.subscribe_slot('review_legacy_part', 'part_c', 0, 1);
  perform pgque.send('review_legacy_part', 'part', 'one', 'key');
  perform pgque.send('review_legacy_part', 'part', 'two', 'key');
end $$;

do $$
begin
  perform pgque.force_next_tick('review_legacy_normal');
  perform pgque.force_next_tick('review_legacy_part');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
  v_ack record;
  v_before jsonb;
  v_after jsonb;
  v_last_tick_before bigint;
  v_last_tick_after bigint;
  v_message pgque.message;
begin
  select * into v_page
  from pgque.receive_page(
    'review_legacy_normal', 'c1', 'normal-worker', 1, interval '1 minute'
  );
  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  select s.sub_last_tick into v_last_tick_before
  from pgque.subscription as s
  where s.sub_batch = v_page.batch_id;
  v_message := (v_page.messages)[1];
  perform pg_temp.expect_legacy_guard(format(
    'select * from pgque.receive(%L, %L, 1)',
    'review_legacy_normal', 'c1'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.nack(%s, row(%s, %s, null, null, null, null, null, null, null, null)::pgque.message, interval %L, %L)',
    v_page.batch_id, v_message.msg_id, v_page.batch_id, '1 second', 'guard'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select * from pgque.next_batch_custom(%L, %L, null, null, null)',
    'review_legacy_normal', 'c1'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.event_retry(%s, %s, clock_timestamp())',
    v_page.batch_id, v_message.msg_id
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.event_retry(%s, %s, 0)',
    v_page.batch_id, v_message.msg_id
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.batch_retry(%s, 0)', v_page.batch_id
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.register_consumer_at(%L, %L, %s)',
    'review_legacy_normal', 'c1', v_last_tick_before
  ));
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  select s.sub_last_tick into v_last_tick_after
  from pgque.subscription as s
  where s.sub_batch = v_page.batch_id;
  assert v_after = v_before,
    'outstanding-page legacy guards must preserve page state';
  assert v_last_tick_after = v_last_tick_before,
    'outstanding-page legacy guards must preserve sub_last_tick';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'normal-worker');
  assert not v_ack.batch_finished, 'first normal page must leave between-page state';
  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  select s.sub_last_tick into v_last_tick_before
  from pgque.subscription as s
  where s.sub_batch = v_page.batch_id;
  perform pg_temp.expect_legacy_guard(format(
    'select * from pgque.receive(%L, %L, 1)',
    'review_legacy_normal', 'c1'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.nack(%s, row(%s, %s, null, null, null, null, null, null, null, null)::pgque.message, interval %L, %L)',
    v_page.batch_id, v_message.msg_id, v_page.batch_id, '1 second', 'guard'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select * from pgque.next_batch_custom(%L, %L, null, null, null)',
    'review_legacy_normal', 'c1'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.event_retry(%s, %s, clock_timestamp())',
    v_page.batch_id, v_message.msg_id
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.event_retry(%s, %s, 0)',
    v_page.batch_id, v_message.msg_id
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.batch_retry(%s, 0)', v_page.batch_id
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.register_consumer_at(%L, %L, %s)',
    'review_legacy_normal', 'c1', v_last_tick_before
  ));
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  select s.sub_last_tick into v_last_tick_after
  from pgque.subscription as s
  where s.sub_batch = v_page.batch_id;
  assert v_after = v_before,
    'between-page legacy guards must preserve the checkpoint';
  assert v_last_tick_after = v_last_tick_before,
    'between-page legacy guards must preserve sub_last_tick';

  perform pgque.claim_slot(
    'review_legacy_part', 'part_c', 0, 'part-worker', interval '1 minute'
  );
  select * into v_page
  from pgque.receive_page_partitioned(
    'review_legacy_part', 'part_c', 0, 1, 'part-worker', 1
  );
  v_message := (v_page.messages)[1];
  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  perform pg_temp.expect_legacy_guard(format(
    'select * from pgque.receive_partitioned(%L, %L, 0, 1, %L, 1)',
    'review_legacy_part', 'part_c', 'part-worker'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.nack_partitioned(%L, %L, 0, 1, %L, row(%s, %s, null, null, null, null, null, null, null, null)::pgque.message, interval %L, %L)',
    'review_legacy_part', 'part_c', 'part-worker', v_message.msg_id,
    v_page.batch_id, '1 second', 'guard'
  ));
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  assert v_after = v_before,
    'partition legacy guards must preserve outstanding page state';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'part-worker');
  assert not v_ack.batch_finished,
    'first partition page must leave between-page state';
  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  perform pg_temp.expect_legacy_guard(format(
    'select * from pgque.receive_partitioned(%L, %L, 0, 1, %L, 1)',
    'review_legacy_part', 'part_c', 'part-worker'
  ));
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.nack_partitioned(%L, %L, 0, 1, %L, row(%s, %s, null, null, null, null, null, null, null, null)::pgque.message, interval %L, %L)',
    'review_legacy_part', 'part_c', 'part-worker', v_message.msg_id,
    v_page.batch_id, '1 second', 'guard'
  ));
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  assert v_after = v_before,
    'between-page partition guards must preserve the checkpoint';
end $$;

/* Cooperative live leases, busy status, legacy skipping and between-page takeover. */
do $$
begin
  perform pgque.create_queue('review_coop_gate');
  perform pgque.register_subconsumer('review_coop_gate', 'main_c', 'w1');
  perform pgque.register_subconsumer('review_coop_gate', 'main_c', 'w2');
  perform pgque.register_subconsumer('review_coop_gate', 'main_c', 'w3');
  perform pgque.send('review_coop_gate', 'coop', 'one');
  perform pgque.send('review_coop_gate', 'coop', 'two');
end $$;

do $$
begin
  perform pgque.force_next_tick('review_coop_gate');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
  v_other record;
  v_ack record;
  v_w1_id int4;
  v_legacy_batch bigint;
  v_victim_batch bigint;
  v_before jsonb;
  v_after jsonb;
begin
  select * into v_page
  from pgque.receive_page_coop(
    'review_coop_gate', 'main_c', 'w1', 'worker-w1',
    1, interval '1 second', interval '1 minute'
  );
  select * into v_other
  from pgque.receive_page_coop(
    'review_coop_gate', 'main_c', 'w1', 'other-worker',
    1, interval '1 second', interval '1 minute'
  );
  assert v_other.status = 'busy' and v_other.page_token is null,
    'a live cooperative page must report busy to another worker';

  select c.co_id into strict v_w1_id
  from pgque.consumer as c where c.co_name = 'main_c.w1';
  update pgque.subscription as s
  set sub_active = clock_timestamp() - interval '2 minutes'
  from pgque.queue as q
  where q.queue_name = 'review_coop_gate'
    and s.sub_queue = q.queue_id
    and s.sub_consumer = v_w1_id;
  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;

  select * into v_other
  from pgque.receive_page_coop(
    'review_coop_gate', 'main_c', 'w2', 'worker-w2',
    1, interval '1 second', interval '1 minute'
  );
  assert v_other.status = 'idle',
    'dead interval alone must not permit takeover while the page lease is live';
  assert exists (
    select 1 from pgque.subscription
    where sub_consumer = v_w1_id and sub_batch = v_page.batch_id
  ), 'live-lease refusal must leave the victim batch assigned';

  v_victim_batch := v_page.batch_id;
  v_legacy_batch := pgque.next_batch(
    'review_coop_gate', 'main_c', 'w3', interval '1 second'
  );
  assert v_legacy_batch is null,
    'legacy allocator must return null when the only victim has paged state';
  assert exists (
    select 1
    from pgque.subscription
    where sub_consumer = v_w1_id and sub_batch = v_victim_batch
  ), 'legacy allocator must preserve the paged victim batch';
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  where ps.active_batch_id = v_page.batch_id;
  assert v_after = v_before,
    'legacy allocator must skip a cooperative victim with paged state';

  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-w1');
  assert not v_ack.batch_finished,
    'first cooperative page must leave a between-page checkpoint';
  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = 'review_coop_gate'
    and ps.consumer_id = v_w1_id;
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.unregister_subconsumer(%L, %L, %L)',
    'review_coop_gate', 'main_c', 'w1'
  ));
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = 'review_coop_gate'
    and ps.consumer_id = v_w1_id;
  assert v_after = v_before,
    'between-page unregister_subconsumer guard must preserve the checkpoint';
  update pgque.subscription as s
  set sub_active = clock_timestamp() - interval '2 minutes'
  from pgque.queue as q
  where q.queue_name = 'review_coop_gate'
    and s.sub_queue = q.queue_id
    and s.sub_consumer = v_w1_id;

  select * into v_other
  from pgque.receive_page_coop(
    'review_coop_gate', 'main_c', 'w2', 'worker-w2',
    1, interval '1 second', interval '1 minute'
  );
  assert v_other.status = 'page' and v_other.page_number = 2,
    'a dead cooperative member may be taken over between pages';
  assert v_other.batch_id <> v_page.batch_id,
    'between-page takeover must mint a new batch token';

  select to_jsonb(ps) into v_before
  from pgque.page_state as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = 'review_coop_gate'
    and ps.consumer_id = (
      select co_id from pgque.consumer where co_name = 'main_c.w2'
    );
  perform pg_temp.expect_legacy_guard(format(
    'select pgque.unregister_subconsumer(%L, %L, %L)',
    'review_coop_gate', 'main_c', 'w2'
  ));
  select to_jsonb(ps) into v_after
  from pgque.page_state as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = 'review_coop_gate'
    and ps.consumer_id = (
      select co_id from pgque.consumer where co_name = 'main_c.w2'
    );
  assert v_after = v_before,
    'unregister_subconsumer guard must preserve the outstanding page';
end $$;

do $$
declare
  v_queue text;
begin
  foreach v_queue in array array[
    'review_owner_normal',
    'review_owner_coop',
    'review_owner_part',
    'review_epoch_fence',
    'review_hash_pages',
    'review_hash_empty',
    'review_legacy_normal',
    'review_legacy_part',
    'review_coop_gate'
  ] loop
    perform pgque.drop_queue(v_queue, true);
  end loop;
end $$;

\echo 'PASS: test_paged_review'
