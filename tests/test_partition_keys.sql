\set ON_ERROR_STOP on

-- Test partition keys (Phase 1A)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Covers user stories US-12.1 .. US-12.7; see
-- blueprints/partition-keys/SPEC.md. Producer idempotency (US-13.x) is
-- covered by tests/test_send_idem.sql. dblink gives the second real
-- backend for US-12.4/12.5 (extensions are allowed in tests/); the
-- two-process variant is tests/two_session_slot_claim.sh.
--
-- PgQ requires insert, ticker, and receive to be in separate transactions
-- (snapshot visibility). Each DO block is a separate transaction.

create extension if not exists dblink;

-- ---------------------------------------------------------------------------
-- US-12.1: keyed send lands in ev_extra1 (jsonb + text overloads)
-- ---------------------------------------------------------------------------
do $$
begin
  perform pgque.create_queue('pk_send');
end $$;

do $$
declare
  v_id bigint;
  v_extra text;
begin
  v_id := pgque.send('pk_send', 'ev', '{"a":1}'::jsonb, 'tenant-1');
  execute format('select ev_extra1 from %s where ev_id = %s',
    pgque.current_event_table('pk_send'), v_id)
  into v_extra;
  assert v_extra = 'tenant-1',
    format('US-12.1: jsonb send must store key in ev_extra1, got %s', v_extra);

  v_id := pgque.send('pk_send', 'ev', '{"b":2}'::text, 'tenant-2');
  execute format('select ev_extra1 from %s where ev_id = %s',
    pgque.current_event_table('pk_send'), v_id)
  into v_extra;
  assert v_extra = 'tenant-2',
    format('US-12.1: text send must store key in ev_extra1, got %s', v_extra);

  raise notice 'PASS US-12.1: keyed send stores partition key in ev_extra1';
end $$;

-- ---------------------------------------------------------------------------
-- US-12.2 / US-12.3 setup: 2 slots, 3 keys, interleaved events
-- ---------------------------------------------------------------------------
do $$
begin
  perform pgque.create_queue('pk_q');
  perform pgque.subscribe_slot('pk_q', 'w', 0, 2);
  perform pgque.subscribe_slot('pk_q', 'w', 1, 2);
  -- Idempotent re-subscribe with the same (slot, n) must not raise.
  perform pgque.subscribe_slot('pk_q', 'w', 0, 2);
end $$;

-- Pin the hash routing (T-G1a): concrete (key, expected slot) pairs.
do $$
begin
  assert (pg_catalog.hashtextextended('tenant-a', 0) % 2 + 2) % 2 = 0,
    'US-12.2: pinned hash: tenant-a must route to slot 0';
  assert (pg_catalog.hashtextextended('tenant-b', 0) % 2 + 2) % 2 = 1,
    'US-12.2: pinned hash: tenant-b must route to slot 1';
  assert (pg_catalog.hashtextextended('tenant-c', 0) % 2 + 2) % 2 = 1,
    'US-12.2: pinned hash: tenant-c must route to slot 1';
end $$;

-- Interleaved: a1 b1 c1 a2 b2 c2 a3 b3 c3 (3 keys x 3 events)
do $$
declare
  i int;
  k text;
begin
  for i in 1..3 loop
    foreach k in array array['tenant-a', 'tenant-b', 'tenant-c'] loop
      perform pgque.send('pk_q', 'ev', format('{"seq":%s}', i)::jsonb, k);
    end loop;
  end loop;
end $$;

do $$
begin
  perform pgque.force_next_tick('pk_q');
  perform pgque.ticker();
end $$;

-- US-12.6 (pre-drain): view shows both slots, unclaimed, with lag
do $$
declare
  v_rows int;
  v_pending bigint;
begin
  select count(*) into v_rows
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w';
  assert v_rows = 2,
    format('US-12.6: expected 2 slot rows in partition_slot_status, got %s', v_rows);

  perform 1
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and n <> 2;
  assert not found, 'US-12.6: all slot rows must show n = 2';

  perform 1
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and owner_pid is not null;
  assert not found, 'US-12.6: unclaimed slots must show owner_pid is null';

  perform 1
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and last_tick is null;
  assert not found, 'US-12.6: registered slots must show last_tick';

  select min(pending_events) into v_pending
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w';
  assert v_pending >= 9,
    format('US-12.6: pre-drain pending_events must be >= 9, got %s', v_pending);

  raise notice 'PASS US-12.6: partition_slot_status shows slots, n, cursor lag';
end $$;

-- Drain both slots into a temp capture table
create temp table pk_got (
  ord bigint generated always as identity,
  slot int not null,
  msg_id bigint not null,
  key text
);

do $$
declare
  v_slot int;
  v_msg pgque.message;
  v_cnt int;
begin
  for v_slot in 0..1 loop
    v_cnt := 0;
    for v_msg in
      select * from pgque.receive_partitioned('pk_q', 'w', v_slot, 2, 100)
    loop
      insert into pk_got (slot, msg_id, key)
      values (v_slot, v_msg.msg_id, v_msg.extra1);
      v_cnt := v_cnt + 1;
    end loop;
    assert v_cnt > 0, format('slot %s should receive at least one event', v_slot);
    perform pgque.ack_partitioned('pk_q', 'w', v_slot, 2);

    -- Same tick window is consumed: a second receive must return nothing.
    v_cnt := 0;
    for v_msg in
      select * from pgque.receive_partitioned('pk_q', 'w', v_slot, 2, 100)
    loop
      v_cnt := v_cnt + 1;
    end loop;
    assert v_cnt = 0,
      format('slot %s: no events expected after ack within one tick window', v_slot);
  end loop;
end $$;

-- US-12.2: per-key affinity + ev_id order; US-12.3: disjoint union = stream
do $$
declare
  v_total int;
  v_distinct int;
begin
  select count(*), count(distinct msg_id) into v_total, v_distinct from pk_got;
  assert v_total = 9,
    format('US-12.3: union of slots must be all 9 events, got %s', v_total);
  assert v_distinct = 9,
    format('US-12.3: slots must be pairwise disjoint, got %s distinct of %s', v_distinct, v_total);

  perform 1
  from (
    select key
    from pk_got
    group by key
    having count(distinct slot) > 1
  ) as x;
  assert not found, 'US-12.2: each key must be delivered by exactly one slot';

  -- Delivered slot must equal the pinned hash slot.
  perform 1
  from pk_got
  where slot <> (pg_catalog.hashtextextended(key, 0) % 2 + 2) % 2;
  assert not found, 'US-12.2: delivered slot must match hash routing';

  -- Per-key delivery order must be ev_id order.
  perform 1
  from (
    select
      key,
      array_agg(msg_id order by ord) as got,
      array_agg(msg_id order by msg_id) as want
    from pk_got
    group by key
  ) as x
  where got <> want;
  assert not found, 'US-12.2: per-key delivery must be in ev_id order';

  raise notice 'PASS US-12.2: per-key affinity + ev_id order';
  raise notice 'PASS US-12.3: slots disjoint, union = whole stream';
end $$;

-- US-12.6 (post-drain): cursor caught up, pending_events = 0
do $$
declare
  v_pending bigint;
begin
  select max(pending_events) into v_pending
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w';
  assert v_pending = 0,
    format('US-12.6: post-drain pending_events must be 0, got %s', v_pending);
end $$;

-- ---------------------------------------------------------------------------
-- US-12.7: wrong N (and out-of-range slot) rejected, never misrouted
-- ---------------------------------------------------------------------------
do $$
declare
  v_raised boolean;
begin
  -- receive with wrong n
  v_raised := false;
  begin
    perform * from pgque.receive_partitioned('pk_q', 'w', 0, 3, 10);
  exception
    when others then
      v_raised := true;
      assert sqlerrm like '%n=%',
        'US-12.7: wrong-N receive error must name the pinned n, got: ' || sqlerrm;
  end;
  assert v_raised, 'US-12.7: receive_partitioned with wrong n must raise';

  -- subscribe with mismatched n
  v_raised := false;
  begin
    perform pgque.subscribe_slot('pk_q', 'w', 0, 3);
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'US-12.7: subscribe_slot with changed n must raise';

  -- slot out of range
  v_raised := false;
  begin
    perform * from pgque.receive_partitioned('pk_q', 'w', 5, 2, 10);
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'US-12.7: out-of-range slot must raise';

  v_raised := false;
  begin
    perform pgque.subscribe_slot('pk_q', 'w2', 2, 2);
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'US-12.7: subscribe_slot slot >= n must raise';

  -- ack with wrong n
  v_raised := false;
  begin
    perform pgque.ack_partitioned('pk_q', 'w', 0, 3);
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'US-12.7: ack_partitioned with wrong n must raise';

  raise notice 'PASS US-12.7: wrong N / out-of-range slot rejected';
end $$;

-- ---------------------------------------------------------------------------
-- US-12.4 / US-12.5: claim/release + second session + crash recovery
-- (dblink gives a second real backend; tests/two_session_slot_claim.sh is
-- the two-process variant of the same stories.)
-- ---------------------------------------------------------------------------
do $$
declare
  v_ok boolean;
  v_pid int;
  v_i int;
begin
  perform dblink_connect('pk_s2',
    format('host=localhost port=%s dbname=%s user=%s',
      current_setting('port'), current_database(), current_user));

  -- This session claims slot 0.
  assert pgque.claim_slot('pk_q', 'w', 0),
    'US-12.5: claim of a free slot must succeed';

  -- Second session cannot claim slot 0 (steered away, US-12.4) ...
  select ok into v_ok
  from dblink('pk_s2',
    $q$select pgque.claim_slot('pk_q', 'w', 0)$q$) as t(ok boolean);
  assert not v_ok, 'US-12.4: second session must not claim an owned slot';

  -- ... and lands on the free slot 1 instead.
  select ok into v_ok
  from dblink('pk_s2',
    $q$select pgque.claim_slot('pk_q', 'w', 1)$q$) as t(ok boolean);
  assert v_ok, 'US-12.4: second session must claim the free slot';

  -- Now slot 1 is owned elsewhere: this session cannot take it.
  assert not pgque.claim_slot('pk_q', 'w', 1),
    'US-12.4: claim of a slot owned by another session must fail';

  -- US-12.6: owner_pid reflects the claim holders.
  select owner_pid into v_pid
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_pid = pg_backend_pid(),
    format('US-12.6: slot 0 owner_pid must be this backend, got %s', v_pid);

  select owner_pid into v_pid
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 1;
  assert v_pid is not null and v_pid <> pg_backend_pid(),
    'US-12.6: slot 1 owner_pid must be the second session';

  -- US-12.5: release at a batch boundary frees the slot.
  assert pgque.release_slot('pk_q', 'w', 0),
    'US-12.5: release of a held slot must return true';
  assert not pgque.release_slot('pk_q', 'w', 0),
    'US-12.5: release of a non-held slot must return false';

  select owner_pid into v_pid
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_pid is null, 'US-12.6: released slot must show owner_pid null';

  -- US-12.5: session death releases the claim immediately (crash recovery).
  perform dblink_disconnect('pk_s2');
  v_ok := false;
  for v_i in 1..100 loop
    if pgque.claim_slot('pk_q', 'w', 1) then
      v_ok := true;
      exit;
    end if;
    perform pg_sleep(0.1);
  end loop;
  assert v_ok, 'US-12.5: dead session''s slot must become claimable';
  perform pgque.release_slot('pk_q', 'w', 1);

  raise notice 'PASS US-12.4: second session steered away from owned slot';
  raise notice 'PASS US-12.5: claim/release + crash recovery';
end $$;

-- ---------------------------------------------------------------------------
-- Cleanup: unsubscribe drops slots; last slot drops the pinned-N row
-- ---------------------------------------------------------------------------
do $$
begin
  perform pgque.unsubscribe_slot('pk_q', 'w', 0);
  perform pgque.unsubscribe_slot('pk_q', 'w', 1);

  perform 1
  from pgque.partition_consumer as pc
  join pgque.queue as q on q.queue_id = pc.queue_id
  where q.queue_name = 'pk_q' and pc.co_name = 'w';
  assert not found,
    'unsubscribe of the last slot must drop the partition_consumer row';

  perform pgque.drop_queue('pk_q');
  perform pgque.drop_queue('pk_send');
  raise notice 'PASS: partition keys (Phase 1A)';
end $$;
