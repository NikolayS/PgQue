\set ON_ERROR_STOP on

-- Test partition keys (Phase 1A)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Covers user stories US-12.1 .. US-12.7; see
-- blueprints/partition-keys/SPEC.md. Producer idempotency (US-13.x) is
-- covered by tests/test_send_idem.sql. The two-process variant of the
-- lease stories is tests/two_session_slot_claim.sh.
--
-- Slot ownership is a batch-granularity LEASE (worker id + TTL + epoch)
-- stored in pgque.partition_slot -- plain transactional DML, so it works
-- under transaction-mode pooling and needs no session state. Ownership is
-- per WORKER ID, not per backend: one session can exercise multi-worker
-- scenarios, which is what the lease tests below do.
--
-- PgQ requires insert, ticker, and receive to be in separate transactions
-- (snapshot visibility). Each DO block is a separate transaction.

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

-- US-12.6 (pre-drain): view shows both slots, unleased, with lag
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
  where queue_name = 'pk_q' and consumer = 'w' and lease_owner is not null;
  assert not found, 'US-12.6: unleased slots must show lease_owner is null';

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

-- Drain both slots into a temp capture table. receive/ack require a live
-- lease (G2 is server-enforced), so each slot is claimed first.
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
  v_epoch bigint;
begin
  for v_slot in 0..1 loop
    v_epoch := pgque.claim_slot('pk_q', 'w', v_slot, 'wk-main');
    assert v_epoch is not null,
      format('slot %s: claim of a free slot must return an epoch', v_slot);

    v_cnt := 0;
    for v_msg in
      select * from pgque.receive_partitioned('pk_q', 'w', v_slot, 2, 'wk-main', 100)
    loop
      insert into pk_got (slot, msg_id, key)
      values (v_slot, v_msg.msg_id, v_msg.extra1);
      v_cnt := v_cnt + 1;
    end loop;
    assert v_cnt > 0, format('slot %s should receive at least one event', v_slot);
    perform pgque.ack_partitioned('pk_q', 'w', v_slot, 2, 'wk-main');

    -- Same tick window is consumed: a second receive must return nothing.
    v_cnt := 0;
    for v_msg in
      select * from pgque.receive_partitioned('pk_q', 'w', v_slot, 2, 'wk-main', 100)
    loop
      v_cnt := v_cnt + 1;
    end loop;
    assert v_cnt = 0,
      format('slot %s: no events expected after ack within one tick window', v_slot);

    perform pgque.release_slot('pk_q', 'w', v_slot, 'wk-main');
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
    perform * from pgque.receive_partitioned('pk_q', 'w', 0, 3, 'wk-main', 10);
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
    perform * from pgque.receive_partitioned('pk_q', 'w', 5, 2, 'wk-main', 10);
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
    perform pgque.ack_partitioned('pk_q', 'w', 0, 3, 'wk-main');
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'US-12.7: ack_partitioned with wrong n must raise';

  raise notice 'PASS US-12.7: wrong N / out-of-range slot rejected';
end $$;

-- ---------------------------------------------------------------------------
-- US-12.4 / US-12.5: lease claim/release, exclusivity, expiry recovery.
-- Leases are keyed by worker id, so exclusivity between two workers is
-- provable in one session; tests/two_session_slot_claim.sh proves the same
-- across two real backends.
-- ---------------------------------------------------------------------------
do $$
declare
  v_e1 bigint;
  v_e2 bigint;
  v_owner text;
begin
  -- Free slot: claim returns an epoch.
  v_e1 := pgque.claim_slot('pk_q', 'w', 0, 'wk-a');
  assert v_e1 is not null, 'US-12.5: claim of a free slot must return an epoch';

  -- A second worker cannot claim a leased slot (steered away, US-12.4).
  assert pgque.claim_slot('pk_q', 'w', 0, 'wk-b') is null,
    'US-12.4: second worker must not claim a leased slot';

  -- Re-claim by the owner renews the lease, same epoch (no takeover).
  v_e2 := pgque.claim_slot('pk_q', 'w', 0, 'wk-a');
  assert v_e2 = v_e1,
    format('US-12.5: owner re-claim must renew with the same epoch, got % -> %', v_e1, v_e2);

  -- ... and lands on the free slot 1 instead.
  assert pgque.claim_slot('pk_q', 'w', 1, 'wk-b') is not null,
    'US-12.4: second worker must claim the free slot';

  -- US-12.6: lease_owner reflects the lease holders.
  select lease_owner into v_owner
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_owner = 'wk-a',
    format('US-12.6: slot 0 lease_owner must be wk-a, got %s', v_owner);

  select lease_owner into v_owner
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 1;
  assert v_owner = 'wk-b',
    format('US-12.6: slot 1 lease_owner must be wk-b, got %s', v_owner);

  -- US-12.5: release at a batch boundary frees the slot; only the owner can.
  assert not pgque.release_slot('pk_q', 'w', 0, 'wk-b'),
    'US-12.5: release by a non-owner must return false';
  assert pgque.release_slot('pk_q', 'w', 0, 'wk-a'),
    'US-12.5: release by the owner must return true';
  assert not pgque.release_slot('pk_q', 'w', 0, 'wk-a'),
    'US-12.5: release of an unleased slot must return false';

  select lease_owner into v_owner
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_owner is null, 'US-12.6: released slot must show lease_owner null';

  perform pgque.release_slot('pk_q', 'w', 1, 'wk-b');

  raise notice 'PASS US-12.4: second worker steered away from leased slot';
  raise notice 'PASS US-12.5: lease claim/release + owner-only release';
end $$;

-- US-12.5 (crash recovery): an expired lease is taken over with an epoch
-- bump. The dead worker never releases -- expiry is the recovery path.
do $$
declare
  v_dead bigint;
  v_new bigint;
begin
  v_dead := pgque.claim_slot('pk_q', 'w', 0, 'wk-dead', '1 second');
  assert v_dead is not null, 'US-12.5: short-TTL claim must succeed';

  -- Lease still live: takeover refused.
  assert pgque.claim_slot('pk_q', 'w', 0, 'wk-heir') is null,
    'US-12.5: live lease must not be taken over';

  perform pg_sleep(1.2);

  v_new := pgque.claim_slot('pk_q', 'w', 0, 'wk-heir');
  assert v_new is not null,
    'US-12.5: expired lease must be claimable by another worker';
  assert v_new > v_dead,
    format('US-12.5: takeover must bump the epoch (fencing), got % -> %', v_dead, v_new);

  perform pgque.release_slot('pk_q', 'w', 0, 'wk-heir');
  raise notice 'PASS US-12.5: expired lease taken over with epoch bump';
end $$;

-- G2 enforcement: receive/ack without holding the lease must raise.
do $$
declare
  v_raised boolean;
begin
  v_raised := false;
  begin
    perform * from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-nobody', 10);
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'G2: receive without a lease must raise';

  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-a');
  v_raised := false;
  begin
    perform pgque.ack_partitioned('pk_q', 'w', 0, 2, 'wk-b');
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'G2: ack by a non-owner must raise';
  perform pgque.release_slot('pk_q', 'w', 0, 'wk-a');

  raise notice 'PASS G2: receive/ack are lease-fenced';
end $$;

-- ---------------------------------------------------------------------------
-- Zombie fencing: after a takeover, the old worker's ack raises and the
-- new owner is re-issued the same open batch (at-least-once, never lost).
-- ---------------------------------------------------------------------------
do $$
begin
  perform pgque.send('pk_q', 'ev', '{"fence":1}'::jsonb, 'tenant-a');
  perform pgque.send('pk_q', 'ev', '{"fence":2}'::jsonb, 'tenant-a');
end $$;

do $$
begin
  perform pgque.force_next_tick('pk_q');
  perform pgque.ticker();
end $$;

do $$
declare
  v_zombie bigint;
  v_heir bigint;
  v_first bigint[];
  v_second bigint[];
  v_raised boolean;
begin
  -- Zombie opens a batch under a short lease, then stalls past the TTL.
  v_zombie := pgque.claim_slot('pk_q', 'w', 0, 'wk-zombie', '1 second');
  assert v_zombie is not null, 'fencing: zombie claim must succeed';

  select array_agg(m.msg_id order by m.msg_id) into v_first
  from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-zombie', 100) as m;
  assert cardinality(v_first) = 2,
    format('fencing: zombie must open a 2-event batch, got %s', coalesce(cardinality(v_first), 0));

  perform pg_sleep(1.2);

  -- Heir takes over the expired lease (epoch bump) and is re-issued the
  -- SAME still-open batch idempotently (engine receive lock, US-12.4).
  v_heir := pgque.claim_slot('pk_q', 'w', 0, 'wk-heir');
  assert v_heir is not null and v_heir > v_zombie,
    'fencing: heir must take over with an epoch bump';

  select array_agg(m.msg_id order by m.msg_id) into v_second
  from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-heir', 100) as m;
  assert v_second = v_first,
    'fencing: heir must be re-issued the zombie''s open batch, not a divergent one';

  -- The zombie is fenced: its ack raises instead of silently double-acking.
  v_raised := false;
  begin
    perform pgque.ack_partitioned('pk_q', 'w', 0, 2, 'wk-zombie');
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'fencing: zombie ack after takeover must raise';

  -- The heir acks normally.
  perform pgque.ack_partitioned('pk_q', 'w', 0, 2, 'wk-heir');
  perform pgque.release_slot('pk_q', 'w', 0, 'wk-heir');

  raise notice 'PASS fencing: zombie ack raises after takeover; heir re-issued same batch';
end $$;

-- ---------------------------------------------------------------------------
-- T-retry-affinity (SPEC section 9): a nacked keyed event is redelivered to
-- the SAME slot only (ev_extra1 preserved through maint_retry_events).
-- ---------------------------------------------------------------------------
do $$
begin
  perform pgque.send('pk_q', 'ev', '{"retry":1}'::jsonb, 'tenant-a');  -- slot 0
  perform pgque.send('pk_q', 'ev', '{"retry":1}'::jsonb, 'tenant-b');  -- slot 1
end $$;

do $$
begin
  perform pgque.force_next_tick('pk_q');
  perform pgque.ticker();
end $$;

create temp table pk_retry (retried_id bigint);

do $$
declare
  v_msg pgque.message;
  v_cnt int := 0;
begin
  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-main');
  for v_msg in
    select * from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-main', 100)
  loop
    v_cnt := v_cnt + 1;
    assert v_msg.extra1 = 'tenant-a',
      format('retry-affinity: slot 0 must only see tenant-a, got %s', v_msg.extra1);
    insert into pk_retry values (v_msg.msg_id);
    perform pgque.nack_partitioned('pk_q', 'w', 0, 2, 'wk-main', v_msg, '0 seconds', 'retry-affinity test');
  end loop;
  assert v_cnt = 1, format('retry-affinity: expected 1 event on slot 0, got %s', v_cnt);
  perform pgque.ack_partitioned('pk_q', 'w', 0, 2, 'wk-main');
  perform pgque.release_slot('pk_q', 'w', 0, 'wk-main');
end $$;

-- Re-inject the retry row and open a new tick window.
do $$
begin
  perform pgque.maint_retry_events();
end $$;

do $$
begin
  perform pgque.force_next_tick('pk_q');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_retried bigint;
  v_seen boolean := false;
begin
  select retried_id into v_retried from pk_retry;

  -- Slot 1 must NOT see the retried event (only its own pending tenant-b).
  perform pgque.claim_slot('pk_q', 'w', 1, 'wk-main');
  for v_msg in
    select * from pgque.receive_partitioned('pk_q', 'w', 1, 2, 'wk-main', 100)
  loop
    assert v_msg.msg_id <> v_retried,
      'retry-affinity: retried event must not leak to another slot';
  end loop;
  perform pgque.ack_partitioned('pk_q', 'w', 1, 2, 'wk-main');
  perform pgque.release_slot('pk_q', 'w', 1, 'wk-main');

  -- Slot 0 must see it again, same key, retry counter incremented.
  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-main');
  for v_msg in
    select * from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-main', 100)
  loop
    if v_msg.msg_id = v_retried then
      v_seen := true;
      assert v_msg.extra1 = 'tenant-a',
        'retry-affinity: retried event must keep its partition key';
      assert v_msg.retry_count >= 1,
        'retry-affinity: redelivered event must carry retry_count >= 1';
    end if;
  end loop;
  assert v_seen, 'retry-affinity: retried event must be redelivered to its own slot';
  perform pgque.ack_partitioned('pk_q', 'w', 0, 2, 'wk-main');
  perform pgque.release_slot('pk_q', 'w', 0, 'wk-main');

  raise notice 'PASS T-retry-affinity: nacked keyed event redelivered to the same slot only';
end $$;

-- ---------------------------------------------------------------------------
-- G2 raw-consumer guard: the plain receive/ack/nack API must refuse a
-- partition slot consumer ("<consumer>#<slot>/<n>"). Otherwise a reader could
-- bypass the lease AND the hash filter by driving the raw slot consumer
-- directly (unfiltered stream on receive, zombie double-ack on ack/nack).
-- Runs while queue 'pk_q' / consumer 'w' (n=2) are still subscribed.
-- ---------------------------------------------------------------------------

-- (a) plain pgque.receive() on a raw slot consumer must raise.
do $$
declare
  v_raised boolean := false;
begin
  begin
    perform * from pgque.receive('pk_q', 'w#0/2', 10);
  exception
    when others then
      v_raised := true;
      assert sqlerrm like '%receive_partitioned%',
        'guard: plain receive error must point to receive_partitioned(), got: ' || sqlerrm;
  end;
  assert v_raised, 'guard: plain receive() on a slot consumer must raise';
  raise notice 'PASS guard: plain receive() refuses a slot consumer';
end $$;

-- (a2) pgque.subscribe() must reject the reserved '#' at registration time.
-- The plain receive/ack/nack guards treat any '#' name as a partition slot
-- consumer, so a plain consumer registered with a '#' in its name would be
-- permanently locked out of receive/ack/nack. Reject '#' up front instead.
do $$
declare
  v_raised boolean := false;
begin
  begin
    perform pgque.subscribe('pk_q', 'team#1');
  exception
    when others then
      v_raised := true;
      assert sqlerrm like '%#%',
        'guard: subscribe reject error must name the reserved #, got: ' || sqlerrm;
  end;
  assert v_raised, 'guard: subscribe() must reject a consumer name containing #';

  -- No half-created state: the rejected name must not leave a consumer row.
  perform 1 from pgque.consumer where co_name = 'team#1';
  assert not found, 'guard: rejected subscribe() must not create the consumer';
  raise notice 'PASS guard: subscribe() refuses # in the consumer name';
end $$;

-- (b)/(c) setup: open a non-empty slot-0 batch under a live lease so the plain
-- ack/nack guards have a real batch id to reject.
do $$
begin
  perform pgque.send('pk_q', 'ev', '{"guard":1}'::jsonb, 'tenant-a');  -- slot 0
end $$;

do $$
begin
  perform pgque.force_next_tick('pk_q');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_open pgque.message;
  v_batch bigint;
  v_raised boolean;
begin
  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-guard');
  for v_msg in
    select * from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-guard', 100)
  loop
    v_batch := v_msg.batch_id;
    v_open := v_msg;
  end loop;
  assert v_batch is not null, 'guard setup: slot 0 batch must be open';

  -- (b) plain ack on a slot batch must raise.
  v_raised := false;
  begin
    perform pgque.ack(v_batch);
  exception
    when others then
      v_raised := true;
      assert sqlerrm like '%ack_partitioned%',
        'guard: plain ack error must point to ack_partitioned(), got: ' || sqlerrm;
  end;
  assert v_raised, 'guard: plain ack() on a slot batch must raise';

  -- (c) plain nack on a slot batch must raise (batch still open after (b)).
  v_raised := false;
  begin
    perform pgque.nack(v_batch, v_open, '0 seconds', 'guard test');
  exception
    when others then
      v_raised := true;
      assert sqlerrm like '%nack_partitioned%',
        'guard: plain nack error must point to nack_partitioned(), got: ' || sqlerrm;
  end;
  assert v_raised, 'guard: plain nack() on a slot batch must raise';

  -- Finish the batch properly through the partitioned API and release.
  perform pgque.ack_partitioned('pk_q', 'w', 0, 2, 'wk-guard');
  perform pgque.release_slot('pk_q', 'w', 0, 'wk-guard');
  raise notice 'PASS guard: plain ack()/nack() refuse a slot batch';
end $$;

-- (d) cheap gap coverage: claim_slot input validation + epoch/lease_until view.
do $$
declare
  v_raised boolean;
begin
  -- ttl below the 1-second floor is rejected.
  v_raised := false;
  begin
    perform pgque.claim_slot('pk_q', 'w', 0, 'wk-x', '500 milliseconds');
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'guard: claim_slot with ttl < 1 second must raise';

  -- Empty worker id is rejected.
  v_raised := false;
  begin
    perform pgque.claim_slot('pk_q', 'w', 0, '');
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'guard: claim_slot with empty worker must raise';

  -- Null worker id is rejected.
  v_raised := false;
  begin
    perform pgque.claim_slot('pk_q', 'w', 0, null);
  exception
    when others then v_raised := true;
  end;
  assert v_raised, 'guard: claim_slot with null worker must raise';

  -- A consumer that was never subscribed as partitioned is rejected.
  v_raised := false;
  begin
    perform pgque.claim_slot('pk_q', 'never-subscribed', 0, 'wk-x');
  exception
    when others then
      v_raised := true;
      assert sqlerrm like '%not a partitioned consumer%',
        'guard: claim_slot on an unsubscribed consumer must say so, got: ' || sqlerrm;
  end;
  assert v_raised, 'guard: claim_slot on a non-partitioned consumer must raise';

  raise notice 'PASS guard: claim_slot input validation';
end $$;

-- Epoch is exposed by partition_slot_status and increases on expiry takeover.
do $$
declare
  v_epoch_before bigint;
  v_epoch_after bigint;
begin
  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-ep1', '1 second');
  select epoch into v_epoch_before
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_epoch_before is not null,
    'guard: partition_slot_status must expose the epoch column';

  perform pg_sleep(1.2);

  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-ep2');  -- takeover of expired lease
  select epoch into v_epoch_after
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_epoch_after > v_epoch_before,
    format('guard: takeover must advance the view epoch, got % -> %', v_epoch_before, v_epoch_after);

  perform pgque.release_slot('pk_q', 'w', 0, 'wk-ep2');
  raise notice 'PASS guard: partition_slot_status.epoch advances on takeover';
end $$;

-- Lease renewal: receive_partitioned moves partition_slot_status.lease_until
-- forward (heartbeat) even when the slot has no pending events.
do $$
declare
  v_until_before timestamptz;
  v_until_after timestamptz;
begin
  perform pgque.claim_slot('pk_q', 'w', 0, 'wk-renew', '2 seconds');
  select lease_until into v_until_before
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;

  perform pg_sleep(0.05);
  perform * from pgque.receive_partitioned('pk_q', 'w', 0, 2, 'wk-renew', 10);

  select lease_until into v_until_after
  from pgque.partition_slot_status
  where queue_name = 'pk_q' and consumer = 'w' and slot = 0;
  assert v_until_after > v_until_before,
    format('guard: receive_partitioned must renew lease_until, got % -> %', v_until_before, v_until_after);

  perform pgque.release_slot('pk_q', 'w', 0, 'wk-renew');
  raise notice 'PASS guard: receive_partitioned renews the lease (heartbeat)';
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

  perform 1 from pgque.partition_slot as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = 'pk_q' and ps.co_name = 'w';
  assert not found,
    'unsubscribe of the last slot must drop its partition_slot lease rows';

  perform pgque.drop_queue('pk_q');
  perform pgque.drop_queue('pk_send');
  raise notice 'PASS: partition keys (Phase 1A)';
end $$;
