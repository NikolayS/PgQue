\set ON_ERROR_STOP on

-- US-12: Partition keys (Fabrizio Case 2 -- multi-tenant storage lifecycle events)
-- As a multi-tenant event producer and operator, I want events that share a
-- partition key (tenant) consumed in order by a single slot at a time, while
-- events of different keys are consumed in parallel across slots -- the
-- log-native (Kafka-partition) model: order within a key, parallelism across keys.
--
-- Covers US-12.1, 12.2, 12.3, 12.4 (single-session facet), 12.5, 12.6, 12.7.
--
-- Slot ownership is a batch-granularity LEASE (worker id + TTL + epoch fencing
-- token) stored in a table -- plain transactional DML, so it works under
-- transaction-mode pooling and needs no session state. Crash recovery is lease
-- EXPIRY, not session death: a dead worker never releases, and its lease is
-- taken over the instant the TTL lapses (with an epoch bump for fencing).
-- Because leases are keyed by worker id, exclusivity between workers is provable
-- in a single session; the cross-backend variant is tests/two_session_slot_claim.sh.
--
-- blueprints/partition-keys/SPEC.md section 17 (User stories)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- CRITICAL: send and tick MUST be in separate DO blocks -- PgQ batches by txid
-- snapshot, so an event inserted in the tick's own transaction is invisible to
-- that tick (matches the us2/us3 separate-DO-block convention).

-- ===========================================================================
-- US-12.1 -- Keyed send: pgque.send(queue, type, payload, partition_key) so each
-- event carries its tenant key, stored in ev_extra1.
-- ===========================================================================

-- Setup
do $$ begin
  perform pgque.create_queue('us12_keyed');
  perform pgque.subscribe('us12_keyed', 'reader');
end $$;

-- Action: keyed send via the jsonb overload and the text overload
do $$ begin
  perform pgque.send('us12_keyed', 'file.created', '{"file":1}'::jsonb, 'tenant-xyz');
  perform pgque.send('us12_keyed', 'file.deleted', '{"file":2}', 'tenant-xyz');
end $$;

-- Tick (force_next_tick bypasses throttle)
do $$ begin
  perform pgque.force_next_tick('us12_keyed');
  perform pgque.ticker();
end $$;

-- Assert: the partition key rides ev_extra1 all the way to pgque.message.extra1
do $$
declare
  v_msg     pgque.message;
  v_count   int := 0;
  v_batch   bigint;
begin
  for v_msg in select * from pgque.receive('us12_keyed', 'reader', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
    assert v_msg.extra1 = 'tenant-xyz',
      'US-12.1: partition key must land in ev_extra1, got ' || coalesce(v_msg.extra1, 'NULL');
  end loop;
  assert v_count = 2, 'US-12.1: expected 2 keyed events, got ' || v_count;
  perform pgque.ack(v_batch);
  raise notice 'PASS: US-12.1 keyed send stores partition key in ev_extra1';
end $$;

-- Cleanup
do $$ begin
  perform pgque.unsubscribe('us12_keyed', 'reader');
  perform pgque.drop_queue('us12_keyed');
end $$;

-- ===========================================================================
-- US-12.2 -- Per-key order: all events of one key delivered by exactly one slot,
-- in ev_id order, so per-tenant processing is sequential. (N=2)
-- ===========================================================================

drop table if exists _us12_order_recv;
create temporary table _us12_order_recv (
  slot     int,
  recv_ord int,
  msg_id   bigint,
  pkey     text
);

-- Setup: two slots on one consumer
do $$ begin
  perform pgque.create_queue('us12_order');
  perform pgque.subscribe_partitioned('us12_order', 'cw', 2);
end $$;

-- Action: interleave two keys A,B,A,B,A -> 3x tenant-A, 2x tenant-B
do $$ begin
  perform pgque.send('us12_order', 'file.event', '{"seq":1}'::jsonb, 'tenant-A');
  perform pgque.send('us12_order', 'file.event', '{"seq":1}'::jsonb, 'tenant-B');
  perform pgque.send('us12_order', 'file.event', '{"seq":2}'::jsonb, 'tenant-A');
  perform pgque.send('us12_order', 'file.event', '{"seq":2}'::jsonb, 'tenant-B');
  perform pgque.send('us12_order', 'file.event', '{"seq":3}'::jsonb, 'tenant-A');
end $$;

do $$ begin
  perform pgque.force_next_tick('us12_order');
  perform pgque.ticker();
end $$;

-- Collect both slots, preserving delivery order (recv_ord). receive/ack are
-- lease-fenced, so each slot is claimed before its receive and released after ack.
do $$
declare
  v_msg pgque.message;
  v_ord int := 0;
begin
  perform pgque.claim_slot('us12_order', 'cw', 0, 'acc-w');
  for v_msg in select * from pgque.receive_partitioned('us12_order', 'cw', 0, 2, 'acc-w', 100)
  loop
    v_ord := v_ord + 1;
    insert into _us12_order_recv values (0, v_ord, v_msg.msg_id, v_msg.extra1);
  end loop;
  perform pgque.ack_partitioned('us12_order', 'cw', 0, 2, 'acc-w');
  perform pgque.release_slot('us12_order', 'cw', 0, 'acc-w');

  perform pgque.claim_slot('us12_order', 'cw', 1, 'acc-w');
  for v_msg in select * from pgque.receive_partitioned('us12_order', 'cw', 1, 2, 'acc-w', 100)
  loop
    v_ord := v_ord + 1;
    insert into _us12_order_recv values (1, v_ord, v_msg.msg_id, v_msg.extra1);
  end loop;
  perform pgque.ack_partitioned('us12_order', 'cw', 1, 2, 'acc-w');
  perform pgque.release_slot('us12_order', 'cw', 1, 'acc-w');
end $$;

-- Assert: tenant-A lands on its hash slot only, delivered in ev_id order
do $$
declare
  v_slot_a     int := (hashtextextended('tenant-A', 0) % 2 + 2) % 2;
  v_here       int;
  v_other      int;
  v_inversions int;
begin
  select count(*) into v_here
  from _us12_order_recv
  where pkey = 'tenant-A'
    and slot = v_slot_a;

  select count(*) into v_other
  from _us12_order_recv
  where pkey = 'tenant-A'
    and slot <> v_slot_a;

  assert v_here = 3,
    format('US-12.2: tenant-A must have 3 events on slot %s, got %s', v_slot_a, v_here);
  assert v_other = 0,
    format('US-12.2: tenant-A must appear on no other slot, got %s', v_other);

  -- FIFO: no later-delivered tenant-A event carries a smaller-or-equal ev_id
  select count(*) into v_inversions
  from _us12_order_recv a
  join _us12_order_recv b
    on a.pkey = 'tenant-A'
   and b.pkey = 'tenant-A'
   and a.recv_ord < b.recv_ord
   and a.msg_id >= b.msg_id;

  assert v_inversions = 0,
    format('US-12.2: tenant-A not delivered in ev_id order (%s inversions)', v_inversions);
  raise notice 'PASS: US-12.2 per-key affinity + FIFO (single slot, ev_id order)';
end $$;

-- Cleanup
do $$ begin
  perform pgque.unsubscribe_slot('us12_order', 'cw', 0);
  perform pgque.unsubscribe_slot('us12_order', 'cw', 1);
  perform pgque.drop_queue('us12_order');
end $$;
drop table if exists _us12_order_recv;

-- ===========================================================================
-- US-12.3 -- Cross-key parallelism: N slots consume disjoint key subsets; the
-- union of slots equals the whole stream, pairwise disjoint, correctly routed. (N=3)
-- ===========================================================================

drop table if exists _us12_par_recv;
create temporary table _us12_par_recv (
  slot   int,
  msg_id bigint,
  pkey   text
);

do $$ begin
  perform pgque.create_queue('us12_parallel');
  perform pgque.subscribe_partitioned('us12_parallel', 'cw', 3);
end $$;

-- Action: 6 keys x 2 events = 12 events spread across the key space
do $$
declare
  v_key text;
  v_i   int;
begin
  foreach v_key in array array['tenant-A','tenant-B','tenant-C','tenant-D','tenant-E','tenant-F']
  loop
    for v_i in 1..2 loop
      perform pgque.send('us12_parallel', 'file.event', jsonb_build_object('k', v_key, 'seq', v_i), v_key);
    end loop;
  end loop;
end $$;

do $$ begin
  perform pgque.force_next_tick('us12_parallel');
  perform pgque.ticker();
end $$;

-- Collect all three slots; each slot is claimed before receive and released after ack
do $$
declare
  v_msg  pgque.message;
  v_slot int;
begin
  for v_slot in 0..2 loop
    perform pgque.claim_slot('us12_parallel', 'cw', v_slot, 'acc-w');
    for v_msg in select * from pgque.receive_partitioned('us12_parallel', 'cw', v_slot, 3, 'acc-w', 100)
    loop
      insert into _us12_par_recv values (v_slot, v_msg.msg_id, v_msg.extra1);
    end loop;
    perform pgque.ack_partitioned('us12_parallel', 'cw', v_slot, 3, 'acc-w');
    perform pgque.release_slot('us12_parallel', 'cw', v_slot, 'acc-w');
  end loop;
end $$;

-- Assert: complete (union = 12), disjoint (no event on two slots), correctly routed
do $$
declare
  v_total    int;
  v_dupes    int;
  v_misroute int;
begin
  select count(distinct msg_id) into v_total from _us12_par_recv;
  assert v_total = 12,
    format('US-12.3: union of slots must equal 12 sent events, got %s', v_total);

  select count(*) into v_dupes
  from (
    select msg_id
    from _us12_par_recv
    group by msg_id
    having count(distinct slot) > 1
  ) d;
  assert v_dupes = 0,
    format('US-12.3: slots must be pairwise disjoint, %s events on multiple slots', v_dupes);

  select count(*) into v_misroute
  from _us12_par_recv
  where slot <> (hashtextextended(pkey, 0) % 3 + 3) % 3;
  assert v_misroute = 0,
    format('US-12.3: %s events routed to the wrong hash slot', v_misroute);
  raise notice 'PASS: US-12.3 cross-key parallelism (complete, disjoint, correctly routed)';
end $$;

-- Cleanup
do $$ begin
  perform pgque.unsubscribe_slot('us12_parallel', 'cw', 0);
  perform pgque.unsubscribe_slot('us12_parallel', 'cw', 1);
  perform pgque.unsubscribe_slot('us12_parallel', 'cw', 2);
  perform pgque.drop_queue('us12_parallel');
end $$;
drop table if exists _us12_par_recv;

-- ===========================================================================
-- US-12.4 -- Single processor per slot: a second receive on the same slot never
-- obtains a divergent batch; the engine receive lock returns the same active
-- batch idempotently.
--
/* Single-session facet proven here: re-calling receive_partitioned on the same
   slot before ack returns the SAME batch (same ev_id set), never a second
   independent batch. The cross-session facets -- a concurrent worker being
   steered off a leased slot, and a takeover re-issuing the still-open batch --
   are covered by tests/two_session_slot_claim.sh (mirrors two_session_receive_lock.sh). */
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us12_batch');
  perform pgque.subscribe_partitioned('us12_batch', 'cw', 1);
end $$;

do $$ begin
  perform pgque.send('us12_batch', 'file.event', '{"seq":1}'::jsonb, 'tenant-A');
  perform pgque.send('us12_batch', 'file.event', '{"seq":2}'::jsonb, 'tenant-A');
end $$;

do $$ begin
  perform pgque.force_next_tick('us12_batch');
  perform pgque.ticker();
end $$;

do $$
declare
  v_first  bigint[];
  v_second bigint[];
begin
  perform pgque.claim_slot('us12_batch', 'cw', 0, 'acc-w');

  select array_agg(m.msg_id order by m.msg_id) into v_first
  from pgque.receive_partitioned('us12_batch', 'cw', 0, 1, 'acc-w', 100) m;

  select array_agg(m.msg_id order by m.msg_id) into v_second
  from pgque.receive_partitioned('us12_batch', 'cw', 0, 1, 'acc-w', 100) m;

  assert v_first is not null and cardinality(v_first) = 2,
    format('US-12.4: first receive should open a 2-event batch, got %s', coalesce(cardinality(v_first), 0));
  assert v_first = v_second,
    'US-12.4: second receive on the same slot must return the SAME batch idempotently, not a divergent one';
  perform pgque.ack_partitioned('us12_batch', 'cw', 0, 1, 'acc-w');
  perform pgque.release_slot('us12_batch', 'cw', 0, 'acc-w');
  raise notice 'PASS: US-12.4 (single-session facet) same batch returned idempotently';
end $$;

do $$ begin
  perform pgque.unsubscribe_slot('us12_batch', 'cw', 0);
  perform pgque.drop_queue('us12_batch');
end $$;

-- ===========================================================================
-- US-12.5 -- Lease claim/release + crash recovery: claim a free slot via
-- pgque.claim_slot and release at a batch boundary via pgque.release_slot; a
-- crashed worker's slot is recovered by lease EXPIRY, with an epoch bump so the
-- stale worker can be fenced.
--
/* Single-session facet proven here: leases are keyed by worker id, so a second
   worker id in the same session exercises exclusivity and takeover exactly as a
   second backend would. claim_slot on a free slot returns an epoch, a competing
   worker is refused, the owner's re-claim renews the SAME epoch, only the owner
   releases, and an expired lease is taken over with a LARGER epoch (fencing).
   The cross-backend variant is tests/two_session_slot_claim.sh. */
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us12_claim');
  perform pgque.subscribe_partitioned('us12_claim', 'cw', 2);
end $$;

do $$
declare
  v_e1 bigint;
  v_e2 bigint;
begin
  -- Claim a free slot: returns the lease epoch (fencing token).
  v_e1 := pgque.claim_slot('us12_claim', 'cw', 0, 'acc-a');
  assert v_e1 is not null, 'US-12.5: claim_slot on a free slot must return an epoch';

  -- A competing worker cannot claim a live lease.
  assert pgque.claim_slot('us12_claim', 'cw', 0, 'acc-b') is null,
    'US-12.5: claim by a second worker on a live lease must return null';

  -- The owner re-claiming renews the lease and returns the SAME epoch.
  v_e2 := pgque.claim_slot('us12_claim', 'cw', 0, 'acc-a');
  assert v_e2 = v_e1,
    format('US-12.5: owner re-claim must renew with the same epoch, got %s -> %s', v_e1, v_e2);

  -- Only the owner can release.
  assert not pgque.release_slot('us12_claim', 'cw', 0, 'acc-b'),
    'US-12.5: release by a non-owner must return false';
  assert pgque.release_slot('us12_claim', 'cw', 0, 'acc-a'),
    'US-12.5: release by the owner must return true';

  -- Freed slot is re-claimable.
  assert pgque.claim_slot('us12_claim', 'cw', 0, 'acc-a') is not null,
    'US-12.5: slot must be re-claimable after release';
  perform pgque.release_slot('us12_claim', 'cw', 0, 'acc-a');
  raise notice 'PASS: US-12.5 lease claim/release + owner-only release';
end $$;

-- Crash recovery: a dead worker never releases; its lease expires and is taken
-- over by another worker with a LARGER epoch (fencing token bump).
do $$
declare
  v_dead bigint;
  v_new  bigint;
begin
  v_dead := pgque.claim_slot('us12_claim', 'cw', 0, 'acc-dead', '1 second');
  assert v_dead is not null, 'US-12.5: short-TTL claim must succeed';

  -- Lease still live: takeover refused.
  assert pgque.claim_slot('us12_claim', 'cw', 0, 'acc-heir') is null,
    'US-12.5: a live lease must not be taken over';

  perform pg_sleep(1.2);

  v_new := pgque.claim_slot('us12_claim', 'cw', 0, 'acc-heir');
  assert v_new is not null,
    'US-12.5: an expired lease must be claimable by another worker';
  assert v_new > v_dead,
    format('US-12.5: takeover must bump the epoch (fencing), got %s -> %s', v_dead, v_new);

  perform pgque.release_slot('us12_claim', 'cw', 0, 'acc-heir');
  raise notice 'PASS: US-12.5 expired lease taken over with epoch bump';
end $$;

-- ===========================================================================
-- US-12.6 -- Observability: pgque.partition_slot_status shows each slot, its
-- lease owner (null if unleased or expired), lease_until, epoch, and cursor lag,
-- so a stalled slot can be alerted on (rotation-pinning risk R7). (reuses queue
-- us12_claim from US-12.5)
-- ===========================================================================

-- Unleased: two rows, correct n, lease_owner null
do $$
declare
  v_rows   int;
  v_leased int;
begin
  select count(*) into v_rows
  from pgque.partition_slot_status
  where queue_name = 'us12_claim'
    and consumer = 'cw'
    and n = 2
    and subscribed
    and slot in (0, 1);
  assert v_rows = 2,
    format('US-12.6: expected 2 slot rows for (us12_claim, cw), got %s', v_rows);

  select count(*) into v_leased
  from pgque.partition_slot_status
  where queue_name = 'us12_claim'
    and consumer = 'cw'
    and lease_owner is not null;
  assert v_leased = 0,
    format('US-12.6: unleased slots must report lease_owner null, got %s leased', v_leased);

  -- pending_events (cursor lag) must be a sane non-negative number
  assert not exists (
    select 1
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'cw'
      and pending_events < 0
  ), 'US-12.6: pending_events (lag) must be non-negative';
  raise notice 'PASS: US-12.6 partition_slot_status unleased view';
end $$;

-- Alpha-compatible single-slot setup remains visible as incomplete.
do $$
declare
  v_raised boolean := false;
begin
  perform pgque.subscribe_slot('us12_claim', 'partial', 0, 2);
  assert (
    select subscribed and pending_events is not null
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'partial'
      and slot = 0
  ), 'US-12.6: materialized alpha slot must report subscribed';
  assert (
    select not subscribed and last_tick is null and pending_events is null
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'partial'
      and slot = 1
  ), 'US-12.6: missing alpha slot must report unknown lag';

  begin
    perform pgque.subscribe_partitioned('us12_claim', 'partial', 2);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%incomplete%',
      format('US-12.6: unexpected partial setup error: %s', sqlerrm);
  end;
  assert v_raised, 'US-12.6: atomic setup must reject partial existing state';
  raise notice 'PASS: US-12.6 partition_slot_status flags incomplete setup';
end $$;

-- Leased by worker acc-a: lease_owner == 'acc-a' for slot 0 only
select pgque.claim_slot('us12_claim', 'cw', 0, 'acc-a') as epoch \gset

do $$
declare v_owner text;
begin
  select lease_owner into v_owner
  from pgque.partition_slot_status
  where queue_name = 'us12_claim'
    and consumer = 'cw'
    and slot = 0;
  assert v_owner = 'acc-a',
    format('US-12.6: leased slot 0 must report lease_owner acc-a, got %s', coalesce(v_owner, 'NULL'));

  assert (
    select lease_owner
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'cw'
      and slot = 1
  ) is null, 'US-12.6: unleased slot 1 must still report lease_owner null';
  raise notice 'PASS: US-12.6 partition_slot_status reflects the live lease owner';
end $$;

-- Release and confirm lease_owner clears
select pgque.release_slot('us12_claim', 'cw', 0, 'acc-a') as released \gset

do $$ begin
  assert (
    select lease_owner
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'cw'
      and slot = 0
  ) is null, 'US-12.6: lease_owner must clear after release_slot';
  raise notice 'PASS: US-12.6 lease_owner clears on release';
end $$;

-- Cleanup us12_claim (shared by US-12.5 and US-12.6)
do $$ begin
  perform pgque.unsubscribe_slot('us12_claim', 'partial', 0);
  perform pgque.unsubscribe_slot('us12_claim', 'cw', 0);
  perform pgque.unsubscribe_slot('us12_claim', 'cw', 1);
  perform pgque.drop_queue('us12_claim');
end $$;

-- ===========================================================================
-- US-12.7 -- Enforced N: a worker calling with the wrong N is rejected with a
-- clear error, never silently misrouted. (N is persisted per (queue, consumer))
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us12_nvalidate');
end $$;

do $$
declare v_raised boolean := false;
begin
  -- First call persists n=2 for (queue, consumer); re-calling with (0,2) is idempotent
  perform pgque.subscribe_slot('us12_nvalidate', 'cw', 0, 2);
  perform pgque.subscribe_slot('us12_nvalidate', 'cw', 0, 2);

  -- A later mismatched n MUST raise, not silently re-home keys
  begin
    perform pgque.subscribe_slot('us12_nvalidate', 'cw', 0, 3);
  exception when others then
    v_raised := true;
  end;
  assert v_raised,
    'US-12.7: subscribe_slot with a changed n must raise, but n=3 was accepted';

  -- receive with the wrong n must raise BEFORE any lease check fires.
  v_raised := false;
  begin
    perform * from pgque.receive_partitioned('us12_nvalidate', 'cw', 0, 3, 'acc-w', 10);
  exception when others then
    v_raised := true;
  end;
  assert v_raised,
    'US-12.7: receive_partitioned with wrong n must raise';

  -- ack with the wrong n must likewise raise.
  v_raised := false;
  begin
    perform pgque.ack_partitioned('us12_nvalidate', 'cw', 0, 3, 'acc-w');
  exception when others then
    v_raised := true;
  end;
  assert v_raised,
    'US-12.7: ack_partitioned with wrong n must raise';
  raise notice 'PASS: US-12.7 enforced N (mismatched n rejected)';
end $$;

-- Cleanup
do $$ begin
  perform pgque.unsubscribe_slot('us12_nvalidate', 'cw', 0);
  perform pgque.drop_queue('us12_nvalidate');
end $$;

\echo 'US-12: PASSED'
