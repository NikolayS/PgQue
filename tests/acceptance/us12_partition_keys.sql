\set ON_ERROR_STOP on

-- US-12: Partition keys (Fabrizio Case 2 -- multi-tenant storage lifecycle events)
-- As a multi-tenant event producer and operator, I want events that share a
-- partition key (tenant) consumed in order by a single slot at a time, while
-- events of different keys are consumed in parallel across slots -- the
-- log-native (Kafka-partition) model: order within a key, parallelism across keys.
--
-- Covers US-12.1, 12.2, 12.3, 12.4 (single-session facet), 12.5, 12.6, 12.7.
-- US-12.4 (cross-session receive-lock blocking + advisory-claim steering) and
-- US-12.5 (cross-session claim exclusivity + connection-death crash recovery)
-- are two-session properties: the single-session facets are proven here and the
-- concurrent facets are covered by tests/two_session_slot_claim.sh.
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
  perform pgque.subscribe_slot('us12_order', 'cw', 0, 2);
  perform pgque.subscribe_slot('us12_order', 'cw', 1, 2);
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

-- Collect both slots, preserving delivery order (recv_ord)
do $$
declare
  v_msg pgque.message;
  v_ord int := 0;
begin
  for v_msg in select * from pgque.receive_partitioned('us12_order', 'cw', 0, 2, 100)
  loop
    v_ord := v_ord + 1;
    insert into _us12_order_recv values (0, v_ord, v_msg.msg_id, v_msg.extra1);
  end loop;
  perform pgque.ack_partitioned('us12_order', 'cw', 0, 2);

  for v_msg in select * from pgque.receive_partitioned('us12_order', 'cw', 1, 2, 100)
  loop
    v_ord := v_ord + 1;
    insert into _us12_order_recv values (1, v_ord, v_msg.msg_id, v_msg.extra1);
  end loop;
  perform pgque.ack_partitioned('us12_order', 'cw', 1, 2);
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
  perform pgque.subscribe_slot('us12_parallel', 'cw', 0, 3);
  perform pgque.subscribe_slot('us12_parallel', 'cw', 1, 3);
  perform pgque.subscribe_slot('us12_parallel', 'cw', 2, 3);
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

-- Collect all three slots
do $$
declare
  v_msg  pgque.message;
  v_slot int;
begin
  for v_slot in 0..2 loop
    for v_msg in select * from pgque.receive_partitioned('us12_parallel', 'cw', v_slot, 3, 100)
    loop
      insert into _us12_par_recv values (v_slot, v_msg.msg_id, v_msg.extra1);
    end loop;
    perform pgque.ack_partitioned('us12_parallel', 'cw', v_slot, 3);
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
   independent batch. The cross-session facets -- a concurrent worker BLOCKING on
   the receive lock, and the advisory slot claim STEERING a second worker onto a
   different slot entirely -- require two live sessions and timing, and are
   covered by tests/two_session_slot_claim.sh (mirrors two_session_receive_lock.sh). */
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us12_batch');
  perform pgque.subscribe_slot('us12_batch', 'cw', 0, 1);
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
  select array_agg(m.msg_id order by m.msg_id) into v_first
  from pgque.receive_partitioned('us12_batch', 'cw', 0, 1, 100) m;

  select array_agg(m.msg_id order by m.msg_id) into v_second
  from pgque.receive_partitioned('us12_batch', 'cw', 0, 1, 100) m;

  assert v_first is not null and cardinality(v_first) = 2,
    format('US-12.4: first receive should open a 2-event batch, got %s', coalesce(cardinality(v_first), 0));
  assert v_first = v_second,
    'US-12.4: second receive on the same slot must return the SAME batch idempotently, not a divergent one';
  perform pgque.ack_partitioned('us12_batch', 'cw', 0, 1);
  raise notice 'PASS: US-12.4 (single-session facet) same batch returned idempotently';
end $$;

do $$ begin
  perform pgque.unsubscribe_slot('us12_batch', 'cw', 0);
  perform pgque.drop_queue('us12_batch');
end $$;

-- ===========================================================================
-- US-12.5 -- Claim/release + crash recovery: claim a free slot via
-- pgque.claim_slot and release at a batch boundary via pgque.release_slot; a dead
-- claiming session frees its slot immediately.
--
/* Single-session facet proven here: slot_lock_key is the pinned stateless key,
   claim_slot takes it on a free slot, release_slot frees it, and the slot is
   re-claimable afterwards. The crash-recovery facet -- a SECOND session's claim
   failing while the first holds it, then succeeding the instant the first
   session's connection dies (Postgres releases the session advisory lock) --
   needs two sessions and is covered by tests/two_session_slot_claim.sh. */
-- ===========================================================================

do $$ begin
  perform pgque.create_queue('us12_claim');
  perform pgque.subscribe_slot('us12_claim', 'cw', 0, 2);
  perform pgque.subscribe_slot('us12_claim', 'cw', 1, 2);
end $$;

do $$
declare v_ok boolean;
begin
  -- slot_lock_key is the pinned stateless namespace all clients share (D7)
  assert pgque.slot_lock_key('us12_claim', 'cw', 0)
       = hashtextextended('pgque.slot:' || 'us12_claim' || '/' || 'cw' || '/' || 0::text, 0),
    'US-12.5: slot_lock_key must match the pinned formula';

  v_ok := pgque.claim_slot('us12_claim', 'cw', 0);
  assert v_ok, 'US-12.5: claim_slot on a free slot must return true';

  v_ok := pgque.release_slot('us12_claim', 'cw', 0);
  assert v_ok, 'US-12.5: release_slot on a held slot must return true';

  v_ok := pgque.claim_slot('us12_claim', 'cw', 0);
  assert v_ok, 'US-12.5: slot must be re-claimable after release';

  perform pgque.release_slot('us12_claim', 'cw', 0);
  raise notice 'PASS: US-12.5 claim/release + stateless slot_lock_key';
end $$;

-- ===========================================================================
-- US-12.6 -- Observability: pgque.partition_slot_status shows each slot, its
-- owner pid (null if unclaimed), and cursor lag, so a stalled slot can be alerted
-- on (rotation-pinning risk R7). (reuses queue us12_claim from US-12.5)
-- ===========================================================================

-- Unclaimed: two rows, correct n, owner_pid null
do $$
declare
  v_rows  int;
  v_owned int;
begin
  select count(*) into v_rows
  from pgque.partition_slot_status
  where queue_name = 'us12_claim'
    and consumer = 'cw'
    and n = 2
    and slot in (0, 1);
  assert v_rows = 2,
    format('US-12.6: expected 2 slot rows for (us12_claim, cw), got %s', v_rows);

  select count(*) into v_owned
  from pgque.partition_slot_status
  where queue_name = 'us12_claim'
    and consumer = 'cw'
    and owner_pid is not null;
  assert v_owned = 0,
    format('US-12.6: unclaimed slots must report owner_pid null, got %s owned', v_owned);

  -- pending_events (cursor lag) must be a sane non-negative number
  assert not exists (
    select 1
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'cw'
      and pending_events < 0
  ), 'US-12.6: pending_events (lag) must be non-negative';
  raise notice 'PASS: US-12.6 partition_slot_status unclaimed view';
end $$;

-- Claimed by THIS session: owner_pid == pg_backend_pid() for slot 0 only
select pgque.claim_slot('us12_claim', 'cw', 0) as claimed \gset

do $$
declare v_pid int;
begin
  select owner_pid into v_pid
  from pgque.partition_slot_status
  where queue_name = 'us12_claim'
    and consumer = 'cw'
    and slot = 0;
  assert v_pid = pg_backend_pid(),
    format('US-12.6: claimed slot 0 must report this backend pid %s, got %s', pg_backend_pid(), coalesce(v_pid::text, 'NULL'));

  assert (
    select owner_pid
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'cw'
      and slot = 1
  ) is null, 'US-12.6: unclaimed slot 1 must still report owner_pid null';
  raise notice 'PASS: US-12.6 partition_slot_status reflects the live claim owner';
end $$;

-- Release and confirm owner_pid clears
select pgque.release_slot('us12_claim', 'cw', 0) as released \gset

do $$ begin
  assert (
    select owner_pid
    from pgque.partition_slot_status
    where queue_name = 'us12_claim'
      and consumer = 'cw'
      and slot = 0
  ) is null, 'US-12.6: owner_pid must clear after release_slot';
  raise notice 'PASS: US-12.6 owner_pid clears on release';
end $$;

-- Cleanup us12_claim (shared by US-12.5 and US-12.6)
do $$ begin
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
  raise notice 'PASS: US-12.7 enforced N (mismatched n rejected)';
end $$;

-- Cleanup
do $$ begin
  perform pgque.unsubscribe_slot('us12_nvalidate', 'cw', 0);
  perform pgque.drop_queue('us12_nvalidate');
end $$;

\echo 'US-12: PASSED'
