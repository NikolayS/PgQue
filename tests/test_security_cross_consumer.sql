-- test_security_cross_consumer.sql
-- Regression test for #106: cross-consumer interference via low-level
-- PgQ-compatible primitives.
--
-- Context: in v0.2.0, register_consumer_at / event_retry / get_batch_events
-- are granted to pgque_reader so apps can use the PgQ-compatible primitive
-- layer. But these primitives operate by queue/consumer name or batch id and
-- do NOT validate caller context. That means a second consumer that already
-- holds pgque_reader (a perfectly normal grant for any consuming app) can
-- reach into another consumer's active batch / cursor.
--
-- v0.2.0 mitigation: keep these three primitives as trusted-operator-only.
-- Revoke from pgque_reader and grant to pgque_admin only. The high-level
-- API (pgque.receive / ack / nack) is SECURITY DEFINER and reaches the
-- primitives via the function owner, so application code is unaffected.
--
-- This test asserts the trusted-operator contract. Two consumer roles A and B
-- both hold pgque_reader. B must NOT be able to:
--   A) reposition A's cursor via pgque.register_consumer_at()
--   B) push A's events into retry via pgque.event_retry()
--   C) read A's active batch payloads via pgque.get_batch_events()
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Idempotent preamble: clean up leftovers from a prior aborted run.
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'cc_consumer_a') then
        revoke all privileges on schema pgque from cc_consumer_a;
        revoke pgque_reader from cc_consumer_a;
        drop role cc_consumer_a;
    end if;
    if exists (select 1 from pg_roles where rolname = 'cc_consumer_b') then
        revoke all privileges on schema pgque from cc_consumer_b;
        revoke pgque_reader from cc_consumer_b;
        drop role cc_consumer_b;
    end if;
exception when others then
    raise notice 'preamble cleanup: % / %', sqlstate, sqlerrm;
end $$;

do $$
begin
    if exists (select 1 from pgque.queue where queue_name = 'q_cc') then
        perform pgque.drop_queue('q_cc', true);
    end if;
exception when others then
    raise notice 'preamble queue cleanup: % / %', sqlstate, sqlerrm;
end $$;

-- Setup -----------------------------------------------------------------------

-- Two consumer roles, both holding pgque_reader. NOLOGIN -- we use set role.
create role cc_consumer_a nologin;
grant pgque_reader to cc_consumer_a;
create role cc_consumer_b nologin;
grant pgque_reader to cc_consumer_b;

-- Queue + two consumers + an event + a tick so consumer_a has an active batch.
select pgque.create_queue('q_cc');
select pgque.subscribe('q_cc', 'cons_a');
select pgque.subscribe('q_cc', 'cons_b');
select pgque.send('q_cc', 'secret', '{"payload":"top-secret"}'::jsonb);
select pgque.force_tick('q_cc');
select pgque.ticker('q_cc');

-- Consumer A opens a batch under pgque_reader privileges and stashes the
-- (batch_id, ev_id) on a session GUC so the attacker block can read them
-- back without re-reaching into A's tables.
set role cc_consumer_a;
do $$
declare
    v_count int;
    v_bid   bigint;
    v_eid   bigint;
begin
    select count(*) into v_count from pgque.receive('q_cc', 'cons_a', 10);
    assert v_count = 1, format('cons_a should have received 1 message, got %s', v_count);

    select sub_batch into v_bid
      from pgque.subscription s
      join pgque.consumer c on c.co_id = s.sub_consumer
     where c.co_name = 'cons_a';
    assert v_bid is not null, 'cons_a should have an active batch_id';

    -- Look up the ev_id while still acting as cons_a (it has the active batch).
    -- pgque.receive() does not return ev_id directly here, so use the message type.
    select msg_id into v_eid
      from pgque.receive('q_cc', 'cons_a', 10)
     limit 1;
    -- Note: a second receive returns nothing because the batch is already open.
    -- Fall back to looking up the event id from the queue data tables, which
    -- pgque_reader can SELECT from.
    if v_eid is null then
        select ev_id into v_eid
          from pgque.event_1 -- queue data table -- pgque_reader has SELECT
         where ev_type = 'secret'
         limit 1;
    end if;
    assert v_eid is not null, 'expected to discover an ev_id for the secret event';

    perform set_config('pgque_test.cc_bid', v_bid::text, false);
    perform set_config('pgque_test.cc_eid', v_eid::text, false);
end $$;
reset role;

-- Attacker tests --------------------------------------------------------------

set role cc_consumer_b;

-- Finding A: register_consumer_at(queue, victim_consumer, tick) lets B
-- reposition cons_a's cursor (clears sub_batch, rewrites sub_last_tick).
do $$
begin
    begin
        perform pgque.register_consumer_at('q_cc', 'cons_a', 1);
        raise exception 'FAIL #106-A: cc_consumer_b repositioned cons_a cursor';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-A (register_consumer_at): denied to pgque_reader';
        when others then
            raise exception 'FAIL #106-A: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- Finding B: event_retry(batch_id, ev_id, n) lets B push cons_a's events
-- into the retry queue. Both timestamptz and integer overloads are separate
-- privilege rows so cover both.
do $$
declare
    v_bid bigint := current_setting('pgque_test.cc_bid')::bigint;
    v_eid bigint := current_setting('pgque_test.cc_eid')::bigint;
begin
    begin
        perform pgque.event_retry(v_bid, v_eid, 0);
        raise exception 'FAIL #106-B: cc_consumer_b retried cons_a event (int overload)';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-B (event_retry int): denied to pgque_reader';
        when others then
            raise exception 'FAIL #106-B (int): unexpected error: % / %', sqlstate, sqlerrm;
    end;

    begin
        perform pgque.event_retry(v_bid, v_eid, now());
        raise exception 'FAIL #106-B: cc_consumer_b retried cons_a event (timestamptz overload)';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-B (event_retry timestamptz): denied to pgque_reader';
        when others then
            raise exception 'FAIL #106-B (timestamptz): unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- Finding C: get_batch_events(batch_id) returns the active payloads of the
-- batch, including ev_data. B must not be able to inspect cons_a's in-flight
-- messages.
do $$
declare
    v_bid bigint := current_setting('pgque_test.cc_bid')::bigint;
begin
    begin
        perform * from pgque.get_batch_events(v_bid);
        raise exception 'FAIL #106-C: cc_consumer_b read cons_a active batch payloads';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-C (get_batch_events): denied to pgque_reader';
        when others then
            raise exception 'FAIL #106-C: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

reset role;

-- Sanity: pgque_admin still has all three primitives via direct grant. --------
do $$
begin
    assert has_function_privilege('pgque_admin',
        'pgque.register_consumer_at(text, text, bigint)', 'EXECUTE'),
        'pgque_admin should retain register_consumer_at';
    assert has_function_privilege('pgque_admin',
        'pgque.event_retry(bigint, bigint, integer)', 'EXECUTE'),
        'pgque_admin should retain event_retry(integer)';
    assert has_function_privilege('pgque_admin',
        'pgque.event_retry(bigint, bigint, timestamptz)', 'EXECUTE'),
        'pgque_admin should retain event_retry(timestamptz)';
    assert has_function_privilege('pgque_admin',
        'pgque.get_batch_events(bigint)', 'EXECUTE'),
        'pgque_admin should retain get_batch_events';
end $$;

-- Sanity: the high-level API (receive/ack/nack) still works for pgque_reader.
-- These wrappers are SECURITY DEFINER and reach the low-level primitives via
-- the function owner, so application code that uses the modern API is
-- unaffected by the revoke.
set role cc_consumer_a;
do $$
declare
    v_bid bigint := current_setting('pgque_test.cc_bid')::bigint;
begin
    perform pgque.ack(v_bid);
end $$;
reset role;

-- Cleanup ---------------------------------------------------------------------

select pgque.unsubscribe('q_cc', 'cons_a');
select pgque.unsubscribe('q_cc', 'cons_b');
select pgque.drop_queue('q_cc', true);

revoke pgque_reader from cc_consumer_a;
revoke pgque_reader from cc_consumer_b;
drop role cc_consumer_a;
drop role cc_consumer_b;

\echo 'PASS: cross-consumer primitive isolation (#106)'
