-- test_security_producer_isolation.sql
-- Regression test for #102 / #106: a producer-only role (pgque_writer
-- without pgque_reader membership) must not be able to ack/finish/inspect
-- another consumer's batch by id.
--
-- This locks in PgQ's original producer/consumer split: pgque_reader and
-- pgque_writer are siblings, not parent/child. Apps that produce and consume
-- must be granted both roles explicitly. See sql/pgque-additions/roles.sql.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Setup -----------------------------------------------------------------------

-- Pure producer (only pgque_writer).
do $$ begin
    create role producer_only login;
exception when duplicate_object then null;
end $$;
do $$ begin
    grant pgque_writer to producer_only;
exception when duplicate_object then null;
end $$;

-- Pure consumer (only pgque_reader).
do $$ begin
    create role consumer_only login;
exception when duplicate_object then null;
end $$;
do $$ begin
    grant pgque_reader to consumer_only;
exception when duplicate_object then null;
end $$;

-- Need a queue + subscription so we have a real batch_id to attempt against.
select pgque.create_queue('q_iso');
select pgque.subscribe('q_iso', 'victim');

-- Producer publishes one event and ticks the queue so a batch is available.
select pgque.send('q_iso', 'evt', '{"k":1}'::jsonb);
select pgque.force_tick('q_iso');
select pgque.ticker('q_iso');

-- Victim opens a batch (under pgque_reader privileges).
set role consumer_only;
do $$
declare
    v_count int;
begin
    select count(*) into v_count
    from pgque.receive('q_iso', 'victim', 10);
    assert v_count = 1, format('victim should have received 1 message, got %s', v_count);

    -- Stash the active batch_id for the attacker test below.
    perform set_config(
        'pgque_test.victim_batch_id',
        (select sub_batch::text
           from pgque.subscription s
           join pgque.queue q on q.queue_id = s.sub_queue
           join pgque.consumer c on c.co_id = s.sub_consumer
          where q.queue_name = 'q_iso' and c.co_name = 'victim'),
        false);
end $$;
reset role;

-- Attacker tests (#102, #106) -------------------------------------------------

set role producer_only;

-- #102: producer_only must not be able to ack the victim's batch.
do $$
declare
    v_bid bigint := current_setting('pgque_test.victim_batch_id')::bigint;
begin
    begin
        perform pgque.ack(v_bid);
        raise exception 'FAIL #102: producer_only acked victim batch %', v_bid;
    exception
        when insufficient_privilege then
            raise notice 'PASS #102 (ack): producer_only denied execute on pgque.ack(bigint)';
        when others then
            raise exception 'FAIL #102: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- #102 (low-level): same on pgque.finish_batch().
do $$
declare
    v_bid bigint := current_setting('pgque_test.victim_batch_id')::bigint;
begin
    begin
        perform pgque.finish_batch(v_bid);
        raise exception 'FAIL #102: producer_only finished victim batch %', v_bid;
    exception
        when insufficient_privilege then
            raise notice 'PASS #102 (finish_batch): producer_only denied execute on pgque.finish_batch';
        when others then
            raise exception 'FAIL #102: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- #106-A: producer_only must not be able to reposition victim's cursor.
do $$
begin
    begin
        perform pgque.register_consumer_at('q_iso', 'victim', 0);
        raise exception 'FAIL #106-A: producer_only repositioned victim cursor';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-A (register_consumer_at): denied';
        when others then
            raise exception 'FAIL #106-A: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- #106-B: producer_only must not be able to read victim's active batch payloads.
do $$
declare
    v_bid bigint := current_setting('pgque_test.victim_batch_id')::bigint;
begin
    begin
        perform pgque.get_batch_events(v_bid);
        raise exception 'FAIL #106-C: producer_only read victim batch payloads';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-C (get_batch_events): denied';
        when others then
            raise exception 'FAIL #106-C: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- #106-C: producer_only must not be able to event_retry victim's events.
do $$
declare
    v_bid bigint := current_setting('pgque_test.victim_batch_id')::bigint;
begin
    begin
        perform pgque.event_retry(v_bid, 0::bigint, 0);
        raise exception 'FAIL #106-B: producer_only retried victim event';
    exception
        when insufficient_privilege then
            raise notice 'PASS #106-B (event_retry): denied';
        when others then
            raise exception 'FAIL #106-B: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- And receive() / next_batch() must be denied too.
do $$
begin
    begin
        perform pgque.next_batch('q_iso', 'victim');
        raise exception 'FAIL: producer_only opened a batch via next_batch';
    exception
        when insufficient_privilege then
            raise notice 'PASS: producer_only denied execute on pgque.next_batch';
        when others then
            raise exception 'FAIL: unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

reset role;

-- Sanity: pgque_admin still has both via membership. ---------------------------
do $$
begin
    assert has_function_privilege('pgque_admin', 'pgque.ack(bigint)', 'EXECUTE'),
        'pgque_admin should retain ack via pgque_reader membership';
    assert has_function_privilege('pgque_admin', 'pgque.send(text, jsonb)', 'EXECUTE'),
        'pgque_admin should retain send via pgque_writer membership';
end $$;

-- Cleanup ---------------------------------------------------------------------

-- Let the victim ack its batch so drop_queue does not fail on a held batch.
set role consumer_only;
do $$
declare
    v_bid bigint := current_setting('pgque_test.victim_batch_id')::bigint;
begin
    perform pgque.ack(v_bid);
end $$;
reset role;

select pgque.unsubscribe('q_iso', 'victim');
select pgque.drop_queue('q_iso', true);

revoke pgque_writer from producer_only;
revoke pgque_reader from consumer_only;
drop role producer_only;
drop role consumer_only;

\echo 'PASS: producer/consumer role isolation (#102, #106)'
