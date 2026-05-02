-- test_security_consumer_isolation.sql
-- Regression test for #164: a pgque_reader role must not be able to ack/nack
-- another consumer's active batch by id.
--
-- This locks in the consumer-vs-consumer ownership boundary on top of the
-- producer/consumer split established in #163. The new 3-arg ack and 6-arg
-- nack overloads carry an explicit (queue, consumer) ownership check
-- against pgque.subscription before delegating to the trusted PgQ primitives
-- (finish_batch, event_retry, event_dead).
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Idempotent preamble: clean up any leftovers from a prior aborted run.
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'app_a') then
        revoke all privileges on schema pgque from app_a;
        revoke pgque_reader from app_a;
        drop role app_a;
    end if;
    if exists (select 1 from pg_roles where rolname = 'app_b') then
        revoke all privileges on schema pgque from app_b;
        revoke pgque_reader from app_b;
        drop role app_b;
    end if;
exception when others then
    raise notice 'preamble role cleanup: % / %', sqlstate, sqlerrm;
end $$;

do $$
begin
    if exists (select 1 from pgque.queue where queue_name = 'q_iso_a') then
        perform pgque.drop_queue('q_iso_a', true);
    end if;
    if exists (select 1 from pgque.queue where queue_name = 'q_iso_b') then
        perform pgque.drop_queue('q_iso_b', true);
    end if;
exception when others then
    raise notice 'preamble queue cleanup: % / %', sqlstate, sqlerrm;
end $$;

-- Setup -----------------------------------------------------------------------

-- Two pure-consumer roles; NOLOGIN since we exercise via `set role`.
create role app_a nologin;
grant pgque_reader to app_a;

create role app_b nologin;
grant pgque_reader to app_b;

-- Each app gets its own queue + consumer subscription.
select pgque.create_queue('q_iso_a');
select pgque.create_queue('q_iso_b');
select pgque.subscribe('q_iso_a', 'consumer_a');
select pgque.subscribe('q_iso_b', 'consumer_b');

-- Publish + tick both queues so each app has a batch waiting.
select pgque.send('q_iso_a', 'evt', '{"k":"a"}'::jsonb);
select pgque.send('q_iso_b', 'evt', '{"k":"b"}'::jsonb);
select pgque.force_tick('q_iso_a');
select pgque.force_tick('q_iso_b');
select pgque.ticker('q_iso_a');
select pgque.ticker('q_iso_b');

-- App A opens a batch on q_iso_a/consumer_a so sub_batch is set. Use the
-- pgque.receive() result directly to capture batch_id and msg_id without
-- touching low-level primitives (which pgque_reader has no execute on).
set role app_a;
do $$
declare
    v_msg pgque.message;
    v_count int := 0;
begin
    for v_msg in select * from pgque.receive('q_iso_a', 'consumer_a', 10) loop
        v_count := v_count + 1;
        perform set_config('pgque_test.app_a_batch_id', v_msg.batch_id::text, false);
        perform set_config('pgque_test.app_a_msg_id',   v_msg.msg_id::text,   false);
    end loop;
    assert v_count = 1, format('app_a should have received 1 message, got %s', v_count);
end $$;
reset role;

-- Cross-consumer rejection -- ack ---------------------------------------------

set role app_b;

-- #164: app_b is subscribed to (q_iso_b, consumer_b). When it calls
-- pgque.ack(queue, consumer, batch_id) with its own identifiers but
-- app_a's batch_id, the ownership check (sub_batch != i_batch_id) must
-- reject the call with insufficient_privilege. This is exactly the threat
-- model from the issue: any pgque_reader could previously call the 1-arg
-- pgque.ack(batch_id) and finish another consumer's batch.
do $$
declare
    v_bid bigint := current_setting('pgque_test.app_a_batch_id')::bigint;
begin
    begin
        perform pgque.ack('q_iso_b', 'consumer_b', v_bid);
        raise exception 'FAIL #164 (ack): app_b acked app_a batch %', v_bid;
    exception
        when insufficient_privilege then
            raise notice 'PASS #164 (ack): cross-consumer ack rejected (insufficient_privilege)';
        when others then
            raise exception 'FAIL #164 (ack): unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

-- #164 nack: same ownership check on the 6-arg overload — app_b passes its
-- own identifiers but app_a's batch_id and msg_id.
do $$
declare
    v_bid bigint := current_setting('pgque_test.app_a_batch_id')::bigint;
    v_mid bigint := current_setting('pgque_test.app_a_msg_id')::bigint;
    v_msg pgque.message;
begin
    v_msg.msg_id := v_mid;
    v_msg.batch_id := v_bid;
    begin
        perform pgque.nack('q_iso_b', 'consumer_b', v_bid, v_msg,
                           '60 seconds'::interval, 'attacker reason');
        raise exception 'FAIL #164 (nack): app_b nacked app_a event %', v_mid;
    exception
        when insufficient_privilege then
            raise notice 'PASS #164 (nack): cross-consumer nack rejected (insufficient_privilege)';
        when others then
            raise exception 'FAIL #164 (nack): unexpected error: % / %', sqlstate, sqlerrm;
    end;
end $$;

reset role;

-- Positive control: app_a's own ack on its own (queue, consumer) succeeds ----

set role app_a;
do $$
declare
    v_bid bigint := current_setting('pgque_test.app_a_batch_id')::bigint;
begin
    perform pgque.ack('q_iso_a', 'consumer_a', v_bid);
    raise notice 'PASS #164 (positive ack): owner ack succeeded';
end $$;
reset role;

-- Verify sub_batch was cleared (queried as superuser since pgque_reader
-- cannot read pgque.subscription directly).
do $$
declare
    v_after bigint;
begin
    select s.sub_batch into v_after
      from pgque.subscription s
      join pgque.queue q on q.queue_id = s.sub_queue
      join pgque.consumer c on c.co_id = s.sub_consumer
     where q.queue_name = 'q_iso_a' and c.co_name = 'consumer_a';
    assert v_after is null,
        format('PASS-control: sub_batch should be cleared after ack, got %s', v_after);
    raise notice 'PASS #164 (positive ack post-check): sub_batch cleared';
end $$;

-- Sanity: queue arg mismatch is also caught (consumer name matches but queue
-- doesn't). Belt-and-braces for the lookup key.
select pgque.send('q_iso_a', 'evt', '{"k":"a2"}'::jsonb);
select pgque.force_tick('q_iso_a');
select pgque.ticker('q_iso_a');

set role app_a;
do $$
declare
    v_msg pgque.message;
    v_count int := 0;
begin
    for v_msg in select * from pgque.receive('q_iso_a', 'consumer_a', 10) loop
        v_count := v_count + 1;
        perform set_config('pgque_test.app_a_batch_id', v_msg.batch_id::text, false);
    end loop;
    assert v_count = 1, format('app_a second receive expected 1, got %s', v_count);
end $$;
reset role;

set role app_a;
do $$
declare
    v_bid bigint := current_setting('pgque_test.app_a_batch_id')::bigint;
begin
    -- Wrong queue name; same consumer name. Must fail with insufficient_privilege.
    begin
        perform pgque.ack('q_iso_b', 'consumer_a', v_bid);
        raise exception 'FAIL #164 (ack queue mismatch): unexpected success';
    exception
        when insufficient_privilege then
            raise notice 'PASS #164 (ack queue mismatch): rejected';
        when others then
            raise exception 'FAIL #164 (ack queue mismatch): unexpected error: % / %', sqlstate, sqlerrm;
    end;

    -- Wrong consumer; same queue. Must also fail.
    begin
        perform pgque.ack('q_iso_a', 'someone_else', v_bid);
        raise exception 'FAIL #164 (ack consumer mismatch): unexpected success';
    exception
        when insufficient_privilege then
            raise notice 'PASS #164 (ack consumer mismatch): rejected';
        when others then
            raise exception 'FAIL #164 (ack consumer mismatch): unexpected error: % / %', sqlstate, sqlerrm;
    end;

    -- Real owner ack to clean up.
    perform pgque.ack('q_iso_a', 'consumer_a', v_bid);
end $$;
reset role;

-- Cleanup ---------------------------------------------------------------------

select pgque.unsubscribe('q_iso_a', 'consumer_a');
select pgque.unsubscribe('q_iso_b', 'consumer_b');
select pgque.drop_queue('q_iso_a', true);
select pgque.drop_queue('q_iso_b', true);

revoke pgque_reader from app_a;
revoke pgque_reader from app_b;
drop role app_a;
drop role app_b;

\echo 'PASS: consumer-vs-consumer ack/nack ownership (#164)'
