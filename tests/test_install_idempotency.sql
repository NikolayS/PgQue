-- test_install_idempotency.sql -- Verify pgque install works correctly
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Run after: \i devel/sql/pgque.sql

-- Test 1: pgque schema exists
do $$
begin
    assert exists (
        select 1 from information_schema.schemata
        where schema_name = 'pgque'
    ), 'pgque schema should exist';
    raise notice 'PASS: pgque schema exists';
end $$;

-- Test 2: Key tables exist
do $$
begin
    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pgque' and table_name = 'queue'
    ), 'pgque.queue table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pgque' and table_name = 'tick'
    ), 'pgque.tick table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pgque' and table_name = 'subscription'
    ), 'pgque.subscription table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pgque' and table_name = 'consumer'
    ), 'pgque.consumer table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pgque' and table_name = 'config'
    ), 'pgque.config table should exist';

    raise notice 'PASS: all key tables exist';
end $$;

-- Test 3: Create a queue, insert event, verify it works
do $$
declare
    ev_id bigint;
begin
    perform pgque.create_queue('test_install_q');

    ev_id := pgque.insert_event('test_install_q', 'test.event', '{"key":"value"}');
    assert ev_id is not null, 'insert_event should return an event id';

    raise notice 'PASS: queue created and event inserted (ev_id=%)', ev_id;
end $$;

-- Test 4: Verify state
do $$
declare
    q_count int;
begin
    select count(*) into q_count
    from pgque.queue where queue_name = 'test_install_q';
    assert q_count = 1, 'test queue should exist';

    raise notice 'PASS: queue state verified';
end $$;

-- Test 5: Verify config singleton
do $$
begin
    assert (select count(*) from pgque.config) = 1,
        'config should have exactly 1 row';
    raise notice 'PASS: config singleton verified';
end $$;

-- Cleanup
select pgque.drop_queue('test_install_q', true);
