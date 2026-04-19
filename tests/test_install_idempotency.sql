-- test_install_idempotency.sql -- Verify pg_current install works correctly
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Run after: \i sql/pg_current.sql

-- Test 1: pg_current schema exists
do $$
begin
    assert exists (
        select 1 from information_schema.schemata
        where schema_name = 'pg_current'
    ), 'pg_current schema should exist';
    raise notice 'PASS: pg_current schema exists';
end $$;

-- Test 2: Key tables exist
do $$
begin
    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pg_current' and table_name = 'queue'
    ), 'pg_current.queue table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pg_current' and table_name = 'tick'
    ), 'pg_current.tick table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pg_current' and table_name = 'subscription'
    ), 'pg_current.subscription table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pg_current' and table_name = 'consumer'
    ), 'pg_current.consumer table should exist';

    assert exists (
        select 1 from information_schema.tables
        where table_schema = 'pg_current' and table_name = 'config'
    ), 'pg_current.config table should exist';

    raise notice 'PASS: all key tables exist';
end $$;

-- Test 3: Create a queue, insert event, verify it works
do $$
declare
    ev_id bigint;
begin
    perform pg_current.create_queue('test_install_q');

    ev_id := pg_current.insert_event('test_install_q', 'test.event', '{"key":"value"}');
    assert ev_id is not null, 'insert_event should return an event id';

    raise notice 'PASS: queue created and event inserted (ev_id=%)', ev_id;
end $$;

-- Test 4: Verify state
do $$
declare
    q_count int;
begin
    select count(*) into q_count
    from pg_current.queue where queue_name = 'test_install_q';
    assert q_count = 1, 'test queue should exist';

    raise notice 'PASS: queue state verified';
end $$;

-- Test 5: Verify config singleton
do $$
begin
    assert (select count(*) from pg_current.config) = 1,
        'config should have exactly 1 row';
    raise notice 'PASS: config singleton verified';
end $$;

-- Cleanup
select pg_current.drop_queue('test_install_q', true);
