-- Test pg_current-specific additions
-- Run against a database with pg_current installed

-- Test 1: config table
do $$
begin
    assert (select count(*) from pg_current.config) = 1, 'config should have exactly 1 row';
    assert (select singleton from pg_current.config) = true, 'singleton should be true';
    raise notice 'PASS: config table';
end $$;

-- Test 2: queue_max_retries column exists
do $$
begin
    assert exists (
        select 1 from information_schema.columns
        where table_schema = 'pg_current' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ), 'queue_max_retries column should exist';
    raise notice 'PASS: queue_max_retries column';
end $$;

-- Test 3: roles exist
do $$
begin
    assert exists (select 1 from pg_roles where rolname = 'pg_current_reader'), 'pg_current_reader role should exist';
    assert exists (select 1 from pg_roles where rolname = 'pg_current_writer'), 'pg_current_writer role should exist';
    assert exists (select 1 from pg_roles where rolname = 'pg_current_admin'), 'pg_current_admin role should exist';
    raise notice 'PASS: roles exist';
end $$;

-- Test 4: lifecycle functions exist
do $$
begin
    perform pg_current.version();
    raise notice 'PASS: version() works, returned %', pg_current.version();
end $$;

-- Test 5: idempotency - re-running additions should not error
-- (This will be tested as part of install idempotency in Issue #4)
