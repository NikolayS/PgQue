-- Test logres-specific additions
-- Run against a database with logres installed

-- Test 1: config table
do $$
begin
    assert (select count(*) from logres.config) = 1, 'config should have exactly 1 row';
    assert (select singleton from logres.config) = true, 'singleton should be true';
    raise notice 'PASS: config table';
end $$;

-- Test 2: queue_max_retries column exists
do $$
begin
    assert exists (
        select 1 from information_schema.columns
        where table_schema = 'logres' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ), 'queue_max_retries column should exist';
    raise notice 'PASS: queue_max_retries column';
end $$;

-- Test 3: roles exist
do $$
begin
    assert exists (select 1 from pg_roles where rolname = 'logres_reader'), 'logres_reader role should exist';
    assert exists (select 1 from pg_roles where rolname = 'logres_writer'), 'logres_writer role should exist';
    assert exists (select 1 from pg_roles where rolname = 'logres_admin'), 'logres_admin role should exist';
    raise notice 'PASS: roles exist';
end $$;

-- Test 4: lifecycle functions exist
do $$
begin
    perform logres.version();
    raise notice 'PASS: version() works, returned %', logres.version();
end $$;

-- Test 5: idempotency - re-running additions should not error
-- (This will be tested as part of install idempotency in Issue #4)
