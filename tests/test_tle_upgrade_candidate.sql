/*
 * Test populated released 0.2.1/0.2.2 TLE upgrades to the generated candidate.
 * Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
 * Run in a fresh database. The caller must supply origin_version,
 * origin_installer, candidate_installer, and expected_version with psql -v.
 */
\set ON_ERROR_STOP on

select set_config('tle_test.origin_version', :'origin_version', false);
select set_config('tle_test.expected_version', :'expected_version', false);

create extension pg_tle;
\i :origin_installer
create extension pgque;

do $$
begin
    assert current_setting('tle_test.origin_version') in ('0.2.1', '0.2.2');
    assert pgque.version() = current_setting('tle_test.origin_version');
    assert (select extversion from pg_extension where extname = 'pgque')
        = current_setting('tle_test.origin_version');
    assert current_setting('tle_test.expected_version') <> pgque.version();
end $$;

\ir test_upgrade_modern_fixture.sql

/* Keep ordinary and cooperative batches open across the extension update. */
select pgque.create_queue('tle_candidate_active');
select pgque.subscribe('tle_candidate_active', 'ordinary');
select pgque.send_batch('tle_candidate_active', array['ordinary-1', 'ordinary-2']);
select pgque.create_queue('tle_candidate_coop');
select pgque.register_subconsumer('tle_candidate_coop', 'group', 'member');
select pgque.send_batch('tle_candidate_coop', array['coop-1', 'coop-2']);
select pgque.force_tick('tle_candidate_active');
select pgque.force_tick('tle_candidate_coop');
select pgque.ticker();

create temporary table tle_candidate_messages as
select 'ordinary'::text as mode, to_jsonb(m) as message
from pgque.receive('tle_candidate_active', 'ordinary', 10) as m
union all
select 'coop', to_jsonb(m)
from pgque.receive_coop('tle_candidate_coop', 'group', 'member', 10) as m;

do $$
begin
    assert (select count(*) from tle_candidate_messages where mode = 'ordinary') = 2;
    assert (select count(*) from tle_candidate_messages where mode = 'coop') = 2;
    assert (select count(*) from pgque.subscription where sub_batch is not null) = 2;
end $$;

/* A pre-existing independent path must survive registration unchanged. */
select pgtle.install_update_path('pgque', 'test-preserved-a', 'test-preserved-b', 'select 1;');
create temporary table tle_candidate_identity as
select
    e.oid as extension_oid,
    e.extversion,
    pg_get_functiondef('pgtle."pgque--test-preserved-a--test-preserved-b.sql"()'::regprocedure)
        as preserved_path
from pg_extension as e
where e.extname = 'pgque';

/* Project old columns so a migration may add columns but cannot change data. */
create temporary table tle_candidate_state as
select
    c.oid::regclass::text as relation_name,
    array_agg(a.attname::text order by a.attnum) as column_names,
    null::jsonb as row_data
from pg_class as c
join pg_attribute as a on a.attrelid = c.oid
where c.oid in ('pgque.queue'::regclass, 'pgque.consumer'::regclass,
    'pgque.subscription'::regclass, 'pgque.retry_queue'::regclass,
    'pgque.dead_letter'::regclass)
and a.attnum > 0 and not a.attisdropped
group by c.oid;

create function pg_temp.tle_rows(i_relation text, i_columns text[])
returns jsonb language plpgsql as $$
declare
    v_rows jsonb;
begin
    execute format(
        'select coalesce(jsonb_agg(projected order by projected::text), ''[]''::jsonb)
        from (
            select (
                select jsonb_object_agg(k, to_jsonb(t) -> k)
                from unnest($1::text[]) as columns(k)
            ) as projected
            from %s as t
        ) as rows', i_relation::regclass
    ) into v_rows using i_columns;
    return v_rows;
end $$;

update tle_candidate_state
set row_data = pg_temp.tle_rows(relation_name, column_names);

create function pg_temp.tle_check_state()
returns void language plpgsql as $$
declare
    v_state record;
begin
    for v_state in select * from tle_candidate_state loop
        assert (
            select count(*)
            from pg_attribute
            where attrelid = v_state.relation_name::regclass
            and attnum > 0 and not attisdropped
            and attname::text = any(v_state.column_names)
        ) = cardinality(v_state.column_names), 'baseline column was removed';
        assert pg_temp.tle_rows(v_state.relation_name, v_state.column_names)
            = v_state.row_data, format('baseline data changed in %s', v_state.relation_name);
    end loop;
end $$;

create function pg_temp.tle_functions()
returns jsonb language sql as $$
    select jsonb_agg(to_jsonb(p) order by p.oid)
    from pg_proc as p
    where p.pronamespace = 'pgque'::regnamespace
$$;

create temporary table tle_candidate_functions as select pg_temp.tle_functions() as snapshot;

\i :candidate_installer
\i :candidate_installer

do $$
declare
    v_source text;
begin
    assert (select oid from pg_extension where extname = 'pgque')
        = (select extension_oid from tle_candidate_identity);
    assert (select extversion from pg_extension where extname = 'pgque')
        = current_setting('tle_test.origin_version');
    assert pgque.version() = current_setting('tle_test.origin_version'),
        'registration must not run the migration';
    assert pg_temp.tle_functions() = (select snapshot from tle_candidate_functions),
        'registration must not change installed function definitions, owners, or ACLs';
    perform pg_temp.tle_check_state();
    assert (select default_version from pgtle.available_extensions() where name = 'pgque')
        = current_setting('tle_test.expected_version');
    assert to_regprocedure(format('pgtle.%I()',
        'pgque--' || current_setting('tle_test.expected_version') || '.sql')) is not null,
        'candidate must have a direct fresh-install body';
    foreach v_source in array array['0.2.1', '0.2.2'] loop
        assert exists (
            select 1
            from pgtle.extension_update_paths('pgque')
            where source = v_source
            and target = current_setting('tle_test.expected_version')
            and path = v_source || '--' || current_setting('tle_test.expected_version')
        ), format('missing direct candidate path from %s', v_source);
    end loop;
    assert pg_get_functiondef('pgtle."pgque--test-preserved-a--test-preserved-b.sql"()'::regprocedure)
        = (select preserved_path from tle_candidate_identity);
    raise notice 'PASS: repeated registration preserves installed release and exact populated state';
end $$;

alter extension pgque update to :'expected_version';

do $$
declare
    v_object record;
begin
    assert (select oid from pg_extension where extname = 'pgque')
        = (select extension_oid from tle_candidate_identity);
    assert (select extversion from pg_extension where extname = 'pgque')
        = current_setting('tle_test.expected_version');
    assert pgque.version() = current_setting('tle_test.expected_version');
    perform pg_temp.tle_check_state();
    for v_object in
        select 'pg_proc'::regclass as class_id, signature::regprocedure::oid as object_id
        from (values ('pgque.subscribe_partitioned(text,text,integer)'),
            ('pgque.unsubscribe_partitioned(text,text)'),
            ('pgque.receive_page(text,text,text,integer,interval)'),
            ('pgque.ack_page(uuid,text,jsonb)')) as functions(signature)
        union all
        select 'pg_class'::regclass, relation::regclass::oid
        from (values ('pgque.partition_consumer'), ('pgque.partition_slot'),
            ('pgque.page_state')) as relations(relation)
    loop
        assert exists (
            select 1
            from pg_depend
            where classid = v_object.class_id and objid = v_object.object_id
            and refclassid = 'pg_extension'::regclass
            and refobjid = (select extension_oid from tle_candidate_identity)
            and deptype = 'e'
        ), 'new API object is not an extension member';
    end loop;
    raise notice 'PASS: explicit update preserves extension OID, exact data, and active cursors';
end $$;

/* Exact wrapper/grant snapshots are checked before this include consumes. */
\ir test_upgrade_modern_assertions.sql

do $$
declare
    v_mode text;
    v_actual jsonb;
    v_expected jsonb;
    v_batch bigint;
begin
    foreach v_mode in array array['ordinary', 'coop'] loop
        select
            jsonb_agg(message order by (message->>'msg_id')::bigint),
            min((message->>'batch_id')::bigint)
        into v_expected, v_batch
        from tle_candidate_messages
        where mode = v_mode;
        if v_mode = 'ordinary' then
            select jsonb_agg(to_jsonb(m) order by m.msg_id) into v_actual
            from pgque.receive('tle_candidate_active', 'ordinary', 10) as m;
        else
            select jsonb_agg(to_jsonb(m) order by m.msg_id) into v_actual
            from pgque.receive_coop('tle_candidate_coop', 'group', 'member', 10) as m;
        end if;
        assert v_actual = v_expected, format('%s active batch changed after update', v_mode);
        perform pgque.ack(v_batch);
    end loop;
    raise notice 'PASS: pre-upgrade ordinary and cooperative batches replay exactly and acknowledge';
end $$;

/* Functional checks moved cursors. Take a new snapshot for idempotency. */
update tle_candidate_state
set row_data = pg_temp.tle_rows(relation_name, column_names);
update tle_candidate_functions set snapshot = pg_temp.tle_functions();
\i :candidate_installer
alter extension pgque update to :'expected_version';
alter extension pgque update to :'expected_version';

do $$
begin
    perform pg_temp.tle_check_state();
    assert pg_temp.tle_functions() = (select snapshot from tle_candidate_functions);
    assert (select oid from pg_extension where extname = 'pgque')
        = (select extension_oid from tle_candidate_identity);
    assert (select extversion from pg_extension where extname = 'pgque')
        = current_setting('tle_test.expected_version');
    assert pg_get_functiondef('pgtle."pgque--test-preserved-a--test-preserved-b.sql"()'::regprocedure)
        = (select preserved_path from tle_candidate_identity);
    raise notice 'PASS: repeated registration and explicit updates are idempotent';
end $$;

drop extension pgque cascade;
create extension pgque;

do $$
begin
    assert (select extversion from pg_extension where extname = 'pgque')
        = current_setting('tle_test.expected_version');
    assert pgque.version() = current_setting('tle_test.expected_version');
    assert to_regprocedure('pgque.subscribe_partitioned(text,text,integer)') is not null;
    assert to_regprocedure('pgque.receive_page(text,text,text,integer,interval)') is not null;
    raise notice 'PASS: fresh recreation uses the registered candidate body';
end $$;

\echo '=== test_tle_upgrade_candidate: ALL PASSED ==='
