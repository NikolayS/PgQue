\set ON_ERROR_STOP on

-- Verify the saved modern-release baseline before public API smoke tests.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Invoke with: psql -v expected_version=0.3.0-rc.2 -f this_file.sql

set timezone = 'UTC';
set datestyle = 'ISO, YMD';
set intervalstyle = 'postgres';

-- psql does not substitute variables inside dollar-quoted DO bodies.
-- A missing expected_version intentionally produces a SQL error here.
select set_config('pgque_upgrade_test.expected_version', :'expected_version', false);

do $$
declare
  v_catalog pgque_upgrade_modern_test.catalog%rowtype;
  v_state pgque_upgrade_modern_test.state%rowtype;
  v_wrapper jsonb;
  v_actual_wrapper jsonb;
  v_rows jsonb;
  v_grant jsonb;
  v_column_count integer;
begin
  select * into strict v_catalog from pgque_upgrade_modern_test.catalog;
  assert pgque.version() = current_setting('pgque_upgrade_test.expected_version'),
    format('expected upgraded version %s, got %s',
      current_setting('pgque_upgrade_test.expected_version'), pgque.version());
  assert pgque.version() <> v_catalog.source_version,
    'installer must change the released baseline version';
  assert to_regprocedure('pgque.subscribe_partitioned(text,text,integer)') is not null,
    'atomic partition setup API must exist after upgrade';
  assert to_regprocedure('pgque.unsubscribe_partitioned(text,text)') is not null,
    'whole-consumer partition teardown API must exist after upgrade';
  assert (select count(*) from pgque_upgrade_modern_test.state) = 5,
    'all five baseline relation snapshots must exist';

  for v_state in select * from pgque_upgrade_modern_test.state
  loop
    select count(*) into v_column_count from pg_attribute as a
      where a.attrelid = format('pgque.%I', v_state.relation_name)::regclass
        and a.attnum > 0 and not a.attisdropped
        and a.attname::text = any(v_state.column_names);
    assert v_column_count = cardinality(v_state.column_names),
      format('upgrade removed a baseline column from %s', v_state.relation_name);

    -- Compare exact values of every old column. Project out added columns,
    -- then compare sorted row arrays. JSON containment alone would miss array
    -- order/duplicate changes; equality also detects missing or extra rows.
    execute format(
      'select coalesce(jsonb_agg(projected order by projected::text), ''[]''::jsonb)
       from (
         select (select jsonb_object_agg(k, to_jsonb(t) -> k)
                 from unnest($1::text[]) as columns(k)) as projected
         from pgque.%I as t where %I = $2
       ) as rows', v_state.relation_name, v_state.scope_column
    ) into v_rows using v_state.column_names, v_state.scope_id;
    assert v_rows = v_state.row_data,
      format('upgrade changed baseline %s rows or column values', v_state.relation_name);
  end loop;

  assert jsonb_array_length(v_catalog.wrappers) = 10;
  for v_wrapper in select value from jsonb_array_elements(v_catalog.wrappers)
  loop
    select jsonb_build_object(
      'signature', v_wrapper ->> 'signature', 'oid', p.oid,
      'owner_oid', p.proowner, 'owner_name', pg_get_userbyid(p.proowner)
    ) into v_actual_wrapper
    from pg_proc as p
    where p.oid = to_regprocedure(v_wrapper ->> 'signature');
    assert v_actual_wrapper is not distinct from v_wrapper,
      format('upgrade changed wrapper OID or owner: %s', v_wrapper ->> 'signature');
  end loop;

  select jsonb_agg(to_jsonb(g) order by to_jsonb(g)::text) into v_grant
  from (
    select p.oid as procedure_oid, acl.*
    from pg_proc as p cross join lateral aclexplode(p.proacl) as acl
    where p.oid = 'pgque.insert_event_bulk(text,text,text[])'::regprocedure
      and acl.grantee = 'pgque_v01_wrapper_owner'::regrole
      and acl.privilege_type = 'EXECUTE'
  ) as g;
  assert v_grant is not distinct from v_catalog.primitive_grant,
    'upgrade must preserve the direct primitive grant, including grantor and grant option';
  assert has_function_privilege('pgque_v01_wrapper_owner',
    'pgque.insert_event_bulk(text,text,text[])', 'EXECUTE'),
    'custom wrapper owner must retain effective primitive EXECUTE';

  raise notice 'PASS: exact modern baseline data, cursors, wrapper OIDs/owners, and direct grant preserved';
end $$;

-- These checks publish and consume messages, which advance the saved cursor.
-- Run them only after the unchanged-state checks above, never before them.
\ir test_upgrade_v0_1_assertions.sql
