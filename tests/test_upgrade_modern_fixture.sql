\set ON_ERROR_STOP on

-- Build a working, populated v0.2.1/v0.2.2 baseline before a HEAD upgrade.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Run in a fresh test database. Keep the snapshot schema across the installer.

set timezone = 'UTC';
set datestyle = 'ISO, YMD';
set intervalstyle = 'postgres';

do $$
begin
  assert pgque.version() in ('0.2.1', '0.2.2'),
    'modern upgrade fixture requires released v0.2.1 or v0.2.2';
end $$;

\ir test_upgrade_v0_1_fixture.sql

-- The shared fixture transfers wrapper ownership. Unlike v0.1.0, these
-- releases already use the restricted bulk primitive. Supply the new owner's
-- required grant now, not during upgrade, and prove the baseline works.
grant execute on function pgque.insert_event_bulk(text, text, text[])
  to pgque_v01_wrapper_owner;

do $$
begin
  assert cardinality(pgque.send_batch(
    'upgrade_v01_q', array['{"baseline":"jsonb-default"}'::jsonb])) = 1;
  assert cardinality(pgque.send_batch(
    'upgrade_v01_q', 'baseline.jsonb',
    array['{"baseline":"jsonb-explicit"}'::jsonb])) = 1;
  assert cardinality(pgque.send_batch(
    'upgrade_v01_q', array['baseline-text-default']::text[])) = 1;
  assert cardinality(pgque.send_batch(
    'upgrade_v01_q', 'baseline.text',
    array['baseline-text-explicit']::text[])) = 1;
  raise notice 'PASS: released modern wrappers work after ownership transfer';
end $$;

-- This schema must survive separate fixture/install/assertion connections.
-- Do not reset an old snapshot silently: each upgrade case needs a fresh DB.
create schema pgque_upgrade_modern_test;
create table pgque_upgrade_modern_test.state (
  relation_name name primary key,
  scope_column name not null,
  scope_id bigint not null,
  column_names text[] not null,
  row_data jsonb not null
);
create table pgque_upgrade_modern_test.catalog (
  singleton boolean primary key default true check (singleton),
  source_version text not null,
  wrappers jsonb not null,
  primitive_grant jsonb not null
);

do $$
declare
  v_queue_id bigint;
  v_consumer_id bigint;
  v_scope record;
  v_columns text[];
  v_rows jsonb;
  v_wrappers jsonb;
  v_grant jsonb;
begin
  select queue_id into strict v_queue_id
    from pgque.queue where queue_name = 'upgrade_v01_q';
  select co_id into strict v_consumer_id
    from pgque.consumer where co_name = 'upgrade_v01_c';

  for v_scope in
    select * from (values
      ('queue', 'queue_id', v_queue_id),
      ('consumer', 'co_id', v_consumer_id),
      ('subscription', 'sub_queue', v_queue_id),
      ('retry_queue', 'ev_queue', v_queue_id),
      ('dead_letter', 'dl_queue_id', v_queue_id)
    ) as scopes(relation_name, scope_column, scope_id)
  loop
    select array_agg(a.attname::text order by a.attnum) into v_columns
      from pg_attribute as a
      where a.attrelid = format('pgque.%I', v_scope.relation_name)::regclass
        and a.attnum > 0 and not a.attisdropped;
    execute format(
      'select coalesce(jsonb_agg(to_jsonb(t) order by to_jsonb(t)::text), ''[]''::jsonb)
       from pgque.%I as t where %I = $1',
      v_scope.relation_name, v_scope.scope_column
    ) into v_rows using v_scope.scope_id;
    assert jsonb_array_length(v_rows) > 0,
      format('baseline %s must contain fixture rows', v_scope.relation_name);
    insert into pgque_upgrade_modern_test.state
      values (v_scope.relation_name, v_scope.scope_column, v_scope.scope_id,
              v_columns, v_rows);
  end loop;

  -- Include both modern default-type overloads, as well as the eight wrappers
  -- whose ownership the shared fixture changes. None should be recreated.
  select jsonb_agg(jsonb_build_object(
    'signature', signatures.sig, 'oid', p.oid,
    'owner_oid', p.proowner, 'owner_name', pg_get_userbyid(p.proowner)
  ) order by signatures.sig) into v_wrappers
  from unnest(array[
    'pgque.send(text,jsonb)', 'pgque.send(text,text)',
    'pgque.send(text,text,jsonb)', 'pgque.send(text,text,text)',
    'pgque.send_batch(text,jsonb[])', 'pgque.send_batch(text,text[])',
    'pgque.send_batch(text,text,jsonb[])', 'pgque.send_batch(text,text,text[])',
    'pgque.subscribe(text,text)', 'pgque.unsubscribe(text,text)'
  ]) as signatures(sig)
  join pg_proc as p on p.oid = signatures.sig::regprocedure;
  assert jsonb_array_length(v_wrappers) = 10;

  select jsonb_agg(to_jsonb(g) order by to_jsonb(g)::text) into v_grant
  from (
    select p.oid as procedure_oid, acl.*
    from pg_proc as p cross join lateral aclexplode(p.proacl) as acl
    where p.oid = 'pgque.insert_event_bulk(text,text,text[])'::regprocedure
      and acl.grantee = 'pgque_v01_wrapper_owner'::regrole
      and acl.privilege_type = 'EXECUTE'
  ) as g;
  assert coalesce(jsonb_array_length(v_grant), 0) = 1,
    'baseline requires a direct primitive EXECUTE grant to the wrapper owner';

  insert into pgque_upgrade_modern_test.catalog
    (source_version, wrappers, primitive_grant)
    values (pgque.version(), v_wrappers, v_grant);
  raise notice 'PASS: modern baseline rows, cursors, wrapper identities, and direct grant saved';
end $$;
