\set ON_ERROR_STOP on

-- Build alpha state whose public functions still expose internal i_* names.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

drop table if exists public.upgrade_v03_named_functions;
create table public.upgrade_v03_named_functions (
  signature text primary key,
  function_oid oid not null,
  owner_name name not null,
  reader_execute boolean not null,
  writer_execute boolean not null,
  admin_execute boolean not null
);

insert into public.upgrade_v03_named_functions (
  signature,
  function_oid,
  owner_name,
  reader_execute,
  writer_execute,
  admin_execute
)
select
  f.signature,
  f.function_oid,
  pg_get_userbyid(p.proowner),
  has_function_privilege('pgque_reader', f.function_oid, 'execute'),
  has_function_privilege('pgque_writer', f.function_oid, 'execute'),
  has_function_privilege('pgque_admin', f.function_oid, 'execute')
from (
  select signature, to_regprocedure(signature)::oid as function_oid
  from unnest(array[
    'pgque.send(text,text,text,text)',
    'pgque.send(text,text,jsonb,text)',
    'pgque.subscribe_partitioned(text,text,integer)',
    'pgque.subscribe_slot(text,text,integer,integer)',
    'pgque.unsubscribe_slot(text,text,integer)',
    'pgque.claim_slot(text,text,integer,text,interval)',
    'pgque.release_slot(text,text,integer,text)',
    'pgque.receive_partitioned(text,text,integer,integer,text,integer)',
    'pgque.ack_partitioned(text,text,integer,integer,text)',
    'pgque.nack_partitioned(text,text,integer,integer,text,pgque.message,interval,text)',
    'pgque.send_idem(text,text,text,text,interval,text)',
    'pgque.send_idem(text,text,jsonb,text,interval,text)',
    'pgque.maint_idem(text)'
  ]) as signatures(signature)
) as f
join pg_proc as p on p.oid = f.function_oid;

do $$
begin
  assert (
    select count(*) in (12, 13)
    from public.upgrade_v03_named_functions
  ), 'all available pre-final public signatures must exist before upgrade';
  assert not exists (
    select 1
    from public.upgrade_v03_named_functions as f
    join pg_proc as p on p.oid = f.function_oid
    where p.proargnames[1] not like 'i\_%' escape '\'
  ), 'alpha fixture must start with internal i_* input names';
end $$;

drop table if exists public.upgrade_v03_named_state;
create table public.upgrade_v03_named_state (
  partition_event_id bigint not null,
  idem_event_id bigint not null,
  partition_key text not null
);

do $$
declare
  v_idem record;
  v_key text;
  v_partition_id bigint;
begin
  perform pgque.create_queue('upgrade_v03_named_q');
  perform pgque.subscribe_slot('upgrade_v03_named_q', 'workers', 0, 2);
  perform pgque.subscribe_slot('upgrade_v03_named_q', 'workers', 1, 2);

  select format('upgrade-named-key-%s', g) into v_key
  from generate_series(1, 10000) as g
  where (pg_catalog.hashtextextended(format('upgrade-named-key-%s', g), 0) % 2 + 2) % 2 = 0
  limit 1;
  v_partition_id := pgque.send(
    'upgrade_v03_named_q', 'upgrade.partition', '{}'::jsonb, v_key);
  select * into v_idem
  from pgque.send_idem(
    'upgrade_v03_named_q', 'upgrade.idem', '{}'::text,
    'upgrade-idem-key', interval '1 hour', v_key);

  insert into public.upgrade_v03_named_state (
    partition_event_id, idem_event_id, partition_key)
  values (v_partition_id, v_idem.event_id, v_key);
end $$;

do $$
begin
  perform pgque.force_next_tick('upgrade_v03_named_q');
  perform pgque.ticker();
  raise notice 'PASS: v0.3.0-alpha.1 named-argument fixture prepared';
end $$;
