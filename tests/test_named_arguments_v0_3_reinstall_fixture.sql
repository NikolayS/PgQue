\set ON_ERROR_STOP on

-- Snapshot stable function identities before an idempotent HEAD reinstall.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

drop view if exists public.named_v03_function_dependency;
drop table if exists public.named_v03_stable_functions;
create table public.named_v03_stable_functions (
  signature text primary key,
  function_oid oid not null,
  owner_name name not null,
  reader_execute boolean not null,
  writer_execute boolean not null,
  admin_execute boolean not null
);

insert into public.named_v03_stable_functions (
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

create view public.named_v03_function_dependency as
select pgque.maint_idem(
  queue_name => '__named_v03_missing_queue__') as result;

do $$
begin
  assert (
    select count(*) = 13
    from public.named_v03_stable_functions
  ), 'stable reinstall fixture must capture all public signatures';
  raise notice 'PASS: stable named-argument OIDs and dependency captured';
end $$;
