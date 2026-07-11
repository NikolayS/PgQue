\set ON_ERROR_STOP on

-- A rejected oversized-alpha upgrade must preserve every existing object.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_event_exists boolean;
  v_event_id bigint;
  v_old_oid oid;
begin
  select event_id into strict v_event_id
  from public.upgrade_v03_oversized_state;
  select function_oid into strict v_old_oid
  from public.upgrade_v03_named_functions
  where signature = 'pgque.subscribe_slot(text,text,integer,integer)';

  assert to_regprocedure(
    'pgque.subscribe_slot(text,text,integer,integer)')::oid = v_old_oid,
    'rejected upgrade must preserve the alpha function identity';
  assert exists (
    select 1
    from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'upgrade_v03_oversized_q'
      and pc.co_name = 'oversized-workers'
      and pc.n = 257
  ), 'rejected upgrade must preserve oversized partition metadata';
  assert exists (
    select 1
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'upgrade_v03_oversized_q'
      and ps.co_name = 'oversized-workers'
      and ps.slot = 0
  ), 'rejected upgrade must preserve the materialized alpha slot';
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'upgrade_v03_oversized_q'
      and c.co_name = 'oversized-workers#0/257'
  ), 'rejected upgrade must preserve the alpha subscription';

  execute format(
    'select exists (select 1 from %s where ev_id = $1)',
    pgque.current_event_table('upgrade_v03_oversized_q')
  ) into v_event_exists using v_event_id;
  assert v_event_exists,
    'rejected upgrade must preserve pending queue data';

  perform pgque.drop_queue('upgrade_v03_oversized_q', true);
  drop table public.upgrade_v03_oversized_state;
  raise notice 'PASS: oversized alpha upgrade failed without changing state';
end $$;
