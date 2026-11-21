\set ON_ERROR_STOP on

-- Monitoring must work without exposing the private naming helper.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
select pgque.create_queue('partition_status_roles');
select pgque.subscribe_slot('partition_status_roles', 'workers', 0, 2);

set role pgque_reader;
do $$
begin
  assert not has_function_privilege(current_user,
    'pgque._slot_name(text,integer,integer)', 'EXECUTE'),
    'status access must not require a private helper grant';
  assert (select count(*) = 2 from pgque.partition_slot_status
    where queue_name = 'partition_status_roles' and consumer = 'workers'),
    'reader must see all expected slots';
  assert (select subscribed and lease_owner is null and last_tick is not null
    from pgque.partition_slot_status where queue_name = 'partition_status_roles'
      and consumer = 'workers' and slot = 0),
    'reader must see the subscribed unleased slot';
  assert (select not subscribed and last_tick is null and pending_events is null
    from pgque.partition_slot_status where queue_name = 'partition_status_roles'
      and consumer = 'workers' and slot = 1),
    'reader must see unknown lag for the missing slot';
  perform pgque.claim_slot('partition_status_roles', 'workers', 0, 'reader-worker');
  assert (select lease_owner = 'reader-worker'
    from pgque.partition_slot_status where queue_name = 'partition_status_roles'
      and consumer = 'workers' and slot = 0),
    'reader must see the active lease owner';
end $$;
reset role;

set role pgque_admin;
do $$
begin
  assert (select count(*) = 2 from pgque.partition_slot_status
    where queue_name = 'partition_status_roles' and consumer = 'workers'),
    'admin must see all expected slots';
  assert (select lease_owner = 'reader-worker'
    from pgque.partition_slot_status where queue_name = 'partition_status_roles'
      and consumer = 'workers' and slot = 0),
    'admin must see the active lease owner';
end $$;
reset role;

select pgque.drop_queue('partition_status_roles', true);
\echo 'PASS: reader/admin slot monitoring preserves private helper boundary'
