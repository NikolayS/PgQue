-- test_security_extra_maint.sql -- Regression: queue_extra_maint owner check in maint()
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Issue #101: pgque_admin can escalate privileges by injecting an attacker-owned
-- function into queue.queue_extra_maint, then calling the SECURITY DEFINER maint().
--
-- Fix (Option A): maint() validates that each queue_extra_maint function name
-- resolves to a regprocedure AND is owned by the same role that owns maint() itself.
-- Functions failing this check are skipped with a WARNING, not executed.
--
-- Red until fix: maint() currently executes queue_extra_maint entries without
-- any ownership check.

-- -------------------------------------------------------------------------
-- Setup: create a pgque_admin member role that will attempt the escalation.
-- -------------------------------------------------------------------------
do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'pgque_attacker') then
    execute 'create role pgque_attacker login';
  end if;
end $$;

do $$
begin
  -- Grant pgque_admin membership so attacker has full admin access.
  grant pgque_admin to pgque_attacker;
end $$;

-- -------------------------------------------------------------------------
-- Setup: create a canary table owned by the attacker (via session_user).
-- -------------------------------------------------------------------------
create table if not exists pgque.evil_canary (
    id serial primary key,
    who text,
    is_superuser bool
);
truncate pgque.evil_canary;

-- -------------------------------------------------------------------------
-- Setup: as pgque_attacker, create an evil function in the pgque schema
-- and register it in queue_extra_maint.
-- -------------------------------------------------------------------------
set role pgque_attacker;

-- Create a queue to attach the evil function to.
select pgque.create_queue('attack_test_queue');

-- Create the evil function (owned by pgque_attacker) inside the pgque schema.
-- pgque_admin has CREATE on schema pgque, so this succeeds.
create or replace function pgque.evil_maint_probe(i_queue text)
returns integer as $$
begin
    -- Record who we are running as.
    insert into pgque.evil_canary (who, is_superuser)
    values (session_user, pg_catalog.current_setting('is_superuser') = 'on');
    return 1;
end;
$$ language plpgsql;

-- Attach the evil function to queue_extra_maint.
update pgque.queue
set queue_extra_maint = array['pgque.evil_maint_probe']
where queue_name = 'attack_test_queue';

reset role;

-- -------------------------------------------------------------------------
-- Exercise: call maint() as superuser (simulating pg_cron / install owner).
-- Before the fix, evil_maint_probe runs as the SECURITY DEFINER owner.
-- After the fix, it must be skipped (not run at all).
-- -------------------------------------------------------------------------
select pgque.maint();

-- -------------------------------------------------------------------------
-- Assert: the canary table must be empty — evil_maint_probe must NOT have run.
-- -------------------------------------------------------------------------
do $$
declare
  v_cnt int;
begin
  select count(*) into v_cnt from pgque.evil_canary;

  assert v_cnt = 0,
    'SECURITY DEFINER escalation: evil_maint_probe executed ' || v_cnt::text
    || ' time(s) under maint() — queue_extra_maint ownership check missing';

  raise notice 'PASS: security_extra_maint - attacker-owned function in queue_extra_maint was not executed by maint()';
end $$;

-- -------------------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------------------
reset role;
select pgque.drop_queue('attack_test_queue');
drop function if exists pgque.evil_maint_probe(text);
drop table if exists pgque.evil_canary;
revoke pgque_admin from pgque_attacker;
drop role if exists pgque_attacker;
