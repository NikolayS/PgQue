-- test_pgtimetable_lifecycle.sql -- Verify pg_timetable lifecycle integration
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- These tests exercise pgque.start_timetable()/stop_timetable().  When real
-- pg_timetable is not installed, the positive-path tests create a tiny fake
-- timetable schema with the same delete_job() contract plus the older 11-argument add_job() signature PgQue supports.
-- This keeps CI coverage deterministic without running the external scheduler.

-- Clean up from interrupted prior runs.
do $$
begin
  if to_regnamespace('timetable') is not null then
    -- Only drop the lightweight fake schema these tests create, never a real
    -- pg_timetable install.
    if to_regclass('timetable.chain') is not null
       and to_regclass('timetable.task') is not null
       and to_regclass('timetable.execution_log') is null then
      drop schema timetable cascade;
    end if;
  end if;
exception when others then
  null;
end $$;

-- Test 1: without pg_timetable, start_timetable() raises an informative error
do $$
begin
  if to_regnamespace('timetable') is not null then
    raise notice 'SKIP: pg_timetable IS installed (cannot test without-pg_timetable path)';
    return;
  end if;

  begin
    perform pgque.start_timetable();
    assert false, 'start_timetable() should raise without pg_timetable';
  exception when others then
    assert sqlerrm like '%pg_timetable%',
      'error should mention pg_timetable, got: ' || sqlerrm;
    raise notice 'PASS: start_timetable() raises informative error without pg_timetable';
  end;
end $$;


-- Test 1b: a fake timetable schema owned by an untrusted role is rejected
-- before SECURITY DEFINER code calls timetable.add_job()/delete_job().
do $$
begin
  if to_regnamespace('timetable') is not null then
    raise notice 'SKIP: timetable schema already exists (cannot test untrusted fake owner path)';
    return;
  end if;

  if not exists (select 1 from pg_roles where rolname = 'pgque_fake_timetable_owner') then
    create role pgque_fake_timetable_owner;
  end if;

  create schema timetable authorization pgque_fake_timetable_owner;
  create domain timetable.cron as text;
  create type timetable.command_kind as enum ('SQL', 'PROGRAM', 'BUILTIN');
  create function timetable.add_job(
    job_name text,
    job_schedule timetable.cron,
    job_command text,
    job_parameters jsonb default null,
    job_kind timetable.command_kind default 'SQL'::timetable.command_kind,
    job_client_name text default null,
    job_max_instances integer default null,
    job_live boolean default true,
    job_self_destruct boolean default false,
    job_ignore_errors boolean default true,
    job_exclusive boolean default false,
    job_on_error text default null
  ) returns bigint language sql as 'select 1::bigint';
  create function timetable.delete_job(job_name text) returns boolean language sql as 'select true';
  alter function timetable.add_job(text,timetable.cron,text,jsonb,timetable.command_kind,text,integer,boolean,boolean,boolean,boolean,text)
    owner to pgque_fake_timetable_owner;
  alter function timetable.delete_job(text) owner to pgque_fake_timetable_owner;

  begin
    perform pgque.start_timetable();
    assert false, 'start_timetable() should reject untrusted fake timetable owner';
  exception when others then
    assert sqlerrm like '%untrusted pg_timetable schema owner%',
      'error should reject untrusted schema owner, got: ' || sqlerrm;
    raise notice 'PASS: start_timetable() rejects untrusted fake timetable schema owner';
  end;

  drop schema timetable cascade;
  drop role pgque_fake_timetable_owner;
end $$;

-- Test harness: if pg_timetable is absent, install a minimal fake schema that
-- matches the SQL API PgQue calls.  It intentionally does not run jobs.
create temp table _pgque_pgtimetable_harness(fake_installed boolean not null);

do $$
begin
  if to_regnamespace('timetable') is not null then
    insert into _pgque_pgtimetable_harness values (false);
    return;
  end if;

  execute 'create schema timetable';
  execute 'create domain timetable.cron as text';
  execute $ddl$create type timetable.command_kind as enum ('SQL', 'PROGRAM', 'BUILTIN')$ddl$;
  execute $sql$
    create table timetable.chain (
      chain_id bigserial primary key,
      chain_name text not null unique,
      run_at timetable.cron,
      max_instances integer,
      live boolean default false
    )
  $sql$;
  execute $sql$
    create table timetable.task (
      task_id bigserial primary key,
      chain_id bigint references timetable.chain(chain_id) on delete cascade,
      task_order double precision not null,
      kind timetable.command_kind not null default 'SQL',
      command text not null,
      ignore_error boolean not null default false,
      autonomous boolean not null default false
    )
  $sql$;
  execute $sql$
    create or replace function timetable.add_job(
      job_name text,
      job_schedule timetable.cron,
      job_command text,
      job_parameters jsonb default null,
      job_kind timetable.command_kind default 'SQL'::timetable.command_kind,
      job_client_name text default null,
      job_max_instances integer default null,
      job_live boolean default true,
      job_self_destruct boolean default false,
      job_ignore_errors boolean default true,
      job_exclusive boolean default false
    ) returns bigint language sql as $fn$
      with c as (
        insert into timetable.chain(chain_name, run_at, max_instances, live)
        values (job_name, job_schedule, job_max_instances, job_live)
        returning chain_id
      ), t as (
        insert into timetable.task(chain_id, task_order, kind, command, ignore_error, autonomous)
        select chain_id, 10, job_kind, job_command, job_ignore_errors, true from c
      )
      select chain_id from c;
    $fn$
  $sql$;
  execute $sql$
    create or replace function timetable.delete_job(job_name text)
    returns boolean language sql as $fn$
      with d as (delete from timetable.chain where chain_name = job_name returning 1)
      select exists(select 1 from d);
    $fn$
  $sql$;

  insert into _pgque_pgtimetable_harness values (true);
  raise notice 'INFO: installed fake pg_timetable schema for lifecycle tests';
end $$;


-- Test 1c: invalid tick rates are rejected once pg_timetable API exists.
do $$
declare
  r record;
begin
  for r in select * from (values
    (0::integer, '0'),
    (-1::integer, '-1'),
    (7::integer, '7'),
    (1001::integer, '1001'),
    (null::integer, 'NULL')
  ) as v(rate, label) loop
    begin
      perform pgque.start_timetable(r.rate);
      assert false, 'start_timetable(' || r.label || ') should fail';
    exception when others then
      assert sqlerrm like '%ticks_per_second%',
        'error should mention ticks_per_second for ' || r.label || ', got: ' || sqlerrm;
      assert sqlerrm like '%' || r.label || '%',
        'error should mention bad value ' || r.label || ', got: ' || sqlerrm;
    end;
  end loop;
  raise notice 'PASS: start_timetable() rejects invalid ticks_per_second values';
end $$;

-- Test 2: start_timetable(10) creates four pg_timetable jobs and stores config
do $$
declare
  v_ticker_id bigint;
  v_maint_id bigint;
  v_scheduler text;
  v_period_ms integer;
  v_job_count integer;
  v_ticker_command text;
  v_status text;
begin
  perform pgque.start_timetable(10);

  select ticker_job_id, maint_job_id, scheduler, tick_period_ms
    into v_ticker_id, v_maint_id, v_scheduler, v_period_ms
  from pgque.config;

  assert v_scheduler = 'pg_timetable', 'scheduler should be pg_timetable, got ' || coalesce(v_scheduler, 'NULL');
  assert v_ticker_id is not null, 'ticker_job_id should be set after start_timetable()';
  assert v_maint_id is not null, 'maint_job_id should be set after start_timetable()';
  assert v_period_ms = 100, 'start_timetable(10) should set tick_period_ms=100, got ' || v_period_ms;

  execute $sql$
    select count(*)
    from timetable.chain
    where chain_name in ('pgque_ticker', 'pgque_retry_events', 'pgque_maint', 'pgque_rotate_step2')
      and live
  $sql$ into v_job_count;
  assert v_job_count = 4,
    'expected 4 live pgque_* timetable jobs, found ' || v_job_count;

  execute $sql$
    select t.command
    from timetable.chain c
    join timetable.task t using (chain_id)
    where c.chain_name = 'pgque_ticker'
  $sql$ into v_ticker_command;
  assert v_ticker_command = 'CALL pgque.ticker_loop()',
    'pg_timetable pgque_ticker should CALL ticker_loop, got: ' || coalesce(v_ticker_command, 'NULL');

  select status into v_status from pgque.status() where component = 'scheduler';
  assert v_status = 'pg_timetable', 'status() scheduler row should show pg_timetable, got ' || coalesce(v_status, 'NULL');
  assert exists (select 1 from pgque.status() where component = 'ticker'),
    'status() should retain backward-compatible ticker row';
  assert exists (select 1 from pgque.status() where component = 'maintenance'),
    'status() should retain backward-compatible maintenance row';

  perform pgque.stop_timetable();
  raise notice 'PASS: start_timetable(10) schedules four jobs and 10/s ticker loop';
end $$;

-- Test 3: stop_timetable() removes pgque_* jobs and clears config
do $$
declare
  v_job_count integer;
begin
  perform pgque.start_timetable(10);
  perform pgque.stop_timetable();

  execute $sql$
    select count(*)
    from timetable.chain
    where chain_name in ('pgque_ticker', 'pgque_retry_events', 'pgque_maint', 'pgque_rotate_step2')
  $sql$ into v_job_count;
  assert v_job_count = 0,
    'expected 0 pgque_* timetable jobs after stop_timetable(), found ' || v_job_count;

  assert (select scheduler from pgque.config) is null,
    'scheduler should be NULL after stop_timetable()';
  assert (select ticker_job_id from pgque.config) is null,
    'ticker_job_id should be NULL after stop_timetable()';
  assert (select maint_job_id from pgque.config) is null,
    'maint_job_id should be NULL after stop_timetable()';

  raise notice 'PASS: stop_timetable() deletes jobs and clears config';
end $$;


-- Test 4: start_timetable() is idempotent; it replaces old PgQue jobs, not duplicates them.
do $$
declare
  v_job_count integer;
begin
  perform pgque.start_timetable(10);
  perform pgque.start_timetable(10);

  execute $sql$
    select count(*)
    from timetable.chain
    where chain_name in ('pgque_ticker', 'pgque_retry_events', 'pgque_maint', 'pgque_rotate_step2')
  $sql$ into v_job_count;
  assert v_job_count = 4,
    'expected exactly 4 pgque_* timetable jobs after repeated start_timetable(), found ' || v_job_count;

  perform pgque.stop_timetable();
  raise notice 'PASS: start_timetable() is idempotent';
end $$;

-- Test 5: generic stop() also stops pg_timetable when it is the active scheduler.
do $$
declare
  v_job_count integer;
begin
  perform pgque.start_timetable(10);
  perform pgque.stop();

  execute $sql$
    select count(*)
    from timetable.chain
    where chain_name in ('pgque_ticker', 'pgque_retry_events', 'pgque_maint', 'pgque_rotate_step2')
  $sql$ into v_job_count;
  assert v_job_count = 0,
    'expected generic stop() to remove pg_timetable jobs, found ' || v_job_count;
  assert (select scheduler from pgque.config) is null,
    'scheduler should be NULL after generic stop() of pg_timetable';
  assert (select ticker_job_id from pgque.config) is null,
    'ticker_job_id should be NULL after generic stop() of pg_timetable';
  assert (select maint_job_id from pgque.config) is null,
    'maint_job_id should be NULL after generic stop() of pg_timetable';

  raise notice 'PASS: stop() delegates to stop_timetable() for pg_timetable';
end $$;

-- Test 6: switching pg_timetable -> pg_cron cleans timetable jobs before scheduling pg_cron.
do $$
declare
  v_job_count integer;
  v_cron_count integer;
begin
  if not exists (select 1 from pg_extension where extname = 'pg_cron') then
    raise notice 'SKIP: pg_cron not installed (cannot test pg_timetable -> pg_cron switch)';
    return;
  end if;

  perform pgque.start_timetable(10);
  perform pgque.start();

  assert (select scheduler from pgque.config) = 'pg_cron',
    'scheduler should be pg_cron after switching from pg_timetable to pg_cron';

  execute $sql$
    select count(*)
    from timetable.chain
    where chain_name in ('pgque_ticker', 'pgque_retry_events', 'pgque_maint', 'pgque_rotate_step2')
  $sql$ into v_job_count;
  assert v_job_count = 0,
    'expected no pg_timetable jobs after switch to pg_cron, found ' || v_job_count;

  execute $sql$
    select count(*)
    from cron.job
    where jobname in ('pgque_ticker', 'pgque_retry_events', 'pgque_maint', 'pgque_rotate_step2')
  $sql$ into v_cron_count;
  assert v_cron_count = 4,
    'expected 4 pg_cron jobs after switch from pg_timetable, found ' || v_cron_count;

  perform pgque.stop();
  raise notice 'PASS: start() switches pg_timetable to pg_cron cleanly';
end $$;

-- Cleanup fake schema, if this test created one.
do $$
begin
  if (select fake_installed from _pgque_pgtimetable_harness) then
    drop schema timetable cascade;
  end if;
end $$;

drop table _pgque_pgtimetable_harness;
