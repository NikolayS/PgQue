-- pgque.config — singleton configuration table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create table if not exists pgque.config (
    singleton       bool primary key default true check (singleton),
    ticker_job_id   bigint,
    maint_job_id    bigint,
    scheduler       text
        constraint config_scheduler_check
        check (scheduler in ('pg_cron', 'pg_timetable')),
    tick_period_ms  integer not null default 100
        constraint config_tick_period_ms_check
        check (
            tick_period_ms between 1 and 1000
            and case
                when tick_period_ms between 1 and 1000 then 1000 % tick_period_ms = 0
                else false
            end
        ),
    installed_at    timestamptz not null default clock_timestamp()
);

-- Idempotent insert
insert into pgque.config (singleton) values (true)
on conflict (singleton) do nothing;

-- Add tick_period_ms on upgrade from a pre-tick-period install.
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'pgque' and table_name = 'config'
          and column_name = 'scheduler'
    ) then
        alter table pgque.config
            add column scheduler text;
    end if;

    alter table pgque.config
        drop constraint if exists config_scheduler_check;
    alter table pgque.config
        add constraint config_scheduler_check
        check (scheduler in ('pg_cron', 'pg_timetable'));

    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'pgque' and table_name = 'config'
          and column_name = 'tick_period_ms'
    ) then
        alter table pgque.config
            add column tick_period_ms integer not null default 100;
    end if;

    -- v0.2.0 safety: ticker_loop runs within pg_cron's 1000 ms slot and uses
    -- integer iteration counts, so only exact divisors of 1000 produce the
    -- reported cadence. Normalize any pre-constraint experimental value before
    -- tightening the check.
    update pgque.config
       set tick_period_ms = 100
     where not case
        when tick_period_ms between 1 and 1000 then 1000 % tick_period_ms = 0
        else false
     end;

    alter table pgque.config
        drop constraint if exists config_tick_period_ms_check;
    alter table pgque.config
        add constraint config_tick_period_ms_check
        check (
            tick_period_ms between 1 and 1000
            and case
                when tick_period_ms between 1 and 1000 then 1000 % tick_period_ms = 0
                else false
            end
        );
end $$;
