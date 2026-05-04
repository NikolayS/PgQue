-- pgque.config — singleton configuration table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create table if not exists pgque.config (
    singleton       bool primary key default true check (singleton),
    ticker_job_id   bigint,
    maint_job_id    bigint,
    tick_period_ms  integer not null default 100
        check (tick_period_ms between 1 and 60000),
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
          and column_name = 'tick_period_ms'
    ) then
        alter table pgque.config
            add column tick_period_ms integer not null default 100
                check (tick_period_ms between 1 and 60000);
    end if;
end $$;
