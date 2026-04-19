-- pg_current.config — singleton configuration table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create table if not exists pg_current.config (
    singleton       bool primary key default true check (singleton),
    ticker_job_id   bigint,
    maint_job_id    bigint,
    installed_at    timestamptz not null default clock_timestamp()
);

-- Idempotent insert
insert into pg_current.config (singleton) values (true)
on conflict (singleton) do nothing;
