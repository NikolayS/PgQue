-- W-skiplocked: a generic single-table queue using FOR UPDATE SKIP LOCKED.
-- Lifecycle: INSERT (enqueue) -> SELECT FOR UPDATE SKIP LOCKED + DELETE (dequeue).
-- Each job leaves one dead tuple behind on DELETE.

drop table if exists jobs;
create table jobs (
  id      bigserial primary key,
  payload text not null,
  created_at timestamptz not null default now()
);

create index on jobs (id);

-- Aggressive per-table autovacuum so we cannot be accused of under-tuning.
alter table jobs set (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01,
  fillfactor = 90
);

-- Helper: counters for throughput accounting.
drop table if exists bench_counters;
create table bench_counters (
  workload text primary key,
  enqueued bigint not null default 0,
  dequeued bigint not null default 0
);
insert into bench_counters(workload) values ('skiplocked'), ('pgque');
