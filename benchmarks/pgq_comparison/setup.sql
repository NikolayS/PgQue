-- PgQue vs PgQ comparison benchmark: database setup
-- Run this AFTER applying tuning (see run.sh for config)

-- Setup pgque database (requires pg_cron in shared_preload_libraries)
-- and cron.database_name = 'bench_pgque'
\c bench_pgque

create extension if not exists pg_cron;
\i ../../sql/pgque-install.sql

select pgque.create_queue('bench');
select pgque.set_queue_config('bench', 'rotation_period', '2 minutes');
select pgque.start();

-- Setup pgq database (PL-only mode, no C extension)
\c bench_pgq

\i pgq_pl_only.sql

select pgq.create_queue('bench');
select pgq.ticker('bench');
select pgq.set_queue_config('bench', 'rotation_period', '2 minutes');

-- Schedule pgq ticker via pg_cron (cross-database from pgque db)
\c bench_pgque
select cron.schedule_in_database('pgq_ticker', '2 seconds',
  'select pgq.ticker()', 'bench_pgq');
select cron.schedule_in_database('pgq_maint', '30 seconds',
  $$select pgq.maint_rotate_tables_step1(queue_name) from pgq.queue;
    select pgq.maint_rotate_tables_step2();
    select pgq.maint_retry_events()$$,
  'bench_pgq');
