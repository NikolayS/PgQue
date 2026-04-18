-- Schedule partman run_maintenance every minute (partitions are 5-min)
SELECT cron.schedule('partman_run', '* * * * *', 'CALL public.run_maintenance_proc()');
-- Verify
SELECT jobname, schedule FROM cron.job WHERE jobname = 'partman_run';
-- Test ash
SELECT count(*) FROM ash.samples(p_interval => '1 minute'::interval, p_limit => 10);
