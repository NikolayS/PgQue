-- Instrumented consumer for pg-boss (SKIP LOCKED + DELETE USING on partitioned job table).
-- pg-boss 'default' queue, state='created'.
DO $$
DECLARE
  n int := 0;
BEGIN
  WITH claimed AS (
    SELECT id FROM pgboss.job
    WHERE name='bench_queue' AND state='created'
    ORDER BY priority DESC, created_on, id
    LIMIT 100 FOR UPDATE SKIP LOCKED
  ), deleted AS (
    DELETE FROM pgboss.job USING claimed WHERE pgboss.job.id = claimed.id
    RETURNING pgboss.job.id
  )
  SELECT count(*) INTO n FROM deleted;
  RAISE NOTICE 'ev ts=% n=%', extract(epoch from clock_timestamp())::bigint, n;
END $$;
