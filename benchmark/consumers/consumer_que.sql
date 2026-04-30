-- Instrumented consumer for que (Ruby Que gem; SKIP LOCKED + DELETE).
-- que_jobs with default queue and priority ordering.
DO $$
DECLARE
  n int := 0;
BEGIN
  WITH claimed AS (
    SELECT id FROM que_jobs
    WHERE finished_at IS NULL AND expired_at IS NULL
    ORDER BY priority, run_at, id
    LIMIT 100 FOR UPDATE SKIP LOCKED
  ), deleted AS (
    DELETE FROM que_jobs USING claimed WHERE que_jobs.id = claimed.id
    RETURNING que_jobs.id
  )
  SELECT count(*) INTO n FROM deleted;
  RAISE NOTICE 'ev ts=% n=%', extract(epoch from clock_timestamp())::bigint, n;
END $$;
