-- Instrumented consumer for river (SKIP LOCKED + DELETE USING).
-- Wrapped in DO block so NOTICE carries the count.
-- Raw DELETE rowcount is ALSO visible in pg_stat_statements via the
-- wrapped statement (total row count over the run — cross-check).
DO $$
DECLARE
  n int := 0;
BEGIN
  WITH claimed AS (
    SELECT id FROM river_job WHERE state='available' AND queue='default'
    ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
  ), deleted AS (
    DELETE FROM river_job USING claimed WHERE river_job.id = claimed.id
    RETURNING river_job.id
  )
  SELECT count(*) INTO n FROM deleted;
  RAISE NOTICE 'ev ts=% n=%', extract(epoch from clock_timestamp())::bigint, n;
END $$;
