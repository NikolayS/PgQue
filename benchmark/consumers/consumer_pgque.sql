-- R6 instrumented consumer for pgque (batch ticker model).
-- Preserves original behavior; adds one NOTICE per call for honest-events/s.
-- Format: NOTICE: ev ts=<epoch_s> n=<events>
DO $$
DECLARE
  b bigint;
  n int := 0;
BEGIN
  PERFORM pgque.ticker('bench_queue');
  SELECT pgque.next_batch('bench_queue', 'bench_consumer') INTO b;
  IF b IS NOT NULL THEN
    SELECT count(*) INTO n FROM pgque.get_batch_events(b);
    PERFORM pgque.finish_batch(b);
  END IF;
  RAISE NOTICE 'ev ts=% n=%', extract(epoch from clock_timestamp())::bigint, n;
END $$;
