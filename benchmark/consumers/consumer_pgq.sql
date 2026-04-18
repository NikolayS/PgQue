-- R6 instrumented consumer for pgq (identical to pgque; schema swap only).
DO $$
DECLARE
  b bigint;
  n int := 0;
BEGIN
  PERFORM pgq.ticker('bench_queue');
  SELECT pgq.next_batch('bench_queue', 'bench_consumer') INTO b;
  IF b IS NOT NULL THEN
    SELECT count(*) INTO n FROM pgq.get_batch_events(b);
    PERFORM pgq.finish_batch(b);
  END IF;
  RAISE NOTICE 'ev ts=% n=%', extract(epoch from clock_timestamp())::bigint, n;
END $$;
