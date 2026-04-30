-- Instrumented consumer for pgmq-partitioned (identical SQL to pgmq;
-- the partitioning is on the schema side only).
DO $$
DECLARE
  msg_ids bigint[];
  n int := 0;
BEGIN
  SELECT array_agg(msg_id) FROM pgmq.read('bench_queue', 30, 50) INTO msg_ids;
  IF msg_ids IS NOT NULL THEN
    n := array_length(msg_ids, 1);
    PERFORM pgmq.delete('bench_queue', msg_ids);
  END IF;
  RAISE NOTICE 'ev ts=% n=%', extract(epoch from clock_timestamp())::bigint, n;
END $$;
