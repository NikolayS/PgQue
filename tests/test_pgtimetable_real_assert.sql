\set ON_ERROR_STOP on

-- Real pg_timetable integration assertion.
-- Assumes test_pgtimetable_real_setup.sql already scheduled PgQue jobs and a
-- real pg_timetable worker is now running.

do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  i int;
begin
  for i in 1..60 loop
    for v_msg in select * from pgque.receive('real_pgtimetable', 'c1', 10) loop
      v_count := v_count + 1;
      assert v_msg.type = 'test.type', 'expected test.type, got ' || coalesce(v_msg.type, 'NULL');
      assert v_msg.payload = '{"real_pg_timetable":true}',
        'unexpected payload: ' || coalesce(v_msg.payload, 'NULL');
      perform pgque.ack(v_msg.batch_id);
    end loop;

    exit when v_count >= 1;
    perform pg_sleep(0.2);
  end loop;

  assert v_count = 1, 'real pg_timetable worker did not deliver event via ticker_loop()';
  raise notice 'PASS: real pg_timetable worker delivered event via ticker_loop()';
end $$;

select pgque.stop();
