\set ON_ERROR_STOP on

-- Real pg_timetable integration setup.
-- Assumes the real pg_timetable schema has already been initialized, but the
-- worker is not running yet. Starting the worker after scheduling makes the
-- test deterministic because pg_timetable loads interval chains at startup.

select pgque.start_timetable(10);

do $$
begin
  perform pgque.create_queue('real_pgtimetable');
  perform pgque.register_consumer('real_pgtimetable', 'c1');
end $$;

do $$
begin
  perform pgque.insert_event('real_pgtimetable', 'test.type', '{"real_pg_timetable":true}');
end $$;
