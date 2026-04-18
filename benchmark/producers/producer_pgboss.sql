INSERT INTO pgboss.job(name,data,priority)
VALUES('bench_queue', json_build_object('data', repeat('x', 200))::jsonb, 0);
