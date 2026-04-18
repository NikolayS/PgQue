SELECT pgmq.send('bench_queue', json_build_object('data', repeat('x', 200))::jsonb);
