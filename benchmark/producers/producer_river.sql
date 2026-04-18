INSERT INTO river_job(kind,args,max_attempts,queue,state)
VALUES('bench', json_build_object('data', repeat('x', 200))::jsonb, 25, 'default', 'available');
