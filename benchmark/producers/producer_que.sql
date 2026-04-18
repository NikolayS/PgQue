INSERT INTO public.que_jobs(job_class, args, priority, queue, run_at, job_schema_version)
VALUES('Bench',
       jsonb_build_array(jsonb_build_object('data', repeat('x', 200))),
       100, 'default', now(), 2);
