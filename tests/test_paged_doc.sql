-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Executable SQL example from docs/paged-batches.md; checks committed progress.
\set ON_ERROR_STOP on
select pgque.create_queue('page_demo');
select pgque.subscribe('page_demo', 'jobs');
select pgque.send('page_demo', 'job', 'first'::text);
select pgque.send('page_demo', 'job', 'second'::text);
select pgque.force_next_tick('page_demo');
select pgque.ticker('page_demo');
do $$
declare
    p pgque.batch_page;
    m pgque.message;
begin
    p := pgque.receive_page('page_demo', 'jobs', 'example-instance-1', 1);
    if p.status = 'page' then
        foreach m in array p.messages loop
            -- Replace this with the application operation.
            raise notice 'processing %: %', m.msg_id, m.payload;
        end loop;
        perform pgque.ack_page(p.page_token, 'example-instance-1');
    end if;
end $$;

do $$
declare
    p pgque.batch_page;
    a record;
begin
    p := pgque.receive_page('page_demo', 'jobs', 'example-instance-1', 1);
    assert p.status = 'page' and p.page_number = 2 and p.is_last;
    assert cardinality(p.messages) = 1 and (p.messages[1]).payload = 'second';
    select * into a from pgque.ack_page(p.page_token, 'example-instance-1');
    assert a.status = 'acked' and a.batch_finished;
end $$;
select pgque.unsubscribe('page_demo', 'jobs');
select pgque.drop_queue('page_demo');
\echo 'PASS: test_paged_doc'
