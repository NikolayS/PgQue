-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

select pgque.create_queue('page_destroy_pending');
select pgque.subscribe('page_destroy_pending', 'c');
select pgque.send('page_destroy_pending', 'x', 'first');
select pgque.send('page_destroy_pending', 'x', 'second');
select pgque.force_next_tick('page_destroy_pending');
select pgque.ticker('page_destroy_pending');
select page_token from pgque.receive_page('page_destroy_pending', 'c', 'worker', 1);

/* Administrative force must not pretend the unprocessed page was acked. */
set role pgque_admin;
select pgque.drop_queue('page_destroy_pending', true);
reset role;
do $$
begin
    assert not exists (select 1 from pgque.queue where queue_name = 'page_destroy_pending');
    assert not exists (
        select 1 from pgque.page_state as p
        left join pgque.subscription as s
            on s.sub_queue = p.queue_id and s.sub_consumer = p.consumer_id
        where s.sub_queue is null
    ), 'queue destruction must cascade page state';
end $$;

/* The between-page guard is equally removable by administrative force. */
select pgque.create_queue('page_destroy_between');
select pgque.subscribe('page_destroy_between', 'c');
select pgque.send('page_destroy_between', 'x', 'first');
select pgque.send('page_destroy_between', 'x', 'second');
select pgque.force_next_tick('page_destroy_between');
select pgque.ticker('page_destroy_between');
do $$
declare p pgque.batch_page;
begin
    p := pgque.receive_page('page_destroy_between', 'c', 'worker', 1);
    perform pgque.ack_page(p.page_token, 'worker');
    begin
        perform pgque.unsubscribe('page_destroy_between', 'c');
        assert false, 'application unsubscribe must remain guarded';
    exception when sqlstate '55000' then null;
    end;
end $$;
set role pgque_admin;
select pgque.drop_queue('page_destroy_between', true);
reset role;

/* Force also removes partition lease state and cooperative member pages. */
select pgque.create_queue('page_destroy_part');
select pgque.subscribe_slot('page_destroy_part', 'c', 0, 1);
select pgque.claim_slot('page_destroy_part', 'c', 0, 'worker');
select pgque.send('page_destroy_part', 'x', 'first');
select pgque.force_next_tick('page_destroy_part');
select pgque.ticker('page_destroy_part');
select page_token from pgque.receive_page_partitioned('page_destroy_part', 'c', 0, 1, 'worker', 1);
set role pgque_admin;
select pgque.drop_queue('page_destroy_part', true);
reset role;
select pgque.create_queue('page_destroy_coop');
select pgque.register_subconsumer('page_destroy_coop', 'c', 'm');
select pgque.send('page_destroy_coop', 'x', 'first');
select pgque.force_next_tick('page_destroy_coop');
select pgque.ticker('page_destroy_coop');
select page_token from pgque.receive_page_coop('page_destroy_coop', 'c', 'm', 'worker', 1);
set role pgque_admin;
select pgque.drop_queue('page_destroy_coop', true);
reset role;
\echo 'PASS: test_paged_destroy'

do $$
begin
    assert not has_function_privilege('pgque_reader',
        'pgque._next_batch_custom(text,text,interval,integer,interval,boolean)', 'execute');
    assert not has_function_privilege('pgque_writer',
        'pgque._next_batch_custom(text,text,interval,integer,interval,boolean)', 'execute');
    assert not has_function_privilege('pgque_admin',
        'pgque._next_batch_custom(text,text,interval,integer,interval,boolean)', 'execute');
end $$;
