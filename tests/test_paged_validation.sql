-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

/* Reject unknown ownership before processing an attacker-controlled payload. */
do $$
begin
    begin
        perform pgque.ack_page(gen_random_uuid(), 'unknown', '{"not":"an array"}');
        assert false, 'forged token must fail';
    exception when sqlstate 'PQP01' then null;
    end;
end $$;

select pgque.create_queue('page_validation');
select pgque.subscribe('page_validation', 'c');
select pgque.send('page_validation', 'x', 'first');
select pgque.send('page_validation', 'x', 'second');
select pgque.force_next_tick('page_validation');
select pgque.ticker('page_validation');
do $$
declare
    p pgque.batch_page;
    v_bad jsonb;
    v_repeat pgque.batch_page;
    v_request jsonb;
    v_ack record;
begin
    p := pgque.receive_page('page_validation', 'c', 'worker', 1);
    foreach v_bad in array array[
        null::jsonb, '{}'::jsonb, '[null]'::jsonb,
        '[{"msg_id":1}]'::jsonb, '[{"msg_id":"99999999"}]'::jsonb,
        '[{"msg_id":"1","unknown":true}]'::jsonb,
        '[{"msg_id":"1","retry_after_seconds":-1}]'::jsonb,
        '[{"msg_id":"1","retry_after_seconds":2147483648}]'::jsonb,
        '[{"msg_id":"1","retry_after_seconds":null}]'::jsonb,
        '[{"msg_id":"1","reason":false}]'::jsonb,
        '[{"msg_id":"9223372036854775808"}]'::jsonb,
        '[{"msg_id":"1"},{"msg_id":"2"}]'::jsonb
    ] loop
        begin
            perform pgque.ack_page(p.page_token, 'worker', v_bad);
            assert false, 'invalid failure descriptor must fail';
        exception when sqlstate '22023' then null;
        end;
        v_repeat := pgque.receive_page('page_validation', 'c', 'worker', 99);
        assert v_repeat.page_token = p.page_token and v_repeat.messages = p.messages;
    end loop;
    v_request := jsonb_build_array(jsonb_build_object('msg_id', (p.messages[1]).msg_id::text));
    select * into v_ack from pgque.ack_page(p.page_token, 'worker', v_request);
    assert v_ack.status = 'acked';
    begin
        perform pgque.ack_page(p.page_token, 'wrong', '{}'::jsonb);
        assert false, 'wrong receipt owner must fail before parsing failures';
    exception when sqlstate 'PQP01' then null;
    end;
    v_request := jsonb_build_array(jsonb_build_object('msg_id', (p.messages[1]).msg_id::text,
        'retry_after_seconds', 60, 'reason', null));
    select * into v_ack from pgque.ack_page(p.page_token, 'worker', v_request);
    assert v_ack.status = 'already_acked', 'equivalent normalized descriptors must replay';
    p := pgque.receive_page('page_validation', 'c', 'worker', 1);
    perform pgque.ack_page(p.page_token, 'worker');
end $$;
select pgque.drop_queue('page_validation', true);
\echo 'PASS: test_paged_validation'
