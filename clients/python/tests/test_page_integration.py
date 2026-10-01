# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
from pgque import PgqueClient


def test_page_live_round_trip(conn, setup_queue):
    queue, consumer = setup_queue
    conn.autocommit = True
    client = PgqueClient(conn)
    ids = [client.send(queue, {"i": i}, type="page.live") for i in range(2)]
    max_id = 2**63 - 1
    conn.execute(
        "select pgque.event_retry_raw(%s,%s,now() - interval '1 second',%s,"
        "now(),0,'page.live','{\"i\":\"max\"}',null,null,null,null)",
        (queue, consumer, max_id),
    )
    conn.execute("select pgque.maint_retry_events()")
    client.force_next_tick(queue)
    client.ticker(queue)
    conn.execute(
        "update pgque.tick set tick_event_seq = %s where tick_id = "
        "(select max(tick_id) from pgque.tick where tick_queue = "
        "(select queue_id from pgque.queue where queue_name = %s))",
        (max_id, queue),
    )
    def fail(_):
        raise RuntimeError("boom")
    try:
        client.process_page(queue, consumer, "live-worker", fail, page_size=2)
    except RuntimeError:
        pass
    else:
        raise AssertionError("handler exception not propagated")
    page = client.receive_page(queue, consumer, "live-worker", page_size=2)
    assert client.renew_page(page.page_token, "live-worker") >= page.lease_until
    conn.execute("create temporary table page_effects (msg_id bigint primary key)")
    cursors = []
    def execute_handler(message):
        cursor = conn.execute("insert into page_effects values (%s)",
                              (message.msg_id,))
        cursors.append(cursor)
        return cursor
    result = client.process_page(queue, consumer, "live-worker", execute_handler,
                                 page_size=2)
    assert result.processed_count == 2 and not result.batch_finished
    assert all(not cursor.closed for cursor in cursors)
    effects = conn.execute(
        "select array_agg(msg_id order by msg_id) from page_effects"
    ).fetchone()[0]
    assert effects == ids
    assert client.ack_page(page.page_token, "live-worker") == ("already_acked", False)
    page = client.receive_page(queue, consumer, "live-worker", page_size=2)
    assert page.status == "page" and page.is_last
    assert [m.msg_id for m in page.messages] == [max_id]
    assert client.renew_page(page.page_token, "live-worker") >= page.lease_until
    failures = [{"msg_id": str(max_id), "retry_after_seconds": 0, "reason": "retry"}]
    assert client.ack_page(page.page_token, "live-worker", failures) == ("acked", True)
    assert client.ack_page(page.page_token, "live-worker", failures) == ("already_acked", True)
    idle = client.receive_page(queue, consumer, "live-worker")
    assert idle.status in ("idle", "advanced") and idle.messages == []
    assert idle.page_token is None
    row = conn.execute("select ev_id, ev_data from pgque.retry_queue where ev_queue = "
                       "(select queue_id from pgque.queue where queue_name = %s)",
                       (queue,)).fetchone()
    assert row[0] == max_id
