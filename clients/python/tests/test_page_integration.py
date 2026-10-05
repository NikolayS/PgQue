# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
from concurrent.futures import Future
from functools import partial

import pytest

from pgque import PgqueClient
from pgque.errors import PgqueError


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
    for factory in (map, filter):
        def lazy_handler(message):
            return factory(lambda _: conn.execute(
                "insert into page_effects values (%s)", (message.msg_id,)), [1])
        with pytest.raises(TypeError, match="lazy"):
            client.process_page(queue, consumer, "live-worker", lazy_handler, page_size=2)
        replay = client.receive_page(queue, consumer, "live-worker", page_size=2)
        assert replay.page_token == page.page_token
        assert conn.execute("select count(*) from page_effects").fetchone()[0] == 0
    def deferred_handler(message):
        return partial(conn.execute, "insert into page_effects values (%s)",
                       (message.msg_id,))
    with pytest.raises(TypeError, match="lazy"):
        client.process_page(queue, consumer, "live-worker", deferred_handler, page_size=2)
    assert client.receive_page(queue, consumer, "live-worker", page_size=2).page_token == page.page_token
    assert conn.execute("select count(*) from page_effects").fetchone()[0] == 0
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


def test_page_mode_adapters(conn, setup_queue):
    queue, consumer = setup_queue
    conn.autocommit = True
    client = PgqueClient(conn)
    coop, member, coop_worker = consumer + "_group", "member-one", "coop-process"
    partitioned, partition_worker = consumer + "_partitioned", "partition-process"
    conn.execute("select pgque.register_subconsumer(%s,%s,%s)",
                 (queue, coop, member))
    conn.execute("select pgque.subscribe_partitioned(%s,%s,3)", (queue, partitioned))
    epoch = conn.execute(
        "select pgque.claim_slot(%s,%s,2,%s,interval '3 minutes')",
        (queue, partitioned, partition_worker),
    ).fetchone()[0]
    assert epoch is not None
    keys = {
        slot: conn.execute(
            "select 'adapter-key-' || i from generate_series(1,1000) as i "
            "where (hashtextextended('adapter-key-' || i,0) %% 3 + 3) %% 3 = %s "
            "order by i limit 1", (slot,),
        ).fetchone()[0]
        for slot in (0, 2)
    }
    payloads = ["target-first", "other-slot", "target-last"]
    event_keys = [keys[2], keys[0], keys[2]]
    ids = [
        conn.execute("select pgque.send(%s,'adapter.mode',%s::text,%s::text)",
                     (queue, payload, key)).fetchone()[0]
        for payload, key in zip(payloads, event_keys)
    ]
    client.force_next_tick(queue)
    client.ticker(queue)

    def check_page(page, indexes):
        assert page.status == "page" and page.page_token
        assert page.batch_id is not None and page.page_number == 1
        assert page.is_last and page.lease_until is not None
        assert [m.msg_id for m in page.messages] == [ids[i] for i in indexes]
        assert [m.payload for m in page.messages] == [payloads[i] for i in indexes]
        assert [m.extra1 for m in page.messages] == [event_keys[i] for i in indexes]
        assert all(m.batch_id == page.batch_id and m.type == "adapter.mode"
                   for m in page.messages)

    def check_ack(page, worker):
        with pytest.raises(PgqueError, match="wrong worker"):
            client.ack_page(page.page_token, "not-the-owner")
        assert client.ack_page(page.page_token, worker) == ("acked", True)
        assert client.ack_page(page.page_token, worker) == ("already_acked", True)

    page = client.receive_page_coop(
        queue, coop, member, coop_worker, page_size=3,
        dead_interval=None, lease="3 minutes",
    )
    check_page(page, [0, 1, 2])
    assert page.fence_epoch is None
    check_ack(page, coop_worker)
    empty = client.receive_page_coop(
        queue, coop, member, coop_worker, page_size=3,
        dead_interval="5 minutes", lease="3 minutes",
    )
    assert empty.status in ("idle", "advanced") and empty.messages == []
    assert empty.page_token is None

    page = client.receive_page_partitioned(
        queue, partitioned, 2, 3, partition_worker, page_size=2,
    )
    check_page(page, [0, 2])
    assert page.fence_epoch == epoch
    check_ack(page, partition_worker)
    empty = client.receive_page_partitioned(
        queue, partitioned, 2, 3, partition_worker, page_size=2,
    )
    assert empty.status in ("idle", "advanced") and empty.messages == []
    assert empty.page_token is None


@pytest.mark.parametrize("state", ["pending", "failed", "succeeded"])
def test_future_result_keeps_live_page_outstanding(conn, setup_queue, state):
    queue, consumer = setup_queue
    conn.autocommit = True
    client = PgqueClient(conn)
    worker = "future-worker"
    msg_id = client.send(queue, {"state": state}, type="page.future")
    client.force_next_tick(queue)
    client.ticker(queue)
    page = client.receive_page(queue, consumer, worker)
    assert page.status == "page"
    assert [message.msg_id for message in page.messages] == [msg_id]
    future = Future()
    if state == "failed":
        future.set_exception(RuntimeError("background handler failed"))
    elif state == "succeeded":
        future.set_result("completed")

    with pytest.raises(TypeError, match="lazy"):
        client.process_page(queue, consumer, worker, lambda _: future)
    replay = client.receive_page(queue, consumer, worker)
    assert replay.page_token == page.page_token
    assert [message.msg_id for message in replay.messages] == [msg_id]
    seen = []
    result = client.process_page(queue, consumer, worker,
                                 lambda message: seen.append(message.msg_id))
    assert seen == [msg_id]
    assert result.processed_count == 1 and result.batch_finished
    assert client.ack_page(page.page_token, worker) == ("already_acked", True)
