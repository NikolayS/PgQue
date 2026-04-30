# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""``Consumer`` end-to-end: dispatch, nack-by-default on missing handler,
opt-in ack-on-unknown, error -> nack."""

import threading
import time

import pgque


def _run_consumer_for(consumer: pgque.Consumer, seconds: float) -> threading.Thread:
    """Start a consumer in a background thread, stop it after `seconds`."""
    t = threading.Thread(target=consumer.start, daemon=True)
    t.start()

    def _stopper():
        time.sleep(seconds)
        consumer.stop()

    threading.Thread(target=_stopper, daemon=True).start()
    return t


def test_consumer_dispatches_by_event_type(dsn, conn, setup_queue):
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"i": 1}, type="evt.a")
    client.send(queue, {"i": 2}, type="evt.b")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    seen_a: list = []
    seen_b: list = []
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    @cons.on("evt.a")
    def _a(m: pgque.Message):
        seen_a.append(m.payload)

    @cons.on("evt.b")
    def _b(m: pgque.Message):
        seen_b.append(m.payload)

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    assert len(seen_a) == 1
    assert len(seen_b) == 1


def test_consumer_default_handler_catches_unknown(dsn, conn, setup_queue):
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"x": 99}, type="never.registered.type")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    fallback: list = []
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    @cons.on("*")
    def _default(m: pgque.Message):
        fallback.append(m)

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    assert len(fallback) == 1
    assert fallback[0].type == "never.registered.type"


def test_consumer_nacks_on_handler_error(dsn, conn, setup_queue):
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"i": 1}, type="evt.fail")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    calls = {"n": 0}
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name,
        poll_interval=1, retry_after=0,
    )

    @cons.on("evt.fail")
    def _boom(m: pgque.Message):
        calls["n"] += 1
        raise RuntimeError("simulated failure")

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    # The handler ran at least once, and the failing message landed in
    # the retry queue (not silently dropped).
    assert calls["n"] >= 1
    cnt = conn.execute(
        "select count(*) from pgque.retry_queue rq "
        "join pgque.queue q on q.queue_id = rq.ev_queue "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert cnt >= 1


def test_consumer_nacks_unhandled_event_type(dsn, conn, setup_queue):
    """Default policy: unhandled event types are nacked (data-safe).

    The message must land in the retry queue (or, at the queue's retry
    limit, the dead-letter queue) and a subsequent ``receive`` must NOT
    return the same msg_id immediately -- proving the consumer advanced
    past the bad batch instead of looping on it.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    msg_id = client.send(queue, {"x": 1}, type="totally.unregistered.type")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    # Consumer with NO handler for the type and NO default handler.
    # Default unknown_handler="nack" applies.
    cons = pgque.Consumer(
        dsn=dsn,
        queue=queue,
        name=consumer_name,
        poll_interval=1,
        retry_after=0,
    )

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    # The message must have been routed to retry_queue OR dead_letter
    # (the latter only if the queue's retry limit is 0).
    retry_cnt = conn.execute(
        "select count(*) from pgque.retry_queue rq "
        "join pgque.queue q on q.queue_id = rq.ev_queue "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    dlq_cnt = conn.execute(
        "select count(*) from pgque.dead_letter dl "
        "join pgque.queue q on q.queue_id = dl.dl_queue_id "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert (retry_cnt + dlq_cnt) >= 1, (
        "unhandled event type with default policy must be nacked "
        "(routed to retry_queue or dead_letter)"
    )

    # A subsequent receive must not immediately return the same message
    # -- the consumer must have advanced past the batch.
    next_msgs = client.receive(queue, consumer_name, max_messages=10)
    assert not any(m.msg_id == msg_id for m in next_msgs), (
        "consumer did not advance past the nacked batch"
    )


def test_consumer_acks_unhandled_event_type_when_opt_in(dsn, conn, setup_queue, caplog):
    """Opt-in ``unknown_handler="ack"``: unhandled types are warned + acked.

    Must NOT appear in retry_queue, and a subsequent ``receive`` must
    not return the same msg_id (proves the batch advanced).
    """
    import logging

    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    msg_id = client.send(queue, {"x": 1}, type="totally.unregistered.type")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    cons = pgque.Consumer(
        dsn=dsn,
        queue=queue,
        name=consumer_name,
        poll_interval=1,
        unknown_handler="ack",
    )

    with caplog.at_level(logging.WARNING, logger="pgque"):
        t = _run_consumer_for(cons, 3.0)
        t.join(timeout=5.0)

    # Must NOT be in retry_queue -- it was acked, not nacked.
    retry_cnt = conn.execute(
        "select count(*) from pgque.retry_queue rq "
        "join pgque.queue q on q.queue_id = rq.ev_queue "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert retry_cnt == 0, (
        "with unknown_handler='ack', unhandled types must not appear in retry_queue"
    )

    # A subsequent receive must not return the same message: it was acked.
    next_msgs = client.receive(queue, consumer_name, max_messages=10)
    assert not any(m.msg_id == msg_id for m in next_msgs), (
        "consumer did not advance past the acked batch"
    )

    # A WARNING must have been logged.
    warning_lines = [
        r.message for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert any("totally.unregistered.type" in m for m in warning_lines), (
        "expected a WARNING mentioning the unhandled event type"
    )


def test_consumer_stop_returns_promptly(dsn, setup_queue):
    queue, consumer_name = setup_queue
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=10
    )
    t = threading.Thread(target=cons.start, daemon=True)
    t.start()
    time.sleep(0.5)  # let it enter the loop
    cons.stop()
    t.join(timeout=15)
    assert not t.is_alive(), "consumer did not stop after stop()"
