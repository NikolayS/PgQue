# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""``Consumer`` end-to-end: dispatch, warn+ack on missing handler, error -> nack."""

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


def test_consumer_warns_and_acks_unhandled_event_type(dsn, conn, setup_queue, caplog):
    """Unhandled event type must emit a WARNING and be acked, not nacked.

    The message must not appear in retry_queue; the WARNING log line must
    contain the event type and message id.
    """
    import logging

    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    msg_id = client.send(queue, {"x": 1}, type="totally.unregistered.type")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    # Consumer with NO handler for "totally.unregistered.type"
    # and NO default handler either.
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    with caplog.at_level(logging.WARNING, logger="pgque"):
        t = _run_consumer_for(cons, 3.0)
        t.join(timeout=5.0)

    # Must NOT be in retry_queue -- it was acked, not nacked.
    cnt = conn.execute(
        "select count(*) from pgque.retry_queue rq "
        "join pgque.queue q on q.queue_id = rq.ev_queue "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert cnt == 0, (
        "unhandled event type was nacked (found in retry_queue); expected ack"
    )

    # A WARNING must have been logged containing the type and event id.
    warning_lines = [
        r.message for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert any("totally.unregistered.type" in m for m in warning_lines), (
        "expected a WARNING mentioning the unhandled event type"
    )
    assert any(str(msg_id) in m for m in warning_lines), (
        "expected a WARNING mentioning the event id"
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
