# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""``Consumer`` end-to-end: dispatch, unknown-type handling, error -> nack."""

import logging
import threading
import time
from unittest import mock

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


def _retry_count_for_msg(conn, queue: str, msg_id: int) -> int:
    return conn.execute(
        "select count(*) from pgque.retry_queue rq "
        "join pgque.queue q on q.queue_id = rq.ev_queue "
        "where q.queue_name = %s and rq.ev_id = %s",
        (queue, msg_id),
    ).fetchone()[0]


def _dead_letter_count_for_msg(conn, queue: str, msg_id: int) -> int:
    return conn.execute(
        "select count(*) from pgque.dead_letter dl "
        "join pgque.queue q on q.queue_id = dl.dl_queue_id "
        "where q.queue_name = %s and dl.ev_id = %s",
        (queue, msg_id),
    ).fetchone()[0]


def test_consumer_nacks_unhandled_event_type(dsn, conn, setup_queue):
    """Default behavior: unhandled type is nacked.

    The message must land in retry_queue (or dead_letter if max_retries=0),
    and a follow-up receive must not return the same msg_id, proving the
    batch advanced.
    """
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

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    # Must be routed to retry_queue (queue_max_retries is unset by default,
    # so the message is retried rather than dead-lettered).
    rq = _retry_count_for_msg(conn, queue, msg_id)
    dlq = _dead_letter_count_for_msg(conn, queue, msg_id)
    assert rq + dlq >= 1, (
        f"unhandled event was not nacked: retry_queue={rq} dead_letter={dlq}"
    )

    # The batch advanced: a fresh receive must not return the same msg_id.
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    follow_up = client.receive(queue, consumer_name, max_messages=10)
    assert all(m.msg_id != msg_id for m in follow_up), (
        "batch did not advance past unhandled msg_id; got it again on receive"
    )


def test_consumer_acks_unhandled_event_type_when_opt_in(
    dsn, conn, setup_queue, caplog
):
    """Opt-in: unknown_handler='ack' restores warn+ack semantics.

    Must NOT appear in retry_queue, must log a WARNING, and a follow-up
    receive must not return the same msg_id.
    """
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
    rq = _retry_count_for_msg(conn, queue, msg_id)
    assert rq == 0, (
        "unhandled event type was nacked (found in retry_queue); expected ack"
    )

    # A WARNING must have been logged containing the type.
    warning_lines = [
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert any("totally.unregistered.type" in m for m in warning_lines), (
        "expected a WARNING mentioning the unhandled event type"
    )

    # The batch advanced: a fresh receive must not return the same msg_id.
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    follow_up = client.receive(queue, consumer_name, max_messages=10)
    assert all(m.msg_id != msg_id for m in follow_up), (
        "batch did not advance past unhandled msg_id; got it again on receive"
    )


def test_consumer_rejects_invalid_unknown_handler(dsn, setup_queue):
    """Constructor must reject values other than 'nack' / 'ack'."""
    queue, consumer_name = setup_queue
    try:
        pgque.Consumer(
            dsn=dsn,
            queue=queue,
            name=consumer_name,
            unknown_handler="bogus",  # type: ignore[arg-type]
        )
    except ValueError:
        return
    raise AssertionError("expected ValueError for invalid unknown_handler")


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


def test_consumer_does_not_ack_when_unknown_type_nack_fails(
    dsn, conn, setup_queue
):
    """If ``nack()`` raises in the unknown-handler path, the batch must
    NOT be acked. PgQ must redeliver the whole batch on the next poll.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    msg_id = client.send(queue, {"x": 1}, type="totally.unregistered.type")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    real_client_init = pgque.PgqueClient.__init__
    ack_calls: list[int] = []
    nack_calls: list[int] = []

    def fake_init(self, c):
        real_client_init(self, c)
        original_ack = self.ack

        def spy_ack(batch_id):
            ack_calls.append(batch_id)
            return original_ack(batch_id)

        def explode_nack(batch_id, msg, retry_after=60, reason=None):
            nack_calls.append(msg.msg_id)
            raise RuntimeError("simulated nack failure")

        self.ack = spy_ack  # type: ignore[method-assign]
        self.nack = explode_nack  # type: ignore[method-assign]

    with mock.patch.object(pgque.PgqueClient, "__init__", fake_init):
        t = _run_consumer_for(cons, 3.0)
        t.join(timeout=5.0)

    assert nack_calls, "nack was never called for the unhandled message"
    assert ack_calls == [], (
        f"ack must not be called when nack raised; got ack_calls={ack_calls}"
    )

    # Message must still be visible: re-receive returns the same msg_id.
    follow_up = client.receive(queue, consumer_name, max_messages=10)
    assert any(m.msg_id == msg_id for m in follow_up), (
        "batch advanced even though nack failed; data was lost"
    )
    # Cleanup: ack the redelivered batch so the queue tear-down is clean.
    if follow_up:
        client.ack(follow_up[0].batch_id)
        conn.commit()


def test_consumer_does_not_ack_when_handler_error_nack_fails(
    dsn, conn, setup_queue
):
    """If ``nack()`` raises in the handler-error path, the batch must
    NOT be acked.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    msg_id = client.send(queue, {"i": 1}, type="evt.fail")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name,
        poll_interval=1, retry_after=0,
    )

    @cons.on("evt.fail")
    def _boom(m: pgque.Message):
        raise RuntimeError("handler boom")

    real_client_init = pgque.PgqueClient.__init__
    ack_calls: list[int] = []
    nack_calls: list[int] = []

    def fake_init(self, c):
        real_client_init(self, c)
        original_ack = self.ack

        def spy_ack(batch_id):
            ack_calls.append(batch_id)
            return original_ack(batch_id)

        def explode_nack(batch_id, msg, retry_after=60, reason=None):
            nack_calls.append(msg.msg_id)
            raise RuntimeError("simulated nack failure")

        self.ack = spy_ack  # type: ignore[method-assign]
        self.nack = explode_nack  # type: ignore[method-assign]

    with mock.patch.object(pgque.PgqueClient, "__init__", fake_init):
        t = _run_consumer_for(cons, 3.0)
        t.join(timeout=5.0)

    assert nack_calls, "nack was never called after handler raised"
    assert ack_calls == [], (
        f"ack must not be called when nack raised; got ack_calls={ack_calls}"
    )

    follow_up = client.receive(queue, consumer_name, max_messages=10)
    assert any(m.msg_id == msg_id for m in follow_up), (
        "batch advanced even though nack failed; data was lost"
    )
    if follow_up:
        client.ack(follow_up[0].batch_id)
        conn.commit()
