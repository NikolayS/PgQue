# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""``Consumer`` end-to-end: dispatch, unknown-type handling, error -> nack."""

import logging
import threading
import time
from unittest import mock
from unittest.mock import MagicMock

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


def test_consumer_default_max_messages_requests_whole_batch(dsn):
    cons = pgque.Consumer(dsn=dsn, queue="q", name="c")
    assert cons.max_messages == 2_147_483_647


def test_consumer_configured_max_messages_is_preserved(dsn):
    cons = pgque.Consumer(dsn=dsn, queue="q", name="c", max_messages=123)
    assert cons.max_messages == 123


def test_consumer_poll_once_passes_default_max_messages(dsn):
    cons = pgque.Consumer(dsn=dsn, queue="q", name="c")
    fake_conn = MagicMock()

    with mock.patch.object(pgque.PgqueClient, "receive", return_value=[]) as receive:
        cons._poll_once(fake_conn)

    receive.assert_called_once_with("q", "c", 2_147_483_647)


def test_consumer_poll_once_passes_configured_max_messages(dsn):
    cons = pgque.Consumer(dsn=dsn, queue="q", name="c", max_messages=123)
    fake_conn = MagicMock()

    with mock.patch.object(pgque.PgqueClient, "receive", return_value=[]) as receive:
        cons._poll_once(fake_conn)

    receive.assert_called_once_with("q", "c", 123)


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

    # Must NOT be in retry_queue or dead_letter -- it was acked, not nacked.
    rq = _retry_count_for_msg(conn, queue, msg_id)
    dlq = _dead_letter_count_for_msg(conn, queue, msg_id)
    assert rq == 0, (
        "unhandled event type was nacked (found in retry_queue); expected ack"
    )
    assert dlq == 0, (
        "unhandled event type was dead-lettered; expected ack"
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
        original_nack = self.nack

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


def test_consumer_warns_on_stale_ack(dsn, conn, setup_queue, caplog):
    """If ``client.ack(batch_id)`` returns 0 (stale/double ack), the
    Consumer must log a WARNING -- matching the TS+Go consumers, which
    log a warn on ``n == 0`` from the new ack-rowcount surface.

    Simulated by monkey-patching the per-poll ``PgqueClient`` so its
    ``ack`` returns 0 instead of delegating to the SQL function. The
    test does not exercise the real stale path (which requires a race
    between two consumers); it locks the warn-log behavior.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"x": 1}, type="order.created")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    @cons.on("order.created")
    def _ok(msg):
        return None

    real_client_init = pgque.PgqueClient.__init__
    ack_returns: list[int] = []

    def fake_init(self, c):
        real_client_init(self, c)

        def fake_ack(batch_id):
            ack_returns.append(0)
            return 0

        self.ack = fake_ack  # type: ignore[method-assign]

    with mock.patch.object(pgque.PgqueClient, "__init__", fake_init):
        with caplog.at_level(logging.WARNING, logger="pgque"):
            t = _run_consumer_for(cons, 3.0)
            t.join(timeout=5.0)

    assert ack_returns, "ack was never called by the consumer"

    warning_lines = [
        r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
    ]
    assert any("stale" in m.lower() or "double" in m.lower() for m in warning_lines), (
        f"expected a WARNING mentioning stale or double ack; got {warning_lines}"
    )


def test_consumer_stop_returns_within_2s_while_waiting(dsn, setup_queue):
    """``stop()`` must unblock the LISTEN/NOTIFY wait promptly, even when
    no notifications arrive and ``poll_interval`` is large.
    """
    queue, consumer_name = setup_queue
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=30
    )
    t = threading.Thread(target=cons.start, daemon=True)
    t.start()
    # Let the consumer enter the LISTEN wait (after the first empty poll).
    time.sleep(1.0)

    t0 = time.monotonic()
    cons.stop()
    t.join(timeout=5.0)
    elapsed = time.monotonic() - t0

    assert not t.is_alive(), "consumer thread did not stop"
    assert elapsed < 2.0, (
        f"stop() took {elapsed:.2f}s; expected prompt (<2s) shutdown"
    )


def test_consumer_wakes_on_pg_notify_before_poll_interval(
    dsn, conn, setup_queue
):
    """A real ``pg_notify('pgque_<queue>', ...)`` from a producer thread
    must wake the consumer well before ``poll_interval`` elapses.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)

    received: list[pgque.Message] = []
    received_evt = threading.Event()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=30
    )

    @cons.on("evt.wake")
    def _on(m: pgque.Message):
        received.append(m)
        received_evt.set()

    t = threading.Thread(target=cons.start, daemon=True)
    t.start()
    # Allow the consumer to drain its first (empty) poll and enter the wait.
    time.sleep(1.5)

    # Send + tick + NOTIFY from the test thread.
    t_send = time.monotonic()
    client.send(queue, {"i": 1}, type="evt.wake")
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    conn.execute(f"notify pgque_{queue}, 'go'")
    conn.commit()

    woke = received_evt.wait(timeout=5.0)
    elapsed = time.monotonic() - t_send

    cons.stop()
    t.join(timeout=5.0)

    assert woke, "consumer did not wake on pg_notify within 5s"
    assert len(received) == 1
    # poll_interval=30s, so anything well under that proves the wait woke
    # on the notification rather than the polling timer.
    assert elapsed < 5.0, (
        f"consumer woke too slowly ({elapsed:.2f}s); LISTEN/NOTIFY did not "
        f"unblock the wait"
    )
