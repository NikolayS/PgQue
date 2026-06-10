# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Consumer poll-loop resilience.

Covers two contracts of ``Consumer.start()``:

  1. Backlog draining: after a non-empty batch, the consumer must
     re-poll immediately instead of waiting up to ``poll_interval``
     for a NOTIFY. Notifies fire only on new ticks, and notifies for
     already-accumulated batches were emitted while the consumer was
     not listening -- waiting on them drains a backlog at one batch
     per ``poll_interval``.
  2. Error survival: a transient database error (failover, restart,
     network blip) must not kill the loop. ``start()`` documents
     "blocks until SIGTERM/SIGINT"; it must log, wait, reconnect,
     and resume.
"""

import json
import threading
import time

from psycopg import conninfo

import pgque


def _as_dicts(payloads: list) -> list:
    """Normalize received payloads (jsonb decodes to dict, text stays str)."""
    return [p if isinstance(p, dict) else json.loads(p) for p in payloads]


def _start_in_thread(consumer: pgque.Consumer) -> threading.Thread:
    t = threading.Thread(target=consumer.start, daemon=True)
    t.start()
    return t


def _send_and_tick(conn, client, queue: str, payload, type: str) -> int:
    """Send one event and make it visible in its own batch."""
    msg_id = client.send(queue, payload, type=type)
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    return msg_id


def test_consumer_drains_backlog_within_one_poll_interval(
    dsn, conn, setup_queue
):
    """With several batches pre-accumulated, all of them must be
    consumed well within one ``poll_interval``.

    Each send+tick cycle below produces a separate PgQ batch, so the
    consumer needs one receive per message. A consumer that waits for
    a NOTIFY after every batch would drain this backlog at one batch
    per ``poll_interval`` (30s here) and only deliver the first
    message before the assertion deadline.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)

    n_batches = 3
    for i in range(n_batches):
        _send_and_tick(conn, client, queue, {"i": i}, "evt.backlog")

    seen: list = []
    all_seen = threading.Event()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=30
    )

    @cons.on("evt.backlog")
    def _h(m: pgque.Message):
        seen.append(m.payload)
        if len(seen) >= n_batches:
            all_seen.set()

    t0 = time.monotonic()
    t = _start_in_thread(cons)
    try:
        drained = all_seen.wait(timeout=10.0)
        elapsed = time.monotonic() - t0
    finally:
        cons.stop()
        t.join(timeout=5.0)

    assert drained, (
        f"backlog not drained: {len(seen)}/{n_batches} messages in "
        f"{elapsed:.1f}s -- consumer waited for NOTIFY between batches"
    )
    assert elapsed < 10.0


def test_consumer_survives_transient_receive_error(dsn, conn, setup_queue):
    """A single failing receive must not kill ``start()``.

    The first receive raises a (simulated) transient database error;
    the consumer must log it, wait, reconnect, and process the message
    on a later poll.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    _send_and_tick(conn, client, queue, {"x": 1}, "evt.recover")

    real_receive = pgque.PgqueClient.receive
    calls = {"n": 0}

    def flaky_receive(self, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise pgque.PgqueError("simulated transient failure")
        return real_receive(self, *args, **kwargs)

    pgque.PgqueClient.receive = flaky_receive  # type: ignore[method-assign]

    seen: list = []
    got_msg = threading.Event()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    @cons.on("evt.recover")
    def _h(m: pgque.Message):
        seen.append(m.payload)
        got_msg.set()

    t = _start_in_thread(cons)
    try:
        recovered = got_msg.wait(timeout=10.0)
        assert t.is_alive(), (
            "consumer thread died after a transient receive error"
        )
    finally:
        pgque.PgqueClient.receive = real_receive  # type: ignore[method-assign]
        cons.stop()
        t.join(timeout=5.0)

    assert calls["n"] >= 2, "receive was not retried after the error"
    assert recovered, (
        "message was not processed after the transient error; "
        "consumer did not recover"
    )
    assert _as_dicts(seen) == [{"x": 1}]


def test_consumer_survives_killed_backend(dsn, conn, setup_queue):
    """Terminating the consumer's server backend (what the consumer
    sees during a DB restart or failover) must not kill ``start()``.
    The consumer must reconnect, re-LISTEN, and resume consuming.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)

    app_name = f"pgque_kill_{consumer_name[-12:]}"
    cons_dsn = conninfo.make_conninfo(dsn, application_name=app_name)

    seen: list = []
    got_msg = threading.Event()

    cons = pgque.Consumer(
        dsn=cons_dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    @cons.on("evt.restart")
    def _h(m: pgque.Message):
        seen.append(m.payload)
        got_msg.set()

    t = _start_in_thread(cons)
    try:
        # Let the consumer connect and enter the LISTEN wait.
        time.sleep(1.0)

        killed = conn.execute(
            "select count(pg_terminate_backend(pid)) "
            "from pg_stat_activity "
            "where application_name = %s and pid <> pg_backend_pid()",
            (app_name,),
        ).fetchone()[0]
        conn.commit()
        assert killed >= 1, "did not find the consumer backend to kill"

        # Give the consumer time to notice the dead connection and
        # reconnect (it waits poll_interval=1s between attempts).
        time.sleep(2.5)
        assert t.is_alive(), (
            "consumer thread died after its backend was terminated"
        )

        # Produce after the kill: only a reconnected consumer sees this.
        _send_and_tick(conn, client, queue, {"r": 1}, "evt.restart")
        conn.execute(f"notify pgque_{queue}, 'go'")
        conn.commit()

        recovered = got_msg.wait(timeout=10.0)
    finally:
        cons.stop()
        t.join(timeout=5.0)

    assert recovered, (
        "message was not processed after backend termination; "
        "consumer did not reconnect"
    )
    assert _as_dicts(seen) == [{"r": 1}]


def test_consumer_stop_is_prompt_during_error_retry_wait(dsn, setup_queue):
    """While waiting to retry after a database error, the consumer
    must (a) still be running and (b) honor ``stop()`` promptly.
    """
    queue, consumer_name = setup_queue

    real_receive = pgque.PgqueClient.receive

    def always_failing_receive(self, *args, **kwargs):
        raise pgque.PgqueError("simulated persistent failure")

    pgque.PgqueClient.receive = always_failing_receive  # type: ignore[method-assign]

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=30
    )
    t = _start_in_thread(cons)
    try:
        time.sleep(1.5)
        # The loop must survive the persistent error (it is inside the
        # retry wait at this point, not dead).
        assert t.is_alive(), (
            "consumer thread died on a database error instead of retrying"
        )

        t0 = time.monotonic()
        cons.stop()
        t.join(timeout=5.0)
        elapsed = time.monotonic() - t0
    finally:
        pgque.PgqueClient.receive = real_receive  # type: ignore[method-assign]
        cons.stop()
        t.join(timeout=5.0)

    assert not t.is_alive(), "consumer did not stop during error retry wait"
    assert elapsed < 2.0, (
        f"stop() took {elapsed:.2f}s during error retry wait; expected <2s"
    )
