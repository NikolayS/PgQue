# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Regression tests for issue #158.

The Consumer LISTEN/NOTIFY wait must:
  1. honor stop() promptly (within ~2s) even when poll_interval is large.
  2. wake up when a NOTIFY arrives, well before poll_interval expires.
"""

import threading
import time

import psycopg
import pytest

import pgque


def test_stop_is_honored_within_two_seconds(dsn, setup_queue):
    """`stop()` must interrupt the LISTEN/NOTIFY wait promptly.

    Regression for #158: the prior `for _notify in conn.notifies(timeout=...)`
    pattern blocked uninterruptibly until the full poll_interval elapsed,
    so a 10s poll_interval forced a 10s shutdown. The bounded-slice fix
    must let stop() take effect within ~2s.
    """
    queue, consumer_name = setup_queue
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=10
    )

    t = threading.Thread(target=cons.start, daemon=True)
    t.start()
    # Give the consumer time to enter the notifies() wait.
    time.sleep(1.0)

    stop_started = time.monotonic()
    cons.stop()
    t.join(timeout=2.5)
    stop_elapsed = time.monotonic() - stop_started

    assert not t.is_alive(), (
        f"consumer thread still alive {stop_elapsed:.2f}s after stop()"
    )
    assert stop_elapsed < 2.0, (
        f"stop() took {stop_elapsed:.2f}s to take effect; expected <2s"
    )


def test_notify_wakes_consumer_before_poll_interval(dsn, conn, setup_queue):
    """A real NOTIFY must wake the LISTEN wait well before poll_interval.

    With poll_interval=10s, a producer sending an event + ticking from a
    separate connection should cause the consumer's handler to fire within
    a few seconds.
    """
    queue, consumer_name = setup_queue

    seen: list = []
    handler_called = threading.Event()

    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=10
    )

    @cons.on("evt.wake")
    def _h(m: pgque.Message):
        seen.append(m.payload)
        handler_called.set()

    t = threading.Thread(target=cons.start, daemon=True)
    t.start()
    try:
        # Let the consumer reach LISTEN + first poll.
        time.sleep(1.0)

        # Send + tick from a separate connection (this triggers
        # pg_notify on channel pgque_<queue> via the queue's NOTIFY
        # trigger, which the consumer is LISTENing on).
        with psycopg.connect(dsn, autocommit=False) as producer:
            client = pgque.PgqueClient(producer)
            client.send(queue, {"v": 1}, type="evt.wake")
            producer.commit()
            producer.execute("select pgque.force_tick(%s)", (queue,))
            producer.execute("select pgque.ticker()")
            producer.commit()

        # Must wake well before the 10s poll_interval expires.
        assert handler_called.wait(timeout=3.0), (
            "handler not invoked within 3s; NOTIFY did not wake consumer"
        )
        assert len(seen) == 1
    finally:
        cons.stop()
        t.join(timeout=3.0)
