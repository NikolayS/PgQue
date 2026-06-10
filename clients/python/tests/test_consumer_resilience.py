# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Consumer poll-loop resilience.

Backlog draining: after a non-empty batch, the consumer must re-poll
immediately instead of waiting up to ``poll_interval`` for a NOTIFY.
Notifies fire only on new ticks, and notifies for already-accumulated
batches were emitted while the consumer was not listening -- waiting
on them drains a backlog at one batch per ``poll_interval``.
"""

import threading
import time

import pgque


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
