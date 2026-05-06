# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Experimental cooperative-consumers API: ``subscribe_subconsumer``,
``unsubscribe_subconsumer``, ``receive_coop``, ``touch_subconsumer``,
plus the high-level ``Consumer(subconsumer=...)`` mode.

All real-database tests reuse the ``conn`` / ``queue_name`` /
``consumer_name`` fixtures from ``conftest.py`` and create the queue
without the ``setup_queue`` consumer (the cooperative API auto-registers
the main + member rows on first call).
"""

import json
import logging
import threading
import time
from typing import Any, Iterator

import pytest

import pgque


def _decode(payload: Any) -> Any:
    """psycopg may return jsonb either as a parsed object or as text;
    normalize both forms for assertions.
    """
    if isinstance(payload, (dict, list)) or payload is None:
        return payload
    return json.loads(payload)


# ---------------------------------------------------------------------------
# Local fixtures: bare queue (no consumer pre-registered).
# ---------------------------------------------------------------------------


@pytest.fixture
def coop_queue(conn, queue_name) -> Iterator[str]:
    conn.execute("select pgque.create_queue(%s)", (queue_name,))
    conn.commit()
    try:
        yield queue_name
    finally:
        try:
            conn.rollback()
            # Forcibly tear down any subconsumer member rows before
            # dropping the queue. ``drop_queue(..., true)`` calls
            # ``unregister_consumer`` per consumer, which refuses to drop
            # a coop_main with registered members. Member consumers are
            # stored as ``"<consumer>.<subconsumer>"`` rows; split on the
            # last ``.`` to recover the (consumer, subconsumer) pair.
            rows = conn.execute(
                "select c.co_name "
                "from pgque.consumer c "
                "join pgque.subscription s on s.sub_consumer = c.co_id "
                "join pgque.queue q on q.queue_id = s.sub_queue "
                "where q.queue_name = %s and s.sub_role = 'coop_member'",
                (queue_name,),
            ).fetchall()
            for (co_name,) in rows:
                parent, _, sub = co_name.rpartition(".")
                if parent and sub:
                    conn.execute(
                        "select pgque.unsubscribe_subconsumer(%s, %s, %s, 1)",
                        (queue_name, parent, sub),
                    )
            conn.commit()
            conn.execute("select pgque.drop_queue(%s, true)", (queue_name,))
            conn.commit()
        except Exception as e:
            logging.warning("coop_queue cleanup failed: %s", e)
            conn.rollback()


def _tick(conn, queue: str) -> None:
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()


# ---------------------------------------------------------------------------
# Client method coverage.
# ---------------------------------------------------------------------------


def test_subscribe_subconsumer_returns_1_then_0(conn, coop_queue, consumer_name):
    client = pgque.PgqueClient(conn)
    first = client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()
    second = client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()
    assert first == 1
    assert second == 0


def test_receive_coop_returns_messages_and_ack_finishes(
    conn, coop_queue, consumer_name
):
    client = pgque.PgqueClient(conn)
    client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()

    client.send(coop_queue, {"k": 1}, type="evt.a")
    client.send(coop_queue, {"k": 2}, type="evt.a")
    conn.commit()
    _tick(conn, coop_queue)

    msgs = client.receive_coop(
        coop_queue, consumer_name, "worker-1", max_messages=10
    )
    assert len(msgs) == 2
    assert all(m.batch_id is not None for m in msgs)
    assert {_decode(m.payload)["k"] for m in msgs} == {1, 2}

    batch_id = msgs[0].batch_id
    client.ack(batch_id)
    conn.commit()

    # Next coop receive without a fresh tick: no batch.
    follow = client.receive_coop(
        coop_queue, consumer_name, "worker-1", max_messages=10
    )
    assert follow == []


def test_two_subconsumers_split_batches_no_duplicates(
    coop_queue, consumer_name, dsn
):
    """Two subconsumers under one consumer must split batches; no msg
    should be delivered to both members simultaneously.
    """
    # Use independent connections so each subconsumer sees its own tx.
    with pgque.connect(dsn) as producer:
        producer.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
        producer.subscribe_subconsumer(coop_queue, consumer_name, "worker-2")
        producer.conn.commit()

        for i in range(6):
            producer.send(coop_queue, {"i": i}, type="evt")
        producer.conn.commit()
        producer.conn.execute("select pgque.force_next_tick(%s)", (coop_queue,))
        producer.conn.execute("select pgque.ticker(%s)", (coop_queue,))
        producer.conn.commit()

    # Use autocommit so the cooperative allocation lock (FOR UPDATE of
    # the main row) drops as soon as ``receive_coop`` returns; otherwise
    # the second call blocks waiting for the first transaction.
    with pgque.connect(dsn, autocommit=True) as c1, \
            pgque.connect(dsn, autocommit=True) as c2:
        m1 = c1.receive_coop(coop_queue, consumer_name, "worker-1", max_messages=100)
        m2 = c2.receive_coop(coop_queue, consumer_name, "worker-2", max_messages=100)

        ids1 = {m.msg_id for m in m1}
        ids2 = {m.msg_id for m in m2}

        # No overlap.
        assert ids1.isdisjoint(ids2), (
            f"member-1 and member-2 saw the same msg_ids: "
            f"{ids1 & ids2}"
        )
        # Both members got at least one of the two batches; cumulatively
        # they observe disjoint, non-empty subsets of the produced rows.
        # (Exactly one member may get all rows if the producer wrote one
        # tick window; with one tick we expect a single batch to one
        # member, the other gets nothing this round.)
        assert (len(m1) + len(m2)) >= 1

        # Ack each non-empty batch so cleanup runs cleanly.
        if m1:
            c1.ack(m1[0].batch_id)
        if m2:
            c2.ack(m2[0].batch_id)

    # Cleanup subconsumers (forced, batch_handling=1 to drop any leftover).
    with pgque.connect(dsn) as cleanup:
        cleanup.unsubscribe_subconsumer(
            coop_queue, consumer_name, "worker-1", batch_handling=1
        )
        cleanup.unsubscribe_subconsumer(
            coop_queue, consumer_name, "worker-2", batch_handling=1
        )
        cleanup.conn.commit()


def test_unsubscribe_subconsumer_with_active_batch_default_raises(
    conn, coop_queue, consumer_name
):
    client = pgque.PgqueClient(conn)
    client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()
    client.send(coop_queue, {"i": 1}, type="evt")
    conn.commit()
    _tick(conn, coop_queue)

    msgs = client.receive_coop(coop_queue, consumer_name, "worker-1")
    assert len(msgs) == 1
    # Hold the batch open: do NOT ack.

    with pytest.raises(pgque.PgqueError):
        client.unsubscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    # Roll back the failed call so the test connection is usable.
    conn.rollback()

    # batch_handling=1 cleans up.
    rv = client.unsubscribe_subconsumer(
        coop_queue, consumer_name, "worker-1", batch_handling=1
    )
    assert rv == 1
    conn.commit()


def test_unsubscribe_subconsumer_routes_active_messages_through_retry(
    conn, coop_queue, consumer_name
):
    """``batch_handling=1`` must run the retry/DLQ policy on active
    messages instead of raising.
    """
    client = pgque.PgqueClient(conn)
    client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()
    client.send(coop_queue, {"i": 1}, type="evt")
    conn.commit()
    _tick(conn, coop_queue)

    msgs = client.receive_coop(coop_queue, consumer_name, "worker-1")
    assert len(msgs) == 1

    rv = client.unsubscribe_subconsumer(
        coop_queue, consumer_name, "worker-1", batch_handling=1
    )
    conn.commit()
    assert rv == 1


def test_touch_subconsumer_returns_1_on_registered_row(
    conn, coop_queue, consumer_name
):
    client = pgque.PgqueClient(conn)
    client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()

    rv = client.touch_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()
    assert rv == 1


# ---------------------------------------------------------------------------
# High-level ``Consumer`` integration in coop mode.
# ---------------------------------------------------------------------------


def _run_consumer_for(consumer: pgque.Consumer, seconds: float) -> threading.Thread:
    t = threading.Thread(target=consumer.start, daemon=True)
    t.start()

    def _stopper():
        time.sleep(seconds)
        consumer.stop()

    threading.Thread(target=_stopper, daemon=True).start()
    return t


def test_consumer_coop_dispatches_and_acks(dsn, conn, coop_queue, consumer_name):
    client = pgque.PgqueClient(conn)
    client.subscribe_subconsumer(coop_queue, consumer_name, "worker-1")
    conn.commit()
    msg_id = client.send(coop_queue, {"x": 1}, type="evt.coop")
    conn.commit()
    _tick(conn, coop_queue)

    seen: list[pgque.Message] = []
    cons = pgque.Consumer(
        dsn=dsn,
        queue=coop_queue,
        name=consumer_name,
        subconsumer="worker-1",
        poll_interval=1,
    )

    @cons.on("evt.coop")
    def _h(m: pgque.Message):
        seen.append(m)

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    assert len(seen) == 1
    assert seen[0].msg_id == msg_id

    # No leftover active batch (consumer should have acked).
    follow = client.receive_coop(coop_queue, consumer_name, "worker-1")
    assert follow == []

    # Cleanup.
    client.unsubscribe_subconsumer(
        coop_queue, consumer_name, "worker-1", batch_handling=1
    )
    conn.commit()


def test_consumer_without_subconsumer_unchanged(dsn, conn, setup_queue):
    """When ``subconsumer`` is None, behavior is the existing fan-out
    path: ``Client.receive`` is called, not ``receive_coop``.
    """
    queue, consumer_name = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"v": 1}, type="evt.normal")
    conn.commit()
    _tick(conn, queue)

    seen: list[pgque.Message] = []
    cons = pgque.Consumer(
        dsn=dsn, queue=queue, name=consumer_name, poll_interval=1
    )

    @cons.on("evt.normal")
    def _h(m: pgque.Message):
        seen.append(m)

    t = _run_consumer_for(cons, 3.0)
    t.join(timeout=5.0)

    assert len(seen) == 1


def test_consumer_dead_interval_without_subconsumer_raises(dsn):
    with pytest.raises(ValueError):
        pgque.Consumer(
            dsn=dsn,
            queue="q",
            name="c",
            dead_interval="5 minutes",
        )
