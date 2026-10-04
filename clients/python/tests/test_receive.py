# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Consumer-side tests: ``Client.receive`` / ``Client.ack``."""

import json

import pgque
import pytest


def test_receive_empty_when_no_tick(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"a": 1})
    # no ticker call -> nothing to batch yet
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert msgs == []


def test_receive_returns_messages_after_tick(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"key": "value"})
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    m = msgs[0]
    assert m.batch_id is not None
    assert m.msg_id is not None
    assert m.type == "default"
    # payload may be auto-decoded by psycopg or come back as string
    got = m.payload if isinstance(m.payload, dict) else \
        __import__("json").loads(m.payload)
    assert got == {"key": "value"}


def test_ack_advances_position(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"k": 1})
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    client.ack(msgs[0].batch_id)
    conn.commit()
    # Next receive: empty (no new tick)
    msgs2 = client.receive(queue, consumer, max_messages=10)
    assert msgs2 == []


def test_receive_rejects_batch_over_max_and_retries_complete(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    for i in range(5):
        client.send(queue, {"i": i})
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    with pytest.raises(
        pgque.ReceiveOverflowError, match="batch exceeds max_return of 3"
    ) as exc_info:
        client.receive(queue, consumer, max_messages=3)
    error = exc_info.value
    assert isinstance(error, pgque.PgqueError)
    assert error.sqlstate == "54000"
    assert error.configured_limit == 3
    assert error.operation == "receive"
    assert "larger resource-safe max_return" in error.hint
    assert error.__cause__.sqlstate == "54000"

    # The failed statement returns no partial list and rolls back its batch
    # allocation. Reset the failed transaction before retrying.
    conn.rollback()
    msgs = client.receive(queue, consumer, max_messages=5)
    assert len(msgs) == 5
    assert {
        (m.payload if isinstance(m.payload, dict) else json.loads(m.payload))["i"]
        for m in msgs
    } == set(range(5))
    client.ack(msgs[0].batch_id)
    conn.commit()
    assert client.receive(queue, consumer, max_messages=5) == []


def test_receive_preserves_event_type(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, {"a": 1}, type="evt.alpha")
    client.send(queue, {"b": 2}, type="evt.beta")
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    types = sorted(m.type for m in msgs)
    assert types == ["evt.alpha", "evt.beta"]
    client.ack(msgs[0].batch_id)
    conn.commit()


def test_message_timestamp_round_trip(conn, setup_queue):
    """created_at should round-trip as a Python datetime."""
    import datetime as dt

    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    before = dt.datetime.now(dt.timezone.utc)
    client.send(queue, {"x": 1})
    conn.commit()
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()
    after = dt.datetime.now(dt.timezone.utc)
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    assert isinstance(msgs[0].created_at, dt.datetime)
    # Allow a generous slop because postgres clock and Python clock differ
    slop = dt.timedelta(seconds=5)
    assert before - slop <= msgs[0].created_at <= after + slop
    client.ack(msgs[0].batch_id)
    conn.commit()
