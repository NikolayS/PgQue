# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Producer-side tests for ``Client.send`` / ``Client.send_batch``."""

import pytest

import pgque


def test_send_returns_int_event_id(conn, setup_queue):
    queue, _ = setup_queue
    client = pgque.PgqueClient(conn)
    eid = client.send(queue, {"order_id": 42})
    assert isinstance(eid, int)
    assert eid > 0


def test_send_with_explicit_type(conn, setup_queue):
    queue, _ = setup_queue
    client = pgque.PgqueClient(conn)
    eid = client.send(queue, {"id": 1}, type="order.created")
    assert isinstance(eid, int)


def test_send_event_object(conn, setup_queue):
    queue, _ = setup_queue
    client = pgque.PgqueClient(conn)
    eid = client.send(queue, pgque.Event(payload={"x": 1}, type="custom.t"))
    assert isinstance(eid, int)


def test_send_str_payload_passes_through(conn, setup_queue):
    queue, _ = setup_queue
    client = pgque.PgqueClient(conn)
    eid = client.send(queue, '"plain string"')
    assert isinstance(eid, int)


def test_send_none_payload(conn, setup_queue):
    queue, _ = setup_queue
    client = pgque.PgqueClient(conn)
    eid = client.send(queue, None)
    assert isinstance(eid, int)


def test_send_unicode_payload(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    payload = {"text": "héllo wörld 🎉 — ünicode тест"}
    client.send(queue, payload)
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    got = msgs[0].payload if isinstance(msgs[0].payload, dict) \
        else __import__("json").loads(msgs[0].payload)
    assert got == payload
    client.ack(msgs[0].batch_id)
    conn.commit()


def test_send_large_payload(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    big = {"data": "x" * 100_000}
    client.send(queue, big)
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    got = msgs[0].payload if isinstance(msgs[0].payload, dict) \
        else __import__("json").loads(msgs[0].payload)
    assert got == big
    client.ack(msgs[0].batch_id)
    conn.commit()


def test_send_batch_returns_ids_in_order(conn, setup_queue):
    queue, _ = setup_queue
    client = pgque.PgqueClient(conn)
    ids = client.send_batch(queue, "batch.test", [
        {"n": 1}, {"n": 2}, {"n": 3}, {"n": 4},
    ])
    assert len(ids) == 4
    assert all(isinstance(i, int) for i in ids)
    assert ids == sorted(ids)


def test_send_to_missing_queue_raises(conn):
    client = pgque.PgqueClient(conn)
    with pytest.raises(pgque.PgqueError):
        client.send("does_not_exist_xyz_12345", {"x": 1})
        conn.commit()
    conn.rollback()
