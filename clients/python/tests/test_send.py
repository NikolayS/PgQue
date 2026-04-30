# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Producer-side tests for ``Client.send`` / ``Client.send_batch``."""

from unittest.mock import MagicMock, call

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
    conn.commit()
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
    conn.commit()
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


@pytest.mark.parametrize("type_val,expect_3arg", [
    (None, False),       # None → 2-arg (default type)
    ("", False),         # "" → 2-arg (empty = default)
    ("default", False),  # "default" → 2-arg
    ("custom", True),    # non-default → 3-arg
])
def test_send_sql_form_selection(type_val, expect_3arg):
    """send() picks 2-arg SQL when type is empty/None/default; 3-arg otherwise."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (999,)
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_cursor

    client = pgque.PgqueClient(mock_conn)
    if type_val is None:
        eid = client.send("q", {"x": 1}, type=None)
    else:
        eid = client.send("q", {"x": 1}, type=type_val)

    assert eid == 999
    sql_used = mock_conn.execute.call_args[0][0]
    if expect_3arg:
        assert "send(%s, %s, %s::jsonb)" in sql_used, (
            f"expected 3-arg form for type={type_val!r}, got: {sql_used!r}"
        )
    else:
        assert "send(%s, %s::jsonb)" in sql_used, (
            f"expected 2-arg form for type={type_val!r}, got: {sql_used!r}"
        )
        assert "send(%s, %s, %s::jsonb)" not in sql_used


@pytest.mark.parametrize("payload,expected", [
    ({"key": "val", "n": 1}, {"key": "val", "n": 1}),  # dict
    ([1, "two", None], [1, "two", None]),               # list
    ('"just a string"', "just a string"),               # JSON string literal
    ("42", 42),                                         # JSON number
    ("null", None),                                     # JSON null
])
def test_jsonb_payload_round_trip(conn, setup_queue, payload, expected):
    """jsonb payloads decode to native Python types after send/receive."""
    import json

    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    client.send(queue, payload)
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    raw = msgs[0].payload
    got = raw if not isinstance(raw, str) else json.loads(raw)
    assert got == expected
    client.ack(msgs[0].batch_id)
    conn.commit()


def test_send_batch_none_payload_produces_json_null(conn, setup_queue):
    """send_batch([None]) must store JSON null, not SQL NULL.

    Regression test for send/send_batch asymmetry: send(None) coerces to
    JSON null via "null" string, but send_batch previously passed Python
    None through psycopg as SQL NULL, bypassing the ::jsonb cast.
    """
    import json

    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    client.send_batch(queue, "default", [None])
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker()")
    conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1, "expected exactly 1 message from send_batch([None])"
    raw = msgs[0].payload
    # payload must be JSON null (Python None after psycopg decode), not a
    # missing/SQL-NULL value that would cause a TypeError on json.dumps.
    got = raw if not isinstance(raw, str) else json.loads(raw)
    assert got is None, (
        f"send_batch([None]) should store JSON null; got {raw!r}"
    )
    client.ack(msgs[0].batch_id)
    conn.commit()
