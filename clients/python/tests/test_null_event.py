# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""NULL ``ev_type`` / ``ev_data`` client contract."""

from datetime import datetime, timezone
from typing import Optional, get_type_hints

from unittest import mock
from unittest.mock import MagicMock

import pgque


def _tick(conn, queue: str) -> None:
    conn.execute("select pgque.force_next_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()


def _null_message() -> pgque.Message:
    return pgque.Message(
        msg_id=1,
        batch_id=2,
        type=None,
        payload=None,
        retry_count=None,
        created_at=datetime.now(timezone.utc),
    )


def test_message_type_annotation_preserves_sql_nullability():
    assert get_type_hints(pgque.Message)["type"] == Optional[str]


def test_receive_preserves_null_type_and_payload(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)

    event_id = conn.execute(
        "select pgque.insert_event(%s, null, null)", (queue,)
    ).fetchone()[0]
    conn.commit()
    _tick(conn, queue)

    [msg] = client.receive(queue, consumer, max_messages=1)
    assert msg.msg_id == event_id
    assert msg.type is None
    assert msg.payload is None
    client.ack(msg.batch_id)
    conn.commit()


def test_receive_coop_preserves_null_type_and_payload(
    conn, queue_name, consumer_name
):
    client = pgque.PgqueClient(conn)
    conn.execute("select pgque.create_queue(%s)", (queue_name,))
    conn.commit()
    try:
        client.subscribe_subconsumer(queue_name, consumer_name, "worker-1")
        conn.commit()
        event_id = conn.execute(
            "select pgque.insert_event(%s, null, null)", (queue_name,)
        ).fetchone()[0]
        conn.commit()
        _tick(conn, queue_name)

        [msg] = client.receive_coop(
            queue_name, consumer_name, "worker-1", max_messages=1
        )
        assert msg.msg_id == event_id
        assert msg.type is None
        assert msg.payload is None
        client.ack(msg.batch_id)
        conn.commit()
    finally:
        conn.rollback()
        client.unsubscribe_subconsumer(
            queue_name, consumer_name, "worker-1", batch_handling=1
        )
        conn.execute("select pgque.drop_queue(%s, true)", (queue_name,))
        conn.commit()


def test_consumer_routes_null_type_as_unknown_even_if_none_handler_registered(dsn):
    msg = _null_message()
    consumer = pgque.Consumer(dsn=dsn, queue="q", name="c")
    handler = MagicMock()
    consumer.on(None)(handler)  # type: ignore[arg-type]
    conn = MagicMock()

    with mock.patch.object(pgque.PgqueClient, "receive", return_value=[msg]), \
         mock.patch.object(pgque.PgqueClient, "nack") as nack, \
         mock.patch.object(pgque.PgqueClient, "ack", return_value=1) as ack:
        assert consumer._poll_once(conn) is True

    handler.assert_not_called()
    nack.assert_called_once()
    ack.assert_called_once_with(msg.batch_id)


def test_consumer_routes_null_type_to_explicit_catch_all(dsn):
    msg = _null_message()
    consumer = pgque.Consumer(dsn=dsn, queue="q", name="c")
    catch_all = MagicMock()
    consumer.on("*")(catch_all)
    conn = MagicMock()

    with mock.patch.object(pgque.PgqueClient, "receive", return_value=[msg]), \
         mock.patch.object(pgque.PgqueClient, "nack") as nack, \
         mock.patch.object(pgque.PgqueClient, "ack", return_value=1) as ack:
        assert consumer._poll_once(conn) is True

    catch_all.assert_called_once_with(msg)
    nack.assert_not_called()
    ack.assert_called_once_with(msg.batch_id)
