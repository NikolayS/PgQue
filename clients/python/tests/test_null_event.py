# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Regression: NULL ev_type / ev_data round-trip cleanly through receive.

The low-level PgQ primitive ``pgque.insert_event(queue, null, null)`` can
produce a row with SQL-NULL ``ev_type`` and ``ev_data``. The driver's
``Message`` type and row mapper must tolerate that shape -- the row must
be returned with ``type`` and ``payload`` set to ``None``, the type
annotations must declare those fields nullable, and ``ack`` / ``nack``
must still work on the surrounding batch.

Regression for NikolayS/pgque#143.
"""

import typing

import pgque
from pgque.types import Message


def test_message_type_and_payload_are_optional():
    """The Message dataclass must declare ``type`` and ``payload`` as Optional."""
    hints = typing.get_type_hints(Message)
    type_args = typing.get_args(hints["type"])
    payload_args = typing.get_args(hints["payload"])
    assert type(None) in type_args, (
        f"Message.type must be Optional, got {hints['type']!r}"
    )
    assert type(None) in payload_args, (
        f"Message.payload must be Optional, got {hints['payload']!r}"
    )


def test_receive_null_ev_type_and_data(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)

    # Bypass pgque.send(): call the low-level primitive directly so the
    # row has SQL-NULL ev_type and ev_data.
    conn.execute(
        "select pgque.insert_event(%s, null::text, null::text)", (queue,)
    )
    conn.commit()
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.commit()

    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    m = msgs[0]
    assert m.type is None, f"expected None type, got {m.type!r}"
    assert m.payload is None, f"expected None payload, got {m.payload!r}"

    # ack must still work on the batch.
    client.ack(m.batch_id)
    conn.commit()
