# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Parity wrappers that should exist across all first-party clients."""

import pytest

import pgque


def test_subscribe_unsubscribe_wrappers(conn, queue_name, consumer_name):
    client = pgque.PgqueClient(conn)
    conn.execute("select pgque.create_queue(%s)", (queue_name,))
    conn.commit()
    try:
        assert client.subscribe(queue_name, consumer_name) == 1
        assert client.subscribe(queue_name, consumer_name) == 0
        conn.commit()

        assert client.unsubscribe(queue_name, consumer_name) == 1
        assert client.unsubscribe(queue_name, consumer_name) == 0
        conn.commit()
    finally:
        conn.rollback()
        conn.execute("select pgque.drop_queue(%s, true)", (queue_name,))
        conn.commit()


def test_ticker_wrapper_returns_tick_id(conn, setup_queue):
    queue, _consumer = setup_queue
    client = pgque.PgqueClient(conn)

    client.send(queue, {"k": "v"})
    conn.commit()
    client.force_next_tick(queue)
    tick_id = client.ticker(queue)
    conn.commit()

    assert isinstance(tick_id, int)


def test_ticker_all_wrapper_returns_queue_count(conn, setup_queue):
    queue, _consumer = setup_queue
    client = pgque.PgqueClient(conn)

    client.send(queue, {"k": "v"})
    conn.commit()
    client.force_next_tick(queue)
    count = client.ticker_all()
    conn.commit()

    assert isinstance(count, int)
    assert count >= 1


def test_consumer_not_found_maps_to_typed_error(conn, queue_name):
    client = pgque.PgqueClient(conn)
    conn.execute("select pgque.create_queue(%s)", (queue_name,))
    conn.commit()
    try:
        with pytest.raises(pgque.PgqueConsumerNotFound):
            client.receive(queue_name, "missing_consumer", max_messages=1)
    finally:
        conn.rollback()
        conn.execute("select pgque.drop_queue(%s, true)", (queue_name,))
        conn.commit()
