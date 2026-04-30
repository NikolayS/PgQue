# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""``Client.nack``: retry routing + DLQ at the retry limit."""

import pytest

import pgque


def _enqueue_and_receive(client, queue, consumer, payload):
    client.send(queue, payload)
    client.conn.commit()
    client.conn.execute("select pgque.force_tick(%s)", (queue,))
    client.conn.execute("select pgque.ticker()")
    client.conn.commit()
    msgs = client.receive(queue, consumer, max_messages=10)
    assert len(msgs) == 1
    return msgs[0]


def test_nack_routes_to_retry_queue(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)

    msg = _enqueue_and_receive(client, queue, consumer, {"k": "retry"})
    client.nack(msg.batch_id, msg, retry_after=0)
    client.ack(msg.batch_id)
    conn.commit()

    # The message should be in the retry queue, NOT the DLQ.
    cnt = conn.execute(
        "select count(*) from pgque.retry_queue rq "
        "join pgque.queue q on q.queue_id = rq.ev_queue "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert cnt == 1

    dlq_cnt = conn.execute(
        "select count(*) from pgque.dead_letter dl "
        "join pgque.queue q on q.queue_id = dl.dl_queue_id "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert dlq_cnt == 0


def test_nack_routes_to_dlq_at_max_retries(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)

    # Set max_retries=0 so the first nack routes straight to the DLQ.
    # nack() reads ev_retry from the canonical event row, ignoring any
    # client-supplied retry_count, so synthesising a retried message in
    # Python alone has no effect on routing.
    conn.execute(
        "update pgque.queue set queue_max_retries = 0 where queue_name = %s",
        (queue,),
    )
    conn.commit()

    msg = _enqueue_and_receive(client, queue, consumer, {"k": "doomed"})
    client.nack(msg.batch_id, msg, retry_after=0, reason="poison pill")
    client.ack(msg.batch_id)
    conn.commit()

    dlq_cnt = conn.execute(
        "select count(*) from pgque.dead_letter dl "
        "join pgque.queue q on q.queue_id = dl.dl_queue_id "
        "where q.queue_name = %s",
        (queue,),
    ).fetchone()[0]
    assert dlq_cnt == 1


def test_nack_invalid_batch_raises(conn, setup_queue):
    queue, consumer = setup_queue
    client = pgque.PgqueClient(conn)
    msg = _enqueue_and_receive(client, queue, consumer, {"x": 1})
    client.ack(msg.batch_id)
    conn.commit()

    with pytest.raises(pgque.PgqueError):
        client.nack(msg.batch_id, msg, retry_after=0)
        conn.commit()
    conn.rollback()
