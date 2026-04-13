import pytest
from pgque import PgqueClient, Consumer, Message

def test_receive_returns_messages(conn, setup_queue):
    client = PgqueClient(conn)
    client.send("pytest_queue", {"key": "value"})
    conn.execute("SELECT pgque.ticker()")
    conn.commit()

    msgs = client.receive("pytest_queue", "pytest_consumer", max_messages=10)
    assert len(msgs) == 1
    assert msgs[0].payload == '{"key": "value"}'
    assert msgs[0].type == "default"
    assert msgs[0].batch_id is not None

def test_ack_advances_position(conn, setup_queue):
    client = PgqueClient(conn)
    client.send("pytest_queue", {"key": "value"})
    conn.execute("SELECT pgque.ticker()")
    conn.commit()

    msgs = client.receive("pytest_queue", "pytest_consumer", max_messages=10)
    client.ack(msgs[0].batch_id)
    conn.commit()

    # Next receive should be empty
    msgs2 = client.receive("pytest_queue", "pytest_consumer", max_messages=10)
    assert len(msgs2) == 0

def test_nack_retries_event(conn, setup_queue):
    client = PgqueClient(conn)
    client.send("pytest_queue", {"key": "retry"})
    conn.execute("SELECT pgque.ticker()")
    conn.commit()

    msgs = client.receive("pytest_queue", "pytest_consumer", max_messages=10)
    client.nack(msgs[0].batch_id, msgs[0], retry_after=0)
    client.ack(msgs[0].batch_id)
    conn.commit()

    # Run maintenance and ticker
    conn.execute("SELECT pgque.maint_retry_events('pytest_queue')")
    conn.execute("SELECT pgque.force_tick('pytest_queue')")
    conn.execute("SELECT pgque.ticker()")
    conn.commit()

    # Should get the event again
    msgs2 = client.receive("pytest_queue", "pytest_consumer", max_messages=10)
    assert len(msgs2) >= 1
