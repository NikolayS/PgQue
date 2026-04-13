import pytest
from pgque import PgqueClient, Message

def test_send_returns_event_id(conn, setup_queue):
    client = PgqueClient(conn)
    eid = client.send("pytest_queue", {"order_id": 42})
    assert eid is not None
    assert isinstance(eid, int)

def test_send_with_type(conn, setup_queue):
    client = PgqueClient(conn)
    eid = client.send("pytest_queue", type="order.created", payload={"id": 1})
    assert eid is not None

def test_send_batch(conn, setup_queue):
    client = PgqueClient(conn)
    ids = client.send_batch("pytest_queue", "batch.test", [
        {"n": 1}, {"n": 2}, {"n": 3}
    ])
    assert len(ids) == 3
