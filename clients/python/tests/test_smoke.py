from pgque import PgqueClient, Consumer


def test_python_client_smoke(conn):
    client = PgqueClient(conn)
    client.send("smoke_py", {"hello": "world"}, "smoke.test")
    client.subscribe("smoke_py", "py-smoke")

    consumer = Consumer(conn, queue="smoke_py", name="py-smoke")
    messages = consumer.receive(limit=10)

    assert len(messages) >= 1
    consumer.ack(messages[0].batch_id)
