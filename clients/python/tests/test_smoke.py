from pgque import PgqueClient


def test_python_client_smoke(conn):
    conn.execute("select pgque.subscribe('smoke_py', 'py-smoke')")
    conn.commit()

    client = PgqueClient(conn)
    client.send("smoke_py", {"hello": "world"}, type="smoke.test")
    conn.commit()

    conn.execute("select pgque.force_tick('smoke_py')")
    conn.execute("select pgque.ticker()")
    conn.commit()

    messages = client.receive("smoke_py", "py-smoke", max_messages=10)
    assert len(messages) >= 1

    client.ack(messages[0].batch_id)
    conn.commit()
