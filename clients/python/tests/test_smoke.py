from logres import LogresClient


def test_python_client_smoke(conn):
    conn.execute("select logres.subscribe('smoke_py', 'py-smoke')")
    conn.commit()

    client = LogresClient(conn)
    client.send("smoke_py", {"hello": "world"}, type="smoke.test")
    conn.commit()

    conn.execute("select logres.force_tick('smoke_py')")
    conn.execute("select logres.ticker()")
    conn.commit()

    messages = client.receive("smoke_py", "py-smoke", max_messages=10)
    assert len(messages) >= 1

    client.ack(messages[0].batch_id)
    conn.commit()
