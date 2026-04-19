from pg_current import PgqueClient


def test_python_client_smoke(conn):
    conn.execute("select pg_current.subscribe('smoke_py', 'py-smoke')")
    conn.commit()

    client = PgqueClient(conn)
    client.send("smoke_py", {"hello": "world"}, type="smoke.test")
    conn.commit()

    conn.execute("select pg_current.force_tick('smoke_py')")
    conn.execute("select pg_current.ticker()")
    conn.commit()

    messages = client.receive("smoke_py", "py-smoke", max_messages=10)
    assert len(messages) >= 1

    client.ack(messages[0].batch_id)
    conn.commit()
