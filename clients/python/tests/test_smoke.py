# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""End-to-end smoke: connect, subscribe, send, tick, receive, ack."""

import pgque


def test_smoke_send_receive_ack(dsn, queue_name, consumer_name):
    with pgque.connect(dsn) as client:
        client.conn.execute("select pgque.subscribe(%s, %s)",
                             (queue_name, consumer_name))
        client.conn.commit()

        try:
            client.send(queue_name, {"hello": "world"}, type="smoke.test")
            client.conn.commit()

            client.conn.execute("select pgque.force_tick(%s)", (queue_name,))
            client.conn.execute("select pgque.ticker()")
            client.conn.commit()

            msgs = client.receive(queue_name, consumer_name, max_messages=10)
            assert len(msgs) == 1
            assert msgs[0].type == "smoke.test"
            assert msgs[0].payload == {"hello": "world"} or \
                   msgs[0].payload == '{"hello": "world"}'

            client.ack(msgs[0].batch_id)
            client.conn.commit()
        finally:
            client.conn.rollback()
            client.conn.execute(
                "select pgque.unregister_consumer(%s, %s)",
                (queue_name, consumer_name))
            client.conn.execute("select pgque.drop_queue(%s, true)",
                                 (queue_name,))
            client.conn.commit()
