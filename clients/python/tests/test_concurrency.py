# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Concurrent producers under one queue."""

import threading

import pgque


def test_concurrent_producers_no_id_collisions(dsn, setup_queue):
    queue, consumer = setup_queue

    N_THREADS = 4
    PER_THREAD = 25
    seen_ids: list[int] = []
    seen_lock = threading.Lock()

    def _producer():
        with pgque.connect(dsn) as client:
            ids = []
            for i in range(PER_THREAD):
                eid = client.send(queue, {"thread": threading.get_ident(),
                                          "i": i})
                ids.append(eid)
            client.conn.commit()
        with seen_lock:
            seen_ids.extend(ids)

    threads = [threading.Thread(target=_producer) for _ in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()

    # Every send returned a unique event ID.
    assert len(seen_ids) == N_THREADS * PER_THREAD
    assert len(set(seen_ids)) == len(seen_ids)
