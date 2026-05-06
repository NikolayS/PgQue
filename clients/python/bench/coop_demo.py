#!/usr/bin/env python3
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Cooperative consumers demo for pgque-py.

Spins up two threaded ``pgque.Consumer`` instances that share one logical
consumer name but use different ``subconsumer=`` values. Each event is
delivered to exactly one of the two workers; the demo prints per-message
handler lines and a final per-worker count summary.

Run::

    PGQUE_TEST_DSN=postgres://localhost/pgque_coop_py \\
        python3 clients/python/bench/coop_demo.py
"""

from __future__ import annotations

import os
import secrets
import sys
import threading
import time
from typing import Iterable

import pgque


DEFAULT_EVENTS = 40
DEFAULT_RUN_SECONDS = 5.0


def _publish(client: pgque.PgqueClient, queue: str, n: int) -> None:
    """Insert ``n`` events spread across several ticks.

    A PgQ batch is delivered to exactly one cooperative subconsumer, so
    splitting the events into multiple ticks lets both workers see at
    least one batch and makes the cooperative split visible in the
    per-worker counts.
    """
    chunks = 4
    chunk = max(1, n // chunks)
    sent = 0
    for start in range(0, n, chunk):
        size = min(chunk, n - start)
        payloads = [
            {"i": start + i, "demo": "coop"} for i in range(size)
        ]
        client.send_batch(queue, "demo.coop", payloads)
        client.conn.commit()
        client.force_next_tick(queue)
        client.conn.execute("select pgque.ticker(%s)", (queue,))
        client.conn.commit()
        sent += size
    assert sent == n


def _make_handler(
    worker: str, counts: dict[str, int], lock: threading.Lock
) -> "callable":
    def _on_event(msg: pgque.Message) -> None:
        with lock:
            counts[worker] = counts.get(worker, 0) + 1
        print(
            f"[{worker}] msg_id={msg.msg_id} type={msg.type} "
            f"payload={msg.payload}",
            flush=True,
        )

    return _on_event


def _spawn_consumer(
    dsn: str,
    queue: str,
    consumer_name: str,
    subconsumer: str,
    counts: dict[str, int],
    lock: threading.Lock,
) -> tuple[pgque.Consumer, threading.Thread]:
    cons = pgque.Consumer(
        dsn=dsn,
        queue=queue,
        name=consumer_name,
        subconsumer=subconsumer,
        poll_interval=1,
    )
    cons.on("demo.coop")(_make_handler(subconsumer, counts, lock))
    t = threading.Thread(target=cons.start, daemon=True, name=subconsumer)
    t.start()
    return cons, t


def _drain_subconsumers(
    client: pgque.PgqueClient,
    queue: str,
    consumer_name: str,
    subconsumers: Iterable[str],
) -> None:
    for sub in subconsumers:
        try:
            client.unsubscribe_subconsumer(
                queue, consumer_name, sub, batch_handling=1
            )
            client.conn.commit()
        except pgque.PgqueError:
            client.conn.rollback()


def main() -> int:
    dsn = os.environ.get("PGQUE_TEST_DSN")
    if not dsn:
        print(
            "PGQUE_TEST_DSN not set; refusing to run cooperative demo",
            file=sys.stderr,
        )
        return 1

    queue = f"coop_demo_{secrets.token_hex(4)}"
    consumer_name = "demo_workers"

    n_events = int(os.environ.get("COOP_DEMO_EVENTS", DEFAULT_EVENTS))
    run_seconds = float(
        os.environ.get("COOP_DEMO_SECONDS", DEFAULT_RUN_SECONDS)
    )

    with pgque.connect(dsn) as setup:
        setup.conn.execute("select pgque.create_queue(%s)", (queue,))
        setup.conn.commit()
        setup.subscribe_subconsumer(queue, consumer_name, "worker-1")
        setup.subscribe_subconsumer(queue, consumer_name, "worker-2")
        setup.conn.commit()
        _publish(setup, queue, n_events)

    counts: dict[str, int] = {}
    lock = threading.Lock()

    print(
        f"# coop_demo: queue={queue} events={n_events} "
        f"run_seconds={run_seconds}",
        flush=True,
    )

    c1, t1 = _spawn_consumer(
        dsn, queue, consumer_name, "worker-1", counts, lock
    )
    c2, t2 = _spawn_consumer(
        dsn, queue, consumer_name, "worker-2", counts, lock
    )

    deadline = time.monotonic() + run_seconds
    try:
        while time.monotonic() < deadline:
            with lock:
                done = sum(counts.values()) >= n_events
            if done:
                break
            time.sleep(0.1)
    finally:
        c1.stop()
        c2.stop()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)

    total = sum(counts.values())
    print()
    print("# coop_demo summary")
    for worker in sorted(counts):
        share = counts[worker] / total if total else 0.0
        print(
            f"  {worker}: {counts[worker]} message(s) "
            f"({share*100:.0f}%)"
        )
    print(f"  total : {total} / {n_events}")

    with pgque.connect(dsn) as cleanup:
        _drain_subconsumers(
            cleanup, queue, consumer_name, ("worker-1", "worker-2")
        )
        try:
            cleanup.conn.execute(
                "select pgque.drop_queue(%s, true)", (queue,)
            )
            cleanup.conn.commit()
        except pgque.PgqueError:
            cleanup.conn.rollback()

    return 0 if total == n_events else 2


if __name__ == "__main__":
    sys.exit(main())
