#!/usr/bin/env python3
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Producer microbenchmarks for pgque-py send-loop vs send_batch."""

from __future__ import annotations

import os
import secrets
import statistics
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass

import pgque

DSN = os.environ.get("PGQUE_TEST_DSN")
BATCH_SIZES = (1, 100, 1000)
REPEATS = int(os.environ.get("PGQUE_BENCH_REPEATS", "3"))


@dataclass(frozen=True)
class Result:
    language: str
    method: str
    batch_size: int
    median_ms: float
    events_per_sec: float
    repeats: int


def main() -> int:
    if not DSN:
        print("PGQUE_TEST_DSN not set; skipping pgque-py producer benchmarks")
        return 0

    results: list[Result] = []
    with pgque.connect(DSN) as client:
        for n in BATCH_SIZES:
            results.append(measure(client, "send_loop", n, lambda q, payloads: send_loop(client, q, payloads)))
            results.append(measure(client, "send_batch", n, lambda q, payloads: send_batch(client, q, payloads)))

    print("# pgque-py producer benchmark")
    print()
    print("| method | batch_size | median_ms | events_per_sec | repeats |")
    print("|---|---:|---:|---:|---:|")
    for r in results:
        print(f"| {display_method(r.method)} | {r.batch_size} | {r.median_ms:.3f} | {r.events_per_sec:.0f} | {r.repeats} |")

    print()
    print("```csv")
    print("language,method,batch_size,median_ms,events_per_sec,repeats")
    for r in results:
        print(f"{r.language},{r.method},{r.batch_size},{r.median_ms:.3f},{r.events_per_sec:.0f},{r.repeats}")
    print("```")
    return 0


def measure(client: pgque.PgqueClient, method: str, n: int, fn: Callable[[str, list[dict]], None]) -> Result:
    durations: list[float] = []
    for _ in range(REPEATS):
        queue = f"pybench_{method}_{n}_{secrets.token_hex(4)}"
        payloads = [{"i": i, "lang": "python", "method": method} for i in range(n)]
        client.conn.execute("select pgque.create_queue(%s)", (queue,))
        client.conn.commit()
        try:
            start = time.perf_counter()
            fn(queue, payloads)
            client.conn.commit()
            elapsed = time.perf_counter() - start
            verify_count(client, queue, n)
            durations.append(elapsed)
        finally:
            client.conn.rollback()
            client.conn.execute("select pgque.drop_queue(%s, true)", (queue,))
            client.conn.commit()

    median_s = statistics.median(durations)
    return Result(
        language="python",
        method=method,
        batch_size=n,
        median_ms=median_s * 1000,
        events_per_sec=n / median_s if median_s > 0 else float("inf"),
        repeats=REPEATS,
    )


def display_method(method: str) -> str:
    return "loop over send()" if method == "send_loop" else "send_batch()"


def send_loop(client: pgque.PgqueClient, queue: str, payloads: list[dict]) -> None:
    for payload in payloads:
        client.send(queue, payload, type="bench.producer")


def send_batch(client: pgque.PgqueClient, queue: str, payloads: list[dict]) -> None:
    client.send_batch(queue, "bench.producer", payloads)


def verify_count(client: pgque.PgqueClient, queue: str, expected: int) -> None:
    table = client.conn.execute("select pgque.current_event_table(%s)", (queue,)).fetchone()[0]
    got = client.conn.execute(f"select count(*) from {table}").fetchone()[0]
    if got != expected:
        raise RuntimeError(f"{queue}: expected {expected} events, got {got}")


if __name__ == "__main__":
    sys.exit(main())
