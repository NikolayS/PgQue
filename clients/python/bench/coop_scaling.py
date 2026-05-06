#!/usr/bin/env python3
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Scaling benchmark for cooperative consumers (pgque-py).

Measures total events/sec as ``N`` cooperative subconsumers drain a
fixed-size queue under one logical consumer. Renders a PNG chart next to
the script.

Usage::

    PGQUE_TEST_DSN=postgres://localhost/pgque_coop_py \\
        python3 clients/python/bench/coop_scaling.py \\
            --subconsumers 1 2 4 8 16 \\
            --events 5000 --payload 64 --runs 3
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import os
import platform
import secrets
import statistics
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pgque

DEFAULT_SUBCONSUMERS = (1, 2, 4, 8, 16)
DEFAULT_EVENTS = 5_000
DEFAULT_PAYLOAD_BYTES = 64
DEFAULT_RUNS = 3
PUBLISH_BATCH = 500
# Number of ticks to spread events across. PgQ delivers each batch to
# exactly one cooperative subconsumer, so a single tick serializes the
# whole workload. Producing multiple batches lets workers process them
# in parallel and is what the chart is meant to illustrate.
DEFAULT_TICKS = 16
CHART_PATH = Path(__file__).resolve().parent / "coop_scaling.png"


@dataclass(frozen=True)
class RunResult:
    subconsumers: int
    events_per_sec: float
    seconds: float


def _make_payload(n_bytes: int) -> dict:
    """Build a JSON payload that round-trips to about ``n_bytes``."""
    filler_size = max(0, n_bytes - 16)
    return {"d": "x" * filler_size}


def _publish_events(
    dsn: str,
    queue: str,
    n_events: int,
    payload_bytes: int,
    n_ticks: int = DEFAULT_TICKS,
) -> None:
    """Pre-publish ``n_events`` events spread across ``n_ticks`` batches.

    Each ``force_next_tick`` + ``ticker`` materializes a batch. PgQ
    cooperative consumers split work across batches, so handing the
    consumer many small batches up front is what lets parallel
    subconsumers actually do work in parallel.
    """
    payload = _make_payload(payload_bytes)
    per_tick = max(1, n_events // max(1, n_ticks))
    sent = 0
    with pgque.connect(dsn) as client:
        while sent < n_events:
            chunk = min(per_tick, n_events - sent)
            for inner_start in range(0, chunk, PUBLISH_BATCH):
                inner = min(PUBLISH_BATCH, chunk - inner_start)
                client.send_batch(queue, "bench.coop", [payload] * inner)
                client.conn.commit()
            client.force_next_tick(queue)
            client.conn.execute("select pgque.ticker(%s)", (queue,))
            client.conn.commit()
            sent += chunk


@contextlib.contextmanager
def _bench_queue(dsn: str, consumer_name: str, subconsumers: Iterable[str]):
    queue = f"coopbench_{secrets.token_hex(4)}"
    with pgque.connect(dsn) as setup:
        setup.conn.execute("select pgque.create_queue(%s)", (queue,))
        setup.conn.commit()
        for sub in subconsumers:
            setup.subscribe_subconsumer(queue, consumer_name, sub)
        setup.conn.commit()
    try:
        yield queue
    finally:
        with pgque.connect(dsn) as cleanup:
            for sub in subconsumers:
                try:
                    cleanup.unsubscribe_subconsumer(
                        queue, consumer_name, sub, batch_handling=1
                    )
                    cleanup.conn.commit()
                except pgque.PgqueError:
                    cleanup.conn.rollback()
            try:
                cleanup.conn.execute(
                    "select pgque.drop_queue(%s, true)", (queue,)
                )
                cleanup.conn.commit()
            except pgque.PgqueError:
                cleanup.conn.rollback()


def _worker_loop(
    dsn: str,
    queue: str,
    consumer_name: str,
    subconsumer: str,
    target: int,
    counter: list[int],
    counter_lock: threading.Lock,
    stop_evt: threading.Event,
) -> None:
    """Tight ``receive_coop`` -> ``ack`` loop until ``target`` is hit.

    Uses an autocommit psycopg connection: holding ``FOR UPDATE`` on the
    cooperative main row past ``receive_coop`` deadlocks parallel
    workers, as flagged in the cooperative consumers PR.
    """
    with pgque.connect(dsn, autocommit=True) as client:
        while not stop_evt.is_set():
            with counter_lock:
                if counter[0] >= target:
                    return
            try:
                msgs = client.receive_coop(
                    queue, consumer_name, subconsumer, max_messages=10_000
                )
            except pgque.PgqueError:
                # In the rare race where ``drop_queue`` already removed
                # the row, exit cleanly. The harness handles cleanup.
                return
            if not msgs:
                # No batch available right now; back off briefly and
                # retry. force_next_tick was called once before workers
                # started, so this only happens after the queue drains.
                time.sleep(0.005)
                continue
            client.ack(msgs[0].batch_id)
            with counter_lock:
                counter[0] += len(msgs)


def _run_once(
    dsn: str,
    n_events: int,
    payload_bytes: int,
    n_workers: int,
    consumer_name: str,
) -> float:
    subconsumers = [f"worker-{i}" for i in range(n_workers)]
    with _bench_queue(dsn, consumer_name, subconsumers) as queue:
        _publish_events(dsn, queue, n_events, payload_bytes)

        counter = [0]
        counter_lock = threading.Lock()
        stop_evt = threading.Event()

        threads = [
            threading.Thread(
                target=_worker_loop,
                args=(
                    dsn,
                    queue,
                    consumer_name,
                    sub,
                    n_events,
                    counter,
                    counter_lock,
                    stop_evt,
                ),
                name=f"bench-{sub}",
                daemon=True,
            )
            for sub in subconsumers
        ]

        start = time.perf_counter()
        for t in threads:
            t.start()

        # Reap workers as they finish; bail out if the run hangs.
        deadline = start + 120.0
        for t in threads:
            timeout = max(0.0, deadline - time.monotonic())
            t.join(timeout=timeout)
            if t.is_alive():
                stop_evt.set()
        elapsed = time.perf_counter() - start

        if counter[0] < n_events:
            stop_evt.set()
            for t in threads:
                t.join(timeout=5.0)
            raise RuntimeError(
                f"only acked {counter[0]}/{n_events} events in "
                f"{elapsed:.1f}s with {n_workers} subconsumers"
            )
        return elapsed


def _measure(
    dsn: str,
    n_events: int,
    payload_bytes: int,
    n_workers: int,
    runs: int,
    consumer_name: str,
) -> RunResult:
    durations: list[float] = []
    for _ in range(runs):
        durations.append(
            _run_once(
                dsn, n_events, payload_bytes, n_workers, consumer_name
            )
        )
    median_s = statistics.median(durations)
    return RunResult(
        subconsumers=n_workers,
        seconds=median_s,
        events_per_sec=n_events / median_s if median_s > 0 else float("inf"),
    )


def _server_version(dsn: str) -> str:
    with pgque.connect(dsn) as client:
        row = client.conn.execute("show server_version").fetchone()
    return row[0] if row else "unknown"


def _render_chart(
    results: list[RunResult],
    server_version: str,
    n_events: int,
    payload_bytes: int,
    out_path: Path,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    xs = [r.subconsumers for r in results]
    ys = [r.events_per_sec for r in results]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(xs, ys, marker="o", linewidth=2)
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.get_xaxis().set_major_formatter(
        matplotlib.ticker.ScalarFormatter()
    )
    ax.set_xlabel("Cooperative subconsumers (N)")
    ax.set_ylabel("Throughput (events/sec)")
    ax.set_title("Cooperative consumer throughput scaling -- Python client")
    ax.grid(True, which="both", linestyle="--", alpha=0.4)

    machine = (
        f"{platform.system()} {platform.machine()} / "
        f"{os.cpu_count() or '?'} CPU"
    )
    footer = (
        f"PG {server_version} | {machine} | "
        f"{n_events} events, {payload_bytes} byte payload"
    )
    fig.text(0.5, 0.01, footer, ha="center", fontsize=8, color="#555")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(out_path, dpi=100)
    plt.close(fig)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--subconsumers",
        type=int,
        nargs="+",
        default=list(DEFAULT_SUBCONSUMERS),
        help="List of N values to benchmark (default: 1 2 4 8 16).",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=DEFAULT_EVENTS,
        help="Total events per run (default: 5000).",
    )
    parser.add_argument(
        "--payload",
        type=int,
        default=DEFAULT_PAYLOAD_BYTES,
        help="Approximate payload size in bytes (default: 64).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help="Number of runs per N; reported as median (default: 3).",
    )
    parser.add_argument(
        "--chart",
        default=str(CHART_PATH),
        help=f"Output PNG path (default: {CHART_PATH}).",
    )
    parser.add_argument(
        "--no-chart",
        action="store_true",
        help="Skip PNG rendering (CSV only).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    dsn = os.environ.get("PGQUE_TEST_DSN")
    if not dsn:
        print(
            "PGQUE_TEST_DSN not set; refusing to run scaling benchmark",
            file=sys.stderr,
        )
        return 1

    consumer_name = f"coopbench_{secrets.token_hex(2)}"
    server_version = _server_version(dsn)

    results: list[RunResult] = []
    for n in args.subconsumers:
        result = _measure(
            dsn,
            args.events,
            args.payload,
            n,
            args.runs,
            consumer_name,
        )
        results.append(result)
        print(
            f"# n={n}: {result.events_per_sec:,.0f} ev/s "
            f"({result.seconds:.3f}s median of {args.runs})",
            file=sys.stderr,
            flush=True,
        )

    writer = csv.writer(sys.stdout)
    writer.writerow(["subconsumers", "events_per_sec", "seconds"])
    for r in results:
        writer.writerow([r.subconsumers, f"{r.events_per_sec:.2f}",
                         f"{r.seconds:.4f}"])

    if not args.no_chart:
        out_path = Path(args.chart)
        _render_chart(
            results,
            server_version,
            args.events,
            args.payload,
            out_path,
        )
        print(f"# chart written to {out_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
