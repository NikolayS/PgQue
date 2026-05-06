#!/usr/bin/env python3
"""Render coop_scaling CSV (stdin) to a matplotlib PNG.

Reads:
    subconsumers,events_per_sec,seconds

Writes:
    PNG path is the first positional argument.

Footer can be customized via environment variables:
    PGQUE_BENCH_PG_VERSION   — e.g. "PostgreSQL 18.3"
    PGQUE_BENCH_MACHINE      — e.g. "darwin/arm64, 16 cores"
    PGQUE_BENCH_EVENTS       — total events per run
    PGQUE_BENCH_PAYLOAD      — per-event payload size in bytes
"""

from __future__ import annotations

import csv
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: plot.py <output.png>", file=sys.stderr)
        return 1
    out = sys.argv[1]

    rows = []
    reader = csv.DictReader(sys.stdin)
    for row in reader:
        try:
            rows.append((int(row["subconsumers"]), float(row["events_per_sec"])))
        except (KeyError, ValueError):
            continue
    if not rows:
        print("plot.py: no rows on stdin", file=sys.stderr)
        return 2
    rows.sort(key=lambda r: r[0])
    xs = [r[0] for r in rows]
    ys = [r[1] for r in rows]

    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)
    ax.plot(xs, ys, marker="o", linewidth=2, color="#1f77b4")
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel("Cooperative subconsumers")
    ax.set_ylabel("Events / second (total)")
    ax.set_title("Cooperative consumer throughput scaling — TypeScript client")
    ax.grid(True, which="both", linestyle=":", alpha=0.5)
    ax.set_ylim(bottom=0)

    pg = os.environ.get("PGQUE_BENCH_PG_VERSION", "PostgreSQL").strip()
    machine = os.environ.get("PGQUE_BENCH_MACHINE", "").strip()
    events = os.environ.get("PGQUE_BENCH_EVENTS", "").strip()
    payload = os.environ.get("PGQUE_BENCH_PAYLOAD", "").strip()
    handler_ms = os.environ.get("PGQUE_BENCH_HANDLER_WORK_MS", "").strip()
    parts = [pg]
    if machine:
        parts.append(machine)
    if events:
        parts.append(f"{events} events/run")
    if payload:
        parts.append(f"~{payload} byte payload")
    if handler_ms:
        parts.append(f"~{handler_ms} ms handler work/msg")
    footer = "  ·  ".join(parts)
    fig.text(0.5, 0.01, footer, ha="center", fontsize=8, color="#444444")

    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
