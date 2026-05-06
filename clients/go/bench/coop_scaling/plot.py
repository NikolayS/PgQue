#!/usr/bin/env python3
"""Plot the cooperative-consumer scaling CSV produced by run.sh.

Usage:
    plot.py <csv-path> <out-png-path> <footer-text>

CSV header: subconsumers,events_per_sec,seconds
"""

import csv
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    csv_path, out_path, footer = sys.argv[1], sys.argv[2], sys.argv[3]

    xs: list[int] = []
    ys: list[float] = []
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            xs.append(int(row["subconsumers"]))
            ys.append(float(row["events_per_sec"]))

    if not xs:
        print("no data rows in CSV", file=sys.stderr)
        return 1

    fig, ax = plt.subplots(figsize=(8, 5), dpi=100)
    ax.plot(xs, ys, marker="o", linewidth=1.5)
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel("Cooperative subconsumers")
    ax.set_ylabel("Events / second (total across workers)")
    ax.set_title("Cooperative consumer throughput scaling -- Go client")
    ax.grid(True, which="both", linestyle=":", alpha=0.5)
    fig.text(0.5, 0.01, footer, ha="center", fontsize=8)

    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(out_path)
    print(f"wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
