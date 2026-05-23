#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from chart_common import load_meta, run_dirs


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/tmp/bench_subc")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    metas = [load_meta(d) for d in run_dirs(root)]
    baseline = metas[0]["wall_s"] if metas else 0
    print("| workers | expected ev/s | observed avg ev/s | drain time s | speedup vs 1 | efficiency |")
    print("|---:|---:|---:|---:|---:|---:|")
    for meta in metas:
        speedup = (baseline / meta["wall_s"]) if meta["wall_s"] > 0 and baseline else 0
        eff = meta["efficiency"] or 0
        print(
            f"| {meta['workers']} | {meta['ideal_ev_s']:.1f} | {meta['avg_ev_s']:.1f} | "
            f"{meta['wall_s']:.2f} | {speedup:.2f} | {eff:.3f} |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
