#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from chart_common import BG, FG, FG_DIM, FG_EMPH, SURFACE, PALETTE, load_meta, load_series, run_dirs


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/tmp/bench_subc")
    ap.add_argument("--out", default=None)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    out = Path(args.out) if args.out else root / "backlog.png"

    plt.rcParams.update({
        "figure.facecolor": BG,
        "axes.facecolor": BG,
        "savefig.facecolor": BG,
        "text.color": FG,
        "axes.labelcolor": FG_EMPH,
        "xtick.color": FG,
        "ytick.color": FG,
        "axes.edgecolor": FG_DIM,
        "grid.color": SURFACE,
        "font.family": ["DejaVu Sans"],
        "font.size": 10,
    })

    fig, ax = plt.subplots(figsize=(12, 7), dpi=130)
    ymax = 0
    for idx, run_dir in enumerate(run_dirs(root)):
        meta = load_meta(run_dir)
        xs, ys = load_series(run_dir / "backlog_per_sec.csv", "second_since_start", "messages_remaining")
        color = PALETTE[idx % len(PALETTE)]
        ax.step(xs, ys, where="post", color=color, linewidth=2.1,
                label=f"{meta['workers']} workers · drained in {meta['wall_s']:.1f}s")
        ymax = max(ymax, max(ys or [0]))

    ax.set_title("PgQue subconsumer scaling · backlog drain", loc="left", color=FG_EMPH,
                 fontsize=14, fontweight="bold")
    ax.text(0.0, 1.02,
            "Same backlog every run. More subconsumers collapse drain time.",
            transform=ax.transAxes, color=FG, fontsize=10)
    ax.set_xlabel("seconds since consumer start")
    ax.set_ylabel("messages remaining")
    ax.set_ylim(0, ymax * 1.05 if ymax else 1)
    ax.grid(True, axis="y", alpha=0.6)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, loc="upper right")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)

    fig.savefig(out, bbox_inches="tight")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
