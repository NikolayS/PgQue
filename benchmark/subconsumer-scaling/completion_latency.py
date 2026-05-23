#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from chart_common import load_meta, run_dirs

BG = '#fbf7ef'
FG = '#222222'
DIM = '#666666'
GRID = '#ddd5c7'
COLORS = ['#222222', '#666666', '#1f77b4', '#d95f02', '#2ca02c', '#9467bd']


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='/tmp/bench_subc_demo')
    ap.add_argument('--out', default=None)
    return ap.parse_args()


def load_completion_curve(run_dir: Path):
    xs = []
    ys = []
    cumulative = 0
    with (run_dir / 'events_consumed_per_sec.csv').open() as f:
        r = csv.DictReader(f)
        for row in r:
            sec = int(row['second_since_start'])
            ev = int(row['events_consumed'])
            for _ in range(ev):
                cumulative += 1
                xs.append(cumulative)
                ys.append(sec)
    return xs, ys


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    out = Path(args.out) if args.out else root / 'completion_latency.png'

    plt.rcParams.update({
        'figure.facecolor': BG,
        'axes.facecolor': BG,
        'savefig.facecolor': BG,
        'text.color': FG,
        'axes.labelcolor': FG,
        'xtick.color': DIM,
        'ytick.color': DIM,
        'axes.edgecolor': GRID,
        'font.family': ['DejaVu Serif'],
        'font.size': 10,
    })

    fig, ax = plt.subplots(figsize=(9.5, 6), dpi=140, constrained_layout=True)
    max_x = 0
    max_y = 0
    lines = []

    for idx, run_dir in enumerate(run_dirs(root)):
        meta = load_meta(run_dir)
        xs, ys = load_completion_curve(run_dir)
        color = COLORS[idx % len(COLORS)]
        ax.plot(xs, ys, color=color, linewidth=2.3)
        max_x = max(max_x, max(xs or [0]))
        max_y = max(max_y, max(ys or [0]))
        lines.append((meta, xs, ys, color))

    ax.set_xlim(0, max_x * 1.18 if max_x else 1)
    ax.set_ylim(0, max_y * 1.06 if max_y else 1)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel('message number in the backlog')
    ax.set_ylabel('seconds until completion')
    ax.set_title('Completion latency by queue position', loc='left', fontsize=15, color=FG, pad=18)
    ax.text(0.0, 1.02,
            'Same 160-message backlog. One line per subconsumer count. Lower is better.',
            transform=ax.transAxes, ha='left', va='bottom', fontsize=10.5, color=DIM)

    # direct end labels with simple anti-overlap
    labels = []
    for meta, xs, ys, color in lines:
        labels.append([ys[-1], xs[-1], meta, color])
    labels.sort(key=lambda t: t[0])
    min_gap = max_y * 0.04 if max_y else 1
    for i in range(1, len(labels)):
        if labels[i][0] - labels[i-1][0] < min_gap:
            labels[i][0] = labels[i-1][0] + min_gap
    for y_lab, x_end, meta, color in labels:
        ax.text(x_end + max_x * 0.02, y_lab,
                f"{meta['workers']} workers  {meta['wall_s']:.1f}s",
                color=color, va='center', ha='left', fontsize=9.5)

    ax.text(0.98, 0.96, '250 ms/message → ~4 msg/s per worker',
            transform=ax.transAxes, ha='right', va='top', fontsize=10, color=DIM)

    fig.savefig(out, bbox_inches='tight')
    print(f'wrote {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
