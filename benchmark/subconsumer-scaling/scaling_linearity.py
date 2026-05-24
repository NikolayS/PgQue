#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from chart_common import load_meta, run_dirs

BG = '#fbf7ef'
FG = '#222222'
DIM = '#666666'
GRID = '#ddd5c7'
IDEAL = '#b7ada0'
OBS = '#1f77b4'
ACCENT = '#8b1e1e'


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='/tmp/bench_subc_demo')
    ap.add_argument('--out', default=None)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    out = Path(args.out) if args.out else root / 'scaling_linearity.png'

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

    metas = [load_meta(d) for d in run_dirs(root)]
    workers = [m['workers'] for m in metas]
    observed = [m['avg_ev_s'] for m in metas]
    ideal = [m['ideal_ev_s'] for m in metas]
    efficiency = [m['efficiency'] or 0 for m in metas]

    fig, ax = plt.subplots(figsize=(9.2, 5.8), dpi=140, constrained_layout=True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)

    bars = ax.bar(workers, observed, width=1.15, color=OBS, alpha=0.88,
                  edgecolor=OBS, linewidth=0.6, label='observed avg throughput')
    ax.plot(workers, ideal, color=IDEAL, linewidth=1.8, linestyle='--',
            marker='o', markersize=3.5, zorder=3, label='ideal: 4 × workers msg/s')

    for bar, x, y, e in zip(bars, workers, observed, efficiency):
        ax.text(x, y + max(observed) * 0.03, f'{y:.1f}', ha='center', va='bottom', color=OBS, fontsize=9)
        ax.text(x, max(y * 0.55, max(observed) * 0.05), f'{e*100:.1f}% eff', ha='center', va='center', color=BG, fontsize=8.5)

    ax.set_xlim(min(workers) - 0.5, max(workers) + 1.8)
    ax.set_ylim(0, max(ideal) * 1.14)
    ax.set_xticks(workers)
    ax.set_xlabel('subconsumers')
    ax.set_ylabel('messages / second')
    ax.set_title('Throughput scales near-linearly with subconsumer count', loc='left', fontsize=15, color=FG, pad=18)
    ax.text(0.0, 1.02,
            'Same 160-message backlog. Same 250 ms email-provider stand-in per message. Only parallelism changes.',
            transform=ax.transAxes, ha='left', va='bottom', fontsize=10.5, color=DIM)
    ax.text(0.98, 0.96,
            'One worker ≈ 4 msg/s\nIdeal line = 4 × workers',
            transform=ax.transAxes, ha='right', va='top', fontsize=10, color=ACCENT)
    ax.legend(frameon=False, loc='upper left')

    fig.savefig(out, bbox_inches='tight')
    print(f'wrote {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
