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
    out = Path(args.out) if args.out else root / 'single_message_latency.png'

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
    observed_ms = [1000.0 / m['avg_ev_s'] for m in metas]
    ideal_ms = [1000.0 / m['ideal_ev_s'] for m in metas]
    efficiency = [m['efficiency'] or 0 for m in metas]

    fig, ax = plt.subplots(figsize=(9.2, 5.8), dpi=140, constrained_layout=True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)

    ax.plot(workers, ideal_ms, color=IDEAL, linewidth=2.0, linestyle='--', marker='o', markersize=4,
            label='ideal: 250 ms / workers')
    ax.plot(workers, observed_ms, color=OBS, linewidth=2.6, marker='o', markersize=6,
            label='observed effective latency')

    y_pad = max(observed_ms) * 0.035
    for x, y, e in zip(workers, observed_ms, efficiency):
        ax.text(x, y + y_pad, f'{y:.1f} ms', ha='center', va='bottom', color=OBS, fontsize=9)
        ax.text(x, y - y_pad * 1.8, f'{e*100:.1f}% eff', ha='center', va='top', color=DIM, fontsize=8.5)

    ax.set_xlim(0, max(workers) + 1.5)
    ax.set_xticks(workers)
    ax.set_xlabel('subconsumers')
    ax.set_ylabel('effective latency per delivered message (ms)')
    ax.set_ylim(0, max(observed_ms) * 1.16)
    ax.set_title('Single-message latency falls almost exactly as 1 / subconsumers', loc='left', fontsize=15, color=FG, pad=18)
    ax.text(0.0, 1.02,
            'Same 160-message backlog. Same 250 ms email-provider stand-in per message. Only parallelism changes.',
            transform=ax.transAxes, ha='left', va='bottom', fontsize=10.5, color=DIM)
    ax.text(0.98, 0.96,
            'Equivalent statement:\nthroughput scales near-linearly',
            transform=ax.transAxes, ha='right', va='top', fontsize=10, color=ACCENT)
    ax.legend(frameon=False, loc='upper right')

    fig.savefig(out, bbox_inches='tight')
    print(f'wrote {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
