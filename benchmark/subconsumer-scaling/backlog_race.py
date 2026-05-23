#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from PIL import Image

from chart_common import load_meta, load_series, run_dirs

BG = '#fbf7ef'
FG = '#222222'
DIM = '#666666'
GRID = '#ddd5c7'
COLORS = ['#222222', '#666666', '#1f77b4', '#d95f02', '#2ca02c', '#9467bd']
FPS = 12
DURATION_S = 7
FRAME_COUNT = FPS * DURATION_S
FIGSIZE = (10, 6)
DPI = 130


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='/tmp/bench_subc_demo')
    ap.add_argument('--png', default=None)
    ap.add_argument('--gif', default=None)
    ap.add_argument('--hero', default=None)
    return ap.parse_args()


def setup_style() -> None:
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


def load_runs(root: Path):
    runs = []
    max_t = 0
    max_backlog = 0
    for idx, run_dir in enumerate(run_dirs(root)):
        meta = load_meta(run_dir)
        xs, ys = load_series(run_dir / 'backlog_per_sec.csv', 'second_since_start', 'messages_remaining')
        runs.append((meta, xs, ys, COLORS[idx % len(COLORS)]))
        max_t = max(max_t, max(xs or [0]), meta['wall_s'])
        max_backlog = max(max_backlog, max(ys or [0]))
    return runs, max_t, max_backlog


def clip_series(xs, ys, now):
    out_x, out_y = [], []
    for x, y in zip(xs, ys):
        if x <= now:
            out_x.append(x)
            out_y.append(y)
        else:
            break
    if out_x and out_x[-1] < now:
        out_x.append(now)
        out_y.append(out_y[-1])
    return out_x, out_y


def label_lines(ax, runs, now=None, final=False):
    min_gap = 6
    labels = []
    for meta, xs, ys, color in runs:
        if now is None:
            x_end, y_end = xs[-1], ys[-1]
        else:
            xc, yc = clip_series(xs, ys, now)
            x_end, y_end = (xc[-1], yc[-1]) if xc else (0, ys[0])
        labels.append([y_end, x_end, meta, color])
    labels.sort(key=lambda t: t[0])
    for i in range(1, len(labels)):
        if labels[i][0] - labels[i-1][0] < min_gap:
            labels[i][0] = labels[i-1][0] + min_gap
    for y_lab, x_end, meta, color in labels:
        txt = f"{meta['workers']} → {meta['wall_s']:.1f}s"
        if final:
            txt = f"{meta['workers']} workers  {meta['wall_s']:.1f}s"
        ax.text(x_end + 0.6, y_lab, txt, color=color, va='center', ha='left', fontsize=9.5)


def draw(now=None, final=False, *, runs, max_t, max_backlog):
    fig, ax = plt.subplots(figsize=FIGSIZE, dpi=DPI, constrained_layout=True)
    ax.set_xlim(0, max_t * 1.20)
    ax.set_ylim(0, max_backlog * 1.03)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel('seconds')
    ax.set_ylabel('messages remaining')

    ax.set_title('Same backlog, same 250 ms/message, more subconsumers drain it faster',
                 loc='left', fontsize=15, color=FG, pad=18)
    ax.text(0.0, 1.02,
            '160 messages queued at t=0. One worker can do ~4 msg/s. The rest is parallelism.',
            transform=ax.transAxes, ha='left', va='bottom', fontsize=10.5, color=DIM)

    ax.axhline(max_backlog, color=GRID, linewidth=0.8)
    ax.text(0, max_backlog + max_backlog * 0.015, f'{max_backlog} queued', color=DIM, fontsize=9)

    for meta, xs, ys, color in runs:
        ax.plot(xs, ys, color=color, linewidth=1.1, alpha=0.18, zorder=1)
        if now is None:
            xp, yp = xs, ys
        else:
            xp, yp = clip_series(xs, ys, now)
        ax.plot(xp, yp, color=color, linewidth=2.5, zorder=2)
        if xp:
            ax.scatter([xp[-1]], [yp[-1]], color=color, s=18, zorder=3)

    if now is not None and not final:
        ax.axvline(now, color=GRID, linewidth=1.0, zorder=0)

    label_lines(ax, runs, now=now, final=final)

    # small note for the key idea
    ax.text(0.98, 0.96, '1 worker ≈ 4 msg/s', transform=ax.transAxes,
            ha='right', va='top', fontsize=10, color=DIM)
    return fig


def make_static(png: Path, *, runs, max_t, max_backlog):
    fig = draw(final=True, runs=runs, max_t=max_t, max_backlog=max_backlog)
    fig.savefig(png, bbox_inches='tight')
    plt.close(fig)


def make_gif(gif: Path, hero: Path, *, runs, max_t, max_backlog):
    frames = []
    for i in range(FRAME_COUNT):
        now = (i / max(FRAME_COUNT - 1, 1)) * max_t
        fig = draw(now=now, runs=runs, max_t=max_t, max_backlog=max_backlog)
        fig.canvas.draw()
        w, h = fig.canvas.get_width_height()
        frame = Image.frombytes('RGBA', (w, h), fig.canvas.buffer_rgba().tobytes()).convert('RGB')
        plt.close(fig)
        frames.append(frame)
    for _ in range(FPS):
        frames.append(frames[-1])
    frames[-1].save(hero, format='PNG', optimize=True)

    gifski = shutil.which('gifski')
    if gifski:
        tmp = gif.parent / '.race_frames'
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        for idx, fr in enumerate(frames):
            fr.save(tmp / f'frame_{idx:04d}.png')
        subprocess.run([
            gifski, '-o', str(gif), '--fps', str(FPS), '--width', str(w),
            '--quality', '90', '--lossy-quality', '100', '--extra',
            *sorted(str(p) for p in tmp.glob('*.png')),
        ], check=True)
        shutil.rmtree(tmp)
    else:
        pal = [f.quantize(colors=96, method=Image.MEDIANCUT, dither=Image.Dither.NONE) for f in frames]
        pal[0].save(gif, save_all=True, append_images=pal[1:], duration=int(1000/FPS), loop=0,
                    optimize=True, disposal=2)


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    png = Path(args.png) if args.png else root / 'backlog_race.png'
    gif = Path(args.gif) if args.gif else root / 'backlog_race.gif'
    hero = Path(args.hero) if args.hero else root / 'backlog_race_hero.png'
    setup_style()
    runs, max_t, max_backlog = load_runs(root)
    make_static(png, runs=runs, max_t=max_t, max_backlog=max_backlog)
    make_gif(gif, hero, runs=runs, max_t=max_t, max_backlog=max_backlog)
    print(f'wrote {png}')
    print(f'wrote {gif}')
    print(f'wrote {hero}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
