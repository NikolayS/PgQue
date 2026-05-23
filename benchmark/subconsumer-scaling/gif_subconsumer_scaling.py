#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from PIL import Image

from chart_common import BG, FG, FG_DIM, FG_EMPH, SURFACE, PALETTE, load_meta, load_series, run_dirs

FPS = 12
DURATION_S = 8
FRAME_COUNT = FPS * DURATION_S
FIGSIZE = (9, 7)
DPI = 100


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/tmp/bench_subc")
    ap.add_argument("--out", default=None)
    ap.add_argument("--hero", default=None)
    return ap.parse_args()


def setup_style() -> None:
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


def load_all(root: Path):
    runs = []
    max_t = 0
    max_eps = 0
    max_backlog = 0
    for idx, run_dir in enumerate(run_dirs(root)):
        meta = load_meta(run_dir)
        tx, ty = load_series(run_dir / "events_consumed_per_sec.csv", "second_since_start", "events_consumed")
        bx, by = load_series(run_dir / "backlog_per_sec.csv", "second_since_start", "messages_remaining")
        runs.append((meta, tx, ty, bx, by, PALETTE[idx % len(PALETTE)]))
        max_t = max(max_t, max(tx or [0]), max(bx or [0]))
        max_eps = max(max_eps, max(ty or [0]), meta["ideal_ev_s"])
        max_backlog = max(max_backlog, max(by or [0]))
    return runs, max_t, max_eps, max_backlog


def clip_series(xs, ys, now):
    out_x, out_y = [], []
    for x, y in zip(xs, ys):
        if x <= now:
            out_x.append(x)
            out_y.append(y)
        else:
            break
    return out_x, out_y


def render_frame(runs, max_t, max_eps, max_backlog, frame_idx):
    now = (frame_idx / max(FRAME_COUNT - 1, 1)) * max_t
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGSIZE, dpi=DPI, constrained_layout=True)
    for meta, tx, ty, bx, by, color in runs:
        txc, tyc = clip_series(tx, ty, now)
        bxc, byc = clip_series(bx, by, now)
        ax1.step(txc, tyc, where="post", color=color, linewidth=2.0)
        ax1.axhline(meta["ideal_ev_s"], color=color, linestyle=":", linewidth=0.9, alpha=0.6)
        ax2.step(bxc, byc, where="post", color=color, linewidth=2.0,
                 label=f"{meta['workers']} workers")

    ax1.set_title("PgQue subconsumer scaling", loc="left", color=FG_EMPH, fontsize=14, fontweight="bold")
    ax1.text(0.0, 1.02, "Throughput rises. Backlog collapses.", transform=ax1.transAxes, color=FG)
    ax1.set_ylabel("events / second")
    ax1.set_xlim(0, max_t)
    ax1.set_ylim(0, max_eps * 1.12 if max_eps else 1)
    ax1.grid(True, axis="y", alpha=0.6)

    ax2.set_xlabel("seconds since consumer start")
    ax2.set_ylabel("messages remaining")
    ax2.set_xlim(0, max_t)
    ax2.set_ylim(0, max_backlog * 1.05 if max_backlog else 1)
    ax2.grid(True, axis="y", alpha=0.6)
    ax2.legend(frameon=False, loc="upper right")

    for ax in (ax1, ax2):
        ax.axvline(now, color=FG_DIM, linewidth=1.0, alpha=0.8)
        for sp in ("top", "right"):
            ax.spines[sp].set_visible(False)

    fig.canvas.draw()
    w, h = fig.canvas.get_width_height()
    image = Image.frombytes("RGBA", (w, h), fig.canvas.buffer_rgba().tobytes())
    plt.close(fig)
    return image.convert("RGB")


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    out = Path(args.out) if args.out else root / "scaling.gif"
    hero = Path(args.hero) if args.hero else root / "scaling_hero.png"
    setup_style()
    runs, max_t, max_eps, max_backlog = load_all(root)
    frames = [render_frame(runs, max_t, max_eps, max_backlog, i) for i in range(FRAME_COUNT)]
    for _ in range(FPS):
        frames.append(frames[-1])
    frames[-1].save(hero, format="PNG", optimize=True)

    gifski = shutil.which("gifski")
    if gifski:
        tmp = root / ".gif_frames"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        for idx, fr in enumerate(frames):
            fr.save(tmp / f"frame_{idx:04d}.png")
        subprocess.run([
            gifski, "-o", str(out), "--fps", str(FPS),
            "--width", str(int(FIGSIZE[0] * DPI)),
            "--quality", "80", "--lossy-quality", "90", "--extra",
            *sorted(str(p) for p in tmp.glob("*.png")),
        ], check=True)
        shutil.rmtree(tmp)
    else:
        pal = [f.quantize(colors=128, method=Image.MEDIANCUT, dither=Image.Dither.NONE) for f in frames]
        pal[0].save(out, save_all=True, append_images=pal[1:], duration=int(1000 / FPS), loop=0,
                    optimize=True, disposal=2)
    print(f"wrote {out}")
    print(f"wrote {hero}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
