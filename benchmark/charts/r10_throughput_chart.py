#!/usr/bin/env python3
"""R10 throughput chart — 8-panel events/sec timeline per system."""
import csv, re as _re
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path("/tmp/bench_r10")
OUT  = Path("/tmp/r10_throughput_chart.png")
SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss", "awa"]
WORKERS = {"pgque":1, "pgq":1, "pgmq":4, "pgmq-partitioned":4, "river":4, "que":4, "pgboss":4, "awa":4}
CLEAN_END, TX_END, RUN_END = 600, 2400, 3000

BG, SURF = "#002b36", "#073642"
FG, FG_EMPH, FG_DIM = "#839496", "#93a1a1", "#586e75"
COLORS = {
    "pgque":            "#268bd2",
    "pgq":              "#2aa198",
    "pgmq":             "#dc322f",
    "pgmq-partitioned": "#d33682",
    "river":            "#cb4b16",
    "que":              "#6c71c4",
    "pgboss":           "#859900",
    "awa":              "#b58900",
}

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG,
    "text.color": FG, "axes.labelcolor": FG_EMPH,
    "xtick.color": FG, "ytick.color": FG, "axes.edgecolor": FG_DIM,
    "grid.color": SURF, "grid.linewidth": 0.6,
    "font.family": ["Helvetica", "Arial", "DejaVu Sans"], "font.size": 9,
})

def load_consumer(sys_name):
    p = ROOT / sys_name / "events_consumed_per_sec.csv"
    if not p.exists(): return None
    xs, ys = [], []
    with open(p) as f:
        r = csv.reader(f); next(r, None)
        for row in r:
            xs.append(int(row[0])); ys.append(int(row[1]))
    return np.array(xs), np.array(ys)

def smooth(ys, w=10):
    if len(ys) < w: return ys
    kernel = np.ones(w) / w
    return np.convolve(ys, kernel, mode="same")

def panel(ax, sys_name):
    d = load_consumer(sys_name)
    if d is None:
        ax.text(0.5, 0.5, f"{sys_name}: no data", ha="center", va="center",
                transform=ax.transAxes, color=FG_DIM); return
    xs, ys = d
    ys_smooth = smooth(ys, 10)
    ax.fill_between(xs, ys_smooth, 0, color=COLORS[sys_name], alpha=0.30)
    ax.plot(xs, ys_smooth, color=COLORS[sys_name], lw=1.2)

    ax.axhline(2000, color=FG_DIM, ls=":", lw=0.6, alpha=0.7)
    ax.text(RUN_END-50, 2000, "  -R 2000", color=FG_DIM, fontsize=7, va="bottom", ha="right")
    ax.axvspan(CLEAN_END, TX_END, color="#FFA500", alpha=0.06, zorder=0)
    ax.axvline(CLEAN_END, color=FG_DIM, ls="--", lw=0.6, alpha=0.7)
    ax.axvline(TX_END,    color=FG_DIM, ls="--", lw=0.6, alpha=0.7)

    ax.set_xlim(0, RUN_END)
    # Cap at 4x target rate so steady-state shape stays legible — recovery
    # burst spikes can hit 50k+ for some systems and would compress everything.
    YCAP = 8000
    raw_peak = ys.max() if len(ys) else 0
    ymax = min(max(ys_smooth.max() * 1.1, 2200), YCAP)
    ax.set_ylim(0, ymax)
    if raw_peak > ymax:
        ax.text(RUN_END * 0.99, ymax * 0.92,
                f"(burst peak {raw_peak/1000:.1f}k ev/s clipped)",
                color=FG_DIM, fontsize=7, ha="right", style="italic")
    ax.set_xticks([0, 600, 1200, 1800, 2400, 3000])
    ax.set_xticklabels(["0", "10m\nTX open", "20m", "30m", "40m\nTX close", "50m"], fontsize=7.5)
    ax.set_ylabel("ev / s", color=FG_EMPH, fontsize=8)

    avg_clean = ys[(xs >= 0)        & (xs < CLEAN_END)].mean() if len(ys) else 0
    avg_tx    = ys[(xs >= CLEAN_END) & (xs < TX_END)].mean() if len(ys) else 0
    avg_recov = ys[(xs >= TX_END)    & (xs < RUN_END)].mean() if len(ys) else 0
    ax.set_title(f"{sys_name}   ·   {WORKERS.get(sys_name)} consumers   ·   "
                 f"clean {avg_clean:.0f}  /  TX {avg_tx:.0f}  /  recov {avg_recov:.0f} ev/s",
                 color=FG_EMPH, loc="left", fontsize=9.5, fontweight="bold")
    ax.grid(True, axis="y", alpha=0.5); ax.set_axisbelow(True)
    for sp in ("top","right"): ax.spines[sp].set_visible(False)

def main():
    fig, axs = plt.subplots(8, 1, figsize=(14, 18), dpi=130,
        gridspec_kw={"hspace":0.55, "top":0.95, "bottom":0.03, "left":0.07, "right":0.98})
    for ax, s in zip(axs, SYSTEMS):
        panel(ax, s)

    fig.text(0.07, 0.985,
      "R10 · consumer throughput · 8 systems · -R 2000 producer · 10m clean + 30m idle-in-tx (orange band) + 10m recovery",
      ha="left", fontsize=12.5, fontweight="bold", color=FG_EMPH)
    fig.text(0.07, 0.972,
      "Per-panel header shows phase averages. Smoothed 10-s rolling mean (raw signal underneath shaded).",
      ha="left", fontsize=8.5, color=FG, style="italic")

    fig.savefig(OUT, dpi=130, bbox_inches="tight", facecolor=BG)
    print(f"wrote {OUT}")

if __name__ == "__main__":
    main()
