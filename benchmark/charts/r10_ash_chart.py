#!/usr/bin/env python3
"""R10 ASH / Performance Insights chart — 8 panels including awa."""
import csv, re as _re
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path("/tmp/bench_r10")
OUT  = Path("/tmp/r10_ash_chart.png")

SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss", "awa"]
WORKERS = {"pgque":1, "pgq":1, "pgmq":4, "pgmq-partitioned":4, "river":4, "que":4, "pgboss":4, "awa":4}
CLEAN_END, TX_END = 600, 2400

# Solarized Dark backgrounds; pg_ash convention foregrounds
BG, SURF = "#002b36", "#073642"
FG, FG_EMPH, FG_DIM = "#839496", "#93a1a1", "#586e75"

# pg_ash color convention: github.com/NikolayS/pg_ash/blob/main/docs/COLOR_SCHEME.md
WET_COLORS = {
    "CPU":        "#50FA7B",   # green
    "IdleTx":     "#F1FA8C",   # light yellow (held tx)
    "IO":         "#1E64FF",   # blue
    "Lock":       "#FF5555",   # red
    "LWLock":     "#FF79C6",   # pink
    "IPC":        "#00C8FF",   # cyan
    "Client":     "#FFDC64",   # yellow
    "Timeout":    "#FFA500",   # orange
    "BufferPin":  "#00D2B4",   # teal
    "Activity":   "#9664FF",   # purple
    "Extension":  "#BE96FF",   # light purple
    "Other":      "#B4B4B4",   # gray
}
WET_ORDER = ["CPU", "IO", "LWLock", "Lock", "IPC", "BufferPin", "Activity", "Extension",
             "Client", "IdleTx", "Timeout", "Other"]

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG,
    "text.color": FG, "axes.labelcolor": FG_EMPH,
    "xtick.color": FG, "ytick.color": FG, "axes.edgecolor": FG_DIM,
    "grid.color": SURF, "grid.linewidth": 0.6,
    "font.family": ["Helvetica", "Arial", "DejaVu Sans"], "font.size": 9,
})

def parse_ts(s):
    s = s.strip().replace(" ", "T")
    if _re.search(r"[+-]\d\d$", s): s += ":00"
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

def load_ash(sys_name):
    p = ROOT / sys_name / "ash.csv"
    if not p.exists():
        print(f"  {sys_name}: no ash.csv"); return None
    rows = list(csv.DictReader(open(p)))
    if not rows: return None
    out = []
    for row in rows:
        try:
            ts = parse_ts(row["sample_time"])
            we = (row.get("wait_event") or "").strip()
            if we.startswith("CPU"):
                wet = "CPU"
            elif we == "Timeout:PgSleep":
                wet = "IdleTx"   # the held-xmin holder uses pg_sleep
            elif ":" in we:
                wet = we.split(":", 1)[0]
            else:
                wet = we or "CPU"
            if wet not in WET_COLORS:
                wet = "Other"
            out.append((ts, wet))
        except Exception:
            pass
    return out or None

def panel(ax, sys_name, samples):
    if not samples:
        ax.text(0.5, 0.5, f"{sys_name}: no ASH data",
                ha="center", va="center", transform=ax.transAxes, color=FG_DIM)
        for sp in ("top","right"): ax.spines[sp].set_visible(False)
        return
    t0 = min(s[0] for s in samples)
    BUCKET = 10
    by_bucket = defaultdict(Counter)
    for ts, wet in samples:
        bi = int((ts - t0) // BUCKET)
        by_bucket[bi][wet] += 1
    if not by_bucket: return
    max_bi = max(by_bucket)
    xs = np.arange(max_bi + 1) * BUCKET
    stacks = {w: np.array([by_bucket[b].get(w, 0)/BUCKET for b in range(max_bi+1)]) for w in WET_ORDER}
    present = [w for w in WET_ORDER if stacks[w].sum() > 0]
    if not present: return
    ys = np.vstack([stacks[w] for w in present])
    ax.stackplot(xs, ys, colors=[WET_COLORS[w] for w in present],
                 labels=present, edgecolor="none", alpha=0.92)

    ax.set_xlim(0, 3000)
    ax.axvspan(CLEAN_END, TX_END, color="#FFA500", alpha=0.06, zorder=0)
    ax.axvline(CLEAN_END, color=FG_DIM, ls="--", lw=0.6, alpha=0.7)
    ax.axvline(TX_END,    color=FG_DIM, ls="--", lw=0.6, alpha=0.7)
    ax.set_xticks([0, 600, 1200, 1800, 2400, 3000])
    ax.set_xticklabels(["0", "10m\nTX open", "20m", "30m", "40m\nTX close", "50m"], fontsize=7.5)
    ax.set_ylabel("avg active\nsessions", color=FG_EMPH, fontsize=8)
    peak = float(ys.sum(axis=0).max())
    w = WORKERS.get(sys_name, "?")
    native = "batch tick" if w == 1 else "polling workers"
    ax.set_title(f"{sys_name}   ·   {w} consumer {native}   ·   peak {peak:.1f} avg active sessions",
                 color=FG_EMPH, loc="left", fontsize=9.5, fontweight="bold")
    ax.grid(True, axis="y", alpha=0.5); ax.set_axisbelow(True)
    for sp in ("top","right"): ax.spines[sp].set_visible(False)

def main():
    fig, axs = plt.subplots(8, 1, figsize=(14, 18), dpi=130,
        gridspec_kw={"hspace":0.55, "top":0.92, "bottom":0.03, "left":0.07, "right":0.98})
    for ax, sname in zip(axs, SYSTEMS):
        panel(ax, sname, load_ash(sname))

    handles = [plt.Rectangle((0,0),1,1, color=WET_COLORS[w], alpha=0.92) for w in WET_ORDER]
    fig.legend(handles, WET_ORDER, loc="upper center", ncol=len(WET_ORDER),
               frameon=False, fontsize=9.5, bbox_to_anchor=(0.525, 0.962),
               labelcolor=FG_EMPH, handlelength=1.4, handletextpad=0.5, columnspacing=1.2)

    fig.text(0.07, 0.985,
      "R10 · ASH / Performance Insights view · 8 systems incl. awa · 10m clean + 30m idle-in-tx + 10m recovery · -R 2000 · PG 18 · i4i.2xlarge",
      ha="left", fontsize=12.5, fontweight="bold", color=FG_EMPH)
    fig.text(0.07, 0.972,
      "Colors: pg_ash convention (github.com/NikolayS/pg_ash/blob/main/docs/COLOR_SCHEME.md) · Y = avg active sessions per 10-s bucket · per-panel auto-scale",
      ha="left", fontsize=8.5, color=FG, style="italic")

    fig.savefig(OUT, dpi=130, bbox_inches="tight", facecolor=BG)
    print(f"wrote {OUT}")

if __name__ == "__main__":
    main()
