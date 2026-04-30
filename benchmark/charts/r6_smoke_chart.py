#!/usr/bin/env python3
"""r6_smoke_chart — events consumed/s + pgque table dead tuples."""
import csv, re
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

ROOT = Path("/tmp/bench_r6_combined")
OUT = Path("/tmp/r6_smoke_chart.png")

# Phase windows for the 20-min smoke (seconds): clean 0-300, xmin 300-900, recovery 900-1200
PHASE_TX = (300, 900)
X_MAX_S = 1200

# Load events_consumed_per_sec.csv
ev_t, ev_rate = [], []
with open(ROOT / "events_consumed_per_sec.csv") as f:
    r = csv.reader(f); next(r)
    for row in r:
        ev_t.append(int(row[0]))
        ev_rate.append(int(row[1]))
ev_t = np.array(ev_t); ev_rate = np.array(ev_rate)

# Load bloat.csv — track key pgque tables over time
TRACKED = {
    "event_1_0": "#268bd2",  # blue (line like pgque accent)
    "event_1_1": "#2aa198",  # cyan
    "event_1_2": "#859900",  # green
    "subscription_0": "#cb4b16",  # orange
    "subscription_1": "#dc322f",  # red
    "subscription_2": "#d33682",  # magenta
    "tick_0": "#6c71c4",  # violet
    "tick_1": "#b58900",  # yellow
    "tick_2": "#93a1a1",  # base1
    "meta_rotation": "#586e75",  # base01
    "queue": "#eee8d5",  # base2
}
tbl_series = {t: ([],[],[]) for t in TRACKED}  # (t_seconds, n_live, n_dead)
t0 = None
with open(ROOT / "bloat.csv") as f:
    r = csv.reader(f); next(r)
    for row in r:
        relname_full = row[1]
        relname = relname_full.split(".")[-1]
        if relname not in TRACKED: continue
        ts = datetime.fromisoformat(row[0].replace("Z",""))
        if t0 is None: t0 = ts
        s = (ts - t0).total_seconds()
        tbl_series[relname][0].append(s)
        tbl_series[relname][1].append(int(row[2]))  # n_live_tup
        tbl_series[relname][2].append(int(row[3]))  # n_dead_tup

# --- Solarized Dark palette ---
BG = "#002b36"; SURF = "#073642"
FG = "#839496"; FG_EMPH = "#93a1a1"; FG_DIM = "#586e75"
ALERT = "#dc322f"

plt.rcParams.update({
    'figure.facecolor': BG, 'axes.facecolor': BG, 'savefig.facecolor': BG,
    'text.color': FG, 'axes.labelcolor': FG_EMPH,
    'xtick.color': FG, 'ytick.color': FG,
    'axes.edgecolor': FG_DIM,
    'grid.color': SURF, 'grid.linewidth': 0.8,
    'font.family': ['Helvetica', 'Arial', 'DejaVu Sans'],
    'font.size': 10,
})

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 6.5), dpi=110,
    gridspec_kw={'height_ratios':[1.0, 0.75], 'hspace':0.35,
                 'top':0.91, 'bottom':0.09, 'left':0.09, 'right':0.94})

for ax in (ax1, ax2):
    ax.axvspan(PHASE_TX[0], PHASE_TX[1], color=SURF, alpha=1.0, zorder=0)
    for b in PHASE_TX: ax.axvline(x=b, color=FG_DIM, lw=0.8, zorder=0.5)
    ax.set_xlim(0, X_MAX_S)
    ax.set_xticks([0, 300, 600, 900, 1200])
    ax.set_xticklabels(["0", "5m", "10m", "15m", "20m"])
    ax.grid(True); ax.set_axisbelow(True)
    for sp in ("top","right"): ax.spines[sp].set_visible(False)
    for sp in ("left","bottom"): ax.spines[sp].set_color(FG_DIM)

# Phase labels
for (s, e, lbl, col) in [(0, PHASE_TX[0], "clean baseline · 5m", FG_DIM),
                          (PHASE_TX[0], PHASE_TX[1], "xmin horizon blocked · 10m", ALERT),
                          (PHASE_TX[1], X_MAX_S, "clean recovery · 5m", FG_DIM)]:
    ax1.text((s+e)/2, 1.04, lbl, transform=ax1.get_xaxis_transform(),
             ha="center", color=col, fontsize=10,
             fontweight="bold" if col==ALERT else "normal")

# Top: events consumed/sec
ax1.plot(ev_t, ev_rate, color="#268bd2", lw=1.5, alpha=0.9)
ax1.axhline(1000, color=FG_DIM, ls="--", lw=0.8, alpha=0.7)
ax1.text(X_MAX_S*0.98, 1020, "producer rate 1000 ev/s (-R 1000)",
         color=FG_DIM, ha="right", fontsize=9, fontstyle="italic")
ax1.set_ylabel("events consumed / s", labelpad=8, color=FG_EMPH)
ax1.set_ylim(0, max(2200, ev_rate.max()*1.1))
ax1.yaxis.set_major_formatter(FuncFormatter(lambda v,_: f"{v:.0f}"))

# Bottom: dead tuples per pgque table
for name, color in TRACKED.items():
    t_s, nlive, ndead = tbl_series[name]
    if not t_s: continue
    ax2.plot(t_s, ndead, color=color, lw=2.0, label=name, alpha=0.95)

ax2.set_ylabel("n_dead_tup per pgque table", labelpad=8, color=FG_EMPH)
ax2.yaxis.set_major_formatter(FuncFormatter(lambda v,_: f"{v:.0f}"))
# Legend top-right
ax2.legend(loc="upper left", frameon=False, fontsize=8, ncol=3, labelcolor=FG)
# Expand y so zero lines are visible; max is meta_rotation=33
ax2.set_ylim(0, max(50, max([max(s[2]) for s in tbl_series.values() if s[2]], default=10)*1.15))

# Titles
fig.text(0.09, 0.965, "pgque smoke — PR #62 rotation + events-consumed instrumentation",
         ha="left", fontsize=12, fontweight="bold", color=FG_EMPH)
fig.text(0.09, 0.94, "20 min (5m clean + 10m held xmin + 5m recovery). Event-rate: producer was rate-capped at 1000 ev/s.",
         ha="left", fontsize=9, color=FG_DIM)

fig.savefig(OUT, dpi=110, bbox_inches="tight", facecolor=BG)
print(f"wrote {OUT}  ({OUT.stat().st_size/1024:.0f} KiB)")
