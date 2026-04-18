#!/usr/bin/env python3
"""R5 full — verdict table + 2-panel chart (dead tuples + consumer latency)."""
import csv, re
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

ROOT = Path("/tmp/bench_r5")
SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss"]
COLORS = {
    "pgque": "#1F4E79", "pgq": "#6B9BD1", "pgmq": "#C0392B",
    "pgmq-partitioned": "#D98880", "river": "#E67E22",
    "que": "#8E44AD", "pgboss": "#16A085",
}
EVENT_RE = {
    "pgque":            re.compile(r"^pgque\.event_\d+_\d+$"),
    "pgq":              re.compile(r"^pgq\.event_\d+_\d+$"),
    "pgmq":             re.compile(r"^pgmq\.q_bench_queue$"),
    "pgmq-partitioned": re.compile(r"^pgmq\.q_bench_queue_(p\d+_\d+|default)$"),
    "river":            re.compile(r"^public\.river_job$"),
    "que":              re.compile(r"^public\.que_jobs$"),
    "pgboss":           re.compile(r"^pgboss\.job_common$|^pgboss\.j[a-f0-9]+$"),
}
PROG_RE = re.compile(r"^progress:\s*([\d.]+)\s*s,\s*([\d.]+)\s*tps,\s*lat\s*([\d.]+)\s*ms\s*stddev\s*([\d.]+)")

def load_dead(sysn):
    p = ROOT / sysn / "bloat.csv"
    if not p.exists(): return np.array([]), np.array([])
    by_ts = {}
    with open(p) as f:
        r = csv.reader(f); next(r)
        for row in r:
            if not EVENT_RE[sysn].match(row[1]): continue
            by_ts.setdefault(row[0], 0); by_ts[row[0]] += int(row[3])
    if not by_ts: return np.array([]), np.array([])
    tss = sorted(by_ts)
    t0 = datetime.fromisoformat(tss[0].replace("Z",""))
    mins = np.array([(datetime.fromisoformat(t.replace("Z","")) - t0).total_seconds()/60 for t in tss])
    dead = np.array([by_ts[t] for t in tss], dtype=float)
    return mins, dead

def load_lat(sysn):
    p = ROOT / sysn / "consumer.log"
    if not p.exists(): return np.array([]), np.array([]), np.array([])
    secs, tps, lats = [], [], []
    with open(p) as f:
        for ln in f:
            m = PROG_RE.match(ln)
            if m:
                secs.append(float(m.group(1))/60.0)
                tps.append(float(m.group(2)))
                lats.append(float(m.group(3)))
    return np.array(secs), np.array(tps), np.array(lats)

# verdict table
print(f"\n{'system':<20} {'peak_dead':>12} {'avg_tps':>10} {'max_lat':>10} {'min_tps_TX':>11}")
print("-"*80)
for sysn in SYSTEMS:
    _, dead = load_dead(sysn)
    mins_lat, tps, lats = load_lat(sysn)
    peak = int(dead.max()) if dead.size else 0
    avg_tps = float(tps.mean()) if tps.size else 0
    max_lat = float(lats.max()) if lats.size else 0
    # TX window: 30-60 min (ideal) — actually started ~30 but check phases
    tx_mask = (mins_lat >= 32) & (mins_lat <= 58)
    min_tps_tx = float(tps[tx_mask].min()) if tx_mask.any() else 0
    print(f"{sysn:<20} {peak:>12,} {avg_tps:>10,.0f} {max_lat:>10.1f} {min_tps_tx:>11,.1f}")

# chart
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 7.6), dpi=110,
                                gridspec_kw={'height_ratios':[1.0, 0.58], 'hspace':0.32,
                                             'top':0.85, 'bottom':0.075, 'left':0.08, 'right':0.94})
for ax in (ax1, ax2):
    ax.axvspan(30, 60, color="#fdeeee", alpha=0.9, zorder=0)
    for b in (30, 60):
        ax.axvline(x=b, color="#ccc", lw=0.8, zorder=0.5)
    ax.set_xlim(0, 90)
    ax.set_xticks([0, 15, 30, 45, 60, 75, 90])
    ax.set_xticklabels(["0", "15m", "30m", "45m", "1h", "1h15", "1h30"])
    ax.grid(True, color="#e8e8e8", lw=0.8)
    ax.set_axisbelow(True)
    for sp in ("top","right"): ax.spines[sp].set_visible(False)
    # strict bottom at 0 — no below-axis whitespace
    ax.set_ymargin(0)

# Phase labels — sit in the whitespace between title and the top of ax1 (not overlapping title)
for (s, e, lbl, col) in [(0,30,"clean baseline · 30m","#888"),
                          (30,60,"xmin horizon blocked · 30m","#c0392b"),
                          (60,90,"clean recovery · 30m","#888")]:
    ax1.text((s+e)/2, 1.03, lbl, transform=ax1.get_xaxis_transform(),
             ha="center", color=col, fontsize=10,
             fontweight="bold" if col=="#c0392b" else "normal")

# Top: dead tuples
for sysn in SYSTEMS:
    mins, dead = load_dead(sysn)
    if mins.size == 0: continue
    lw = 3.0 if sysn=="pgque" else 2.0
    ax1.plot(mins, dead, color=COLORS[sysn], lw=lw, label=sysn, zorder=3 if sysn=="pgque" else 2)

def fmt_dead(v,_):
    v = abs(v)
    if v>=1e6: return f"{v/1e6:.1f}M"
    if v>=1e3: return f"{v/1e3:.0f}k"
    return f"{v:.0f}"
ax1.yaxis.set_major_formatter(FuncFormatter(fmt_dead))
ax1.set_ylabel("event-table dead tuples", labelpad=8)
ax1.legend(loc="upper left", frameon=False, fontsize=9, ncol=2)

# Bottom: latency (clipped)
LAT_CLIP = 2000
for sysn in SYSTEMS:
    mins, _, lats = load_lat(sysn)
    if mins.size == 0: continue
    y = np.minimum(lats, LAT_CLIP)
    lw = 3.0 if sysn=="pgque" else 2.0
    ax2.plot(mins, y, color=COLORS[sysn], lw=lw, zorder=3 if sysn=="pgque" else 2)

def fmt_lat(v,_):
    if v>=1000: return f"{v/1000:.1f}s"
    if v>=10: return f"{v:.0f}ms"
    if v>=1: return f"{v:.1f}ms"
    return f"{v:.2f}ms"
ax2.yaxis.set_major_formatter(FuncFormatter(fmt_lat))
ax2.set_ylabel("consumer latency (mean)", labelpad=8)

fig.text(0.08, 0.97, "R5 full — 7 systems · 1.5 h · held xmin horizon for 30 min",
         ha="left", fontsize=13, fontweight="bold")
fig.text(0.08, 0.948, "producer -R 1000, pgbench --aggregate-interval=10 --log · consumer latency clipped at 2s",
         ha="left", fontsize=9, color="#666")

fig.savefig("/tmp/r5_full_chart.png", dpi=110, bbox_inches="tight")
print(f"\nwrote /tmp/r5_full_chart.png ({Path('/tmp/r5_full_chart.png').stat().st_size/1024:.0f} KB)")
