#!/usr/bin/env python3
"""R10 system metrics chart — CPU + NVMe write per system over time."""
import csv
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path("/tmp/bench_r10")
OUT  = Path("/tmp/r10_sysmetrics_chart.png")
SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss", "awa"]
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

def load_metrics(sys_name):
    p = ROOT / sys_name / "sys_metrics.csv"
    if not p.exists(): return None
    rows = list(csv.DictReader(open(p)))
    if not rows: return None
    t0 = datetime.fromisoformat(rows[0]["ts_iso"].replace("Z","+00:00")).timestamp()
    out = {"t": [], "cpu": [], "iowait": [], "wmib": [], "wiops": []}
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["ts_iso"].replace("Z","+00:00")).timestamp()
            out["t"].append(ts - t0)
            out["cpu"].append(float(r["cpu_user_pct"]) + float(r["cpu_system_pct"]))
            out["iowait"].append(float(r["cpu_iowait_pct"]))
            out["wmib"].append(float(r["disk_write_mib_s"]))
            out["wiops"].append(float(r["disk_write_iops"]))
        except: pass
    return {k: np.array(v) for k, v in out.items()}

def add_phase_bands(ax):
    ax.axvspan(CLEAN_END, TX_END, color="#FFA500", alpha=0.06, zorder=0)
    ax.axvline(CLEAN_END, color=FG_DIM, ls="--", lw=0.6, alpha=0.7)
    ax.axvline(TX_END,    color=FG_DIM, ls="--", lw=0.6, alpha=0.7)

def main():
    fig, axs = plt.subplots(3, 1, figsize=(14, 11), dpi=130,
        gridspec_kw={"hspace":0.4, "top":0.93, "bottom":0.07, "left":0.06, "right":0.98})
    ax_cpu, ax_wmib, ax_wiops = axs

    for s in SYSTEMS:
        m = load_metrics(s)
        if m is None: continue
        ax_cpu.plot(m["t"], m["cpu"], label=s, color=COLORS[s], lw=1.4, alpha=0.92)
        ax_wmib.plot(m["t"], m["wmib"], label=s, color=COLORS[s], lw=1.4, alpha=0.92)
        ax_wiops.plot(m["t"], m["wiops"], label=s, color=COLORS[s], lw=1.4, alpha=0.92)

    for ax, ylabel, title in [
        (ax_cpu,   "CPU user+sys %",       "CPU utilisation (user+system)"),
        (ax_wmib,  "NVMe write MiB/s",     "NVMe write throughput"),
        (ax_wiops, "NVMe write IOPS",      "NVMe write IOPS"),
    ]:
        add_phase_bands(ax)
        ax.set_xlim(0, RUN_END)
        ax.set_xticks([0, 600, 1200, 1800, 2400, 3000])
        ax.set_xticklabels(["0", "10m\nTX open", "20m", "30m", "40m\nTX close", "50m"], fontsize=7.5)
        ax.set_ylabel(ylabel, color=FG_EMPH, fontsize=8.5)
        ax.set_title(title, color=FG_EMPH, loc="left", fontsize=10.5, fontweight="bold")
        ax.grid(True, axis="y", alpha=0.5); ax.set_axisbelow(True)
        ax.legend(loc="upper right", ncol=4, frameon=False, fontsize=8, labelcolor=FG)
        for sp in ("top","right"): ax.spines[sp].set_visible(False)

    ax_cpu.set_ylim(0, 100)

    fig.text(0.06, 0.97,
      "R10 · system metrics · 8 systems · CPU · NVMe write MiB/s · NVMe write IOPS · -R 2000 · idle-in-tx in orange band",
      ha="left", fontsize=12.5, fontweight="bold", color=FG_EMPH)

    fig.savefig(OUT, dpi=130, bbox_inches="tight", facecolor=BG)
    print(f"wrote {OUT}")

if __name__ == "__main__":
    main()
