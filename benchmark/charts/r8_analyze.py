#!/usr/bin/env python3
"""r8_analyze.py — primary R8 analysis chart (Solarized Dark).

6-panel per-system overlay (7 systems as colors):
  1) throughput (events consumed / s, 60s rolling mean)
  2) dead_tup on queue tables (bloat)
  3) CPU user+sys %
  4) NVMe write MiB/s
  5) backlog (producer_total − consumer_total_at_time_t) over time
  6) delivery-lag p99 (ms, clipped at 5s, LINEAR scale)

LINEAR y-axes everywhere. No log/symlog.

Inputs:  /tmp/bench_r8_full/<sys>/{events_consumed_per_sec.csv,bloat.csv,
                                   sys_metrics.csv,events_consumed_summary.txt,
                                   producer.log}
Output:  /tmp/r8_main_chart.png + /tmp/r8_summary.json + /tmp/r8_table.md
"""
from __future__ import annotations
import csv, json, re, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss"]

# Solarized Dark palette
BG = "#002b36"; SURF = "#073642"
FG = "#839496"; FG_EMPH = "#93a1a1"; FG_DIM = "#586e75"
ALERT = "#dc322f"

# Per-system accent colors (Solarized-coherent)
COLORS = {
    "pgque":            "#268bd2",  # blue (hero)
    "pgq":              "#2aa198",  # cyan
    "pgmq":             "#cb4b16",  # orange
    "pgmq-partitioned": "#dc322f",  # red
    "river":            "#b58900",  # yellow
    "que":              "#6c71c4",  # violet
    "pgboss":           "#859900",  # green
}

TX_START_MIN, TX_END_MIN = 30, 90
TOTAL_MIN = 120
LAG_CLIP_MS = 5000  # clip p99 to 5s (linear scale)


def parse_ts(s):
    if not s: return None
    s2 = s.strip().replace(" ", "T").replace("Z", "+00:00")
    m = re.search(r"([+-])(\d{2})$", s2)
    if m:
        s2 = s2[:m.start()] + m.group(1) + m.group(2) + ":00"
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def read_csv(p: Path):
    if not p.is_file(): return []
    with p.open() as f:
        return list(csv.DictReader(f))


def load_events(d: Path):
    """events_consumed_per_sec.csv → (minutes[], ev/s[], p99_lag_ms[], cumulative_consumed[])"""
    rows = read_csv(d / "events_consumed_per_sec.csv")
    if not rows: return [], [], [], []
    xs, ev, p99, cum = [], [], [], []
    c = 0
    for r in rows:
        try:
            s = int(r["second_since_start"])
            n = int(r["events_consumed"])
            p = int(r.get("p99_lag_ms", "0") or 0)
        except Exception:
            continue
        c += n
        xs.append(s / 60.0); ev.append(n); p99.append(p); cum.append(c)
    return xs, ev, p99, cum


def smooth(ys, window=30):
    if not ys: return ys
    out = []
    w = window
    for i in range(len(ys)):
        a = max(0, i - w); b = min(len(ys), i + w + 1)
        out.append(sum(ys[a:b]) / (b - a))
    return out


def load_bloat(d: Path):
    rows = read_csv(d / "bloat.csv")
    if not rows: return [], [], []
    by_ts = defaultdict(lambda: {"dead": 0, "live": 0})
    for r in rows:
        ts = r.get("ts") or r.get("sample_time") or ""
        try:
            dt = int(r.get("n_dead_tup", "0") or 0)
            lv = int(r.get("n_live_tup", "0") or 0)
        except Exception:
            continue
        by_ts[ts]["dead"] += dt
        by_ts[ts]["live"] += lv
    if not by_ts: return [], [], []
    ts_sorted = sorted(by_ts.keys())
    t0 = parse_ts(ts_sorted[0])
    if t0 is None: return [], [], []
    xs, dead, live = [], [], []
    for ts in ts_sorted:
        t = parse_ts(ts)
        if t is None: continue
        xs.append((t - t0).total_seconds() / 60.0)
        dead.append(by_ts[ts]["dead"])
        live.append(by_ts[ts]["live"])
    return xs, dead, live


def load_sys(d: Path):
    rows = read_csv(d / "sys_metrics.csv")
    if not rows: return [], [], []
    t0 = parse_ts(rows[0]["ts_iso"])
    if t0 is None: return [], [], []
    xs, cpu, wmib = [], [], []
    for r in rows:
        t = parse_ts(r["ts_iso"])
        if t is None: continue
        xs.append((t - t0).total_seconds() / 60.0)
        try:
            cpu.append(float(r["cpu_user_pct"]) + float(r["cpu_system_pct"]))
            wmib.append(float(r.get("disk_write_mib_s", "0") or 0))
        except Exception:
            cpu.append(0); wmib.append(0)
    return xs, cpu, wmib


def load_summary(d: Path):
    p = d / "events_consumed_summary.txt"
    out = {}
    if not p.is_file(): return out
    with p.open() as f:
        for ln in f:
            if "=" in ln:
                k, v = ln.strip().split("=", 1)
                out[k] = v
    return out


def load_producer_total(d: Path):
    """Grep 'number of transactions actually processed' from producer.log."""
    p = d / "producer.log"
    if not p.is_file(): return 0
    with p.open() as f:
        for ln in f:
            m = re.search(r"number of transactions actually processed:\s*(\d+)", ln)
            if m:
                return int(m.group(1))
    return 0


def tx_slice(xs, ys):
    return [y for x, y in zip(xs, ys) if TX_START_MIN <= x <= TX_END_MIN]


def mean(xs): return sum(xs) / len(xs) if xs else 0


def fmt_thousands(v, _):
    v = abs(v)
    if v >= 1e6: return f"{v/1e6:.1f}M"
    if v >= 1e3: return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def main():
    base = Path("/tmp/bench_r8_full")

    plt.rcParams.update({
        'figure.facecolor': BG, 'axes.facecolor': BG, 'savefig.facecolor': BG,
        'text.color': FG, 'axes.labelcolor': FG_EMPH,
        'xtick.color': FG, 'ytick.color': FG,
        'axes.edgecolor': FG_DIM,
        'grid.color': SURF, 'grid.linewidth': 0.8,
        'font.family': ['Helvetica', 'Arial', 'DejaVu Sans'],
        'font.size': 10,
    })

    fig, axes = plt.subplots(6, 1, figsize=(14, 15), sharex=True, dpi=110)
    titles = [
        "1) Throughput — events consumed / s (60s rolling mean)",
        "2) Bloat — n_dead_tup on queue tables",
        "3) CPU — user + system %",
        "4) NVMe write — MiB/s",
        "5) Backlog — producer_total minus consumer_cum (events stuck in queue)",
        "6) Delivery-lag p99 — head-of-queue age per batch, ms (clipped 5s, LINEAR)",
    ]
    ylabels = ["ev/s", "dead tuples", "%", "MiB/s", "events", "ms"]

    summary = {}
    table_rows = []

    for sys_name in SYSTEMS:
        color = COLORS[sys_name]
        d = base / sys_name
        xs_ev, ev, p99, cum = load_events(d)
        xs_b, dead, live = load_bloat(d)
        xs_s, cpu, wmib = load_sys(d)
        sm = load_summary(d)
        prod_total = load_producer_total(d)
        cons_total = int(sm.get("total_events_consumed", "0") or 0)

        ev_s = smooth(ev, window=30)
        p99_s = smooth(p99, window=30)
        p99_clip = [min(v, LAG_CLIP_MS) for v in p99_s]

        # backlog at time t = producer_rate_so_far(t) − cumulative_consumed_so_far(t)
        # producer is -R 2000 (constant). We can approximate producer_cum(t) = min(2000*t, prod_total)
        # More accurate: if bench ran full duration, prod_cum = 2000 * (t*60). Use prod_total at end.
        prod_rate = 2000.0  # target rate
        bench_dur_s = 7200
        backlog = []
        for t_min, c_cum in zip(xs_ev, cum):
            t_s = t_min * 60
            p_cum = min(prod_rate * t_s, prod_total)
            backlog.append(max(0, p_cum - c_cum))

        lw = 2.5 if sys_name == "pgque" else 1.5
        z = 5 if sys_name == "pgque" else 3

        axes[0].plot(xs_ev, ev_s,   color=color, lw=lw, zorder=z, label=sys_name)
        axes[1].plot(xs_b,  dead,   color=color, lw=lw, zorder=z)
        axes[2].plot(xs_s,  cpu,    color=color, lw=lw, zorder=z)
        axes[3].plot(xs_s,  wmib,   color=color, lw=lw, zorder=z)
        axes[4].plot(xs_ev, backlog,color=color, lw=lw, zorder=z)
        axes[5].plot(xs_ev, p99_clip, color=color, lw=lw, zorder=z)

        tx_ev = tx_slice(xs_ev, ev)
        tx_p99 = tx_slice(xs_ev, p99)
        p50_overall = int(sm.get("overall_lag_p50_ms", "0") or 0)
        p99_overall = int(sm.get("overall_lag_p99_ms", "0") or 0)

        true_backlog = max(0, prod_total - cons_total)
        table_rows.append({
            "system": sys_name,
            "producer_total": prod_total,
            "consumer_total": cons_total,
            "tx_avg_evs": mean(tx_ev),
            "p50_lag_ms": p50_overall,
            "p99_lag_ms": p99_overall,
            "tx_p99_lag_ms": max(tx_p99) if tx_p99 else 0,
            "true_backlog": true_backlog,
            "peak_cpu": max(cpu) if cpu else 0,
            "peak_wmib": max(wmib) if wmib else 0,
        })
        summary[sys_name] = table_rows[-1]

    for ax, t, yl in zip(axes, titles, ylabels):
        ax.set_title(t, fontsize=10, loc='left', color=FG_EMPH)
        ax.axvspan(TX_START_MIN, TX_END_MIN, color=SURF, alpha=0.55, zorder=0)
        for b in (TX_START_MIN, TX_END_MIN):
            ax.axvline(x=b, color=ALERT, lw=0.8, alpha=0.55, zorder=0.5)
        ax.grid(True, alpha=0.35)
        ax.set_axisbelow(True)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
        ax.set_ylabel(yl, color=FG_EMPH)
        ax.set_xlim(0, TOTAL_MIN)

    # Large-number formatter for panels that need it
    axes[1].yaxis.set_major_formatter(FuncFormatter(fmt_thousands))
    axes[4].yaxis.set_major_formatter(FuncFormatter(fmt_thousands))
    axes[0].yaxis.set_major_formatter(FuncFormatter(fmt_thousands))

    axes[-1].set_xticks([0, 15, 30, 45, 60, 75, 90, 105, 120])
    axes[-1].set_xlabel(
        "minutes since bench start  ·  TX phase (held xmin) shaded 30-90m",
        color=FG_EMPH)

    # Legend at top; 7 systems in one row
    axes[0].legend(loc="upper center", bbox_to_anchor=(0.5, 1.55), ncol=7,
                   fontsize=9, frameon=False)

    fig.suptitle(
        "R8 — 7 Postgres queue systems · 2h (30m clean + 60m held-xmin + 30m recovery) · R=2000/s",
        y=0.995, color=FG_EMPH, fontsize=13, fontweight='bold')
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig("/tmp/r8_main_chart.png", dpi=110, bbox_inches="tight", facecolor=BG)

    with open("/tmp/r8_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    with open("/tmp/r8_table.md", "w") as f:
        f.write("| system | producer total | consumer total | TX-avg ev/s | p50 lag ms | p99 lag ms | TX p99 lag ms | true backlog | peak CPU % | peak NVMe write MiB/s |\n")
        f.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
        for r in table_rows:
            f.write(
                f"| {r['system']} | {r['producer_total']:,} | {r['consumer_total']:,} | "
                f"{r['tx_avg_evs']:.0f} | {r['p50_lag_ms']} | {r['p99_lag_ms']} | "
                f"{r['tx_p99_lag_ms']} | {r['true_backlog']:,} | "
                f"{r['peak_cpu']:.1f} | {r['peak_wmib']:.1f} |\n")

    out = Path("/tmp/r8_main_chart.png")
    print(f"wrote {out} ({out.stat().st_size/1024:.0f} KB) + /tmp/r8_summary.json + /tmp/r8_table.md")


if __name__ == "__main__":
    main()
