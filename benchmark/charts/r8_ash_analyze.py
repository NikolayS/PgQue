#!/usr/bin/env python3
"""ash_analyze.py — R8 ASH post-processing (Solarized Dark).

Input:  /tmp/bench_r8_full/<sys>/ash.csv per system.
Output: /tmp/r8_ash_chart.png + /tmp/r8_ash_summary.json

Per-system stacked-area of **active session count** by wait-event category
over 2h bench, 1-min buckets. Each ash.csv row is one active-backend sample;
stack thickness at time t = number of sessions sampled in that category in
that bucket. LINEAR y-axis (integer count). NO log/symlog anywhere.

ash.csv schema:
  sample_time,database_name,active_backends,wait_event,query_id,query_text
  where wait_event values include: CPU*, IO:DataFileRead, LWLock:BufferContent,
  Lock:relation, Client:ClientRead, IPC:ProcArrayGroupUpdate, Activity:*, ...
"""
from __future__ import annotations
import csv, json, re, sys
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss"]
TX_START_MIN, TX_END_MIN = 30, 90  # 30..90m = idle-in-tx phase
TOTAL_MIN = 120

# Solarized Dark palette
BG = "#002b36"; SURF = "#073642"
FG = "#839496"; FG_EMPH = "#93a1a1"; FG_DIM = "#586e75"
ALERT = "#dc322f"

CATEGORIES = ["CPU*", "IO", "LWLock", "Lock", "Client", "IPC", "Activity", "Other"]
CAT_COLORS = {
    "CPU*":    "#859900",  # green — active, healthy
    "IO":      "#268bd2",  # blue — disk
    "LWLock":  "#dc322f",  # red — internal lock (bad)
    "Lock":    "#cb4b16",  # orange — relation/tuple lock
    "Client":  "#2aa198",  # cyan — waiting on client
    "IPC":     "#d33682",  # magenta
    "Activity":"#586e75",  # dim — idle
    "Other":   "#b58900",  # yellow
}


def parse_ts(s: str):
    if not s: return None
    s = s.strip()
    # Postgres default "2026-04-19 02:49:09+00" — normalize to isoformat
    # Handles "+00", "+00:00", "Z", trailing microseconds.
    s2 = s.replace(" ", "T")
    s2 = s2.replace("Z", "+00:00")
    # match trailing +HH (no colon) and expand to +HH:00
    m = re.search(r"([+-])(\d{2})$", s2)
    if m:
        s2 = s2[:m.start()] + m.group(1) + m.group(2) + ":00"
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None


def cat_of(wait_event: str) -> str:
    """Map ash.csv wait_event values to our 8 categories."""
    if not wait_event:
        return "Other"
    we = wait_event.strip()
    if we in ("", "NULL"):
        return "Other"
    if we == "CPU*":
        return "CPU*"
    prefix = we.split(":")[0] if ":" in we else we
    if prefix == "LWLock": return "LWLock"
    if prefix == "Lock":   return "Lock"
    if prefix == "IO":     return "IO"
    if prefix == "Client": return "Client"
    if prefix == "IPC":    return "IPC"
    if prefix == "Activity": return "Activity"
    if prefix == "Timeout": return "Other"
    return "Other"


def find_t0(bench_dir: Path):
    """First epoch from producer_agg.* (pgbench aggregate log). Lines start with
    an epoch second as first whitespace-separated token.
    """
    for c in sorted(bench_dir.glob("producer_agg.*")):
        try:
            with c.open() as f:
                for line in f:
                    m = re.match(r"^(\d{10})\s", line)
                    if m:
                        return int(m.group(1))
        except OSError:
            continue
    return None


def load_ash(bench_dir: Path):
    """Return (t0, list[(off_s, cat, active_backends)], meta)."""
    p = bench_dir / "ash.csv"
    if not p.is_file():
        return None, [], {}
    t0 = find_t0(bench_dir)
    rows = []
    qid_counts = defaultdict(int)
    we_counts = defaultdict(int)
    parse_fail = 0
    with p.open() as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            dt = parse_ts(r.get("sample_time", ""))
            if dt is None:
                parse_fail += 1
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            epoch = int(dt.timestamp())
            if t0 is None:
                t0 = epoch
            off = epoch - t0
            if off < -60 or off > TOTAL_MIN * 60 + 300:
                continue
            we = r.get("wait_event") or ""
            cat = cat_of(we)
            try:
                ab = int(r.get("active_backends") or 1)
            except ValueError:
                ab = 1
            rows.append((off, cat, ab))
            qid_counts[r.get("query_id", "")] += ab
            we_counts[we or "CPU*"] += ab
    meta = {
        "top_qids": sorted(qid_counts.items(), key=lambda x: -x[1])[:10],
        "top_we":   sorted(we_counts.items(),  key=lambda x: -x[1])[:15],
        "samples":  len(rows),
        "parse_fail": parse_fail,
    }
    return t0, rows, meta


def bucket_stack(rows, bucket_s=60, total_s=TOTAL_MIN * 60):
    """Returns dict[cat] = np.array of mean active-session COUNT per bucket.

    ash.csv samples at ~1Hz. Each row is one active backend in a specific
    wait state. For a 60s bucket, we want the average number of sessions in
    that wait state per sample. So we count rows per (bucket, cat) and divide
    by the number of distinct samples in the bucket — yielding mean count.
    """
    n = total_s // bucket_s
    mat = {c: np.zeros(n, dtype=float) for c in CATEGORIES}
    samples_in_bucket = defaultdict(set)  # bucket -> set of sample offsets (seconds)
    for off, cat, ab in rows:
        b = int(off // bucket_s)
        if 0 <= b < n:
            mat[cat][b] += 1
            samples_in_bucket[b].add(off)
    # Normalize per-bucket by number of distinct sample timestamps → mean count
    for b in range(n):
        num_samples = len(samples_in_bucket.get(b, ())) or 1
        for c in CATEGORIES:
            mat[c][b] /= num_samples
    return mat, n


def main():
    base = Path("/tmp/bench_r8_full")

    plt.rcParams.update({
        'figure.facecolor': BG, 'axes.facecolor': BG, 'savefig.facecolor': BG,
        'text.color': FG, 'axes.labelcolor': FG_EMPH,
        'xtick.color': FG, 'ytick.color': FG,
        'axes.edgecolor': FG_DIM,
        'grid.color': SURF, 'grid.linewidth': 0.8,
        'font.family': ['Helvetica', 'Arial', 'DejaVu Sans'],
        'font.size': 9,
    })

    fig, axes = plt.subplots(len(SYSTEMS), 1, figsize=(13, 1.9 * len(SYSTEMS)),
                             sharex=True, dpi=110)
    if len(SYSTEMS) == 1:
        axes = [axes]

    summary = {}
    bucket_s = 60
    total_s = TOTAL_MIN * 60
    n = total_s // bucket_s
    xs = np.array([i * bucket_s / 60 for i in range(n)])

    # First pass: compute per-system stacks so we can set a consistent y-max per row
    all_stacks = {}
    for sys_name in SYSTEMS:
        d = base / sys_name
        t0, rows, meta = load_ash(d)
        if rows:
            stack, _ = bucket_stack(rows, bucket_s=bucket_s, total_s=total_s)
            all_stacks[sys_name] = (stack, meta)
        else:
            all_stacks[sys_name] = (None, meta)

    for ax, sys_name in zip(axes, SYSTEMS):
        stack, meta = all_stacks[sys_name]
        if stack is None:
            ax.text(0.5, 0.5, f"{sys_name}: no ash data",
                    ha='center', va='center', transform=ax.transAxes, color=ALERT)
            ax.set_yticks([])
            ax.set_ylabel(sys_name, rotation=0, labelpad=55, ha='right', va='center',
                          color=FG_EMPH, fontweight='bold')
            continue
        ys = [stack[c] for c in CATEGORIES]
        colors = [CAT_COLORS[c] for c in CATEGORIES]
        ax.stackplot(xs, *ys, labels=CATEGORIES, colors=colors, alpha=0.95)
        # Phase bands
        ax.axvspan(TX_START_MIN, TX_END_MIN, color=SURF, alpha=0.55, zorder=0)
        ax.axvline(TX_START_MIN, color=ALERT, lw=0.8, alpha=0.6, zorder=0.5)
        ax.axvline(TX_END_MIN,   color=ALERT, lw=0.8, alpha=0.6, zorder=0.5)
        # Y-limit: max(total) across buckets, rounded up to an integer; ensure min >=1
        totals = sum(stack[c] for c in CATEGORIES)
        ymax = max(1.0, float(np.max(totals)))
        # Round up to nearest integer, ensuring some headroom
        ymax_int = int(np.ceil(ymax)) + 1
        ax.set_ylim(0, ymax_int)
        # Integer ticks: 0, 2, 4, ...; choose step to get ~4-5 ticks
        step = max(1, ymax_int // 4)
        ax.set_yticks(list(range(0, ymax_int + 1, step)))
        ax.set_xlim(0, TOTAL_MIN)
        ax.set_ylabel(sys_name, rotation=0, labelpad=55, ha='right', va='center',
                      color=FG_EMPH, fontweight='bold')
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
        summary[sys_name] = {
            "top_we": meta.get("top_we", [])[:10],
            "top_qids": meta.get("top_qids", [])[:10],
            "samples": meta.get("samples"),
            "peak_total_active": float(np.max(totals)),
        }

    # Shared legend at top
    handles = [plt.Rectangle((0, 0), 1, 1, fc=CAT_COLORS[c]) for c in CATEGORIES]
    axes[-1].set_xlabel("minutes since bench start  ·  TX phase (held xmin) shaded 30-90m",
                        color=FG_EMPH)
    axes[-1].set_xticks([0, 15, 30, 45, 60, 75, 90, 105, 120])

    fig.suptitle("R8 — ASH active sessions (count) by wait-event category, per system · 1-min buckets · linear y",
                 y=0.998, color=FG_EMPH, fontsize=13, fontweight='bold')
    fig.tight_layout(rect=[0.0, 0.0, 1.0, 0.94])
    fig.legend(handles, CATEGORIES, loc="upper center",
               bbox_to_anchor=(0.5, 0.955), ncol=len(CATEGORIES),
               frameon=False, fontsize=9)
    out = Path("/tmp/r8_ash_chart.png")
    fig.savefig(out, dpi=110, bbox_inches="tight", facecolor=BG)
    with open("/tmp/r8_ash_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"wrote {out} ({out.stat().st_size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
