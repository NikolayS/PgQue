#!/usr/bin/env python3
"""pgfr_analyze.py — R8 pgfr enriched post-processing (Solarized Dark).

Per-system row, 4 columns of pgfr-derived insights:
  Col 1: top-5 queries by cumulative total_exec_time (labelled with actual
         truncated query text, not q1/q2/q3).
  Col 2: buffer hit rate per top query (shared_blks_hit / (hit + read)).
  Col 3: WAL bytes per top query (how write-amplifying each query is).
  Col 4: global WAL rate (MiB/s) time series + active-backends count overlay
         (shows consumer-side contention during TX phase).

LINEAR axes everywhere. No log/symlog.

Fallbacks:
- Systems without pgfr_record instrumentation (pgq/pgmq/river in R8) use pgss.csv
  for top-query column only; other columns show "pgfr not installed".
- If pgfr_statement_snapshots.csv exists but is empty, fall back to pgss.csv.

Inputs under /tmp/bench_r8_full/<sys>/:
  pgfr_statement_snapshots.csv   (per-snapshot pg_stat_statements)
  pgfr_table_snapshots.csv       (per-snapshot pg_stat_user_tables)
  pgfr_snapshots.csv             (global wal/io/bgwriter/ckpt)
  pgss.csv                       (point-in-time pg_stat_statements fallback)

Output: /tmp/r8_pgfr_chart.png + /tmp/r8_pgfr_summary.json
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
TX_START_MIN, TX_END_MIN = 30, 90
TOTAL_MIN = 120

# Solarized Dark
BG = "#002b36"; SURF = "#073642"
FG = "#839496"; FG_EMPH = "#93a1a1"; FG_DIM = "#586e75"
ALERT = "#dc322f"; OK = "#859900"; WARN = "#b58900"
ACCENT = "#268bd2"  # blue — top query bars
ACCENT2 = "#2aa198"  # cyan — secondary
ACCENT3 = "#cb4b16"  # orange — WAL
ACCENT4 = "#6c71c4"  # violet — DELETE churn

# Avoid expanding DO into pg_stat_statements block comment; grab first DML.
DML_RE = re.compile(
    r"(?:PERFORM|SELECT|INSERT|UPDATE|DELETE|TRUNCATE|WITH|CALL|COPY)\s+[^\n]{0,150}",
    re.IGNORECASE)


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


def read_csv(path: Path):
    if not path.is_file(): return []
    try:
        with path.open() as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return []
    return rows


def shorten_query(q: str, maxlen: int = 60) -> str:
    """Return a readable label for a query.
    - Collapse whitespace.
    - If DO $$..$$ block, find first DML keyword inside and use it.
    - Truncate with ellipsis.
    """
    if not q:
        return "(empty)"
    q = q.strip()
    # DO block — pull first DML statement from the body
    if q.upper().startswith("DO"):
        m = DML_RE.search(q)
        if m:
            q = "DO{" + m.group(0).strip() + "}"
    # Collapse whitespace
    q = re.sub(r"\s+", " ", q)
    if len(q) > maxlen:
        q = q[:maxlen - 1] + "…"
    return q


def load_stmt_top(d: Path, k: int = 5):
    """Return list of dicts: {qid, label, exec_s, hit, read, wal_bytes}
    for top-k queries by cumulative total_exec_time. Falls back to pgss.csv.
    """
    p = d / "pgfr_statement_snapshots.csv"
    rows = read_csv(p)
    source = None
    if rows:
        source = "pgfr"
        # For each qid, take the row with the MAX total_exec_time (cumulative).
        by_q = {}
        for r in rows:
            qid = r.get("queryid") or ""
            try:
                te = float(r.get("total_exec_time", "0") or 0)
            except Exception:
                te = 0
            if qid not in by_q or te > by_q[qid]["exec"]:
                by_q[qid] = {
                    "qid": qid,
                    "exec": te,
                    "preview": r.get("query_preview", "") or "",
                    "hit": float(r.get("shared_blks_hit", "0") or 0),
                    "read": float(r.get("shared_blks_read", "0") or 0),
                    "wal_bytes": float(r.get("wal_bytes", "0") or 0),
                }
        tops = sorted(by_q.values(), key=lambda x: -x["exec"])[:k]
        return source, [
            {
                "qid": t["qid"],
                "label": shorten_query(t["preview"]),
                "exec_s": t["exec"] / 1000.0,
                "hit": t["hit"],
                "read": t["read"],
                "wal_bytes": t["wal_bytes"],
            }
            for t in tops
        ]
    # Fallback: pgss.csv (query,calls,total_exec_time,rows)
    p2 = d / "pgss.csv"
    rows2 = read_csv(p2)
    if not rows2:
        return None, []
    source = "pgss"
    items = []
    for r in rows2:
        try:
            te = float(r.get("total_exec_time", "0") or 0)
        except Exception:
            te = 0
        items.append({
            "qid": "",
            "label": shorten_query(r.get("query", "")),
            "exec_s": te / 1000.0,
            "hit": 0.0, "read": 0.0, "wal_bytes": 0.0,
        })
    items.sort(key=lambda x: -x["exec_s"])
    return source, items[:k]


def find_bench_t0(bench_dir: Path):
    """First epoch from producer_agg.* (pgbench aggregate log)."""
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


def load_wal_rate_ts(d: Path):
    """Global WAL rate (MiB/s) from pgfr_snapshots.wal_bytes.
    Clipped to [bench_t0, bench_t0 + 7200s]. Returns (xs_minutes, ys_mib_s).
    """
    rows = read_csv(d / "pgfr_snapshots.csv")
    if not rows:
        return np.array([]), np.array([])
    t0_epoch = find_bench_t0(d)
    if t0_epoch is None:
        return np.array([]), np.array([])
    pts = []
    for r in rows:
        dt = parse_ts(r.get("captured_at", ""))
        if dt is None: continue
        try:
            wb = float(r.get("wal_bytes", "0") or 0)
        except Exception:
            continue
        pts.append((dt.timestamp(), wb))
    pts.sort(key=lambda x: x[0])
    # Filter to bench window with one-sample buffer
    lo, hi = t0_epoch - 60, t0_epoch + TOTAL_MIN * 60 + 60
    pts = [(t, w) for (t, w) in pts if lo <= t <= hi]
    if len(pts) < 2:
        return np.array([]), np.array([])
    xs, ys = [], []
    for i in range(1, len(pts)):
        t0p, w0 = pts[i - 1]
        t1, w1 = pts[i]
        dt = t1 - t0p
        if dt <= 0: continue
        dw = w1 - w0
        if dw < 0: continue  # counter reset
        mid_t = (t0p + t1) / 2.0
        xs.append((mid_t - t0_epoch) / 60.0)
        ys.append(dw / dt / (1024 * 1024))  # MiB/s
    return np.array(xs), np.array(ys)


def load_active_backends_ts(d: Path):
    """Count distinct active backends per minute from pgfr_activity_samples_archive.csv."""
    p = d / "pgfr_activity_samples_archive.csv"
    rows = read_csv(p)
    if not rows:
        return np.array([]), np.array([])
    t0_epoch = find_bench_t0(d)
    if t0_epoch is None:
        return np.array([]), np.array([])
    per_min = defaultdict(set)
    for r in rows:
        dt = parse_ts(r.get("captured_at", ""))
        if dt is None: continue
        off_s = dt.timestamp() - t0_epoch
        if off_s < 0 or off_s > TOTAL_MIN * 60: continue
        if r.get("state") != "active": continue
        bucket = int(off_s // 60)
        per_min[bucket].add(r.get("pid"))
    if not per_min:
        return np.array([]), np.array([])
    xs = sorted(per_min.keys())
    return np.array(xs, dtype=float), np.array([len(per_min[x]) for x in xs], dtype=float)


def fmt_sec(v, _):
    if v >= 3600: return f"{v/3600:.1f}h"
    if v >= 60: return f"{v/60:.0f}m"
    if v >= 1: return f"{v:.0f}s"
    return f"{v*1000:.0f}ms"


def fmt_bytes(v, _):
    v = abs(v)
    if v >= 1e12: return f"{v/1e12:.1f}T"
    if v >= 1e9:  return f"{v/1e9:.1f}G"
    if v >= 1e6:  return f"{v/1e6:.1f}M"
    if v >= 1e3:  return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def fmt_k(v, _):
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
        'font.size': 8,
    })

    nrows = len(SYSTEMS)
    ncols = 4
    fig, axes = plt.subplots(nrows, ncols, figsize=(20, 2.2 * nrows), dpi=110,
                             gridspec_kw={'width_ratios': [1.6, 0.7, 0.7, 1.0],
                                          'wspace': 0.35, 'hspace': 0.55})
    if nrows == 1:
        axes = [axes]

    col_titles = ["top-5 queries · cumulative exec time",
                  "buffer hit rate",
                  "WAL bytes / query",
                  "global WAL rate (MiB/s) + active backends"]

    summary = {}
    for i, sys_name in enumerate(SYSTEMS):
        d = base / sys_name
        source, tops = load_stmt_top(d, k=5)
        xs_wal, ys_wal = load_wal_rate_ts(d)
        xs_ab, ys_ab = load_active_backends_ts(d)

        row_label = f"{sys_name}\n({source or 'no data'})"

        # --- Col 0: top queries by exec time ---
        ax = axes[i][0]
        if tops:
            labels = [t["label"] for t in tops]
            vals = [t["exec_s"] for t in tops]
            y = np.arange(len(tops))
            ax.barh(y, vals, color=ACCENT, edgecolor=FG_DIM, height=0.75)
            ax.set_yticks(y)
            ax.set_yticklabels(labels, fontsize=7, color=FG)
            ax.invert_yaxis()
            ax.xaxis.set_major_formatter(FuncFormatter(fmt_sec))
            ax.set_xlabel("cum exec time", fontsize=8, color=FG_DIM)
        else:
            ax.text(0.5, 0.5, "no query data",
                    ha='center', va='center', transform=ax.transAxes, color=ALERT)
            ax.set_yticks([])
        ax.set_ylabel(row_label, rotation=0, labelpad=50, ha='right', va='center',
                      color=FG_EMPH, fontweight='bold', fontsize=9)
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)
        ax.grid(True, axis='x', alpha=0.3)

        # --- Col 1: buffer hit rate per top query ---
        ax = axes[i][1]
        if tops and source == "pgfr" and any(t["hit"] + t["read"] > 0 for t in tops):
            vals = []
            for t in tops:
                tot = t["hit"] + t["read"]
                vals.append(t["hit"] / tot if tot > 0 else 0.0)
            y = np.arange(len(tops))
            colors = [OK if v >= 0.99 else (WARN if v >= 0.95 else ALERT) for v in vals]
            ax.barh(y, vals, color=colors, edgecolor=FG_DIM, height=0.75)
            ax.set_xlim(0.0, 1.0)
            ax.set_xticks([0, 0.5, 1.0])
            ax.set_yticks([])
            ax.invert_yaxis()
            ax.set_xlabel("hit / (hit+read)", fontsize=8, color=FG_DIM)
        else:
            ax.text(0.5, 0.5, "pgfr not installed" if source != "pgfr" else "n/a",
                    ha='center', va='center', transform=ax.transAxes,
                    color=FG_DIM, fontsize=8)
            ax.set_xticks([]); ax.set_yticks([])
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)

        # --- Col 2: WAL bytes per top query ---
        ax = axes[i][2]
        if tops and source == "pgfr" and any(t["wal_bytes"] > 0 for t in tops):
            vals = [t["wal_bytes"] for t in tops]
            y = np.arange(len(tops))
            ax.barh(y, vals, color=ACCENT3, edgecolor=FG_DIM, height=0.75)
            ax.xaxis.set_major_formatter(FuncFormatter(fmt_bytes))
            ax.set_yticks([])
            ax.invert_yaxis()
            ax.set_xlabel("WAL bytes", fontsize=8, color=FG_DIM)
        else:
            ax.text(0.5, 0.5, "pgfr not installed" if source != "pgfr" else "n/a",
                    ha='center', va='center', transform=ax.transAxes,
                    color=FG_DIM, fontsize=8)
            ax.set_xticks([]); ax.set_yticks([])
        for sp in ("top", "right"): ax.spines[sp].set_visible(False)

        # --- Col 3: global WAL rate over time + active-backend overlay ---
        ax = axes[i][3]
        plotted = False
        if xs_wal.size:
            ax.plot(xs_wal, ys_wal, color=ACCENT3, lw=1.4, label="WAL MiB/s")
            ax.set_xlim(0, TOTAL_MIN)
            ax.axvspan(TX_START_MIN, TX_END_MIN, color=SURF, alpha=0.55, zorder=0)
            for b in (TX_START_MIN, TX_END_MIN):
                ax.axvline(x=b, color=ALERT, lw=0.6, alpha=0.5, zorder=0.5)
            ax.set_xticks([0, 30, 60, 90, 120])
            ax.set_ylabel("MiB/s", color=ACCENT3, fontsize=8)
            ax.tick_params(axis='y', colors=ACCENT3)
            plotted = True
        if xs_ab.size:
            ax2 = ax.twinx() if plotted else ax
            ax2.plot(xs_ab, ys_ab, color=ACCENT4, lw=1.2, alpha=0.75, label="active backends")
            ax2.set_xlim(0, TOTAL_MIN)
            ax2.set_ylabel("active backends", color=ACCENT4, fontsize=8)
            ax2.tick_params(axis='y', colors=ACCENT4)
            for sp in ("top",): ax2.spines[sp].set_visible(False)
            if ax2 is not ax:
                ax2.spines['right'].set_color(ACCENT4)
            plotted = True
        if not plotted:
            ax.text(0.5, 0.5, "pgfr not installed",
                    ha='center', va='center', transform=ax.transAxes,
                    color=FG_DIM, fontsize=8)
            ax.set_xticks([]); ax.set_yticks([])
        else:
            ax.set_xlabel("min  ·  TX shaded", fontsize=8, color=FG_DIM)
        for sp in ("top",): ax.spines[sp].set_visible(False)
        ax.grid(True, alpha=0.3)

        summary[sys_name] = {
            "source": source,
            "top_queries": [
                {"qid": t["qid"], "label": t["label"], "exec_s": t["exec_s"],
                 "hit": t["hit"], "read": t["read"], "wal_bytes": t["wal_bytes"]}
                for t in tops
            ],
            "wal_rate_points": int(xs_wal.size),
            "active_backend_points": int(xs_ab.size),
        }

    # Column titles on top row
    for j, t in enumerate(col_titles):
        axes[0][j].set_title(t, fontsize=10, color=FG_EMPH, pad=10, loc='center')

    fig.suptitle(
        "R8 — pgfr deep dive: top queries (real text) · buffer hit rate · WAL/query · global WAL rate + active backends  "
        "(pgq/pgmq/river: pgfr not installed, pgss fallback)",
        y=0.998, color=FG_EMPH, fontsize=11, fontweight='bold')
    fig.tight_layout(rect=[0.03, 0, 1, 0.965])
    fig.savefig("/tmp/r8_pgfr_chart.png", dpi=110, bbox_inches="tight", facecolor=BG)

    with open("/tmp/r8_pgfr_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    out = Path("/tmp/r8_pgfr_chart.png")
    print(f"wrote {out} ({out.stat().st_size/1024:.0f} KB) + /tmp/r8_pgfr_summary.json")


if __name__ == "__main__":
    main()
