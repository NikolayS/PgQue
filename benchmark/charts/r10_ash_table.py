#!/usr/bin/env python3
"""Per-system wait-event-type breakdown across phases (8 systems incl. awa)."""
import csv, re as _re
from pathlib import Path
from datetime import datetime
from collections import Counter

ROOT = Path("/tmp/bench_r10")
SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss", "awa"]
CLEAN, TX, RECOV = (0,600), (600,2400), (2400,3000)

def parse_ts(s):
    s = s.strip().replace(" ", "T")
    if _re.search(r"[+-]\d\d$", s): s += ":00"
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

def wet_of(we):
    we = (we or "").strip()
    if we.startswith("CPU"): return "CPU"
    if we == "Timeout:PgSleep": return "IdleTx"
    if ":" in we: return we.split(":", 1)[0]
    return we or "CPU"

def load(sys_name):
    p = ROOT / sys_name / "ash.csv"
    if not p.exists(): return None
    rows = list(csv.DictReader(open(p)))
    return rows or None

print("| system | phase | avg active sessions | top wait-event mix |")
print("|---|---|---:|---|")
for sys_name in SYSTEMS:
    rows = load(sys_name)
    if not rows:
        print(f"| {sys_name} | — | — | _no data_ |"); continue
    ts_min = min(parse_ts(r["sample_time"]) for r in rows)
    for name, (a, b) in [("Clean", CLEAN), ("TX", TX), ("Recovery", RECOV)]:
        in_phase = [r for r in rows if a <= (parse_ts(r["sample_time"]) - ts_min) < b]
        duration = b - a
        if not in_phase:
            print(f"| {sys_name} | {name} | 0.0 | — |"); continue
        mix = Counter(wet_of(r["wait_event"]) for r in in_phase)
        avg = len(in_phase) / duration
        top3 = ", ".join(f"{w} {100*mix[w]/len(in_phase):.0f}%" for w in sorted(mix, key=lambda x: -mix[x])[:3])
        print(f"| {sys_name} | {name} | {avg:.2f} | {top3} |")
