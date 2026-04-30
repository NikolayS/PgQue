#!/usr/bin/env python3
"""R10 summary table — per-system per-phase ev/s + true backlog + peak CPU + peak NVMe."""
import csv, re as _re
from pathlib import Path
from datetime import datetime

ROOT = Path("/tmp/bench_r10")
SYSTEMS = ["pgque", "pgq", "pgmq", "pgmq-partitioned", "river", "que", "pgboss", "awa"]
CLEAN_END, TX_END, RUN_END = 600, 2400, 3000

def producer_total(sys_name):
    """Read pgbench (or producer_awa.py) final summary for total events.
    Falls back to tps × duration if no explicit count line is present."""
    p = ROOT / sys_name / "producer.log"
    if not p.exists(): return None
    text = p.read_text()
    # awa producer prints: "done: produced 5781300 jobs in 3000.0s"
    m = _re.search(r"done:\s*produced\s+(\d+)\s+jobs", text)
    if m:
        return int(m.group(1))
    # pgbench: "tps = 2000.026908 (without initial connection time)"
    # Use the average tps × duration. Fallback only.
    m = _re.search(r"tps = ([\d.]+)", text)
    if m:
        return int(float(m.group(1)) * RUN_END)
    return None

def consumer_total_and_phases(sys_name):
    """Sum events_consumed_per_sec.csv across the run + phase totals."""
    p = ROOT / sys_name / "events_consumed_per_sec.csv"
    if not p.exists(): return None
    pts = []
    with open(p) as f:
        r = csv.reader(f); next(r, None)
        for row in r:
            pts.append((int(row[0]), int(row[1])))
    total = sum(n for _, n in pts)
    clean_n = sum(n for s, n in pts if s < CLEAN_END)
    tx_n    = sum(n for s, n in pts if CLEAN_END <= s < TX_END)
    recov_n = sum(n for s, n in pts if TX_END <= s < RUN_END)
    return {
        "total": total,
        "clean_avg": clean_n / CLEAN_END if CLEAN_END else 0,
        "tx_avg":    tx_n / (TX_END - CLEAN_END) if (TX_END > CLEAN_END) else 0,
        "recov_avg": recov_n / (RUN_END - TX_END) if (RUN_END > TX_END) else 0,
    }

def sys_metrics_phase_peaks(sys_name):
    """Per-phase peak CPU% and NVMe write MiB/s + IOPS."""
    p = ROOT / sys_name / "sys_metrics.csv"
    if not p.exists(): return {}
    rows = list(csv.DictReader(open(p)))
    if not rows: return {}
    t0 = datetime.fromisoformat(rows[0]["ts_iso"].replace("Z","+00:00")).timestamp()
    buckets = {"clean": [], "tx": [], "recov": []}
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["ts_iso"].replace("Z","+00:00")).timestamp() - t0
            cpu = float(r["cpu_user_pct"]) + float(r["cpu_system_pct"])
            wmib = float(r["disk_write_mib_s"])
            wiops = float(r["disk_write_iops"])
            phase = "clean" if ts < CLEAN_END else ("tx" if ts < TX_END else "recov")
            buckets[phase].append((cpu, wmib, wiops))
        except: pass
    out = {}
    for phase in ("clean","tx","recov"):
        if not buckets[phase]:
            out.update({f"{phase}_peak_cpu":0, f"{phase}_peak_wmib":0, f"{phase}_peak_wiops":0,
                        f"{phase}_avg_cpu":0, f"{phase}_avg_wmib":0})
            continue
        cpus = [t[0] for t in buckets[phase]]
        wmib = [t[1] for t in buckets[phase]]
        wiops = [t[2] for t in buckets[phase]]
        out[f"{phase}_peak_cpu"] = max(cpus)
        out[f"{phase}_peak_wmib"] = max(wmib)
        out[f"{phase}_peak_wiops"] = max(wiops)
        out[f"{phase}_avg_cpu"] = sum(cpus)/len(cpus)
        out[f"{phase}_avg_wmib"] = sum(wmib)/len(wmib)
    return out

print("| System | Workers | Producer total | Consumer total | TX-avg ev/s | Clean ev/s | Recov ev/s | True backlog | TX-peak CPU% | TX-peak NVMe MiB/s |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for s in SYSTEMS:
    p_total = producer_total(s) or 0
    c = consumer_total_and_phases(s) or {"total":0, "clean_avg":0, "tx_avg":0, "recov_avg":0}
    m = sys_metrics_phase_peaks(s)
    backlog = max(0, p_total - c["total"])
    workers = 1 if s in ("pgque","pgq") else 4
    print(f"| **{s}** | {workers} | {p_total:,} | {c['total']:,} | "
          f"**{c['tx_avg']:.0f}** | {c['clean_avg']:.0f} | {c['recov_avg']:.0f} | "
          f"**{backlog:,}** | "
          f"{m.get('tx_peak_cpu',0):.1f} | {m.get('tx_peak_wmib',0):.1f} |")
