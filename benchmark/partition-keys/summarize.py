#!/usr/bin/env python3
"""summarize.py -- turn the partition-keys bench CSVs into a markdown report.

Reads $OUT/phases.csv (the manifest run_bench.sh writes) and, per phase
directory, parses:
  producer_agg.*      pgbench aggregate log -> produced events, achieved TPS
  acks.log            slot-worker ack log   -> consumed events, throughput
  slot_status.csv     per-slot lease + lag  -> pending_events percentiles,
                                               stalled-slot timeline
  queue_rate.csv      get_queue_info         -> rotation-floor (ntables) evidence
  sys_metrics.csv     CPU/mem sampler        -> peak+avg CPU and memory
  pgss_end.csv        pg_stat_statements      -> read-amp (buffers per event)

Output: a markdown summary ending in a headline table ready to paste into a
GitHub PR comment. Maps directly to SPEC R2 (read amplification ~N x) and R7
(a stalled slot pins rotation for the whole queue).

Stdlib only.
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import statistics
from datetime import datetime

# Rotating event tables: parents (event_N) and children (event_N_M). Everything
# else in bloat.csv is a metadata table.
EVENT_TBL_RE = re.compile(r"^pgque\.event_\d+(_\d+)?$")


def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def parse_iso(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def pctile(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def fmt(n: float) -> str:
    return f"{n:,.0f}"


def fmt_bytes(n: float) -> str:
    """Binary-unit size (CLAUDE.md: always KiB/MiB/GiB/TiB)."""
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n:.0f} B" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TiB"


def gib(n: float) -> float:
    return float(n) / (1024 ** 3)


def _bloat_int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _nearest_ts(all_ts: list[datetime], target: datetime) -> datetime:
    return min(all_ts, key=lambda t: abs((t - target).total_seconds()))


def bloat_stats(phase_dir: str) -> dict | None:
    """Parse bloat.csv into dead-tuple / footprint stats.

    Self-contained: depends on bloat.csv alone, so the bloat section renders
    even when the other phase CSVs are absent. Returns None if there is no
    bloat.csv (caller notes the section was skipped).
    """
    rows = read_csv(os.path.join(phase_dir, "bloat.csv"))
    if not rows:
        return None
    per_table: dict[str, list[dict]] = {}
    parsed: list[dict] = []
    ts_set: set[datetime] = set()
    for r in rows:
        ts = parse_iso(r.get("ts", ""))
        if ts is None:
            continue
        rec = dict(
            ts=ts,
            table=r.get("table", ""),
            n_live=_bloat_int(r.get("n_live_tup")),
            n_dead=_bloat_int(r.get("n_dead_tup")),
            total=_bloat_int(r.get("total_bytes")),
        )
        parsed.append(rec)
        per_table.setdefault(rec["table"], []).append(rec)
        ts_set.add(ts)
    if not parsed:
        return None
    all_ts = sorted(ts_set)
    first_ts, last_ts = all_ts[0], all_ts[-1]
    for recs in per_table.values():
        recs.sort(key=lambda x: x["ts"])

    # Split metadata vs rotating event tables.
    meta: dict[str, dict] = {}
    event_tables: set[str] = set()
    for tbl, recs in per_table.items():
        if EVENT_TBL_RE.match(tbl):
            event_tables.add(tbl)
            continue
        last = recs[-1]
        meta[tbl] = dict(
            peak_dead=max(x["n_dead"] for x in recs),
            end_dead=last["n_dead"],
            end_bytes=last["total"],
        )

    # Event-table aggregates per timestamp: summed bytes, present-count, and the
    # peak dead-tuple count across all event tables (append-only -> want ~0).
    ev_bytes_by_ts = {t: 0 for t in all_ts}
    ev_present_by_ts = {t: 0 for t in all_ts}
    ev_peak_dead = 0
    for tbl in event_tables:
        for rec in per_table[tbl]:
            ev_bytes_by_ts[rec["ts"]] += rec["total"]
            ev_present_by_ts[rec["ts"]] += 1
            ev_peak_dead = max(ev_peak_dead, rec["n_dead"])

    total_by_ts = {t: 0 for t in all_ts}
    for rec in parsed:
        total_by_ts[rec["ts"]] += rec["total"]

    return dict(
        all_ts=all_ts,
        meta=meta,
        ev_bytes_by_ts=ev_bytes_by_ts,
        ev_peak_dead=ev_peak_dead,
        ev_bytes_start=ev_bytes_by_ts[first_ts],
        ev_bytes_end=ev_bytes_by_ts[last_ts],
        ev_bytes_peak=max(ev_bytes_by_ts.values()),
        ev_present_start=ev_present_by_ts[first_ts],
        ev_present_end=ev_present_by_ts[last_ts],
        total_end=total_by_ts[last_ts],
        total_peak=max(total_by_ts.values()),
        meta_peak_dead=max((v["peak_dead"] for v in meta.values()), default=0),
    )


# ---------------------------------------------------------------------------
def produced_events(phase_dir: str) -> int:
    """Sum pgbench aggregate-log transaction counts (field 2)."""
    total = 0
    for path in glob.glob(os.path.join(phase_dir, "producer_agg.*")):
        with open(path) as f:
            for ln in f:
                parts = ln.split()
                if len(parts) >= 2:
                    try:
                        total += int(parts[1])
                    except ValueError:
                        pass
    return total


def achieved_tps(phase_dir: str) -> float:
    for path in glob.glob(os.path.join(phase_dir, "producer.log")):
        with open(path) as f:
            for ln in f:
                if ln.strip().startswith("tps ="):
                    try:
                        return float(ln.split("=")[1].split("(")[0].strip())
                    except (IndexError, ValueError):
                        pass
    return 0.0


def consumed(phase_dir: str) -> tuple[int, int, dict[str, int]]:
    """(total_events, ack_count, per_worker_events) from acks.log."""
    total = 0
    acks = 0
    per_worker: dict[str, int] = {}
    path = os.path.join(phase_dir, "acks.log")
    if not os.path.exists(path):
        return 0, 0, {}
    with open(path) as f:
        for ln in f:
            parts = ln.strip().split(",")
            if len(parts) < 4:
                continue
            worker, ev = parts[1], parts[3]
            try:
                e = int(ev)
            except ValueError:
                continue
            total += e
            acks += 1
            per_worker[worker] = per_worker.get(worker, 0) + e
    return total, acks, per_worker


def pending_by_slot(rows: list[dict]) -> dict[int, list[float]]:
    out: dict[int, list[float]] = {}
    for r in rows:
        try:
            slot = int(r["slot"])
            pe = float(r["pending_events"])
        except (KeyError, ValueError):
            continue
        out.setdefault(slot, []).append(pe)
    return out


def cpu_mem(phase_dir: str) -> dict:
    rows = read_csv(os.path.join(phase_dir, "sys_metrics.csv"))
    busy, mem_used = [], []
    for r in rows:
        try:
            busy.append(float(r["cpu_user_pct"]) + float(r["cpu_system_pct"]))
            mem_used.append(float(r["mem_used_mb"]))
        except (KeyError, ValueError):
            continue
    if not busy:
        return {}
    return {
        "cpu_avg": statistics.mean(busy),
        "cpu_peak": max(busy),
        "mem_avg_gib": statistics.mean(mem_used) / 1024,
        "mem_peak_gib": max(mem_used) / 1024,
    }


def read_amp(phase_dir: str, produced: int) -> dict:
    rows = read_csv(os.path.join(phase_dir, "pgss_end.csv"))
    recv = {"calls": 0, "rows": 0, "hit": 0, "read": 0, "ms": 0.0}
    for r in rows:
        if "receive_partitioned" not in r.get("query_head", ""):
            continue
        try:
            recv["calls"] += int(r["calls"])
            recv["rows"] += int(r["rows"])
            recv["hit"] += int(r["shared_blks_hit"])
            recv["read"] += int(r["shared_blks_read"])
            recv["ms"] += float(r["total_exec_time_ms"])
        except (KeyError, ValueError):
            continue
    blks = recv["hit"] + recv["read"]
    recv["blks_total"] = blks
    recv["blks_per_event"] = blks / produced if produced else 0.0
    recv["read_per_event"] = recv["read"] / produced if produced else 0.0
    return recv


def stall_timeline(phase_dir: str, stall_slot: int) -> dict | None:
    rows = read_csv(os.path.join(phase_dir, "slot_status.csv"))
    if not rows:
        return None
    # Series for the stalled slot, ordered by ts.
    series = []
    for r in rows:
        try:
            if int(r["slot"]) != stall_slot:
                continue
            ts = parse_iso(r["ts"])
            pe = float(r["pending_events"])
            owner = r.get("lease_owner", "")
            series.append((ts, pe, owner))
        except (KeyError, ValueError):
            continue
    series = [s for s in series if s[0] is not None]
    series.sort(key=lambda x: x[0])
    if len(series) < 3:
        return None

    # The stall window = the contiguous span with no live lease owner (the
    # SIGSTOPped worker stops renewing, so its lease expires).
    stalled = [i for i, s in enumerate(series) if not s[2]]
    if not stalled:
        return {"detected": False, "peak_pending": max(s[1] for s in series)}
    i0, i1 = stalled[0], stalled[-1]
    t0, t1 = series[i0][0], series[i1][0]
    stall_secs = (t1 - t0).total_seconds()
    baseline = min(s[1] for s in series[:i0]) if i0 > 0 else series[0][1]
    peak = max(s[1] for s in series[i0 : i1 + 1])
    growth_rate = (peak - baseline) / stall_secs if stall_secs > 0 else 0.0

    # Catch-up: after resume, time until pending falls back near baseline.
    catchup_secs = None
    thresh = baseline + max(1.0, 0.1 * (peak - baseline))
    for ts, pe, _ in series[i1 + 1 :]:
        if pe <= thresh:
            catchup_secs = (ts - t1).total_seconds()
            break

    # Rotation floor: queue table count over the phase.
    qrows = read_csv(os.path.join(phase_dir, "queue_rate.csv"))
    ntables = []
    for r in qrows:
        try:
            ntables.append(int(r["ntables"]))
        except (KeyError, ValueError):
            continue
    return {
        "detected": True,
        "t0": t0,
        "t1": t1,
        "stall_secs": stall_secs,
        "baseline": baseline,
        "peak_pending": peak,
        "growth_rate": growth_rate,
        "catchup_secs": catchup_secs,
        "ntables_min": min(ntables) if ntables else None,
        "ntables_max": max(ntables) if ntables else None,
    }


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/bench/pk")
    args = ap.parse_args()

    manifest = read_csv(os.path.join(args.out, "phases.csv"))
    if not manifest:
        print(f"# partition-keys bench\n\nNo phases.csv under {args.out}.")
        return 0

    L: list[str] = []
    L.append("# Partition-keys read-amplification benchmark")
    L.append("")
    L.append("Supabase-Storage profile: keyed producer + N slot workers on the real")
    L.append("pgque v0.8 lease API. Maps to SPEC R2 (read amplification ~N x) and R7")
    L.append("(a stalled slot pins rotation for the whole queue).")
    L.append("")

    phase_metrics = []
    for ph in manifest:
        d = ph["dir"]
        n = int(ph["n"])
        dur = int(ph["dur_s"])
        prod = produced_events(d)
        tps = achieved_tps(d)
        ctot, acks, per_worker = consumed(d)
        cthru = ctot / dur if dur else 0.0
        pend = pending_by_slot(read_csv(os.path.join(d, "slot_status.csv")))
        all_pending = [v for vs in pend.values() for v in vs]
        cm = cpu_mem(d)
        ra = read_amp(d, prod)
        bloat = bloat_stats(d)
        # Stall phases (R7): recover the stalled slot from its dominant lag peak,
        # then its lease-expiry window. Computed once here, reused by the bloat
        # block and the stalled-slot section.
        is_stall = ph["label"].startswith("stalled") or "stall" in os.path.basename(d.rstrip("/"))
        stall_slot, stall = None, None
        if is_stall:
            peaks = {s: max(v) for s, v in pend.items() if v}
            stall_slot = max(peaks, key=peaks.get) if peaks else 0
            stall = stall_timeline(d, stall_slot)
        phase_metrics.append(
            dict(label=ph["label"], n=n, rate=int(ph["rate"]), dur=dur,
                 produced=prod, tps=tps, consumed=ctot, cthru=cthru,
                 per_worker=per_worker, pend=pend, all_pending=all_pending,
                 cm=cm, ra=ra, dir=d, queue=ph["queue"], consumer=ph["consumer"],
                 bloat=bloat, is_stall=is_stall, stall_slot=stall_slot, stall=stall))

    # --- per-phase detail ---------------------------------------------------
    for m in phase_metrics:
        L.append(f"## Phase `{m['label']}`  (N={m['n']}, target {m['rate']} ev/s, {m['dur']//60} min)")
        L.append("")
        L.append(f"- Producer: **{fmt(m['tps'])} ev/s** achieved, {fmt(m['produced'])} events sent")
        L.append(f"- Consumers: **{fmt(m['cthru'])} ev/s** end-to-end, {fmt(m['consumed'])} events acked")
        if m["cm"]:
            L.append(f"- CPU: {m['cm']['cpu_avg']:.0f}% avg / {m['cm']['cpu_peak']:.0f}% peak"
                     f" | RAM used: {m['cm']['mem_avg_gib']:.1f} / {m['cm']['mem_peak_gib']:.1f} GiB")
        if m["all_pending"]:
            L.append(f"- Pending events across slots: p50 {fmt(pctile(m['all_pending'],0.5))},"
                     f" p99 {fmt(pctile(m['all_pending'],0.99))}, max {fmt(max(m['all_pending']))}")
        ra = m["ra"]
        if ra["calls"]:
            L.append(f"- Read-amp (receive_partitioned): {fmt(ra['blks_total'])} buffers"
                     f" ({fmt(ra['hit'])} hit / {fmt(ra['read'])} read) over {fmt(ra['calls'])} calls"
                     f" = **{ra['blks_per_event']:.2f} buffers/event**")
        if m["per_worker"]:
            pw = ", ".join(f"{w}:{fmt(c)}" for w, c in sorted(m["per_worker"].items(),
                          key=lambda kv: int(kv[0].lstrip("abcdefghijklmnopqrstuvwxyz") or 0)))
            L.append(f"- Per-slot consumed: {{ {pw} }}")
        b = m["bloat"]
        if b is None:
            L.append("- Bloat & dead tuples: _no bloat.csv in this phase dir; section skipped_")
        else:
            L.append("")
            L.append("**Bloat & dead tuples**")
            L.append("")
            top = sorted(b["meta"].items(), key=lambda kv: kv[1]["peak_dead"], reverse=True)[:5]
            L.append("- Metadata tables by peak dead tuples (end size):")
            for tbl, s in top:
                L.append(f"    - `{tbl}`: peak {fmt(s['peak_dead'])} dead"
                         f" (end {fmt(s['end_dead'])} dead, {fmt_bytes(s['end_bytes'])})")
            ps = b["meta"].get("pgque.partition_slot")
            if ps:
                L.append(f"- `pgque.partition_slot` (v0.8 lease, 2 UPDATEs/batch):"
                         f" peak **{fmt(ps['peak_dead'])}** dead, end {fmt(ps['end_dead'])} dead,"
                         f" {fmt_bytes(ps['end_bytes'])} — HOT updates should keep this tiny")
            for tbl in ("pgque.tick", "pgque.subscription"):
                s = b["meta"].get(tbl)
                if s:
                    L.append(f"- `{tbl}` peak dead tuples: {fmt(s['peak_dead'])} (rotation-metadata churn)")
            L.append(f"- Event tables: {b['ev_present_start']} present at phase start ->"
                     f" {b['ev_present_end']} at end (rotation dropping old `event_N_M`)")
            L.append(f"- Event-table bytes: start {fmt_bytes(b['ev_bytes_start'])},"
                     f" end {fmt_bytes(b['ev_bytes_end'])}, peak {fmt_bytes(b['ev_bytes_peak'])}")
            L.append(f"- Event-table peak dead tuples: {fmt(b['ev_peak_dead'])}"
                     f" (append-only — expect ~0; nonzero means retries)")
            L.append(f"- Total DB footprint: end {fmt_bytes(b['total_end'])}, peak {fmt_bytes(b['total_peak'])}")
            if m["is_stall"]:
                st = m["stall"]
                if st and st.get("detected") and st.get("t0") and st.get("t1"):
                    at = lambda tgt: b["ev_bytes_by_ts"][_nearest_ts(b["all_ts"], tgt)]
                    L.append(f"- R7 pin/release — event-table bytes: stall start {fmt_bytes(at(st['t0']))}"
                             f" -> stall end {fmt_bytes(at(st['t1']))} -> phase end {fmt_bytes(b['ev_bytes_end'])}"
                             f" (grows while the stalled cursor pins rotation, drops after resume)")
                else:
                    L.append("- R7 pin/release: stall window not resolvable (needs slot_status.csv)")
        L.append("")

    # --- read-amp comparison (R2) ------------------------------------------
    steady = [m for m in phase_metrics if m["label"].startswith("steady")]
    if len(steady) >= 2:
        steady.sort(key=lambda m: m["n"])
        lo, hi = steady[0], steady[-1]
        L.append("## Read amplification: N-scaling (SPEC R2)")
        L.append("")
        L.append("| N | buffers/event | read/event | ratio vs smallest N |")
        L.append("|--:|--:|--:|--:|")
        base = lo["ra"]["blks_per_event"] or 1.0
        for m in steady:
            r = m["ra"]["blks_per_event"] / base if base else 0.0
            L.append(f"| {m['n']} | {m['ra']['blks_per_event']:.2f} |"
                     f" {m['ra']['read_per_event']:.2f} | {r:.2f}x |")
        L.append("")
        L.append(f"Every slot scans the full stream and filters server-side, so buffers"
                 f" touched per produced event scale ~linearly with N. Observed"
                 f" {hi['n']}/{lo['n']} = {(hi['ra']['blks_per_event']/base):.2f}x"
                 f" (ideal {hi['n']/lo['n']:.0f}x).")
        L.append("")

    # --- stalled-slot timeline (R7) ----------------------------------------
    stall_phases = [m for m in phase_metrics if m["label"].startswith("stalled")]
    if stall_phases:
        m = stall_phases[0]
        stall_slot = m["stall_slot"] or 0
        st = m["stall"]
        L.append("## Stalled-slot: rotation pinning (SPEC R7)")
        L.append("")
        L.append(f"Stalled slot (by peak lag): **slot {stall_slot}**.")
        if st and st.get("detected"):
            L.append(f"- Lease-expiry stall window: {st['stall_secs']:.0f}s (no live owner)")
            L.append(f"- Pending events: baseline {fmt(st['baseline'])} -> peak {fmt(st['peak_pending'])}")
            L.append(f"- Lag growth during stall: **{fmt(st['growth_rate'])} events/s**")
            if st["catchup_secs"] is not None:
                L.append(f"- Catch-up after resume: **{st['catchup_secs']:.0f}s** back to baseline")
            else:
                L.append("- Catch-up after resume: did not return to baseline within the phase")
            if st["ntables_max"] is not None:
                L.append(f"- Rotation floor: queue held {st['ntables_min']}..{st['ntables_max']}"
                         f" event tables (the stalled slot pins the drop floor)")
        else:
            L.append("- Stall window not clearly detected in slot_status.csv"
                     f" (peak pending {fmt(st['peak_pending']) if st else 'n/a'}).")
        L.append("")

    # --- headline table -----------------------------------------------------
    L.append("## Headline")
    L.append("")
    L.append("| Phase | N | Producer ev/s | Consume ev/s | Pending p99 | Buffers/event | CPU peak | Peak dead tup (meta) | Event tbl GiB end |")
    L.append("|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for m in phase_metrics:
        p99 = fmt(pctile(m["all_pending"], 0.99)) if m["all_pending"] else "-"
        bpe = f"{m['ra']['blks_per_event']:.2f}" if m["ra"]["calls"] else "-"
        cpu = f"{m['cm']['cpu_peak']:.0f}%" if m["cm"] else "-"
        b = m["bloat"]
        meta_dead = fmt(b["meta_peak_dead"]) if b else "-"
        ev_gib = f"{gib(b['ev_bytes_end']):.2f}" if b else "-"
        L.append(f"| {m['label']} | {m['n']} | {fmt(m['tps'])} |"
                 f" {fmt(m['cthru'])} | {p99} | {bpe} | {cpu} | {meta_dead} | {ev_gib} |")
    L.append("")

    print("\n".join(L))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
