#!/usr/bin/env python3
"""Failure by MVCC — animated GIF v17 Solarized Dark (v16 dark palette swap).

Ethan Schoonover's Solarized Dark palette. Backgrounds: base03 (#002b36)
for the figure + axes, base02 (#073642) for the TX phase-band wash and
gridlines. Text in three tiers: base0 (#839496) primary, base1 (#93a1a1)
emphasized (axis y-labels, column headers, now-cursor), base01 (#586e75)
de-emphasized (spines, phase-boundary axvlines, inactive phase labels).
Alert: Solarized red (#dc322f) for the hinge flash box + active TX-phase
label + TX ribbon.

Line palette uses the Solarized accent colors:
  pgque  = blue    #268bd2  (hero — most saturated)
  pgq    = cyan    #2aa198  (sibling, cool family)
  pgmq   = red     #dc322f
  river  = orange  #cb4b16
  que    = violet  #6c71c4
  pgboss = green   #859900  (warm yellow-green)
Yellow (#b58900) reserved for future highlighting.

Edge handling: if a tail would fall off-axis (near t=0 or t=150), flip
that arrow's direction only so it stays on-axis; if flipping would make
the two tails coincide, rebalance with joint-clamp ±TAIL_DX_MIN.

Everything else unchanged from v12: clean set_ylim(0, y_peak*1.05),
uniform legend, mono numbers, inline endpoint labels + anti-overlap,
no title, hinge flash, 6 systems.

Outputs:
  /tmp/bench_r4_24h/death_spiral.gif
  /tmp/bench_r4_24h/death_spiral_hero.png
"""
import csv
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.transforms as mtransforms
import numpy as np
from matplotlib.ticker import FuncFormatter
from PIL import Image

# ---- tunables ----------------------------------------------------------------
ROOT      = Path("/tmp/bench_r4_24h")
OUT_GIF   = ROOT / "death_spiral.gif"
OUT_HERO  = ROOT / "death_spiral_hero.png"
FPS           = 25
DURATION_S    = 22
FRAME_COUNT   = FPS * DURATION_S
HOLD_FRAMES   = FPS * 1
FIG_W_IN, FIG_H_IN, DPI = 9.0, 7.0, 100   # 900x700 px
X_MAX_MIN     = 150
FLASH_FRAMES  = 12
LAT_CLIP_MS   = 1200
INLINE_MIN_DEAD = 50_000    # suppress inline dead-tuple labels below this
INLINE_MIN_LAT_MS = 10      # suppress inline latency labels below this
# -----------------------------------------------------------------------------

SYSTEMS = ["pgque", "pgq", "pgmq", "river", "que", "pgboss"]
# --- Solarized Dark palette (Ethan Schoonover) ---------------------------
# Backgrounds
BG         = "#002b36"   # base03 — primary figure + axes background
SURFACE    = "#073642"   # base02 — surface/highlight + TX phase-band wash
# Text tiers
FG         = "#839496"   # base0  — primary text (ticks, values, inline)
FG_EMPH    = "#93a1a1"   # base1  — emphasized (axis y-labels, column hdrs)
FG_DIM     = "#586e75"   # base01 — de-emphasized (inactive phase labels)
# Grid / rules / structure
GRID       = SURFACE     # grid matches surface: reads as structure
SPINE      = FG_DIM      # base01 for spines
PHASE_RULE = FG_DIM      # base01 for boundary axvlines at t=30/90
# Alert (Solarized's canonical red, no separate ember wash)
RED        = "#dc322f"   # hinge flash + TX-phase ribbon + active phase label
TX_BAND    = SURFACE     # TX phase wash = surface, not an alert hue
# Solarized accents (for reference):
# blue #268bd2  cyan #2aa198  red #dc322f  magenta #d33682
# orange #cb4b16  violet #6c71c4  green #859900  yellow #b58900
# -------------------------------------------------------------------------

COLORS = {
    "pgque":   "#268bd2",   # Solarized blue — hero, most saturated
    "pgq":     "#2aa198",   # Solarized cyan — sibling (cool family)
    "pgmq":    "#dc322f",   # Solarized red
    "river":   "#cb4b16",   # Solarized orange
    "que":     "#6c71c4",   # Solarized violet
    "pgboss":  "#859900",   # Solarized green (warm yellow-green)
}
LW = {s: (3.0 if s == "pgque" else 2.2) for s in SYSTEMS}
EVENT_RE = {
    "pgque":   re.compile(r"^pgque\.event_\d+_\d+$"),
    "pgq":     re.compile(r"^pgq\.event_\d+_\d+$"),
    "pgmq":    re.compile(r"^pgmq\.q_bench_queue$"),
    "river":   re.compile(r"^public\.river_job$"),
    "que":     re.compile(r"^public\.que_jobs$"),
    "pgboss":  re.compile(r"^pgboss\.job_common$|^pgboss\.j[a-f0-9]+$"),
}
PHASE_BOUNDS = [(0, 30, "clean"), (30, 90, "tx"), (90, 150, "clean")]

plt.rcParams.update({
    "font.family": ["Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.spines.bottom": True,
    "axes.spines.left": True,
    "axes.edgecolor": SPINE,
    "axes.linewidth": 0.9,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "axes.axisbelow": True,
    # --- dark theme colors (applied to figure, axes, text, ticks) ---
    "figure.facecolor": BG,
    "axes.facecolor":   BG,
    "savefig.facecolor": BG,
    "text.color":       FG,
    "axes.labelcolor":  FG,
    "axes.titlecolor":  FG,
    "xtick.color":      FG,
    "ytick.color":      FG,
})


def _mins_from(tss):
    t0 = datetime.fromisoformat(tss[0].replace("Z", ""))
    return np.array([
        (datetime.fromisoformat(t.replace("Z", "")) - t0).total_seconds() / 60
        for t in tss
    ])


def load_bloat():
    series = {}
    for sysn in SYSTEMS:
        p = ROOT / sysn / "bloat.csv"
        if not p.exists():
            continue
        by_ts = {}
        with open(p) as f:
            r = csv.reader(f)
            next(r)
            for row in r:
                try:
                    if not EVENT_RE[sysn].match(row[1]):
                        continue
                    by_ts.setdefault(row[0], 0)
                    by_ts[row[0]] += int(row[3])
                except Exception:
                    pass
        tss = sorted(by_ts)
        if not tss:
            continue
        mins = _mins_from(tss)
        dead = np.array([by_ts[t] for t in tss], dtype=float)
        series[sysn] = {"mins": mins, "dead": dead}
    return series


_PROGRESS_RE = re.compile(
    r"^progress:\s*([\d.]+)\s*s,\s*[\d.]+\s*tps,\s*lat\s*([\d.]+)\s*ms\s*stddev\s*([\d.]+)"
)


def load_latency():
    series = {}
    for sysn in SYSTEMS:
        p = ROOT / sysn / "consumer.log"
        if not p.exists():
            continue
        mins, lat, std = [], [], []
        with open(p) as f:
            for ln in f:
                m = _PROGRESS_RE.match(ln)
                if not m:
                    continue
                t_s = float(m.group(1))
                mins.append(t_s / 60.0)
                lat.append(float(m.group(2)))
                std.append(float(m.group(3)))
        if not mins:
            continue
        mins = np.array(mins); lat = np.array(lat); std = np.array(std)
        series[sysn] = {"mins": mins, "lat": lat, "std": std}
    return series


def fmt_y_dead(v, _):
    if v >= 1e6: return f"{v/1e6:.1f}M"
    if v >= 1e3: return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def fmt_y_lat(v, _):
    if v >= 1000: return f"{v/1000:.1f}s"
    if v >= 10:   return f"{v:.0f}ms"
    if v >= 1:    return f"{v:.1f}ms"
    return f"{v:.2f}ms"  # sub-ms: show real number, not 0


def current_phase(now_min):
    for start, end, kind in PHASE_BOUNDS:
        if start <= now_min < end:
            return kind, start, end
    s, e, k = PHASE_BOUNDS[-1]
    return k, s, e


def _apply_phase_bands(ax):
    for start, end, kind in PHASE_BOUNDS:
        if kind == "tx":
            # Solarized: TX band uses surface (base02) at full alpha so
            # the phase reads as a visibly-darker-than-bg region without
            # any alert-color wash.
            ax.axvspan(start, end, color=TX_BAND, alpha=1.0, zorder=0)
    for b in (30, 90):
        ax.axvline(x=b, color=PHASE_RULE, linewidth=0.8, zorder=0.5)


def _plot_series(ax, series, sysn, now_min, y_key, y_clip=None,
                 inline_threshold=None, inline_fmt=None, inline_out=None):
    """Plot a series; return the endpoint y value.

    If inline_threshold/inline_fmt/inline_out are provided and the endpoint
    y is above threshold, append a proposed inline-label dict to
    inline_out for later anti-overlap placement in data coordinates.
    """
    mins = series[sysn]["mins"]
    y = series[sysn][y_key].copy()
    if y_clip is not None:
        y = np.minimum(y, y_clip)
    mask = mins <= now_min
    if mask.sum() < 1:
        return None
    x_sel = mins[mask]; y_sel = y[mask]
    if mask.sum() < len(mins):
        idx = mask.sum()
        if 0 < idx < len(mins):
            prev_x, next_x = mins[idx - 1], mins[idx]
            if next_x > prev_x:
                frac = min(max((now_min - prev_x) / (next_x - prev_x), 0), 1)
                y_i = y[idx - 1] + frac * (y[idx] - y[idx - 1])
                x_sel = np.append(x_sel, now_min)
                y_sel = np.append(y_sel, y_i)
    alpha = 1.0 if sysn == "pgque" else 0.85
    ax.plot(x_sel, y_sel, color=COLORS[sysn], linewidth=LW[sysn], alpha=alpha,
            zorder=3 if sysn == "pgque" else 2, solid_capstyle="round")
    if len(x_sel) > 0:
        x_end, y_end = x_sel[-1], y_sel[-1]
        ax.plot([x_end], [y_end], marker="o", markersize=5,
                color=COLORS[sysn], markeredgecolor=BG,
                markeredgewidth=1.0, zorder=5)
        if (inline_threshold is not None and inline_fmt is not None
                and inline_out is not None and y_end >= inline_threshold):
            inline_out.append({
                "sysn": sysn,
                "x": x_end,
                "y_true": y_end,
                "text": f"{sysn}: {inline_fmt(y_end, None)}",
                "color": COLORS[sysn],
                "weight": "bold" if sysn == "pgque" else "normal",
            })
    return y_sel[-1] if len(y_sel) else None


# v13: arrows form a "V" — pgque tail LEFT of cursor, pgq tail RIGHT of
# cursor. Both heads meet at the cursor on the pgque/pgq lines, so the
# two shafts diverge outward at ~45°. Labels sit directly under each tail
# (no separate stagger variable — label x = tail x).
# v14 spacing: tail at -0.06, label at -0.07 (label sits just below the
# arrow tail — much snugger than v13's -0.06 / -0.10).
TAIL_DX_MIN = 5.0          # tail offset in data-minutes; tuned for ~45°
ARROW_TAIL_AX_FRAC  = -0.06   # unchanged from v13
ARROW_LABEL_AX_FRAC = -0.07   # v14: raised from -0.10, snug under tail


def _draw_cursor_arrows(ax, series, y_key, y_peak, now_min):
    """Draw two hairline up-arrows in a divergent "V" pattern. Head of each
    arrow is on the pgque/pgq line at x = now_min (pure data coords). Tail
    is offset outward by TAIL_DX_MIN minutes (pgque tail left, pgq tail
    right) and below the axes by ARROW_TAIL_AX_FRAC in axes-fraction — so
    each shaft tilts outward at ~45° in pixel space.

    If a tail would fall outside the axes x-range (near t=0 / t=150), flip
    that arrow's direction so it stays on-axis, then widen the gap if the
    flip would make the two labels collide.
    """
    x_data_min, x_data_max = ax.get_xlim()
    now_clamped = max(min(now_min, x_data_max), x_data_min)

    # Directional tail offset per system (pgque goes left, pgq goes right).
    tail_dir = {"pgque": -1, "pgq": +1}
    tail_x_raw = {s: now_clamped + tail_dir[s] * TAIL_DX_MIN for s in tail_dir}

    # Edge handling: if a tail is off-axis, flip it to the other side. If
    # BOTH end up on the same side after flipping, spread them apart with
    # ±TAIL_DX_MIN around the cursor (falling back to clamped positions).
    for s in ("pgque", "pgq"):
        if tail_x_raw[s] < x_data_min or tail_x_raw[s] > x_data_max:
            tail_x_raw[s] = now_clamped - tail_dir[s] * TAIL_DX_MIN
    # If the flip made them coincide / cross, rebalance.
    if abs(tail_x_raw["pgque"] - tail_x_raw["pgq"]) < TAIL_DX_MIN:
        # place them symmetrically around the cursor, then clamp joint-wise
        lo = max(now_clamped - TAIL_DX_MIN, x_data_min)
        hi = lo + 2 * TAIL_DX_MIN
        if hi > x_data_max:
            hi = x_data_max
            lo = hi - 2 * TAIL_DX_MIN
        tail_x_raw = {"pgque": lo, "pgq": hi}
    # Final per-axis clamp (belt & suspenders).
    tail_x_for = {s: max(min(tail_x_raw[s], x_data_max), x_data_min)
                  for s in ("pgque", "pgq")}

    # Blended transform: x = data, y = axes-fraction.
    tail_trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)

    for sysn in ("pgque", "pgq"):
        if sysn not in series:
            continue
        mins = series[sysn]["mins"]
        ys   = series[sysn][y_key]
        if len(mins) == 0 or now_clamped < mins[0] or now_clamped > mins[-1]:
            continue
        y_target = float(np.interp(now_clamped, mins, ys))
        weight = "bold" if sysn == "pgque" else "normal"
        tail_x = tail_x_for[sysn]
        # Arrow: head on the line (data coords at now_clamped), tail at
        # (tail_x, ARROW_TAIL_AX_FRAC). Tilted ~45° because head x != tail x.
        ax.annotate(
            "",
            xy=(now_clamped, y_target),
            xycoords=ax.transData,
            xytext=(tail_x, ARROW_TAIL_AX_FRAC),
            textcoords=tail_trans,
            arrowprops=dict(
                arrowstyle="->",
                color=COLORS[sysn],
                linewidth=0.8,
                shrinkA=0, shrinkB=2,
            ),
            annotation_clip=False,
            zorder=12,
        )
        # Label sits directly under its OWN tail.
        ax.text(tail_x, ARROW_LABEL_AX_FRAC, sysn,
                transform=tail_trans,
                ha="center", va="top",
                fontsize=8.5, color=COLORS[sysn],
                fontweight=weight,
                clip_on=False, zorder=12)


def _place_inline_labels(ax, proposals, y_peak, gap_frac=0.04):
    """Top-down vertical nudge so labels don't overlap.
    Drops any label pushed out of [0, y_peak]. Draws with monospace.
    """
    if not proposals:
        return
    gap = y_peak * gap_frac
    # Sort descending by true y; walk top-down, push each label down if too close
    ordered = sorted(proposals, key=lambda p: -p["y_true"])
    prev_y = None
    for p in ordered:
        y = p["y_true"]
        if prev_y is not None and y > prev_y - gap:
            y = prev_y - gap
        p["y_draw"] = y
        prev_y = y
    dx = X_MAX_MIN * 0.015
    for p in ordered:
        y = p["y_draw"]
        if y < 0 or y > y_peak:
            continue  # label forced out of frame — drop it
        ax.text(p["x"] + dx, y, p["text"],
                color=p["color"], fontsize=8.5,
                fontfamily="monospace", fontweight=p["weight"],
                ha="left", va="center", zorder=6)


def render_frame(bloat, latency, frame_idx, total_frames, y_peak_dead, y_peak_lat):
    now_min = X_MAX_MIN * (frame_idx / max(total_frames - 1, 1))

    fig = plt.figure(figsize=(FIG_W_IN, FIG_H_IN), dpi=DPI)
    # No title block — top whitespace reclaimed for the axes.
    # v12: hspace + bottom margin sized so each panel has figure-level
    # whitespace BELOW its x-axis tick labels for the pgque/pgq arrows
    # + labels (which live outside the axes via negative axes-fraction y).
    # Tuned so top panel bottom lands at ~42% of figure height (not
    # compressed) while leaving room below bot-panel for arrow+label.
    gs = fig.add_gridspec(2, 1, hspace=0.45, top=0.95, bottom=0.10,
                          left=0.10, right=0.96,
                          height_ratios=[1.0, 0.58])
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])

    for ax in (ax1, ax2):
        _apply_phase_bands(ax)

    # Phase labels on top axis only
    for start, end, kind in PHASE_BOUNDS:
        label = ("xmin horizon blocked · 1h" if kind == "tx"
                 else ("clean baseline · 30m" if start == 0 else "clean recovery · 1h"))
        color = RED if kind == "tx" else FG_DIM
        is_current = (start <= now_min < end)
        weight = "bold" if (is_current or kind == "tx") else "normal"
        alpha = 1.0 if is_current else (0.7 if kind == "tx" else 0.55)
        ax1.text((start + end) / 2, 1.04, label,
                 transform=ax1.get_xaxis_transform(),
                 ha="center", color=color, fontsize=10,
                 fontweight=weight, alpha=alpha)

    # --- panel 1: dead tuples ---
    dead_vals = {}
    dead_inline = []
    for sysn in SYSTEMS:
        if sysn in bloat:
            v = _plot_series(ax1, bloat, sysn, now_min, "dead",
                             inline_threshold=INLINE_MIN_DEAD,
                             inline_fmt=fmt_y_dead,
                             inline_out=dead_inline)
            if v is not None:
                dead_vals[sysn] = v
    _place_inline_labels(ax1, dead_inline, y_peak_dead)
    _draw_cursor_arrows(ax1, bloat, "dead", y_peak_dead, now_min)

    # --- panel 2: latency ---
    lat_vals = {}
    lat_inline = []
    for sysn in SYSTEMS:
        if sysn in latency:
            v = _plot_series(ax2, latency, sysn, now_min, "lat", y_clip=LAT_CLIP_MS,
                             inline_threshold=INLINE_MIN_LAT_MS,
                             inline_fmt=fmt_y_lat,
                             inline_out=lat_inline)
            if v is not None:
                lat_vals[sysn] = v
    _place_inline_labels(ax2, lat_inline, y_peak_lat)
    _draw_cursor_arrows(ax2, latency, "lat", y_peak_lat, now_min)

    # Hinge flash — dark theme: black-bg + bright-red border + bright-red text
    hinge_min = 30
    frames_since_hinge = (now_min - hinge_min) / X_MAX_MIN * (total_frames - 1)
    if 0 <= frames_since_hinge <= FLASH_FRAMES:
        flash_alpha = 1.0 - (frames_since_hinge / FLASH_FRAMES)
        ax1.axvline(x=hinge_min, color=RED, linewidth=3.5,
                    alpha=flash_alpha * 0.85, zorder=6)
        ax2.axvline(x=hinge_min, color=RED, linewidth=3.5,
                    alpha=flash_alpha * 0.85, zorder=6)
        ax1.text(hinge_min + 1, y_peak_dead * 0.92,
                 "xmin horizon blocked\nvacuum can't advance",
                 color=RED, fontsize=11, fontweight="bold",
                 alpha=flash_alpha, zorder=7,
                 bbox=dict(boxstyle="round,pad=0.3", facecolor=BG,
                           edgecolor=RED, linewidth=1.2, alpha=flash_alpha))

    # Now cursor on both panels
    for ax, y_peak in ((ax1, y_peak_dead), (ax2, y_peak_lat)):
        ax.axvline(x=now_min, color=FG_EMPH, linewidth=1.2, alpha=0.70, zorder=8)
        ax.plot([now_min], [y_peak * 1.005], marker="v", color=FG_EMPH,
                markersize=7, zorder=9, clip_on=False)

    # Phase / time readout
    kind, p_start, p_end = current_phase(now_min)
    ribbon_color = RED if kind == "tx" else FG_DIM
    ribbon_label = ("XMIN HORIZON BLOCKED (vacuum stalled)" if kind == "tx"
                    else ("CLEAN BASELINE" if p_start == 0 else "CLEAN RECOVERY"))
    # Footer: phase label stays sans; t = ... is mono for stable digit columns.
    # Compose as two adjacent fig.text calls anchored around figure center.
    phase_text = f"phase: {ribbon_label}"
    time_text  = f"t = {now_min:5.1f} min / 150 min"
    fig.text(0.5, 0.015, phase_text + "  |  ",
             ha="right", fontsize=10, color=ribbon_color, fontweight="bold")
    fig.text(0.5, 0.015, time_text,
             ha="left", fontsize=10, color=ribbon_color, fontweight="bold",
             fontfamily="monospace")

    # Axes
    for ax in (ax1, ax2):
        ax.set_xlim(-1, X_MAX_MIN)
        ax.set_xticks([0, 30, 60, 90, 120, 150])
        ax.set_xticklabels(["0", "30m", "1h", "1h30", "2h", "2h30"])
    # v8: no below-axis headroom (start at 0), but keep '0' tick label hidden
    # so the baseline gridline stays without the "0" text crowding the axis.
    # v12: clean axis bounds — no negative headroom. The arrows live
    # OUTSIDE the axes rectangle via mixed (data, axes-fraction) transforms
    # and annotation_clip=False, so the plot data area is not compressed.
    ax1.set_ylim(0, y_peak_dead * 1.05)
    ax1.yaxis.set_major_formatter(FuncFormatter(fmt_y_dead))
    ax1.set_ylabel("event-table dead tuples", labelpad=8)

    ax2.set_ylim(0, y_peak_lat * 1.05)
    ax2.yaxis.set_major_formatter(FuncFormatter(fmt_y_lat))
    ax2.set_ylabel("consumer latency (mean)", labelpad=8)

    # Mono-only for numeric y-tick labels; hide the '0' tick.
    for ax in (ax1, ax2):
        for lbl, pos in zip(ax.get_yticklabels(), ax.get_yticks()):
            lbl.set_fontfamily("monospace")
            if pos == 0:
                lbl.set_visible(False)

    # Legend: uniform row grid, baseline-aligned, two cells per row.
    # Name (sans) right-aligned at x=0.88; value (mono) right-aligned at x=0.985.
    # Using va="baseline" for every cell so sans and mono glyphs sit on the
    # same typographic baseline regardless of family metrics.
    # ROW_SPACING is in axes-fraction — bottom panel is ~0.58x the height of
    # top, so we scale to keep visual row rhythm consistent in physical pixels.
    def _draw_legend(ax, vals, fmt, header, row_spacing, header_gap):
        header_y   = 0.97
        rows_start = header_y - header_gap
        ax.text(0.985, header_y, header,
                transform=ax.transAxes, ha="right", va="baseline",
                color=FG_EMPH, fontsize=8.5, zorder=10)
        visible = [s for s in SYSTEMS if s in vals]
        for i, sysn in enumerate(visible):
            cur = vals[sysn]
            weight = "bold" if sysn == "pgque" else "normal"
            y = rows_start - i * row_spacing
            ax.text(0.88, y, sysn,
                    transform=ax.transAxes, ha="right", va="baseline",
                    color=COLORS[sysn], fontsize=9, fontweight=weight,
                    zorder=10)
            ax.text(0.985, y, fmt(cur, None),
                    transform=ax.transAxes, ha="right", va="baseline",
                    color=COLORS[sysn], fontsize=9, fontweight=weight,
                    fontfamily="monospace", zorder=10)

    # Scale row spacing by inverse of panel height so visual pixel rhythm
    # matches between the hero (top) and supporting (bottom) panels.
    _draw_legend(ax1, dead_vals, fmt_y_dead, "dead tuples (event tables)",
                 row_spacing=0.065, header_gap=0.05)
    _draw_legend(ax2, lat_vals, fmt_y_lat, "latency (mean)",
                 row_spacing=0.112, header_gap=0.086)

    fig.canvas.draw()
    buf = np.asarray(fig.canvas.buffer_rgba())
    img = Image.fromarray(buf).convert("RGB")
    plt.close(fig)
    return img


def build_animation():
    print("Loading bloat CSVs...")
    bloat = load_bloat()
    print(f"  bloat series: {sorted(bloat.keys())}")
    print("Loading latency from consumer logs...")
    latency = load_latency()
    print(f"  latency series: {sorted(latency.keys())}")

    y_peak_dead = max(bloat[s]["dead"].max() for s in bloat)
    peak_lat_observed = max(
        min(latency[s]["lat"].max(), LAT_CLIP_MS) for s in latency
    )
    y_peak_lat = peak_lat_observed
    print(f"  y_peak dead = {y_peak_dead:.0f}, y_peak lat = {y_peak_lat:.1f} ms")

    print(f"Rendering {FRAME_COUNT} frames ({FIG_W_IN*DPI:.0f}x{FIG_H_IN*DPI:.0f} px)...")
    frames = []
    for i in range(FRAME_COUNT):
        if i % 50 == 0:
            print(f"  frame {i}/{FRAME_COUNT}")
        frames.append(render_frame(bloat, latency, i, FRAME_COUNT, y_peak_dead, y_peak_lat))
    for _ in range(HOLD_FRAMES):
        frames.append(frames[-1])

    frames[-1].save(OUT_HERO, format="PNG", optimize=True)
    print(f"Wrote hero PNG: {OUT_HERO}")

    gifski = shutil.which("gifski")
    if gifski:
        print("Using gifski...")
        tmp_dir = Path("/tmp/r4_gif_frames_v17_solarized")
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir()
        for idx, fr in enumerate(frames):
            fr.save(tmp_dir / f"frame_{idx:04d}.png")
        # Solarized dark has large flat-color background regions (base03
        # figure bg + base02 TX phase wash) that low gifski quality turns
        # into visible speckle. Push quality to default=90, add --extra for
        # the slower-but-cleaner palette path, and --lossy-quality=100 to
        # disable the noise/streak dither that produces the speckle.
        subprocess.run(
            [gifski, "-o", str(OUT_GIF), "--fps", str(FPS),
             "--width", str(int(FIG_W_IN * DPI)),
             "--quality", "68",
             "--lossy-quality", "75",
             "--extra",
             *sorted(str(p) for p in tmp_dir.glob("*.png"))],
            check=True,
        )
        shutil.rmtree(tmp_dir)
    else:
        print("gifski missing; using PIL...")
        pal_frames = [f.quantize(colors=128, method=Image.MEDIANCUT, dither=Image.Dither.NONE)
                      for f in frames]
        pal_frames[0].save(OUT_GIF, save_all=True,
                           append_images=pal_frames[1:],
                           duration=int(1000 / FPS), loop=0,
                           optimize=True, disposal=2)
    print(f"Wrote GIF: {OUT_GIF} ({OUT_GIF.stat().st_size/1e6:.2f} MB)")


if __name__ == "__main__":
    build_animation()
