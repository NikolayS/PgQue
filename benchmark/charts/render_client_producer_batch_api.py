#!/usr/bin/env python3
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Render the client producer batch API SVG from benchmark CSV."""

from __future__ import annotations

import csv
import html
import math
from pathlib import Path

HERE = Path(__file__).resolve().parent
CSV_PATH = HERE / "client_producer_batch_api.csv"
SVG_PATH = HERE / "client_producer_batch_api.svg"

LANGS = ["python", "go", "typescript"]
LABEL = {"python": "Python", "go": "Go", "typescript": "TypeScript"}
SHORT = {"python": "Py", "go": "Go", "typescript": "Ty"}
COLORS = {"python": "#2563eb", "go": "#0891b2", "typescript": "#7c3aed"}


def load() -> dict[tuple[str, str, int], dict[str, float]]:
    data = {}
    with CSV_PATH.open(newline="") as f:
        for row in csv.DictReader(f):
            key = (row["language"], row["method"], int(row["batch_size"]))
            data[key] = {
                "median_ms": float(row["median_ms"]),
                "events_per_sec": float(row["events_per_sec"]),
                "repeats": int(row["repeats"]),
            }
    return data


def text(x, y, body, **attrs):
    base = {
        "x": x,
        "y": y,
        "font-family": "Inter,Arial,sans-serif",
        "font-size": attrs.pop("font_size", 13),
        "fill": attrs.pop("fill", "#6b7280"),
    }
    if "anchor" in attrs:
        base["text-anchor"] = attrs.pop("anchor")
    if "weight" in attrs:
        base["font-weight"] = attrs.pop("weight")
    base.update(attrs)
    a = " ".join(f'{k}="{html.escape(str(v))}"' for k, v in base.items())
    return f"<text {a}>{html.escape(str(body))}</text>"


def rect(x, y, w, h, fill, rx=4, **attrs):
    base = {"x": x, "y": y, "width": w, "height": h, "fill": fill, "rx": rx}
    base.update(attrs)
    a = " ".join(f'{k}="{html.escape(str(v))}"' for k, v in base.items())
    return f"<rect {a}/>"


def line(x1, y1, x2, y2, stroke="#e5e7eb", **attrs):
    base = {"x1": x1, "y1": y1, "x2": x2, "y2": y2, "stroke": stroke}
    base.update(attrs)
    a = " ".join(f'{k}="{html.escape(str(v))}"' for k, v in base.items())
    return f"<line {a}/>"


def speedup_label(v: float) -> str:
    if v < 1:
        return f"{v:.2f}×"
    if v < 10:
        return f"{v:.1f}×"
    return f"{v:.0f}×"


def main() -> None:
    data = load()
    out = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="860" viewBox="0 0 1280 860">',
        rect("0", "0", "100%", "100%", "#ffffff", rx=0),
        text(640, 34, "PgQue clients: batching vs loop over send()", anchor="middle", font_size=24, weight=800, fill="#111827"),
        text(640, 60, "Single sequential producer, one DB connection/client, median of 3 repeats on GitHub Actions PostgreSQL 18 runner", anchor="middle", font_size=13),
        text(92, 82, "A. Throughput speedup from batch API", font_size=17, weight=700, fill="#111827"),
        text(92, 100, "batch events/sec ÷ loop-over-send events/sec; higher is better", font_size=12),
    ]

    # Panel A: speedup bars.
    left_x, top_y, bottom_y, width = 92, 126.5, 372.0, 520
    max_speed = 50.0
    for tick in [0, 10, 20, 30, 40, 50]:
        y = bottom_y - (tick / max_speed) * (bottom_y - top_y)
        out.append(line(left_x, f"{y:.1f}", left_x + width, f"{y:.1f}"))
        out.append(text(82, f"{y + 4:.1f}", f"{tick}×", anchor="end", font_size=11))

    groups = {1: 172, 100: 342, 1000: 512}
    for n, center in groups.items():
        out.append(text(center, 396, f"n={n}", anchor="middle", font_size=12))
        for i, lang in enumerate(LANGS):
            loop = data[(lang, "send_loop", n)]["events_per_sec"]
            batch = data[(lang, "send_batch", n)]["events_per_sec"]
            speed = batch / loop
            h = max(1.5, (speed / max_speed) * (bottom_y - top_y))
            x = center - 60 + i * 40
            y = bottom_y - h
            out.append(rect(f"{x:.1f}", f"{y:.1f}", 34, f"{h:.1f}", COLORS[lang]))
            out.append(text(f"{x + 17:.1f}", f"{y - 6:.1f}", speedup_label(speed), anchor="middle", font_size=10, weight=700, fill=COLORS[lang]))

    for x, lang in [(147, "python"), (287, "go"), (427, "typescript")]:
        out.append(rect(x, 416, 13, 13, COLORS[lang], rx=3) + text(x + 19, 427, LABEL[lang], font_size=12))

    # Panel B: latency sticks on log scale.
    out.extend([
        text(710, 82, "B. Producer latency per batch", font_size=17, weight=700, fill="#111827"),
        text(710, 100, "milliseconds to publish the whole batch; log scale, lower is better", font_size=12),
    ])
    log_bottom = 372.0
    def y_ms(ms: float) -> float:
        return 357.7 - 92.3 * math.log10(ms)

    for ms in [1, 3, 10, 30, 100, 300]:
        y = y_ms(ms)
        out.append(line(710, f"{y:.1f}", 1180, f"{y:.1f}"))
        out.append(text(700, f"{y + 4:.1f}", f"{ms} ms", anchor="end", font_size=11))

    x0, step = 730, 53.75
    idx = 0
    for lang in LANGS:
        for n in [1, 100, 1000]:
            cx = x0 + idx * step
            loop_ms = data[(lang, "send_loop", n)]["median_ms"]
            batch_ms = data[(lang, "send_batch", n)]["median_ms"]
            out.append(line(f"{cx - 7:.1f}", log_bottom, f"{cx - 7:.1f}", f"{y_ms(loop_ms):.1f}", "#94a3b8", **{"stroke-width": 7, "stroke-linecap": "round"}))
            out.append(line(f"{cx + 7:.1f}", log_bottom, f"{cx + 7:.1f}", f"{y_ms(batch_ms):.1f}", "#16a34a", **{"stroke-width": 7, "stroke-linecap": "round"}))
            out.append(text(f"{cx:.1f}", 390, SHORT[lang], anchor="middle", font_size=9))
            out.append(text(f"{cx:.1f}", 403, n, anchor="middle", font_size=9))
            idx += 1

    out.append(rect(810, 422, 14, 10, "#94a3b8", rx=2) + text(830, 432, "loop over send()", font_size=12))
    out.append(rect(970, 422, 14, 10, "#16a34a", rx=2) + text(990, 432, "batch API", font_size=12))

    out.extend([
        rect(92, 480, 1088, 300, "#f8fafc", rx=16, stroke="#e2e8f0"),
        text(116, 516, "What bottleneck is this exposing?", font_size=18, weight=800, fill="#111827"),
    ])
    notes = [
        ("loop over send(), n=100/1000", "Dominated by client↔Postgres round trips and per-call driver overhead. Each message is one SQL statement."),
        ("batch API, n=100/1000", "Round trips collapse to one SQL call. Remaining cost is payload JSON serialization + one set-based server insert."),
        ("server-side note", "Current send_batch uses the set-based insert_event_bulk path; green bars are the GA baseline, not the old pre-#159 path."),
        ("n=1", "Batch and single send are intentionally similar; batching pays off once multiple events share one logical publish operation."),
        ("single producer", "These are not max cluster throughput numbers. They isolate producer API overhead for one sequential producer/client."),
    ]
    for i, (head, body) in enumerate(notes):
        y = 544 + i * 40
        out.append(f'<circle cx="122" cy="{y + 3}" r="5" fill="#0f766e"/>')
        out.append(text(138, y, head, font_size=13, weight=700, fill="#111827"))
        out.append(text(327, y, body, font_size=13))

    out.append(text(92, 832, "Source: GitHub Actions run 26383024550, branch bench-client-producer-rc3. Metrics are medians over 3 repeats; each run creates a fresh queue and verifies inserted row count.", font_size=11))
    out.append("</svg>")
    SVG_PATH.write_text("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
