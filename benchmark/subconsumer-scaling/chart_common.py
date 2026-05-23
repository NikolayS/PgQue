from __future__ import annotations

import csv
import json
from pathlib import Path

BG = "#002b36"
SURFACE = "#073642"
FG = "#839496"
FG_EMPH = "#93a1a1"
FG_DIM = "#586e75"
PALETTE = ["#268bd2", "#2aa198", "#b58900", "#cb4b16", "#6c71c4", "#859900"]


def run_dirs(root: Path) -> list[Path]:
    return sorted(p for p in root.iterdir() if p.is_dir() and p.name.endswith("-workers"))


def load_meta(run_dir: Path) -> dict:
    return json.loads((run_dir / "run_meta.json").read_text())


def load_series(path: Path, x_col: str, y_col: str) -> tuple[list[int], list[int]]:
    xs, ys = [], []
    with path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            xs.append(int(row[x_col]))
            ys.append(int(row[y_col]))
    return xs, ys
