# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Resolve the client version from installed or source metadata."""

from importlib import metadata
from pathlib import Path
import re


_DISTRIBUTION_NAME = "pgque-py"
_PYPROJECT_PATH = Path(__file__).resolve().parent.parent / "pyproject.toml"
_UNKNOWN_VERSION = "0+unknown"
_VERSION_ASSIGNMENT = re.compile(
    r"^version\s*=\s*(['\"])([^'\"]+)\1\s*(?:#.*)?$"
)


def source_version(pyproject_path: Path) -> str:
    """Read ``project.version`` when running from an unpackaged source tree."""
    try:
        lines = pyproject_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return _UNKNOWN_VERSION

    in_project_table = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("["):
            in_project_table = stripped == "[project]"
            continue
        if not in_project_table:
            continue

        match = _VERSION_ASSIGNMENT.fullmatch(stripped)
        if match:
            return match.group(2)

    return _UNKNOWN_VERSION


def resolve_version() -> str:
    """Return installed metadata, falling back for direct source imports."""
    try:
        return metadata.version(_DISTRIBUTION_NAME)
    except metadata.PackageNotFoundError:
        return source_version(_PYPROJECT_PATH)
