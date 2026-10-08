# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Resolve the client version from installed or source metadata."""

from importlib import metadata
from pathlib import Path
try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


_DISTRIBUTION_NAME = "pgque-py"
_PYPROJECT_PATH = Path(__file__).resolve().parent.parent / "pyproject.toml"
_UNKNOWN_VERSION = "0+unknown"


def source_version(pyproject_path: Path) -> str:
    """Read ``project.version`` when running from an unpackaged source tree."""
    try:
        with pyproject_path.open("rb") as source:
            project = tomllib.load(source).get("project", {})
    except (OSError, ValueError):
        return _UNKNOWN_VERSION

    version = project.get("version") if isinstance(project, dict) else None
    return version if isinstance(version, str) and version else _UNKNOWN_VERSION


def resolve_version() -> str:
    """Return installed metadata, falling back for direct source imports."""
    try:
        return metadata.version(_DISTRIBUTION_NAME)
    except metadata.PackageNotFoundError:
        return source_version(_PYPROJECT_PATH)
