# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

"""Version metadata stays consistent in installed and source contexts."""

from importlib import metadata, reload
from pathlib import Path

import pytest

import pgque
import pgque._version as version_module


def test_distribution_metadata_is_authoritative(monkeypatch):
    monkeypatch.setattr(
        version_module.metadata,
        "version",
        lambda distribution: "1.2.3" if distribution == "pgque-py" else None,
    )

    assert version_module.resolve_version() == "1.2.3"


def test_runtime_exposes_distribution_version(monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(version_module.metadata, "version", lambda _: "1.2.3rc1")
        assert reload(pgque).__version__ == "1.2.3rc1"
    reload(pgque)


def test_source_tree_fallback_reads_project_version(monkeypatch, tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[tool.example]
version = "9.9.9"

[project]
name = "pgque-py"
version = "1.2.3rc1"
""".lstrip(),
        encoding="utf-8",
    )

    def package_not_found(_distribution):
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(version_module.metadata, "version", package_not_found)
    monkeypatch.setattr(version_module, "_PYPROJECT_PATH", pyproject)

    assert version_module.resolve_version() == "1.2.3rc1"


@pytest.mark.parametrize(
    "header",
    [
        "[project] # package metadata",
        "[project]#package metadata",
        "  [ project ]\t# package metadata  ",
    ],
)
def test_source_tree_fallback_reads_commented_project_header(
    monkeypatch, tmp_path, header
):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        f'[tool.example]\nversion = "9.9.9"\n{header}\n'
        'version = "1.2.3rc1" # release candidate\n',
        encoding="utf-8",
    )

    def package_not_found(_distribution):
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(version_module.metadata, "version", package_not_found)
    monkeypatch.setattr(version_module, "_PYPROJECT_PATH", pyproject)

    assert version_module.resolve_version() == "1.2.3rc1"


def test_source_tree_fallback_does_not_read_other_table_version(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project] # package metadata\nname = "pgque-py"\n'
        '[tool.example] # not package metadata\nversion = "9.9.9"\n',
        encoding="utf-8",
    )

    assert version_module.source_version(pyproject) == "0+unknown"


def test_source_tree_fallback_is_explicit_when_project_is_missing(
    monkeypatch, tmp_path
):
    def package_not_found(_distribution):
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(version_module.metadata, "version", package_not_found)
    monkeypatch.setattr(
        version_module, "_PYPROJECT_PATH", tmp_path / "missing.toml"
    )

    assert version_module.resolve_version() == "0+unknown"


def test_runtime_version_matches_available_package_metadata():
    try:
        expected = metadata.version("pgque-py")
    except metadata.PackageNotFoundError:
        expected = version_module.source_version(
            Path(__file__).parents[1] / "pyproject.toml"
        )

    assert pgque.__version__ == expected
