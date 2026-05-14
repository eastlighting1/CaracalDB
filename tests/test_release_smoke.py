"""Release artifact smoke tests.

Confirms the package metadata is in shape for the current release. We do
not exercise ``hatch build`` from inside pytest — wheel building is a CI
job and depends on hatch being installed; the tests here only check the
declarative state that Hatch reads.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

import caracaldb
from tools.check_dist_archives import check_wheel

REPO_ROOT = Path(__file__).resolve().parent.parent
RELEASE_VERSION = caracaldb.__version__


def test_version_is_current_release() -> None:
    assert RELEASE_VERSION == "1.0.0"


def test_changelog_contains_release_section() -> None:
    text = (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert f"[{RELEASE_VERSION}]" in text
    assert "sample_gnn_subgraph" in text
    assert "neighbor_loader" in text
    assert "query_nodes" in text


def test_release_notes_exist() -> None:
    notes = REPO_ROOT / "docs" / "release" / f"v{RELEASE_VERSION}.md"
    body = notes.read_text(encoding="utf-8")
    assert f"v{RELEASE_VERSION}" in body
    assert "uv pip install" in body


def test_pyproject_declares_caracal_entrypoint() -> None:
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'caracal = "caracaldb.cli:main"' in text


def test_pyproject_declares_rust_extension_build_metadata() -> None:
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "[tool.maturin]" in text
    assert 'module-name = "caracaldb._caracaldb_rust"' in text
    assert 'manifest-path = "crates/caracaldb-python/Cargo.toml"' in text


def test_release_workflow_smokes_wheel_outside_checkout() -> None:
    text = (REPO_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert 'SMOKE_DIR="$(mktemp -d)"' in text
    assert 'cd "$SMOKE_DIR"' in text
    assert "python tools/check_dist_archives.py dist" in text
    assert "skip-existing: true" in text
    assert "print-hash: true" in text


def test_dist_archive_checker_rejects_trailing_wheel_data() -> None:
    text = (REPO_ROOT / "tools" / "check_dist_archives.py").read_text(encoding="utf-8")
    assert "ZIP archive has {trailing} trailing byte(s)" in text
    assert "expected_end != len(data)" in text
    assert "Rust extension missing from wheel" in text


def test_dist_archive_checker_detects_actual_trailing_bytes(tmp_path: Path) -> None:
    wheel = tmp_path / "caracaldb-1.0.0-cp311-abi3-test.whl"
    with zipfile.ZipFile(wheel, "w") as zf:
        zf.writestr("caracaldb/__init__.py", "")
        zf.writestr("caracaldb/_caracaldb_rust.so", b"native")
    with wheel.open("ab") as fh:
        fh.write(b"trailing")

    with pytest.raises(SystemExit, match="trailing byte"):
        check_wheel(wheel)
