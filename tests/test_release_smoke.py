"""Release artifact smoke tests.

Confirms the package metadata is in shape for the current release. We do
not exercise ``hatch build`` from inside pytest — wheel building is a CI
job and depends on hatch being installed; the tests here only check the
declarative state that Hatch reads.
"""

from __future__ import annotations

from pathlib import Path

import caracaldb

REPO_ROOT = Path(__file__).resolve().parent.parent
RELEASE_VERSION = caracaldb.__version__


def test_version_is_current_release() -> None:
    assert RELEASE_VERSION == "0.3.0"


def test_changelog_contains_release_section() -> None:
    text = (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert f"[{RELEASE_VERSION}]" in text
    assert "graphrag_search" in text
    assert "graph_ecosystem" in text


def test_release_notes_exist() -> None:
    notes = REPO_ROOT / "docs" / "release" / f"v{RELEASE_VERSION}.md"
    body = notes.read_text(encoding="utf-8")
    assert f"v{RELEASE_VERSION}" in body
    assert "uv pip install" in body


def test_pyproject_declares_caracal_entrypoint() -> None:
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'caracal = "caracaldb.cli:main"' in text
