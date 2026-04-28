from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.snapshot import (
    create_snapshot,
    list_snapshots,
    open_snapshot,
    release_snapshot,
)


def test_create_and_open_snapshot_round_trip(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "b")
    snap = create_snapshot(bundle, "v1")
    assert snap.name == "v1"
    assert snap.lsn_high == 0
    # Reopen and resolve.
    again = open_snapshot(bundle, "v1")
    assert again.lsn_high == snap.lsn_high

    reopened = open_bundle(bundle.path)
    assert "v1" in reopened.manifest.snapshots


def test_snapshot_refcount_keeps_entry_alive(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "b")
    create_snapshot(bundle, "v1")
    open_snapshot(bundle, "v1")  # refcount 2
    open_snapshot(bundle, "v1")  # refcount 3
    assert release_snapshot(bundle, "v1") is False
    assert release_snapshot(bundle, "v1") is False
    assert release_snapshot(bundle, "v1") is True  # final release removes the entry
    assert "v1" not in [e.name for e in list_snapshots(bundle)]


def test_create_snapshot_rejects_duplicate_name(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "b")
    create_snapshot(bundle, "v1")
    with pytest.raises(CaracalError) as exc:
        create_snapshot(bundle, "v1")
    assert exc.value.code == "CDB-8012"


def test_open_unknown_snapshot_raises(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "b")
    with pytest.raises(CaracalError) as exc:
        open_snapshot(bundle, "missing")
    assert exc.value.code == "CDB-8013"
