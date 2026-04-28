from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.checkpoint import checkpoint
from caracaldb.storage.recovery import recover
from caracaldb.storage.wal import Wal


def test_recover_replays_records_after_checkpoint(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal") as wal:
        wal.append("INSERT_NODE", b"a")
        wal.append("INSERT_NODE", b"b")
        checkpoint(bundle, wal)
        wal.append("INSERT_NODE", b"c")
        wal.append("COMMIT", b"")

    reopened = open_bundle(bundle.path)
    seen: list[tuple[str, bytes]] = []
    report = recover(
        reopened,
        handlers={
            "INSERT_NODE": lambda rec: seen.append((rec.kind, rec.payload)),
            "COMMIT": lambda rec: seen.append((rec.kind, rec.payload)),
        },
    )

    assert [s[1] for s in seen] == [b"c", b""]
    assert report.replayed == 2
    # The two pre-checkpoint inserts plus the checkpoint marker itself sit at lsn ≤ boundary.
    assert report.skipped_pre_checkpoint == 3
    assert report.last_lsn == 5
    assert reopened.manifest.last_lsn == 5


def test_recover_advances_checkpoint_lsn_from_marker(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal") as wal:
        wal.append("INSERT_NODE", b"a")
        cp = checkpoint(bundle, wal)

    # Manually rewind the manifest to simulate stale persisted state.
    from dataclasses import replace as dc_replace

    stale = dc_replace(bundle.manifest, checkpoint_lsn=0, last_lsn=0)
    stale.write_atomic(bundle.path / "MANIFEST")

    reopened = open_bundle(bundle.path)
    report = recover(reopened)
    assert report.checkpoint_lsn == cp.checkpoint_lsn
    assert reopened.manifest.checkpoint_lsn == cp.checkpoint_lsn


def test_recover_propagates_handler_failure(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal") as wal:
        wal.append("BOOM", b"x")

    def boom(_: object) -> None:
        raise RuntimeError("intentional")

    reopened = open_bundle(bundle.path)
    with pytest.raises(CaracalError) as exc:
        recover(reopened, handlers={"BOOM": boom})
    assert exc.value.code == "CDB-7070"


def test_recover_tolerates_unknown_kinds(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal") as wal:
        wal.append("UNKNOWN_FUTURE_KIND", b"data")

    reopened = open_bundle(bundle.path)
    report = recover(reopened, handlers={})
    assert report.replayed == 1
    assert report.handler_invocations == {"UNKNOWN_FUTURE_KIND": 1}
