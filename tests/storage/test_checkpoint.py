from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.checkpoint import CHECKPOINT_KIND, checkpoint
from caracaldb.storage.wal import Wal, iter_all_records


def test_checkpoint_writes_marker_and_updates_manifest(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal") as wal:
        wal.append("INSERT_NODE", b"a")
        wal.append("INSERT_NODE", b"b")
        result = checkpoint(bundle, wal)

    assert result.checkpoint_lsn == 3  # 2 inserts + checkpoint marker
    assert bundle.manifest.checkpoint_lsn == 3
    assert bundle.manifest.last_lsn == 3

    reopened = open_bundle(bundle.path)
    assert reopened.manifest.checkpoint_lsn == 3

    records = list(iter_all_records(bundle.path / "wal"))
    # truncate_before removes segments whose max_lsn ≤ marker; the marker may sit alone in
    # the open segment which is preserved, so at minimum the marker survives.
    assert any(r.kind == CHECKPOINT_KIND for r in records)


def test_checkpoint_truncates_old_segments(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal", roll_size=200) as wal:
        for _ in range(5):
            wal.append("X", b"0123456789" * 6)
        result = checkpoint(bundle, wal)
    # After checkpoint we should retain at most a small number of segments.
    remaining = sorted((bundle.path / "wal").glob("*.wal"))
    assert result.checkpoint_lsn == 6
    assert len(remaining) <= 2  # current open segment ± one


def test_checkpoint_refuses_lsn_regression(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    with Wal(bundle.path / "wal") as wal:
        wal.append("X", b"")
        checkpoint(bundle, wal)

    # Synthesize an "advanced" manifest, then call checkpoint with a fresh WAL whose
    # last_lsn is behind it — the checkpointer should refuse rather than regress.
    from dataclasses import replace

    advanced = replace(bundle.manifest, checkpoint_lsn=999)
    advanced.write_atomic(bundle.path / "MANIFEST")
    object.__setattr__(bundle, "manifest", advanced)

    with Wal(bundle.path / "wal") as wal2:
        with pytest.raises(CaracalError) as exc:
            checkpoint(bundle, wal2)
        assert exc.value.code == "CDB-7060"
