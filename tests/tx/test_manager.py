from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.wal import Wal, iter_all_records
from caracaldb.tx import (
    BEGIN_KIND,
    COMMIT_KIND,
    ROLLBACK_KIND,
    TransactionManager,
)


def test_commit_logs_begin_and_commit(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        with mgr.transaction() as tx:
            tx.record_write("Gene", 1)
        kinds = [r.kind for r in iter_all_records(tmp_path / "wal")]
    assert BEGIN_KIND in kinds and COMMIT_KIND in kinds


def test_rollback_logs_abort(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        try:
            with mgr.transaction():
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        kinds = [r.kind for r in iter_all_records(tmp_path / "wal")]
    assert ROLLBACK_KIND in kinds and COMMIT_KIND not in kinds


def test_write_write_conflict_raises_cdb_8002(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        # Tx A and B both start at lsn 0.
        a = mgr.begin()
        b = mgr.begin()
        a.record_write("Gene", 7)
        b.record_write("Gene", 7)
        mgr.commit(a)
        with pytest.raises(CaracalError) as exc:
            mgr.commit(b)
    assert exc.value.code == "CDB-8002"


def test_non_overlapping_writes_commit(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        a = mgr.begin()
        b = mgr.begin()
        a.record_write("Gene", 1)
        b.record_write("Gene", 2)
        mgr.commit(a)
        mgr.commit(b)
    # Both succeed; no exceptions raised.


def test_serial_writes_to_same_key_succeed_when_snapshot_advances(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        a = mgr.begin()
        a.record_write("Gene", 5)
        mgr.commit(a)
        # New tx after the commit observes a fresh snapshot.
        b = mgr.begin()
        b.record_write("Gene", 5)
        mgr.commit(b)
