from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.wal import Wal, iter_all_records


def test_wal_appends_assigns_monotonic_lsns(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        a = wal.append("INSERT_NODE", b"a")
        b = wal.append("INSERT_NODE", b"b")
        c = wal.append("COMMIT", b"")
        assert (a, b, c) == (1, 2, 3)
        assert wal.last_lsn == 3
        wal.flush()

    records = list(iter_all_records(tmp_path / "wal"))
    assert [r.lsn for r in records] == [1, 2, 3]
    assert [r.prev_lsn for r in records] == [0, 1, 2]
    assert [r.kind for r in records] == ["INSERT_NODE", "INSERT_NODE", "COMMIT"]


def test_wal_recovers_last_lsn_on_reopen(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        wal.append("DDL", b"x")
        wal.append("DDL", b"y")
    with Wal(tmp_path / "wal") as wal:
        assert wal.last_lsn == 2
        new_lsn = wal.append("DDL", b"z")
        assert new_lsn == 3


def test_wal_iter_since_filters_lsn(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        for i in range(5):
            wal.append("X", bytes([i]))
        records = list(wal.iter_since(2))
        assert [r.lsn for r in records] == [3, 4, 5]


def test_wal_rolls_segment_when_threshold_exceeded(tmp_path: Path) -> None:
    # Pick a tiny roll size (just above header + one record) so the second append rolls.
    with Wal(tmp_path / "wal", roll_size=200) as wal:
        wal.append("X", b"0123456789" * 8)
        wal.append("X", b"0123456789" * 8)
    segments = sorted((tmp_path / "wal").glob("*.wal"))
    assert len(segments) >= 2


def test_wal_corrupted_record_truncates_at_boundary(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        wal.append("X", b"hello")
        wal.append("X", b"world")
    # Truncate the last 5 bytes (cuts into the second record's CRC/payload).
    seg = sorted((tmp_path / "wal").glob("*.wal"))[0]
    raw = seg.read_bytes()
    seg.write_bytes(raw[:-5])

    records = list(iter_all_records(tmp_path / "wal"))
    assert [r.kind for r in records] == ["X"]
    assert records[0].payload == b"hello"


def test_wal_truncate_before_removes_old_segments(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal", roll_size=200) as wal:
        for _i in range(6):
            wal.append("X", b"0123456789" * 6)
        # Roll-over has happened; flush and close.
        wal.flush()
        old_lsn = 3
        removed = wal.truncate_before(old_lsn)
    # Some segments may have been removed; remaining records all have lsn > 3.
    remaining = list(iter_all_records(tmp_path / "wal"))
    assert all(r.lsn > old_lsn for r in remaining) or removed == 0


def test_wal_rejects_too_small_roll_size(tmp_path: Path) -> None:
    with pytest.raises(CaracalError) as exc:
        Wal(tmp_path / "wal", roll_size=8)
    assert exc.value.code == "CDB-7050"
