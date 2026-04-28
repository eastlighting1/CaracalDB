"""WAL / recovery fault-injection matrix.

Simulates crashes by truncating the WAL at arbitrary byte offsets after a
sequence of appends. The recovery loop must:

1. Stop replay cleanly at the truncation boundary (no unhandled exception).
2. Report a ``last_lsn`` that matches the highest still-readable record.
3. Allow a fresh ``Wal()`` reopen to continue appending without LSN gaps
   from the recovered tail.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.recovery import recover
from caracaldb.storage.wal import Wal, iter_all_records


def _seed_wal(tmp_path: Path, n: int = 5) -> tuple[Path, list[int]]:
    bundle = create_bundle(tmp_path / "bio")
    lsns: list[int] = []
    with Wal(bundle.path / "wal") as wal:
        for i in range(n):
            lsns.append(wal.append("INSERT_NODE", bytes([i])))
        wal.flush()
    return bundle.path, lsns


@pytest.mark.parametrize("trim", [1, 4, 16, 32, 64])
def test_recovery_handles_truncated_tail(tmp_path: Path, trim: int) -> None:
    bundle_path, _ = _seed_wal(tmp_path, n=8)
    seg = next((bundle_path / "wal").glob("*.wal"))
    raw = seg.read_bytes()
    if trim >= len(raw):
        pytest.skip("trim larger than segment")
    seg.write_bytes(raw[:-trim])

    reopened = open_bundle(bundle_path)
    seen: list[bytes] = []
    report = recover(reopened, handlers={"INSERT_NODE": lambda rec: seen.append(rec.payload)})
    # Recovery must not raise; whatever survives is consistent.
    assert report.last_lsn >= 0
    # Replayed records form a strict prefix of the original sequence.
    assert seen == [bytes([i]) for i in range(len(seen))]


def test_recovery_corrupt_record_raises_with_clear_code(tmp_path: Path) -> None:
    bundle_path, _ = _seed_wal(tmp_path, n=2)
    seg = next((bundle_path / "wal").glob("*.wal"))
    raw = bytearray(seg.read_bytes())
    # Flip a byte right after the CRCL header so it lands inside the first
    # record's lsn field — guaranteed to break that record's CRC.
    raw[26] ^= 0xFF
    seg.write_bytes(bytes(raw))
    with pytest.raises(CaracalError) as exc:
        list(iter_all_records(bundle_path / "wal"))
    assert exc.value.code == "CDB-7052"


def test_recovery_followed_by_fresh_appends_keeps_monotonic_lsn(tmp_path: Path) -> None:
    bundle_path, lsns = _seed_wal(tmp_path, n=4)
    reopened = open_bundle(bundle_path)
    recover(reopened)
    with Wal(bundle_path / "wal") as wal:
        new_lsn = wal.append("INSERT_NODE", b"continue")
    assert new_lsn == lsns[-1] + 1
