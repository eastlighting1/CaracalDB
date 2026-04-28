import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.mvcc import RowVersion, SnapshotId, visibility_mask


def test_snapshot_id_string_with_and_without_name() -> None:
    assert "@10" in str(SnapshotId(lsn_high=10))
    assert "v1" in str(SnapshotId(lsn_high=10, name="v1"))


def test_row_version_visibility() -> None:
    snap = SnapshotId(lsn_high=10)
    rows = [
        RowVersion(created_lsn=5),
        RowVersion(created_lsn=12),
        RowVersion(created_lsn=3, deleted_lsn=8),
        RowVersion(created_lsn=3, deleted_lsn=20),
    ]
    mask = visibility_mask(rows, snap)
    # 0: visible (created<=10, not deleted before snap)
    # 1: invisible (created>10)
    # 2: invisible (deleted at 8 ≤ 10)
    # 3: visible (deleted at 20 > 10)
    assert mask == [True, False, False, True]


def test_snapshot_id_negative_lsn_raises() -> None:
    with pytest.raises(CaracalError) as exc:
        SnapshotId(lsn_high=-1)
    assert exc.value.code == "CDB-8001"
