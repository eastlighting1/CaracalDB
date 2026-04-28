import pyarrow as pa
import pytest

from caracaldb.exec.operator import PhysicalOperator, run_pipeline
from caracaldb.exec.operators import HashJoinOperator
from caracaldb.lang.diagnostics import CaracalError


class _Source(PhysicalOperator):
    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        super().__init__()
        self._batches = list(batches)

    def _next_batch(self):
        if not self._batches:
            return None
        return self._batches.pop(0)


def _people() -> _Source:
    return _Source(
        [
            pa.record_batch(
                {"id": pa.array([1, 2, 3], type=pa.int64()), "name": pa.array(["A", "B", "C"])}
            ),
        ]
    )


def _orders() -> _Source:
    return _Source(
        [
            pa.record_batch(
                {
                    "oid": pa.array([10, 11, 12, 13], type=pa.int64()),
                    "buyer": pa.array([1, 2, 1, 4], type=pa.int64()),
                }
            ),
        ]
    )


def test_hash_join_inner_emits_matches() -> None:
    op = HashJoinOperator(_people(), _orders(), build_key="id", probe_key="buyer", kind="inner")
    out = list(run_pipeline(op))[0]
    rows = list(zip(out.column("name").to_pylist(), out.column("oid").to_pylist(), strict=False))
    assert sorted(rows) == [("A", 10), ("A", 12), ("B", 11)]


def test_hash_join_left_keeps_unmatched_probe_rows() -> None:
    op = HashJoinOperator(_people(), _orders(), build_key="id", probe_key="buyer", kind="left")
    out = list(run_pipeline(op))[0]
    # buyer=4 has no match → name should be null in that row
    pairs = list(zip(out.column("name").to_pylist(), out.column("oid").to_pylist(), strict=False))
    assert (None, 13) in pairs


def test_hash_join_with_prefixes_disambiguates_columns() -> None:
    op = HashJoinOperator(
        _people(),
        _orders(),
        build_key="id",
        probe_key="buyer",
        build_prefix="p",
        probe_prefix="o",
    )
    out = list(run_pipeline(op))[0]
    assert "p.id" in out.schema.names and "o.buyer" in out.schema.names


def test_hash_join_rejects_missing_key_column() -> None:
    bad_probe = _Source(
        [pa.record_batch({"oid": pa.array([1], type=pa.int64()), "x": pa.array([1])})]
    )
    op = HashJoinOperator(_people(), bad_probe, build_key="id", probe_key="buyer")
    with pytest.raises(CaracalError) as exc:
        list(run_pipeline(op))
    assert exc.value.code == "CDB-6040"
