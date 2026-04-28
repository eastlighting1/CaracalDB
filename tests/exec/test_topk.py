import pyarrow as pa
import pytest

from caracaldb.exec.operator import PhysicalOperator, run_pipeline
from caracaldb.exec.operators import TopKOperator
from caracaldb.lang.diagnostics import CaracalError


class _Source(PhysicalOperator):
    def __init__(self, batches):
        super().__init__()
        self._batches = list(batches)

    def _next_batch(self):
        if not self._batches:
            return None
        return self._batches.pop(0)


def _scores():
    return _Source(
        [
            pa.record_batch(
                {
                    "name": pa.array(["A", "B", "C", "D", "E"]),
                    "score": pa.array([3, 1, 5, 2, 4], type=pa.int64()),
                }
            )
        ]
    )


def test_topk_descending_limit() -> None:
    op = TopKOperator(_scores(), keys=[("score", True)], limit=3)
    out = pa.Table.from_batches(list(run_pipeline(op)))
    assert out.column("name").to_pylist() == ["C", "E", "A"]


def test_topk_offset_skips_rows() -> None:
    op = TopKOperator(_scores(), keys=[("score", True)], limit=2, offset=2)
    out = pa.Table.from_batches(list(run_pipeline(op)))
    assert out.column("name").to_pylist() == ["A", "D"]


def test_topk_orderby_only_returns_full_set_sorted() -> None:
    op = TopKOperator(_scores(), keys=[("score", False)])  # ascending, no limit
    out = pa.Table.from_batches(list(run_pipeline(op)))
    assert out.column("name").to_pylist() == ["B", "D", "A", "E", "C"]


def test_topk_rejects_missing_sort_key() -> None:
    op = TopKOperator(_scores(), keys=[("missing", False)], limit=2)
    with pytest.raises(CaracalError) as exc:
        list(run_pipeline(op))
    assert exc.value.code == "CDB-6042"
