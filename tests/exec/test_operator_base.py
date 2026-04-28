import pyarrow as pa
import pytest

from caracaldb.exec.operator import ExecCtx, PhysicalOperator, run_pipeline
from caracaldb.lang.diagnostics import CaracalError


class _ListSource(PhysicalOperator):
    name = "ListSource"

    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        super().__init__()
        self._batches = list(batches)

    def _next_batch(self) -> pa.RecordBatch | None:
        if not self._batches:
            return None
        return self._batches.pop(0)


def _batch(values: list[int]) -> pa.RecordBatch:
    return pa.record_batch({"v": pa.array(values, type=pa.int64())})


def test_run_pipeline_streams_until_eos() -> None:
    op = _ListSource([_batch([1, 2]), _batch([3])])
    out = list(run_pipeline(op))
    assert [b.column("v").to_pylist() for b in out] == [[1, 2], [3]]


def test_next_batch_before_open_raises() -> None:
    op = _ListSource([_batch([1])])
    with pytest.raises(CaracalError) as exc:
        op.next_batch()
    assert exc.value.code == "CDB-6002"


def test_double_open_raises() -> None:
    op = _ListSource([_batch([1])])
    op.open(ExecCtx())
    with pytest.raises(CaracalError) as exc:
        op.open(ExecCtx())
    assert exc.value.code == "CDB-6001"


def test_close_is_idempotent_and_returns_none_post_eos() -> None:
    op = _ListSource([])
    op.open(ExecCtx())
    assert op.next_batch() is None
    op.close()
    op.close()  # second close is a no-op
