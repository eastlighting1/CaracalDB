import pyarrow as pa
import pytest

from caracaldb.exec.operator import PhysicalOperator, run_pipeline
from caracaldb.exec.operators import HashAggregateOperator
from caracaldb.lang.diagnostics import CaracalError


class _Source(PhysicalOperator):
    def __init__(self, batches):
        super().__init__()
        self._batches = list(batches)

    def _next_batch(self):
        if not self._batches:
            return None
        return self._batches.pop(0)


def _orders():
    return _Source(
        [
            pa.record_batch(
                {
                    "buyer": pa.array([1, 1, 2, 2, 3], type=pa.int64()),
                    "amount": pa.array([10.0, 20.0, 5.0, 15.0, 100.0]),
                }
            )
        ]
    )


def test_hash_agg_count_star() -> None:
    op = HashAggregateOperator(
        _orders(),
        group_keys=["buyer"],
        aggregates=[(None, "count_star", "n")],
    )
    out = list(run_pipeline(op))[0]
    rows = dict(zip(out.column("buyer").to_pylist(), out.column("n").to_pylist(), strict=False))
    assert rows == {1: 2, 2: 2, 3: 1}


def test_hash_agg_sum_and_mean() -> None:
    op = HashAggregateOperator(
        _orders(),
        group_keys=["buyer"],
        aggregates=[("amount", "sum", "total"), ("amount", "mean", "avg")],
    )
    out = list(run_pipeline(op))[0]
    rows = {
        b: (t, a)
        for b, t, a in zip(
            out.column("buyer").to_pylist(),
            out.column("total").to_pylist(),
            out.column("avg").to_pylist(),
            strict=False,
        )
    }
    assert rows[1] == (30.0, 15.0)
    assert rows[2] == (20.0, 10.0)
    assert rows[3] == (100.0, 100.0)


def test_hash_agg_collect_list() -> None:
    op = HashAggregateOperator(
        _orders(), group_keys=["buyer"], aggregates=[("amount", "collect", "items")]
    )
    out = list(run_pipeline(op))[0]
    rows = dict(zip(out.column("buyer").to_pylist(), out.column("items").to_pylist(), strict=False))
    assert sorted(rows[1]) == [10.0, 20.0]


def test_hash_agg_rejects_unknown_kernel() -> None:
    op = HashAggregateOperator(
        _orders(), group_keys=["buyer"], aggregates=[("amount", "stddev", "s")]
    )
    with pytest.raises(CaracalError) as exc:
        list(run_pipeline(op))
    assert exc.value.code == "CDB-6041"
