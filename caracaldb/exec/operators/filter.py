"""Filter and Project operators.

``FilterOperator`` evaluates a boolean ``ExprFn`` per batch and keeps rows
whose mask is true. ``ProjectOperator`` evaluates a list of
``(ExprFn, output_name)`` pairs per batch and assembles a new RecordBatch in
the requested order.

Both operators are vectorised: they call into the upstream operator until they
have a non-empty batch to return (so consumers never see empty filtered
batches).
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from caracaldb.exec.expr import ExprFn
from caracaldb.exec.operator import ExecCtx, PhysicalOperator


class FilterOperator(PhysicalOperator):
    name = "Filter"

    def __init__(self, child: PhysicalOperator, predicate: ExprFn) -> None:
        super().__init__()
        self._child = child
        self._predicate = predicate

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        while True:
            batch = self._child.next_batch()
            if batch is None:
                return None
            mask = self._predicate(batch)
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                return filtered

    def _close(self) -> None:
        self._child.close()


class ProjectOperator(PhysicalOperator):
    name = "Project"

    def __init__(
        self,
        child: PhysicalOperator,
        projections: Sequence[tuple[ExprFn, str]],
    ) -> None:
        super().__init__()
        self._child = child
        self._projections = list(projections)

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        batch = self._child.next_batch()
        if batch is None:
            return None
        arrays = [fn(batch) for fn, _ in self._projections]
        names = [name for _, name in self._projections]
        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _close(self) -> None:
        self._child.close()


__all__ = ["FilterOperator", "ProjectOperator"]
