"""OrderBy / TopK operator.

The operator drains its child, sorts the materialised table by the requested
keys, and either returns the full sorted table (``OrderBy``) or only the
first ``k`` rows (``TopK``). For genuinely large inputs M5 will swap in a
heap-based partial sort with external-sort fallback; the M2 implementation
relies on Arrow's vectorised ``sort_by`` and trims to ``k`` afterwards, which
is sufficient for the case-A goldens.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.lang.diagnostics import CaracalError


class TopKOperator(PhysicalOperator):
    name = "TopK"

    def __init__(
        self,
        child: PhysicalOperator,
        *,
        keys: Sequence[tuple[str, bool]],  # (column, descending)
        limit: int | None = None,
        offset: int = 0,
    ) -> None:
        super().__init__()
        if limit is not None and limit < 0:
            raise CaracalError(code="CDB-6042", message="limit must be >= 0")
        if offset < 0:
            raise CaracalError(code="CDB-6042", message="offset must be >= 0")
        self._child = child
        self._keys = list(keys)
        self._limit = limit
        self._offset = offset
        self._batches: list[pa.RecordBatch] = []
        self._consumed = False

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)
        try:
            collected: list[pa.RecordBatch] = []
            while True:
                batch = self._child.next_batch()
                if batch is None:
                    break
                collected.append(batch)
        finally:
            self._child.close()
        if not collected:
            self._batches = []
            return

        table = pa.Table.from_batches(collected)
        sort_keys = [(col, "descending" if desc else "ascending") for col, desc in self._keys]
        for col, _ in self._keys:
            if col not in table.column_names:
                raise CaracalError(code="CDB-6042", message=f"sort key column missing: {col!r}")
        sorted_indices = pa.compute.sort_indices(table, sort_keys=sort_keys)
        sorted_table = table.take(sorted_indices)
        if self._offset:
            sorted_table = sorted_table.slice(self._offset)
        if self._limit is not None:
            sorted_table = sorted_table.slice(0, self._limit)
        self._batches = list(sorted_table.to_batches())

    def _next_batch(self) -> pa.RecordBatch | None:
        if self._consumed or not self._batches:
            return None
        # Emit one chunk per call so callers can stream.
        batch = self._batches.pop(0)
        if not self._batches:
            self._consumed = True
        return batch

    def _close(self) -> None:
        return None


__all__ = ["TopKOperator"]
