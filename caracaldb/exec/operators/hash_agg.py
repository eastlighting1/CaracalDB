"""HashAggregate operator.

The M2 implementation drains the upstream into a single ``pa.Table`` and
delegates to ``Table.group_by(...).aggregate(...)`` so the heavy lifting is
done by Arrow's native kernels (which release the GIL). Result batches are
re-emitted in chunks of ``ctx.batch_size``.

Aggregate spec format (matches the planner / api translator):

    [(input_column, kernel, output_name), ...]

Supported kernels: ``count``, ``sum``, ``min``, ``max``, ``mean``, ``list``
(arrow's ``hash_list`` / ``list``), and a synthetic ``count_star`` that maps
to ``Table.add_column``-prepared dummy column → count.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.lang.diagnostics import CaracalError

_ARROW_KERNEL_MAP = {
    "count": "count",
    "sum": "sum",
    "min": "min",
    "max": "max",
    "mean": "mean",
    "avg": "mean",
    "list": "list",
    "collect": "list",
}


class HashAggregateOperator(PhysicalOperator):
    name = "HashAggregate"

    def __init__(
        self,
        child: PhysicalOperator,
        *,
        group_keys: Sequence[str],
        aggregates: Sequence[tuple[str | None, str, str]],
    ) -> None:
        """``aggregates`` is ``[(column_or_None, kernel, output_name), ...]``.

        ``column_or_None == None`` together with ``kernel == "count_star"``
        emits a count of all rows per group.
        """
        super().__init__()
        self._child = child
        self._group_keys = list(group_keys)
        self._agg_specs = list(aggregates)
        self._batches: list[pa.RecordBatch] = []
        self._iter = iter(())

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)
        try:
            tables = []
            while True:
                batch = self._child.next_batch()
                if batch is None:
                    break
                tables.append(batch)
        finally:
            self._child.close()
        if not tables:
            self._iter = iter(())
            return

        table = pa.Table.from_batches(tables)
        # Inject a synthetic "__one__" column for count_star.
        if any(spec[1] == "count_star" for spec in self._agg_specs):
            table = table.append_column("__one__", pa.array([1] * table.num_rows))

        kernels: list[tuple[str, str]] = []
        rename: dict[str, str] = {}
        for column, kernel, output in self._agg_specs:
            if kernel == "count_star":
                kernels.append(("__one__", "count"))
                rename["__one___count"] = output
                continue
            arrow_kernel = _ARROW_KERNEL_MAP.get(kernel)
            if arrow_kernel is None:
                raise CaracalError(
                    code="CDB-6041", message=f"unsupported aggregate kernel: {kernel}"
                )
            if column is None:
                raise CaracalError(
                    code="CDB-6041", message=f"aggregate {kernel!r} requires a column"
                )
            if column not in table.column_names:
                raise CaracalError(
                    code="CDB-6041", message=f"aggregate input column missing: {column!r}"
                )
            kernels.append((column, arrow_kernel))
            rename[f"{column}_{arrow_kernel}"] = output

        result = table.group_by(self._group_keys).aggregate(kernels)
        # Drop synthetic column from output names.
        new_names = [rename.get(name, name) for name in result.column_names]
        result = result.rename_columns(new_names)
        self._batches = list(result.to_batches())
        self._iter = iter(self._batches)

    def _next_batch(self) -> pa.RecordBatch | None:
        return next(self._iter, None)

    def _close(self) -> None:
        # Child already closed in _open's finally; nothing else to do.
        return None


__all__ = ["HashAggregateOperator"]
