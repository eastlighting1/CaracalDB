"""NodeScan physical operator.

Streams record batches out of a class-partitioned ``NodeStore`` (CDB-020).
Two pushdown surfaces are exposed so the planner rules can drive them:

* ``columns`` — projection prune; passed straight through to ``NodeStore.scan``.
* ``predicate`` — optional ``ExprFn`` whose mask is applied to each batch.

Both are advisory: the operator falls back to a full scan if neither is set.
Class bitmap filtering (per WBS) becomes meaningful once the catalog tracks
class-id partitions; for M1 the per-class store partitioning provides the same
effect — every batch already belongs to ``class_iri``.
"""

from __future__ import annotations

import pyarrow as pa

from caracaldb.exec.expr import ExprFn
from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.storage.node_store import NodeStore


class NodeScanOperator(PhysicalOperator):
    name = "NodeScan"

    def __init__(
        self,
        store: NodeStore,
        *,
        columns: list[str] | None = None,
        predicate: ExprFn | None = None,
    ) -> None:
        super().__init__()
        self._store = store
        self._columns = list(columns) if columns is not None else None
        self._predicate = predicate
        self._iter = None

    def _open(self, ctx: ExecCtx) -> None:
        self._iter = self._store.scan(columns=self._columns)

    def _next_batch(self) -> pa.RecordBatch | None:
        assert self._iter is not None
        for batch in self._iter:
            if self._predicate is not None:
                mask = self._predicate(batch)
                batch = batch.filter(mask)
                if batch.num_rows == 0:
                    continue
            return batch
        return None


__all__ = ["NodeScanOperator"]
