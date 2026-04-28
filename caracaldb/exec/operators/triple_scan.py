"""TripleScan operator: SPARQL-style basic graph patterns.

Each ``MATCH TRIPLES { ?s pred ?o . }`` block compiles down to a sequence of
``TriplePatternStep`` records, each binding a variable triple (subject,
predicate, object). The operator reads the corresponding edge store, applies
predicate-side filters, and emits one row per matching edge with three
columns named after the variables (``s``, ``o`` plus the optional predicate
binding ``p``).

Multi-pattern Basic Graph Pattern joins are deferred to the planner —
``TripleScanOperator`` handles a single triple at a time so its semantics
stay tight (one edge store, one stream of rows).
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.edge_store import DST_COLUMN, SRC_COLUMN, EdgeStore


@dataclass(frozen=True, slots=True)
class TriplePatternStep:
    subject_var: str | None
    subject_const: int | None
    predicate_iri: str
    object_var: str | None
    object_const: int | None


class TripleScanOperator(PhysicalOperator):
    name = "TripleScan"

    def __init__(self, store: EdgeStore, step: TriplePatternStep) -> None:
        super().__init__()
        if step.subject_var is None and step.subject_const is None:
            raise CaracalError(
                code="CDB-6080", message="triple subject must bind a var or constant"
            )
        if step.object_var is None and step.object_const is None:
            raise CaracalError(code="CDB-6080", message="triple object must bind a var or constant")
        if step.subject_var == step.object_var and step.subject_var is not None:
            raise CaracalError(
                code="CDB-6080",
                message="triple subject and object cannot share the same variable name",
            )
        self._store = store
        self._step = step
        self._iter = None

    def _open(self, ctx: ExecCtx) -> None:
        self._iter = iter(self._store.scan(columns=[SRC_COLUMN, DST_COLUMN]))

    def _next_batch(self) -> pa.RecordBatch | None:
        assert self._iter is not None
        for batch in self._iter:
            filtered = self._apply_constants(batch)
            if filtered.num_rows == 0:
                continue
            return self._project(filtered)
        return None

    def _apply_constants(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        if self._step.subject_const is not None:
            mask = pa.compute.equal(batch.column(SRC_COLUMN), self._step.subject_const)
            batch = batch.filter(mask)
        if self._step.object_const is not None and batch.num_rows > 0:
            mask = pa.compute.equal(batch.column(DST_COLUMN), self._step.object_const)
            batch = batch.filter(mask)
        return batch

    def _project(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        names: list[str] = []
        cols: list[pa.Array] = []
        if self._step.subject_var is not None:
            names.append(self._step.subject_var)
            cols.append(batch.column(SRC_COLUMN))
        if self._step.object_var is not None:
            names.append(self._step.object_var)
            cols.append(batch.column(DST_COLUMN))
        if not names:
            # Subject and object both constant → emit a single boolean column "found".
            names.append("found")
            cols.append(pa.array([True] * batch.num_rows))
        return pa.RecordBatch.from_arrays(cols, names=names)


__all__ = ["TriplePatternStep", "TripleScanOperator"]
