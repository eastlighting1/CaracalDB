"""ClosureScan operator: ``SUBCLASSOF*`` materialised at query time.

Given a base class IRI ``C``, the operator enumerates all descendant classes
through the ``ClassClosureIndex`` (CDB-017) and emits the union of their
node-store rows. ``include_self`` follows the Tuft ``SUBCLASSOF*`` semantics
(reflexive) by default; passing ``include_self=False`` matches the strict
``SUBCLASSOF+`` form.

Result rows always carry a synthetic ``class_iri`` column so downstream
operators can disambiguate which subclass produced each row. Other columns
take the *intersection* of the per-class schemas — fields present only in a
subset of subclasses are dropped to keep the output schema stable.
"""

from __future__ import annotations

from collections.abc import Iterable

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.closure import ClassClosureIndex
from caracaldb.storage import Bundle
from caracaldb.storage.node_store import open_node_store


class ClosureScanOperator(PhysicalOperator):
    name = "ClosureScan"

    def __init__(
        self,
        bundle: Bundle,
        closure: ClassClosureIndex,
        *,
        base_iri: str,
        include_self: bool = True,
        columns: list[str] | None = None,
    ) -> None:
        super().__init__()
        if base_iri not in closure.class_id_by_iri:
            raise CaracalError(
                code="CDB-6070",
                message=f"ClosureScan base class not in catalog: {base_iri!r}",
            )
        self._bundle = bundle
        self._closure = closure
        self._base_iri = base_iri
        self._include_self = include_self
        self._columns = list(columns) if columns is not None else None
        self._iter: Iterable[pa.RecordBatch] | None = None

    def _open(self, ctx: ExecCtx) -> None:
        self._iter = iter(self._collect_batches())

    def _next_batch(self) -> pa.RecordBatch | None:
        assert self._iter is not None
        try:
            return next(self._iter)  # type: ignore[arg-type]
        except StopIteration:
            return None

    def _collect_batches(self) -> list[pa.RecordBatch]:
        descendant_iris = self._closure.descendant_iris(
            self._base_iri, include_self=self._include_self
        )
        # Compute intersection of column names across all descendants so the
        # output schema is well-defined.
        per_class_batches: list[tuple[str, list[pa.RecordBatch]]] = []
        for iri in descendant_iris:
            local = _local_name(iri)
            try:
                store = open_node_store(self._bundle, class_iri=iri, local_name=local)
            except CaracalError as exc:
                if exc.code == "CDB-7012":
                    # Subclass declared in catalogue but no rows persisted yet — skip.
                    continue
                raise
            batches = list(store.scan())
            if batches:
                per_class_batches.append((iri, batches))

        if not per_class_batches:
            return []

        # Schema intersection (stable order from the first store).
        first_names = list(per_class_batches[0][1][0].schema.names)
        intersection: list[str] = list(first_names)
        for _, batches in per_class_batches[1:]:
            names = set(batches[0].schema.names)
            intersection = [name for name in intersection if name in names]

        if self._columns is not None:
            intersection = [name for name in intersection if name in self._columns]

        out: list[pa.RecordBatch] = []
        for iri, batches in per_class_batches:
            for batch in batches:
                projected = batch.select(intersection)
                class_col = pa.array([iri] * projected.num_rows, type=pa.string())
                out.append(
                    pa.RecordBatch.from_arrays(
                        [*projected.columns, class_col],
                        names=[*intersection, "class_iri"],
                    )
                )
        return out


def _local_name(iri: str) -> str:
    return iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1].rsplit(":", 1)[-1]


__all__ = ["ClosureScanOperator"]
