"""Column-name transform operators used by the multi-hop pattern pipeline.

The pattern compiler decomposes ``(a:A)-[:rel]->(b:B)`` into a ``NodeScan`` per
class plus an ``Expand`` that produces only the seed/target id columns. To
recover seed and target *properties* in the joined output, the pipeline scans
each class twice and joins back on the id column. That join produces duplicate
key columns and unprefixed property names, so two small helpers tidy the schema
between stages:

* ``RenameOperator`` — apply a rename map (``"nid" -> "a.nid"``); unmapped
  columns pass through. Used right after a ``NodeScan`` to attach the alias
  prefix that downstream Expand/Join refer to by name.
* ``DropColumnsOperator`` — drop the named columns. Used after ``HashJoin`` to
  remove the duplicated probe-side join key.
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator


class RenameOperator(PhysicalOperator):
    name = "Rename"

    def __init__(self, child: PhysicalOperator, mapping: Mapping[str, str]) -> None:
        super().__init__()
        self._child = child
        self._mapping = dict(mapping)

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        batch = self._child.next_batch()
        if batch is None:
            return None
        new_names = [self._mapping.get(name, name) for name in batch.schema.names]
        return pa.RecordBatch.from_arrays(list(batch.columns), names=new_names)

    def _close(self) -> None:
        self._child.close()


class DropColumnsOperator(PhysicalOperator):
    name = "DropColumns"

    def __init__(self, child: PhysicalOperator, drop: tuple[str, ...]) -> None:
        super().__init__()
        self._child = child
        self._drop = tuple(drop)

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        batch = self._child.next_batch()
        if batch is None:
            return None
        names = batch.schema.names
        # When a name appears multiple times (e.g. HashJoin places the build-
        # side key first and the matching probe-side key second), drop only the
        # *trailing* occurrences. The build-side column carries the
        # reconstructed identity we want to preserve.
        drop_counts: dict[str, int] = {}
        for name in self._drop:
            drop_counts[name] = drop_counts.get(name, 0) + 1
        first_seen: dict[str, int] = {}
        keep_mask: list[bool] = []
        for i, name in enumerate(names):
            if name in drop_counts and drop_counts[name] > 0:
                if name not in first_seen:
                    first_seen[name] = i
                    keep_mask.append(True)
                else:
                    drop_counts[name] -= 1
                    keep_mask.append(False)
            else:
                keep_mask.append(True)
        # If a name only appears once and we asked to drop it, drop the first.
        for name, remaining in drop_counts.items():
            if remaining > 0 and name in first_seen:
                keep_mask[first_seen[name]] = False
        kept_arrays = [batch.column(i) for i, keep in enumerate(keep_mask) if keep]
        kept_names = [names[i] for i, keep in enumerate(keep_mask) if keep]
        return pa.RecordBatch.from_arrays(kept_arrays, names=kept_names)

    def _close(self) -> None:
        self._child.close()


class UnionAllOperator(PhysicalOperator):
    """Concatenate the batches of N children with identical schemas.

    Used by the pattern pipeline to fan rel-type unions (``-[:p|q]->``) out as
    one ``Expand`` per relation, then re-merge the per-relation pair streams
    into a single ``(src, dst)`` stream that downstream HashJoins can probe.
    """

    name = "UnionAll"

    def __init__(self, children: tuple[PhysicalOperator, ...]) -> None:
        super().__init__()
        if not children:
            raise ValueError("UnionAllOperator requires at least one child")
        self._children = children
        self._cursor = 0

    def _open(self, ctx: ExecCtx) -> None:
        for child in self._children:
            child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        while self._cursor < len(self._children):
            batch = self._children[self._cursor].next_batch()
            if batch is not None:
                return batch
            self._cursor += 1
        return None

    def _close(self) -> None:
        for child in self._children:
            child.close()


__all__ = ["DropColumnsOperator", "RenameOperator", "UnionAllOperator"]
