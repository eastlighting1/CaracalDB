"""Expand operator: seed batch × CSR → neighbour batch.

The Expand operator binds a child operator (typically NodeScan) that produces
a column ``<src_alias>`` of UInt64 nids, and a ``CsrReader`` for one edge
type. For each upstream batch it fans every seed out via
``CsrReader.batch_neighbors``, optionally pulling the matching ``eid`` along.

Direction handling:
- ``"out"`` uses the forward CSR.
- ``"in"`` expects a CSC reader passed in; the operator is symmetrical
  otherwise.
- ``"both"`` requires both readers and concatenates the two fan-outs per
  batch. Duplicates are not deduplicated here; ``LVarPath`` (CDB-040) layers
  on dedup.

The output schema is fixed: ``<src_alias>: UInt64`` (the seed nid),
``<dst_alias>: UInt64`` (the neighbour nid), and optionally
``<edge_alias>: UInt64`` (the eid). Joining back to seed properties is done
by the planner via a HashJoin on ``<src_alias>``.
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError

Direction = Literal["out", "in", "both"]


class ExpandOperator(PhysicalOperator):
    name = "Expand"

    def __init__(
        self,
        child: PhysicalOperator,
        *,
        forward: CsrReader | None = None,
        reverse: CsrReader | None = None,
        direction: Direction = "out",
        src_alias: str = "src",
        dst_alias: str = "dst",
        edge_alias: str | None = None,
        seed_column: str = "nid",
    ) -> None:
        super().__init__()
        if direction == "out" and forward is None:
            raise CaracalError(code="CDB-6030", message="Expand(out) requires a forward CSR")
        if direction == "in" and reverse is None:
            raise CaracalError(code="CDB-6030", message="Expand(in) requires a reverse CSC")
        if direction == "both" and (forward is None or reverse is None):
            raise CaracalError(
                code="CDB-6030", message="Expand(both) requires both forward and reverse readers"
            )
        self._child = child
        self._forward = forward
        self._reverse = reverse
        self._direction: Direction = direction
        self._src_alias = src_alias
        self._dst_alias = dst_alias
        self._edge_alias = edge_alias
        self._seed_column = seed_column

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        while True:
            batch = self._child.next_batch()
            if batch is None:
                return None
            seeds = batch.column(self._seed_column).to_numpy(zero_copy_only=False).astype(np.uint64)
            arrays = self._fan_out(seeds)
            if arrays is None:
                continue
            return arrays

    def _close(self) -> None:
        self._child.close()

    # ------------------------------------------------------------------
    def _fan_out(self, seeds: np.ndarray) -> pa.RecordBatch | None:
        if seeds.size == 0:
            return None
        want_eid = self._edge_alias is not None
        chunks: list[tuple[np.ndarray, np.ndarray, np.ndarray | None]] = []
        if self._direction in ("out", "both"):
            assert self._forward is not None
            if want_eid and self._forward.has_eids:
                src, dst, eid = self._forward.batch_neighbors(seeds, return_eids=True)
                chunks.append((src, dst, eid))
            else:
                src, dst = self._forward.batch_neighbors(seeds)
                chunks.append((src, dst, None))
        if self._direction in ("in", "both"):
            assert self._reverse is not None
            if want_eid and self._reverse.has_eids:
                src, dst, eid = self._reverse.batch_neighbors(seeds, return_eids=True)
                chunks.append((src, dst, eid))
            else:
                src, dst = self._reverse.batch_neighbors(seeds)
                chunks.append((src, dst, None))

        src_all = np.concatenate([c[0] for c in chunks])
        dst_all = np.concatenate([c[1] for c in chunks])
        if src_all.size == 0:
            return None

        names = [self._src_alias, self._dst_alias]
        cols = [pa.array(src_all, type=pa.uint64()), pa.array(dst_all, type=pa.uint64())]
        if want_eid:
            eid_chunks: list[np.ndarray] = []
            for c in chunks:
                if c[2] is None:
                    eid_chunks.append(np.zeros(c[0].size, dtype=np.uint64))
                else:
                    eid_chunks.append(c[2])
            eid_all = np.concatenate(eid_chunks) if eid_chunks else np.empty(0, dtype=np.uint64)
            names.append(self._edge_alias or "eid")
            cols.append(pa.array(eid_all, type=pa.uint64()))
        return pa.RecordBatch.from_arrays(cols, names=names)


__all__ = ["Direction", "ExpandOperator"]
