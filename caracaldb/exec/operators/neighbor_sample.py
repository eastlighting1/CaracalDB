"""NeighborSample operator: layered fan-out with optional uniform sampling.

The operator consumes a seed batch from its child and walks ``len(layers)``
hops. Each hop expands the current frontier through one or more
``CsrReader`` instances (one per edge type). When ``fanout`` is positive the
operator subsamples ``fanout`` distinct neighbours per (frontier, etype)
group; when ``fanout == 0`` every neighbour is kept (useful for full-graph
GNN baselines).

Outputs are emitted as one ``RecordBatch`` per call: ``(src, dst, etype)``
plus an optional ``layer`` column. Frontier dedup is enforced after each hop
so the next layer never re-expands the same vertex twice.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError


class NeighborSampleOperator(PhysicalOperator):
    name = "NeighborSample"

    def __init__(
        self,
        child: PhysicalOperator,
        *,
        edge_readers: dict[str, CsrReader],
        layers: Sequence[int],
        seed_column: str = "nid",
        seed: int = 0,
    ) -> None:
        super().__init__()
        if not edge_readers:
            raise CaracalError(code="CDB-6100", message="NeighborSample needs at least one CSR")
        if not layers:
            raise CaracalError(code="CDB-6100", message="layers must be non-empty")
        if any(f < 0 for f in layers):
            raise CaracalError(code="CDB-6100", message="fan-out values must be >= 0")
        self._child = child
        # Stable iteration order for determinism.
        self._readers = list(edge_readers.items())
        self._etype_id = {name: i for i, (name, _) in enumerate(self._readers)}
        self._layers = list(layers)
        self._seed_column = seed_column
        self._rng = np.random.default_rng(seed)
        self._batches: list[pa.RecordBatch] = []
        self._iter = iter(())

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)
        try:
            self._batches = self._sample_all()
        finally:
            self._child.close()
        self._iter = iter(self._batches)

    def _next_batch(self) -> pa.RecordBatch | None:
        return next(self._iter, None)

    def _close(self) -> None:
        return None

    # ------------------------------------------------------------------
    def _drain_seeds(self) -> np.ndarray:
        seeds: list[np.ndarray] = []
        while True:
            batch = self._child.next_batch()
            if batch is None:
                break
            arr = (
                batch.column(self._seed_column)
                .to_numpy(zero_copy_only=False)
                .astype(np.uint64, copy=False)
            )
            if arr.size:
                seeds.append(arr)
        if not seeds:
            return np.empty(0, dtype=np.uint64)
        return np.unique(np.concatenate(seeds))

    def _sample_all(self) -> list[pa.RecordBatch]:
        frontier = self._drain_seeds()
        out: list[pa.RecordBatch] = []
        for layer_idx, fanout in enumerate(self._layers):
            if frontier.size == 0:
                break
            srcs: list[np.ndarray] = []
            dsts: list[np.ndarray] = []
            etype_ids: list[np.ndarray] = []
            next_frontier_chunks: list[np.ndarray] = []
            for etype, csr in self._readers:
                src_rep, dst = csr.batch_neighbors(frontier)
                if dst.size == 0:
                    continue
                if fanout > 0:
                    src_rep, dst = self._reservoir(src_rep, dst, fanout)
                srcs.append(src_rep)
                dsts.append(dst)
                etype_ids.append(np.full(dst.shape, self._etype_id[etype], dtype=np.uint32))
                next_frontier_chunks.append(dst)
            if not srcs:
                break
            src_all = np.concatenate(srcs)
            dst_all = np.concatenate(dsts)
            et_all = np.concatenate(etype_ids)
            layer_col = np.full(src_all.shape, layer_idx, dtype=np.uint32)
            out.append(
                pa.RecordBatch.from_arrays(
                    [
                        pa.array(src_all, type=pa.uint64()),
                        pa.array(dst_all, type=pa.uint64()),
                        pa.array(et_all, type=pa.uint32()),
                        pa.array(layer_col, type=pa.uint32()),
                    ],
                    names=["src", "dst", "etype", "layer"],
                )
            )
            frontier = np.unique(np.concatenate(next_frontier_chunks))
        return out

    def _reservoir(
        self, src_rep: np.ndarray, dst: np.ndarray, fanout: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Per-source uniform sample without replacement."""
        if src_rep.size == 0:
            return src_rep, dst
        # Sort by src so segments are contiguous.
        order = np.argsort(src_rep, kind="stable")
        src_sorted = src_rep[order]
        dst_sorted = dst[order]
        # Find segment boundaries.
        change = np.concatenate(([True], src_sorted[1:] != src_sorted[:-1]))
        seg_start = np.flatnonzero(change)
        seg_end = np.concatenate((seg_start[1:], [src_sorted.size]))
        keep_src = []
        keep_dst = []
        for s, e in zip(seg_start, seg_end, strict=False):
            seg_dst = dst_sorted[s:e]
            if seg_dst.size <= fanout:
                keep_src.append(src_sorted[s:e])
                keep_dst.append(seg_dst)
            else:
                idx = self._rng.choice(seg_dst.size, size=fanout, replace=False)
                keep_src.append(np.full(fanout, src_sorted[s], dtype=src_sorted.dtype))
                keep_dst.append(seg_dst[idx])
        return np.concatenate(keep_src), np.concatenate(keep_dst)


__all__ = ["NeighborSampleOperator"]
