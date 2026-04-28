"""Variable-length path operator (`*min..max`).

Repeatedly applies ``ExpandOperator`` semantics until the hop budget is
exhausted, accumulating reached destinations and de-duplicating per
``(seed, dst)`` pair (so cycles in the graph do not blow up the result set).

The operator materialises the seed batch on first ``next_batch`` — variable
length traversal benefits from a single seed frontier rather than per-batch
streaming, since the visited bitset must span all input seeds.

Output schema is two columns: ``<src_alias>: UInt64`` (the originating
seed) and ``<dst_alias>: UInt64`` (the reached vertex). One row per unique
(seed, dst) pair whose hop count falls in ``[hop_min, hop_max]``.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError


class VarPathOperator(PhysicalOperator):
    name = "VarPath"

    def __init__(
        self,
        child: PhysicalOperator,
        *,
        forward: CsrReader,
        hop_min: int = 1,
        hop_max: int = 1,
        seed_column: str = "nid",
        src_alias: str = "src",
        dst_alias: str = "dst",
    ) -> None:
        super().__init__()
        if hop_min < 0 or hop_max < hop_min:
            raise CaracalError(
                code="CDB-6031",
                message=f"invalid hop range: hop_min={hop_min}, hop_max={hop_max}",
            )
        self._child = child
        self._forward = forward
        self._hop_min = hop_min
        self._hop_max = hop_max
        self._seed_column = seed_column
        self._src_alias = src_alias
        self._dst_alias = dst_alias
        self._emitted = False

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        if self._emitted:
            return None
        # Drain the upstream so we have a full seed list (variable-length
        # traversal must dedupe across all seeds).
        seed_chunks: list[np.ndarray] = []
        while True:
            batch = self._child.next_batch()
            if batch is None:
                break
            seeds = batch.column(self._seed_column).to_numpy(zero_copy_only=False).astype(np.uint64)
            if seeds.size:
                seed_chunks.append(seeds)
        self._emitted = True
        if not seed_chunks:
            return None
        seeds_arr = np.concatenate(seed_chunks)
        # For each seed, walk up to hop_max hops, dedupe (seed, vertex) pairs.
        return self._traverse(seeds_arr)

    def _close(self) -> None:
        self._child.close()

    # ------------------------------------------------------------------
    def _traverse(self, seeds: np.ndarray) -> pa.RecordBatch | None:
        # The frontier carries (seed_origin, current_vertex) so destinations
        # remain attributable to their starting seed even after multiple hops.
        # We use a per-seed visited set encoded as a Python set keyed by tuple.
        # M2 trades elegance for clarity; M3 swaps to a NumPy bitset per seed.
        out_src: list[int] = []
        out_dst: list[int] = []
        seen: set[tuple[int, int]] = set()

        # Hop 0 (only emitted when hop_min == 0).
        frontier_src = seeds.astype(np.uint64, copy=False)
        frontier_dst = seeds.astype(np.uint64, copy=False)
        if self._hop_min == 0:
            for s, d in zip(frontier_src.tolist(), frontier_dst.tolist(), strict=False):
                if (s, d) not in seen:
                    seen.add((s, d))
                    out_src.append(s)
                    out_dst.append(d)

        for hop in range(1, self._hop_max + 1):
            if frontier_dst.size == 0:
                break
            src_rep, dst = self._forward.batch_neighbors(frontier_dst)
            if dst.size == 0:
                break
            # Map back: each (frontier_src[i], dst[k]) pair is a candidate.
            # Build the new (seed_origin, dst) pairs by repeating frontier_src
            # along the same shape as the fan-out — `src_rep` already comes
            # from frontier_dst, so we need a parallel lookup from frontier_dst
            # to its corresponding seed_origin.
            if hop == 1:
                origin_per_dst = src_rep  # frontier_dst == seeds at hop 1
            else:
                # Build origin map only for the current frontier; since
                # batch_neighbors keeps the per-source ordering, we replicate
                # frontier_src by the same degree pattern.
                degrees = self._forward.degrees(frontier_dst)
                origin_per_dst = np.repeat(frontier_src, degrees)
            keep_src: list[int] = []
            keep_dst: list[int] = []
            new_frontier_src: list[int] = []
            new_frontier_dst: list[int] = []
            for s, d in zip(origin_per_dst.tolist(), dst.tolist(), strict=False):
                pair = (s, d)
                if pair in seen:
                    continue
                seen.add(pair)
                if hop >= self._hop_min:
                    keep_src.append(s)
                    keep_dst.append(d)
                new_frontier_src.append(s)
                new_frontier_dst.append(d)
            out_src.extend(keep_src)
            out_dst.extend(keep_dst)
            frontier_src = np.array(new_frontier_src, dtype=np.uint64)
            frontier_dst = np.array(new_frontier_dst, dtype=np.uint64)

        if not out_src:
            return None
        return pa.RecordBatch.from_arrays(
            [
                pa.array(out_src, type=pa.uint64()),
                pa.array(out_dst, type=pa.uint64()),
            ],
            names=[self._src_alias, self._dst_alias],
        )


__all__ = ["VarPathOperator"]
