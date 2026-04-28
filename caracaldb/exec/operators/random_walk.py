"""Random walk / Node2Vec walker.

Generates ``num_walks`` walks of fixed ``length`` per seed. ``p`` and ``q``
implement Node2Vec's return / in-out bias: ``p`` controls the probability of
returning to the previous vertex, ``q`` the probability of moving to a
vertex *not* connected to the previous one. With ``p == q == 1`` the walker
collapses to a uniform random walk.

Output schema: ``walk_id: UInt64`` (which walk), ``step: UInt32`` (position
along the walk), ``nid: UInt64`` (the visited vertex).
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError


class RandomWalkOperator(PhysicalOperator):
    name = "RandomWalk"

    def __init__(
        self,
        child: PhysicalOperator,
        *,
        forward: CsrReader,
        length: int,
        num_walks: int = 1,
        p: float = 1.0,
        q: float = 1.0,
        seed_column: str = "nid",
        seed: int = 0,
    ) -> None:
        super().__init__()
        if length <= 0:
            raise CaracalError(code="CDB-6101", message="walk length must be positive")
        if num_walks <= 0:
            raise CaracalError(code="CDB-6101", message="num_walks must be positive")
        if p <= 0 or q <= 0:
            raise CaracalError(code="CDB-6101", message="p/q must be positive")
        self._child = child
        self._forward = forward
        self._length = length
        self._num_walks = num_walks
        self._p = float(p)
        self._q = float(q)
        self._seed_column = seed_column
        self._rng = np.random.default_rng(seed)
        self._emitted = False
        self._batch: pa.RecordBatch | None = None

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)
        try:
            seeds = self._drain_seeds()
        finally:
            self._child.close()
        if seeds.size == 0:
            return
        self._batch = self._walk(seeds)

    def _next_batch(self) -> pa.RecordBatch | None:
        if self._emitted:
            return None
        self._emitted = True
        return self._batch

    def _close(self) -> None:
        return None

    # ------------------------------------------------------------------
    def _drain_seeds(self) -> np.ndarray:
        chunks: list[np.ndarray] = []
        while True:
            batch = self._child.next_batch()
            if batch is None:
                break
            arr = batch.column(self._seed_column).to_numpy(zero_copy_only=False)
            chunks.append(arr.astype(np.uint64, copy=False))
        return np.concatenate(chunks) if chunks else np.empty(0, dtype=np.uint64)

    def _walk(self, seeds: np.ndarray) -> pa.RecordBatch | None:
        offsets = np.asarray(self._forward.offsets)
        nbrs = np.asarray(self._forward.neighbors)
        is_node2vec = self._p != 1.0 or self._q != 1.0

        all_walk_ids: list[int] = []
        all_steps: list[int] = []
        all_nids: list[int] = []
        walk_id = 0
        for start in seeds.tolist():
            for _ in range(self._num_walks):
                prev: int | None = None
                cur = int(start)
                all_walk_ids.append(walk_id)
                all_steps.append(0)
                all_nids.append(cur)
                for step in range(1, self._length):
                    s = int(offsets[cur])
                    e = int(offsets[cur + 1])
                    if s == e:
                        # Dead-end; stop the walk early.
                        break
                    candidates = nbrs[s:e]
                    if not is_node2vec or prev is None:
                        nxt = int(self._rng.choice(candidates))
                    else:
                        nxt = int(self._biased_pick(candidates, prev))
                    all_walk_ids.append(walk_id)
                    all_steps.append(step)
                    all_nids.append(nxt)
                    prev = cur
                    cur = nxt
                walk_id += 1
        if not all_walk_ids:
            return None
        return pa.RecordBatch.from_arrays(
            [
                pa.array(all_walk_ids, type=pa.uint64()),
                pa.array(all_steps, type=pa.uint32()),
                pa.array(all_nids, type=pa.uint64()),
            ],
            names=["walk_id", "step", "nid"],
        )

    def _biased_pick(self, candidates: np.ndarray, prev: int) -> int:
        offsets = np.asarray(self._forward.offsets)
        nbrs = np.asarray(self._forward.neighbors)
        prev_neighbours = set(nbrs[int(offsets[prev]) : int(offsets[prev + 1])].tolist())
        weights = np.empty(candidates.size, dtype=np.float64)
        for i, n in enumerate(candidates.tolist()):
            if n == prev:
                weights[i] = 1.0 / self._p
            elif n in prev_neighbours:
                weights[i] = 1.0
            else:
                weights[i] = 1.0 / self._q
        weights /= weights.sum()
        return int(self._rng.choice(candidates, p=weights))


__all__ = ["RandomWalkOperator"]
