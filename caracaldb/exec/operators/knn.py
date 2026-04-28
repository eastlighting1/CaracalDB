"""Knn operator: ``TOP_K BY SIMILARITY(...)``.

Wraps an ``HnswIndex`` and emits a single batch of (label, distance) pairs
for the supplied query vector. Optional metadata filtering is performed via
a callable that receives the matched node ids and returns a boolean mask —
the planner uses this to fold predicates into the k-NN call.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.graph.hnsw import HnswIndex
from caracaldb.lang.diagnostics import CaracalError


class KnnOperator(PhysicalOperator):
    name = "Knn"

    def __init__(
        self,
        index: HnswIndex,
        *,
        query: np.ndarray,
        k: int,
        ef: int | None = None,
        metadata_filter: Callable[[np.ndarray], np.ndarray] | None = None,
        nid_alias: str = "nid",
        distance_alias: str = "distance",
    ) -> None:
        super().__init__()
        if k <= 0:
            raise CaracalError(code="CDB-6090", message="k must be positive")
        self._index = index
        self._query = np.asarray(query, dtype=np.float32)
        self._k = k
        self._ef = ef
        self._metadata_filter = metadata_filter
        self._nid_alias = nid_alias
        self._distance_alias = distance_alias
        self._emitted = False

    def _open(self, ctx: ExecCtx) -> None:
        return None

    def _next_batch(self) -> pa.RecordBatch | None:
        if self._emitted:
            return None
        self._emitted = True
        labels, dists = self._index.search(self._query, k=self._k, ef=self._ef)
        labels_flat = labels[0]
        dists_flat = dists[0]
        if self._metadata_filter is not None:
            mask = np.asarray(self._metadata_filter(labels_flat), dtype=bool)
            labels_flat = labels_flat[mask]
            dists_flat = dists_flat[mask]
        if labels_flat.size == 0:
            return None
        return pa.RecordBatch.from_arrays(
            [
                pa.array(labels_flat.astype(np.uint64), type=pa.uint64()),
                pa.array(dists_flat.astype(np.float32), type=pa.float32()),
            ],
            names=[self._nid_alias, self._distance_alias],
        )


__all__ = ["KnnOperator"]
