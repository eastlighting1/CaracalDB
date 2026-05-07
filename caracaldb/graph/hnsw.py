"""HNSW vector index wrapper around ``hnswlib``.

Stores embeddings keyed by node id (``label`` in hnswlib parlance) in a
single index per (class, vector property). The wrapper is intentionally thin
so the heavy work happens inside hnswlib's native loop (GIL-released by the
underlying C++ library). Index files are written via ``save_index`` /
``load_index`` and live under ``<bundle>/vec/<class>.<property>.hnsw``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import hnswlib
import numpy as np

from caracaldb.lang.diagnostics import CaracalError

Metric = Literal["cosine", "l2", "ip"]
_HNSW_SPACE = {"cosine": "cosine", "l2": "l2", "ip": "ip"}


@dataclass(frozen=True, slots=True)
class HnswConfig:
    dim: int
    M: int = 16
    ef_construction: int = 200
    metric: Metric = "cosine"
    max_elements: int = 1024
    random_seed: int = 100


class HnswIndex:
    def __init__(self, config: HnswConfig) -> None:
        if config.dim <= 0:
            raise CaracalError(code="CDB-7090", message="HNSW dim must be positive")
        if config.metric not in _HNSW_SPACE:
            raise CaracalError(code="CDB-7090", message=f"unknown metric: {config.metric}")
        self._config = config
        self._index = hnswlib.Index(space=_HNSW_SPACE[config.metric], dim=config.dim)
        self._index.init_index(
            max_elements=max(1, config.max_elements),
            M=config.M,
            ef_construction=config.ef_construction,
            random_seed=config.random_seed,
        )
        self._loaded_dim = config.dim

    @property
    def dim(self) -> int:
        return self._loaded_dim

    @property
    def metric(self) -> Metric:
        return self._config.metric

    def __len__(self) -> int:
        return int(self._index.get_current_count())

    def add(self, ids: Sequence[int] | np.ndarray, vectors: np.ndarray) -> None:
        ids_arr = np.asarray(ids, dtype=np.uint64)
        vec = np.asarray(vectors, dtype=np.float32)
        if vec.ndim != 2 or vec.shape[1] != self._loaded_dim:
            raise CaracalError(
                code="CDB-7091",
                message=f"vectors must be (N, {self._loaded_dim}); got {vec.shape}",
            )
        if ids_arr.shape[0] != vec.shape[0]:
            raise CaracalError(code="CDB-7091", message="ids/vectors length mismatch")
        needed = int(self._index.get_current_count()) + int(vec.shape[0])
        if needed > self._index.get_max_elements():
            self._index.resize_index(needed)
        self._index.add_items(vec, ids_arr)

    def set_ef(self, ef: int) -> None:
        self._index.set_ef(max(1, ef))

    def search(
        self, query: np.ndarray, *, k: int, ef: int | None = None
    ) -> tuple[np.ndarray, np.ndarray]:
        q = np.asarray(query, dtype=np.float32)
        if q.ndim == 1:
            q = q.reshape(1, -1)
        if q.shape[1] != self._loaded_dim:
            raise CaracalError(
                code="CDB-7091",
                message=f"query dim {q.shape[1]} != index dim {self._loaded_dim}",
            )
        if ef is not None:
            self.set_ef(ef)
        labels, distances = self._index.knn_query(q, k=min(k, len(self)))
        return labels.astype(np.uint64), distances.astype(np.float32)

    def save(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # hnswlib writes directly; do an atomic rename for crash safety.
        tmp = target.with_name(f"{target.name}.tmp")
        self._index.save_index(str(tmp))
        tmp.replace(target)
        return target

    @classmethod
    def load(cls, path: str | Path, *, config: HnswConfig) -> HnswIndex:
        instance = cls.__new__(cls)
        instance._config = config
        instance._index = hnswlib.Index(space=_HNSW_SPACE[config.metric], dim=config.dim)
        instance._index.load_index(str(path), max_elements=max(1, config.max_elements))
        instance._loaded_dim = config.dim
        return instance


__all__ = ["HnswConfig", "HnswIndex", "Metric"]
