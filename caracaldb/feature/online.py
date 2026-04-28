"""Online feature lookup view (point-in-time, p99 < 5ms target).

The view materialises a full-class feature table once at construction and
keeps it in memory keyed by ``nid`` (NumPy array). ``lookup(nid)`` returns
a per-feature dict; ``lookup_many(nids)`` returns column-aligned arrays
suitable for direct embedding into a model's input tensor.

The Python implementation deliberately preloads everything because the
target use case (refresh-style feature serving) walks all nodes regularly;
the M5 Rust port replaces this with mmap'd column slices.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.node_store import open_node_store


@dataclass(slots=True)
class OnlineLookupStats:
    lookups: int = 0
    rows_returned: int = 0
    p50_ms: float = 0.0
    p99_ms: float = 0.0


class OnlineFeatureView:
    def __init__(
        self,
        bundle: Bundle,
        *,
        class_iri: str,
        local_name: str,
        feature_columns: Sequence[str],
    ) -> None:
        if not feature_columns:
            raise CaracalError(code="CDB-6150", message="feature_columns is empty")
        store = open_node_store(bundle, class_iri=class_iri, local_name=local_name)
        table = store.to_table(columns=["nid", *feature_columns])
        self._features: dict[str, np.ndarray] = {
            name: np.asarray(table.column(name).combine_chunks().to_numpy(zero_copy_only=False))
            for name in feature_columns
        }
        self._nid_to_idx = {
            int(nid): i
            for i, nid in enumerate(
                table.column("nid").combine_chunks().to_numpy(zero_copy_only=False)
            )
        }
        self._stats = OnlineLookupStats()
        self._latencies_ms: list[float] = []

    def lookup(self, nid: int) -> dict[str, object]:
        idx = self._nid_to_idx.get(int(nid))
        start = time.perf_counter()
        if idx is None:
            self._record(start)
            return {}
        out = {name: arr[idx] for name, arr in self._features.items()}
        self._stats.rows_returned += 1
        self._record(start)
        return out

    def lookup_many(self, nids: np.ndarray) -> pa.Table:
        start = time.perf_counter()
        idx_arr = np.array(
            [self._nid_to_idx.get(int(n), -1) for n in nids.tolist()], dtype=np.int64
        )
        valid = idx_arr >= 0
        idx_safe = np.where(valid, idx_arr, 0)
        columns: dict[str, pa.Array] = {"nid": pa.array(nids, type=pa.uint64())}
        for name, arr in self._features.items():
            gathered = arr[idx_safe]
            arrow_arr = pa.array(gathered)
            null_mask = pa.array((~valid).tolist())
            columns[name] = pa.compute.if_else(
                null_mask, pa.scalar(None, type=arrow_arr.type), arrow_arr
            )
        self._stats.rows_returned += int(valid.sum())
        self._record(start)
        return pa.table(columns)

    def stats(self) -> OnlineLookupStats:
        if self._latencies_ms:
            sorted_l = sorted(self._latencies_ms)
            self._stats.p50_ms = sorted_l[len(sorted_l) // 2]
            self._stats.p99_ms = sorted_l[max(0, int(len(sorted_l) * 0.99) - 1)]
        return self._stats

    def _record(self, start: float) -> None:
        self._stats.lookups += 1
        self._latencies_ms.append((time.perf_counter() - start) * 1000.0)


__all__ = ["OnlineFeatureView", "OnlineLookupStats"]
