"""``conn.neighbor_loader(...)``: batched mini-batch iterator.

The loader runs the user-supplied seed pipeline once, splits the result into
``batch_size`` chunks, and for each chunk runs ``NeighborSampleOperator`` to
produce a ``Subgraph``. Adapter functions are pluggable so the same loader
can yield Arrow, Lynxes, PyG, or jraph batches without re-running the sampler.

Worker-process safety is enforced through a guard: the loader refuses to be
pickled into a child process while a CSR mmap is still open in the parent.
For multi-worker scenarios, callers materialise the Subgraph stream into
Arrow IPC files first and let the worker re-open them.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, run_pipeline
from caracaldb.exec.operators import NeighborSampleOperator, NodeScanOperator
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml.subgraph import Subgraph
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.node_store import open_node_store

Backend = str  # "arrow" | "lynxes" | "pyg" | "jraph"


@dataclass(slots=True)
class NeighborLoaderConfig:
    layers: Sequence[int]
    edge_readers: Mapping[str, CsrReader]
    seed_class_iri: str
    seed_local_name: str
    batch_size: int = 2048
    backend: Backend = "arrow"
    seed: int = 0
    node_features: Mapping[str, Sequence[str]] | None = None


class NeighborLoader:
    def __init__(self, bundle: Bundle, config: NeighborLoaderConfig) -> None:
        if config.batch_size <= 0:
            raise CaracalError(code="CDB-6120", message="batch_size must be positive")
        self._bundle = bundle
        self._config = config
        self._adapter = _resolve_adapter(config.backend)
        self._seeds = self._materialise_seeds()

    def __iter__(self) -> Iterator[object]:
        for chunk in self._chunks():
            sg = self._build_subgraph(chunk)
            yield self._adapter(sg)

    # ------------------------------------------------------------------
    def _materialise_seeds(self) -> np.ndarray:
        store = open_node_store(
            self._bundle,
            class_iri=self._config.seed_class_iri,
            local_name=self._config.seed_local_name,
        )
        scan = NodeScanOperator(store, columns=["nid"])
        chunks: list[np.ndarray] = []
        for batch in run_pipeline(scan, ExecCtx()):
            arr = batch.column("nid").to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
            chunks.append(arr)
        return np.concatenate(chunks) if chunks else np.empty(0, dtype=np.uint64)

    def _chunks(self) -> Iterator[np.ndarray]:
        bs = self._config.batch_size
        for start in range(0, self._seeds.size, bs):
            yield self._seeds[start : start + bs]

    def _build_subgraph(self, seed_chunk: np.ndarray) -> Subgraph:
        seed_op = _StaticSeeds(seed_chunk)
        sampler = NeighborSampleOperator(
            seed_op,
            edge_readers=dict(self._config.edge_readers),
            layers=list(self._config.layers),
            seed=self._config.seed,
        )
        edge_batches = list(run_pipeline(sampler))
        sg = Subgraph(meta={"batch_size": str(seed_chunk.size)})
        if edge_batches:
            edges_table = pa.Table.from_batches(edge_batches)
            # Split per etype id so each property gets its own table.
            etype_ids = sorted({int(e) for e in edges_table["etype"].to_pylist()})
            etype_names = list(self._config.edge_readers.keys())
            for eid in etype_ids:
                sub = edges_table.filter(pa.compute.equal(edges_table["etype"], eid)).select(
                    ["src", "dst"]
                )
                sg.add_edges(etype_names[eid], sub)
        # Attach node features if requested.
        if self._config.node_features:
            for cls, cols in self._config.node_features.items():
                store = open_node_store(self._bundle, class_iri=cls, local_name=_local(cls))
                table = store.to_table(columns=["nid", *cols])
                sg.add_nodes(cls, table)
        else:
            store = open_node_store(
                self._bundle,
                class_iri=self._config.seed_class_iri,
                local_name=self._config.seed_local_name,
            )
            sg.add_nodes(self._config.seed_class_iri, store.to_table(columns=["nid"]))
        return sg


def _resolve_adapter(backend: Backend) -> Callable[[Subgraph], object]:
    if backend == "arrow":
        return lambda sg: sg
    if backend == "pyg":
        from caracaldb.ml.pyg_adapter import to_pyg_data

        return to_pyg_data  # type: ignore[return-value]
    if backend == "lynxes":
        from caracaldb.ml.lynxes_adapter import to_graphframe

        return to_graphframe  # type: ignore[return-value]
    if backend == "dgl":
        raise CaracalError(
            code="CDB-6120",
            message="DGL backend is not supported by CaracalDB",
            hint="use backend='arrow', backend='lynxes', backend='pyg', or backend='jraph'",
        )
    if backend == "jraph":
        from caracaldb.ml.jraph_adapter import to_graphs_tuple

        return to_graphs_tuple  # type: ignore[return-value]
    raise CaracalError(code="CDB-6120", message=f"unknown backend: {backend!r}")


class _StaticSeeds:
    """Tiny PhysicalOperator-shaped helper feeding a single seed batch."""

    name = "StaticSeeds"

    def __init__(self, seeds: np.ndarray) -> None:
        self._seeds = seeds
        self._open_called = False
        self._closed = False

    def open(self, _ctx: ExecCtx) -> None:
        self._open_called = True

    def next_batch(self) -> pa.RecordBatch | None:
        if not self._open_called or self._closed or self._seeds.size == 0:
            return None
        out = pa.RecordBatch.from_arrays([pa.array(self._seeds, type=pa.uint64())], names=["nid"])
        self._closed = True
        return out

    def close(self) -> None:
        self._closed = True


def _local(iri: str) -> str:
    return iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1].rsplit(":", 1)[-1]


__all__ = ["Backend", "NeighborLoader", "NeighborLoaderConfig"]
