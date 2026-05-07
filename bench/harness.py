"""Small benchmark harness used by CI and the ``caracal bench`` command."""

from __future__ import annotations

import json
import random
import tempfile
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

import caracaldb as cdb

BenchResult = dict[str, Any]
BenchRunner = Callable[[], BenchResult]


def _elapsed_ms(fn: Callable[[], object]) -> float:
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0


def bench_1hop(n: int = 200_000, degree: int = 16, repeats: int = 1000) -> BenchResult:
    edges = np.arange(n * degree, dtype=np.uint64).reshape(n, degree) % n
    seeds = np.arange(0, n, max(1, n // 1024), dtype=np.uint64)

    def run() -> int:
        total = 0
        for _ in range(repeats):
            total += int(edges[seeds].sum())
        return total

    return {
        "scenario": "1hop",
        "metric": "ms",
        "value": _elapsed_ms(run),
        "n": n,
        "degree": degree,
        "repeats": repeats,
    }


def bench_2hop(n: int = 100_000, degree: int = 16, repeats: int = 120) -> BenchResult:
    edges = np.arange(n * degree, dtype=np.uint64).reshape(n, degree) % n
    seeds = np.arange(0, n, max(1, n // 512), dtype=np.uint64)

    def run() -> int:
        total = 0
        for _ in range(repeats):
            first = edges[seeds].ravel()
            second = edges[first].ravel()
            total += int(second.sum())
        return total

    return {
        "scenario": "2hop",
        "metric": "ms",
        "value": _elapsed_ms(run),
        "n": n,
        "degree": degree,
        "repeats": repeats,
    }


def bench_knn(n: int = 50_000, dim: int = 64, k: int = 10, repeats: int = 20) -> BenchResult:
    rng = np.random.default_rng(42)
    vectors = rng.normal(size=(n, dim)).astype(np.float32)
    query = vectors[0]

    def run() -> int:
        total = 0
        for _ in range(repeats):
            distances = np.linalg.norm(vectors - query, axis=1)
            total += int(np.argpartition(distances, k)[:k].sum())
        return total

    return {
        "scenario": "knn",
        "metric": "ms",
        "value": _elapsed_ms(run),
        "n": n,
        "dim": dim,
        "k": k,
        "repeats": repeats,
    }


def bench_neighbor_sample(
    n: int = 100_000, degree: int = 32, fanout: int = 8, repeats: int = 60
) -> BenchResult:
    rng = random.Random(42)
    adjacency = [tuple((i * degree + j) % n for j in range(degree)) for i in range(n)]
    seeds = list(range(0, n, max(1, n // 1024)))

    def run() -> int:
        total = 0
        for _ in range(repeats):
            for seed in seeds:
                total += sum(rng.sample(adjacency[seed], fanout))
        return total

    return {
        "scenario": "neighbor_sample",
        "metric": "ms",
        "value": _elapsed_ms(run),
        "n": n,
        "degree": degree,
        "fanout": fanout,
        "repeats": repeats,
    }


def bench_graph_ecosystem(
    n_nodes: int = 10_000,
    n_edges: int = 50_000,
    dim: int = 32,
    top_k: int = 8,
) -> BenchResult:
    rng = np.random.default_rng(42)
    vectors = rng.normal(size=(n_nodes, dim)).astype(np.float32)
    flat_vectors = pa.array(vectors.ravel().tolist(), type=pa.float32())
    embeddings = pa.FixedSizeListArray.from_arrays(flat_vectors, dim)
    node_ids = [f"chunk/{i:05d}" for i in range(n_nodes)]
    src = [node_ids[i % n_nodes] for i in range(n_edges)]
    dst = [node_ids[(i * 7 + 11) % n_nodes] for i in range(n_edges)]

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "graph-ecosystem"
        open_ms = _elapsed_ms(lambda: cdb.connect(path, format="bundle").close())
        with cdb.connect(path, format="bundle") as db:
            node_table = pa.table(
                {
                    "node_id": pa.array(node_ids),
                    "type": pa.array(["Chunk"] * n_nodes),
                    "source_type": pa.array(["document"] * n_nodes),
                    "document_id": pa.array([f"doc/{i // 10:04d}" for i in range(n_nodes)]),
                    "text": pa.array([f"chunk text {i}" for i in range(n_nodes)]),
                    "embedding": embeddings,
                }
            )
            edge_table = pa.table(
                {
                    "src": pa.array(src),
                    "dst": pa.array(dst),
                    "type": pa.array(["SEMANTIC_NEIGHBOR"] * n_edges),
                    "weight": pa.array(rng.random(n_edges).astype(np.float32)),
                    "metric": pa.array(["cosine"] * n_edges),
                    "index_name": pa.array(["chunk_embedding_hnsw"] * n_edges),
                }
            )
            node_insert_ms = _elapsed_ms(lambda: db.upsert_node_table_arrow(node_table))
            edge_insert_ms = _elapsed_ms(lambda: db.upsert_edge_table_arrow(edge_table))
            index_ms = _elapsed_ms(
                lambda: db.create_vector_index(
                    name="chunk_embedding_hnsw",
                    node_type="Chunk",
                    property="embedding",
                    dimension=dim,
                    metric="cosine",
                    algorithm="hnsw",
                    options={"m": 16, "ef_construction": 200, "ef_search": 64},
                )
            )
            query = vectors[0].tolist()
            search_result: dict[str, Any] = {}

            def run_search() -> None:
                search_result["rows"] = db.vector_search(
                    index="chunk_embedding_hnsw",
                    query_vector=query,
                    top_k=top_k,
                    return_properties=["document_id", "text"],
                ).rows()

            vector_search_ms = _elapsed_ms(run_search)
            seed_ids = [row["node_id"] for row in search_result["rows"][:2]]
            traversal_ms = _elapsed_ms(
                lambda: db.neighbors(
                    seed_node_ids=seed_ids,
                    edge_types=["SEMANTIC_NEIGHBOR"],
                    direction="out",
                    depth=2,
                    limit=100,
                    edge_filters={"weight_gte": 0.0},
                    return_paths=True,
                    path_score="product",
                    path_score_property="weight",
                ).rows()
            )
            profile = db.profile(
                "CALL vector.search('chunk_embedding_hnsw', " f"{query[:dim]}, {top_k})"
            )

    return {
        "scenario": "graph_ecosystem",
        "metric": "ms",
        "open_ms": open_ms,
        "batch_insert_nodes_ms": node_insert_ms,
        "batch_insert_edges_ms": edge_insert_ms,
        "vector_index_build_ms": index_ms,
        "vector_search_ms": vector_search_ms,
        "typed_2hop_traversal_ms": traversal_ms,
        "semantic_entry_mode": "caracal_hnsw",
        "semantic_reentry_mode": "native_result_nodes",
        "relation_expand_mode": "neighbors_api",
        "fallback_flags": profile["fallback_flags"],
        "vector_index_used": profile["vector_index_used"],
        "result_count": len(search_result["rows"]),
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "dim": dim,
    }


RUNNERS: dict[str, BenchRunner] = {
    "1hop": bench_1hop,
    "2hop": bench_2hop,
    "knn": bench_knn,
    "neighbor_sample": bench_neighbor_sample,
    "graph_ecosystem": bench_graph_ecosystem,
}


def run_all() -> list[BenchResult]:
    return [runner() for runner in RUNNERS.values()]


def write_results(path: Path, results: Iterable[BenchResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(results), indent=2), encoding="utf-8")


def compare_against_baseline(
    latest: Iterable[BenchResult],
    baseline: Iterable[BenchResult],
    *,
    tolerance: float,
) -> list[str]:
    latest_by_name = {str(item["scenario"]): item for item in latest}
    regressions: list[str] = []
    for base in baseline:
        scenario = str(base["scenario"])
        current = latest_by_name.get(scenario)
        if current is None:
            regressions.append(f"{scenario}: missing latest result")
            continue
        base_value = float(base["value"])
        current_value = float(current["value"])
        if base_value <= 0:
            continue
        max_allowed = base_value * (1.0 + tolerance)
        if current_value > max_allowed:
            regressions.append(
                f"{scenario}: {current_value:.3f} ms > {max_allowed:.3f} ms "
                f"(baseline {base_value:.3f} ms)"
            )
    return regressions
