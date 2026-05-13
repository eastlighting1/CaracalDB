"""Small benchmark harness used by CI and the ``caracal bench`` command."""

from __future__ import annotations

import io
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
    n_entities = max(4, min(256, max(4, n_nodes // 20)))
    entity_vectors = rng.normal(size=(n_entities, dim)).astype(np.float32)
    all_vectors = np.vstack([entity_vectors, vectors])
    flat_vectors = pa.array(all_vectors.ravel().tolist(), type=pa.float32())
    embeddings = pa.FixedSizeListArray.from_arrays(flat_vectors, dim)
    entity_ids = [f"entity/{i:04d}" for i in range(n_entities)]
    node_ids = [f"chunk/{i:05d}" for i in range(n_nodes)]
    mention_edges = min(max(n_nodes, n_entities), max(1, n_edges // 5))
    relation_edges = min(n_entities * 2, max(1, n_edges // 20))
    evidence_edges = min(n_entities * 2, max(1, n_edges // 20))
    semantic_edges = max(0, n_edges - mention_edges - relation_edges - evidence_edges)
    semantic_src = [node_ids[i % n_nodes] for i in range(semantic_edges)]
    semantic_dst = [node_ids[(i * 7 + 11) % n_nodes] for i in range(semantic_edges)]
    mention_src = [node_ids[i % n_nodes] for i in range(mention_edges)]
    mention_dst = [entity_ids[i % n_entities] for i in range(mention_edges)]
    relation_src = [entity_ids[i % n_entities] for i in range(relation_edges)]
    relation_dst = [entity_ids[(i * 3 + 1) % n_entities] for i in range(relation_edges)]
    evidence_src = [entity_ids[i % n_entities] for i in range(evidence_edges)]
    evidence_dst = [node_ids[(i * 11 + 3) % n_nodes] for i in range(evidence_edges)]
    src = semantic_src + mention_src + relation_src + evidence_src
    dst = semantic_dst + mention_dst + relation_dst + evidence_dst
    edge_types = (
        ["SEMANTIC_NEIGHBOR"] * semantic_edges
        + ["MENTIONS"] * mention_edges
        + ["RELATED_TO"] * relation_edges
        + ["EVIDENCED_BY"] * evidence_edges
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "graph-ecosystem"
        open_ms = _elapsed_ms(lambda: cdb.connect(path, format="bundle").close())
        with cdb.connect(path, format="bundle") as db:
            node_table = pa.table(
                {
                    "node_id": pa.array(entity_ids + node_ids),
                    "type": pa.array(["Entity"] * n_entities + ["Chunk"] * n_nodes),
                    "source_type": pa.array([None] * n_entities + ["document"] * n_nodes),
                    "document_id": pa.array(
                        [None] * n_entities + [f"doc/{i // 10:04d}" for i in range(n_nodes)]
                    ),
                    "text": pa.array(
                        [None] * n_entities
                        + [
                            f"chunk text {i} mentions entity {i % n_entities}"
                            for i in range(n_nodes)
                        ]
                    ),
                    "name": pa.array([f"Entity {i}" for i in range(n_entities)] + [None] * n_nodes),
                    "canonical_name": pa.array(
                        [f"entity {i}" for i in range(n_entities)] + [None] * n_nodes
                    ),
                    "aliases": pa.array(
                        [[f"E{i}"] for i in range(n_entities)] + [[] for _ in range(n_nodes)]
                    ),
                    "entity_type": pa.array(["topic"] * n_entities + [None] * n_nodes),
                    "embedding": embeddings,
                }
            )
            edge_table = pa.table(
                {
                    "src": pa.array(src),
                    "dst": pa.array(dst),
                    "type": pa.array(edge_types),
                    "weight": pa.array(rng.random(len(src)).astype(np.float32)),
                    "metric": pa.array(
                        ["cosine" if kind == "SEMANTIC_NEIGHBOR" else None for kind in edge_types]
                    ),
                    "index_name": pa.array(
                        [
                            "chunk_embedding_hnsw" if kind == "SEMANTIC_NEIGHBOR" else None
                            for kind in edge_types
                        ]
                    ),
                }
            )
            node_insert_ms = _elapsed_ms(lambda: db.upsert_node_table_arrow(node_table))
            edge_insert_ms = _elapsed_ms(lambda: db.upsert_edge_table_arrow(edge_table))
            db.create_text_index(
                name="entity_name_text_idx",
                node_type="Entity",
                properties=["name", "canonical_name", "aliases"],
            )
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
                result = db.graphrag_search(
                    query_text="Entity 0 evidence",
                    query_vector=query,
                    chunk_vector_index="chunk_embedding_hnsw",
                    entity_text_index="entity_name_text_idx",
                    edge_types=["MENTIONS", "RELATED_TO", "EVIDENCED_BY", "SEMANTIC_NEIGHBOR"],
                    max_depth=2,
                    semantic_top_k=top_k,
                    entity_top_k=4,
                    evidence_top_k=max(top_k, 8),
                    return_properties=["document_id", "text"],
                )
                search_result["rows"] = result.rows()
                search_result["profile"] = result.profile

            graph_search_ms = _elapsed_ms(run_search)
            vector_search_ms = search_result["profile"]["operator_timings"]["vector_graph_search"]
            entity_linking_ms = search_result["profile"]["operator_timings"]["link_entities"]
            traversal_ms = search_result["profile"]["operator_timings"]["evidence_search"]
            profile = search_result["profile"]

    return {
        "scenario": "graph_ecosystem",
        "metric": "ms",
        "open_ms": open_ms,
        "batch_insert_nodes_ms": node_insert_ms,
        "batch_insert_edges_ms": edge_insert_ms,
        "vector_index_build_ms": index_ms,
        "vector_search_ms": vector_search_ms,
        "entity_linking_ms": entity_linking_ms,
        "graph_search_ms": graph_search_ms,
        "typed_2hop_traversal_ms": traversal_ms,
        "semantic_entry_mode": "caracal_graphrag_search",
        "query_entity_linking_mode": "caracal_link_entities",
        "semantic_reentry_mode": "native_result_nodes",
        "relation_expand_mode": "caracal_evidence_search",
        "fallback_flags": profile["fallback_flags"],
        "vector_index_used": profile["vector_index_used"],
        "result_count": len(search_result["rows"]),
        "n_nodes": n_nodes,
        "n_edges": len(src),
        "dim": dim,
    }


def _rust() -> Any:
    from caracaldb import _caracaldb_rust

    return _caracaldb_rust


def _ipc_stream(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _read_ipc_streams(streams: Iterable[bytes]) -> pa.Table:
    tables = [pa.ipc.open_stream(io.BytesIO(stream)).read_all() for stream in streams]
    if not tables:
        return pa.table({})
    return pa.concat_tables(tables, promote_options="default")


def _rust_bundle() -> tuple[Any, Path, str]:
    rust = _rust()
    tmp = tempfile.TemporaryDirectory()
    path = Path(tmp.name) / "bench.crcl"
    rust.create_bundle(str(path), False)
    return rust, path, tmp


def _rust_node_table(n: int = 4096) -> pa.Table:
    return pa.table(
        {
            "symbol": pa.array([f"n{i}" for i in range(n)]),
            "score": pa.array(np.arange(n, dtype=np.uint64)),
        }
    )


def _rust_edge_table(n: int = 4096, vertices: int = 1024) -> pa.Table:
    src = np.arange(n, dtype=np.uint64) % vertices
    dst = (src + 1) % vertices
    return pa.table({"src": pa.array(src), "dst": pa.array(dst)})


def _rust_storage_scenario(name: str, kind: str, scan: bool = False) -> BenchResult:
    rust, path, tmp = _rust_bundle()
    with tmp:
        if kind == "node":
            table = _rust_node_table()
            rust.open_node_store(str(path), "http://example.org/Gene", "Gene", True)

            def run() -> int:
                rust.append_node_batch(
                    str(path),
                    "http://example.org/Gene",
                    "Gene",
                    _ipc_stream(table),
                    1,
                )
                if scan:
                    return _read_ipc_streams(
                        rust.scan_node_store(str(path), "http://example.org/Gene", "Gene", None)
                    ).num_rows
                return table.num_rows

        else:
            table = _rust_edge_table()
            rust.open_edge_store(str(path), "http://example.org/REL", "REL", True)

            def run() -> int:
                rust.append_edge_batch(
                    str(path),
                    "http://example.org/REL",
                    "REL",
                    _ipc_stream(table),
                    1,
                )
                if scan:
                    return _read_ipc_streams(
                        rust.scan_edge_store(str(path), "http://example.org/REL", "REL", None)
                    ).num_rows
                return table.num_rows

        return {
            "scenario": name,
            "metric": "ms",
            "value": _elapsed_ms(run),
            "engine": "rust",
            "rows": table.num_rows,
        }


def _rust_csr_scenario(name: str, mode: str) -> BenchResult:
    rust = _rust()
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "graph.csr"
        n = 4096
        src = (np.arange(n * 4, dtype=np.uint64) % n).tolist()
        dst = ((np.arange(n * 4, dtype=np.uint64) + 1) % n).tolist()
        seeds = [0, 17, 1024, 2048]

        if mode == "build":

            def run() -> int:
                meta = rust.build_csr(str(path), n, src, dst)
                return int(meta["num_edges"])

        elif mode == "neighbors":
            rust.build_csr(str(path), n, src, dst)

            def run() -> int:
                rows = rust.csr_neighbor_sample_rows(str(path), seeds, 8, False)
                return len(rows)

        elif mode == "pattern":
            rust.build_csr(str(path), n, src, dst)

            def run() -> int:
                total = 0
                for seed in seeds:
                    total += len(rust.csr_neighbors(str(path), seed)["neighbors"])
                return total

        else:
            rust.build_csr(str(path), n, src, dst)

            def run() -> int:
                return len(rust.csr_k_hop_rows(str(path), seeds, 1, 3))

        return {
            "scenario": name,
            "metric": "ms",
            "value": _elapsed_ms(run),
            "engine": "rust",
            "vertices": n,
            "edges": len(src),
        }


def bench_rust_node_append() -> BenchResult:
    return _rust_storage_scenario("rust_node_append", "node", scan=False)


def bench_rust_edge_append() -> BenchResult:
    return _rust_storage_scenario("rust_edge_append", "edge", scan=False)


def bench_rust_node_scan() -> BenchResult:
    return _rust_storage_scenario("rust_node_scan", "node", scan=True)


def bench_rust_edge_scan() -> BenchResult:
    return _rust_storage_scenario("rust_edge_scan", "edge", scan=True)


def bench_rust_csr_build() -> BenchResult:
    return _rust_csr_scenario("rust_csr_build", "build")


def bench_rust_neighbor_traversal() -> BenchResult:
    return _rust_csr_scenario("rust_neighbor_traversal", "neighbors")


def bench_rust_pattern_query() -> BenchResult:
    return _rust_csr_scenario("rust_pattern_query", "pattern")


def bench_rust_var_path_query() -> BenchResult:
    return _rust_csr_scenario("rust_var_path_query", "var_path")


RUNNERS: dict[str, BenchRunner] = {
    "1hop": bench_1hop,
    "2hop": bench_2hop,
    "knn": bench_knn,
    "neighbor_sample": bench_neighbor_sample,
    "graph_ecosystem": bench_graph_ecosystem,
    "rust_node_append": bench_rust_node_append,
    "rust_edge_append": bench_rust_edge_append,
    "rust_node_scan": bench_rust_node_scan,
    "rust_edge_scan": bench_rust_edge_scan,
    "rust_csr_build": bench_rust_csr_build,
    "rust_neighbor_traversal": bench_rust_neighbor_traversal,
    "rust_pattern_query": bench_rust_pattern_query,
    "rust_var_path_query": bench_rust_var_path_query,
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
