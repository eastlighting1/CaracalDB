from pathlib import Path

import pyarrow as pa

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import NeighborSampleOperator, NodeScanOperator
from caracaldb.graph import build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed(tmp_path: Path):
    bundle = create_bundle(tmp_path / "g")
    nodes = open_node_store(bundle, class_iri="http://x/V", local_name="V", create=True)
    nodes.append(pa.record_batch({"label": pa.array([f"n{i}" for i in range(6)])}))
    p = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    p.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 0, 1, 1, 2, 3, 4], type=pa.uint64()),
                "dst": pa.array([1, 2, 3, 2, 3, 4, 5, 5], type=pa.uint64()),
            }
        )
    )
    csr_p = tmp_path / "p.csr"
    build_csr(p, num_vertices=6, out_path=csr_p, with_eids=True)
    return NodeScanOperator(nodes, columns=["nid"]), {"p": CsrReader(csr_p)}


def test_neighbor_sample_with_full_fanout(tmp_path: Path) -> None:
    seeds, readers = _seed(tmp_path)
    op = NeighborSampleOperator(seeds, edge_readers=readers, layers=[0])  # fanout=0 = all
    batches = list(run_pipeline(op))
    total = sum(b.num_rows for b in batches)
    # Total edges in the seed expansion equals the number of edges (8).
    assert total == 8
    assert all("etype" in b.schema.names and "layer" in b.schema.names for b in batches)


def test_neighbor_sample_caps_per_seed(tmp_path: Path) -> None:
    seeds, readers = _seed(tmp_path)
    op = NeighborSampleOperator(seeds, edge_readers=readers, layers=[2], seed=7)
    batches = list(run_pipeline(op))
    counts: dict[int, int] = {}
    for b in batches:
        for s in b.column("src").to_pylist():
            counts[s] = counts.get(s, 0) + 1
    # Each source must contribute at most fanout=2 rows.
    assert all(v <= 2 for v in counts.values())


def test_neighbor_sample_two_layers(tmp_path: Path) -> None:
    seeds, readers = _seed(tmp_path)
    op = NeighborSampleOperator(seeds, edge_readers=readers, layers=[0, 0])
    batches = list(run_pipeline(op))
    layer_ids = sorted({lv for b in batches for lv in b.column("layer").to_pylist()})
    assert layer_ids == [0, 1]
