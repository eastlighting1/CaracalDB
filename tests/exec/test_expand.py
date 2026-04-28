from pathlib import Path

import pyarrow as pa

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import ExpandOperator, NodeScanOperator
from caracaldb.graph import build_csc, build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed_graph(tmp_path: Path) -> tuple[NodeScanOperator, CsrReader, CsrReader]:
    bundle = create_bundle(tmp_path / "g")
    nodes = open_node_store(bundle, class_iri="http://x/V", local_name="V", create=True)
    nodes.append(pa.record_batch({"label": pa.array(["a", "b", "c", "d"])}))
    edges = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    edges.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2], type=pa.uint64()),
                "dst": pa.array([1, 2, 3, 3], type=pa.uint64()),
            }
        )
    )
    fwd = tmp_path / "p.csr"
    rev = tmp_path / "p.csc"
    build_csr(edges, num_vertices=4, out_path=fwd, with_eids=True)
    build_csc(edges, num_vertices=4, out_path=rev, with_eids=True)
    return NodeScanOperator(nodes, columns=["nid"]), CsrReader(fwd), CsrReader(rev)


def test_expand_out_emits_seed_dst_pairs(tmp_path: Path) -> None:
    seeds, fwd, _ = _seed_graph(tmp_path)
    expand = ExpandOperator(seeds, forward=fwd, direction="out")
    batches = list(run_pipeline(expand))
    pairs = sorted(
        (s, d)
        for batch in batches
        for s, d in zip(
            batch.column("src").to_pylist(), batch.column("dst").to_pylist(), strict=False
        )
    )
    assert pairs == [(0, 1), (0, 2), (1, 3), (2, 3)]


def test_expand_in_walks_reverse(tmp_path: Path) -> None:
    seeds, _, rev = _seed_graph(tmp_path)
    expand = ExpandOperator(seeds, reverse=rev, direction="in")
    batches = list(run_pipeline(expand))
    # The dst in expand output corresponds to in-neighbours.
    pairs = sorted(
        (s, d)
        for batch in batches
        for s, d in zip(
            batch.column("src").to_pylist(), batch.column("dst").to_pylist(), strict=False
        )
    )
    # vertex 3 has in-edges from {1, 2}; vertex 1 from {0}; vertex 2 from {0}.
    assert (3, 1) in pairs and (3, 2) in pairs and (1, 0) in pairs and (2, 0) in pairs


def test_expand_both_direction_concatenates(tmp_path: Path) -> None:
    seeds, fwd, rev = _seed_graph(tmp_path)
    expand = ExpandOperator(seeds, forward=fwd, reverse=rev, direction="both")
    batches = list(run_pipeline(expand))
    total = sum(b.num_rows for b in batches)
    # forward: 4 edges; reverse: 4 edges → total 8 emitted rows.
    assert total == 8


def test_expand_with_edge_alias_returns_eids(tmp_path: Path) -> None:
    seeds, fwd, _ = _seed_graph(tmp_path)
    expand = ExpandOperator(seeds, forward=fwd, direction="out", edge_alias="eid")
    batch = list(run_pipeline(expand))[0]
    assert "eid" in batch.schema.names
    assert sorted(batch.column("eid").to_pylist()) == [0, 1, 2, 3]
