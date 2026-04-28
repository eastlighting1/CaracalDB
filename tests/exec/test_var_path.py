from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import NodeScanOperator, VarPathOperator
from caracaldb.graph import build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed(tmp_path: Path) -> tuple[NodeScanOperator, CsrReader]:
    bundle = create_bundle(tmp_path / "g")
    nodes = open_node_store(bundle, class_iri="http://x/V", local_name="V", create=True)
    nodes.append(pa.record_batch({"label": pa.array(["a", "b", "c", "d"])}))
    edges = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    # 0 → 1 → 2 → 3, 0 → 2 (so 0 reaches {1,2,3} within 2 hops)
    edges.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2], type=pa.uint64()),
                "dst": pa.array([1, 2, 2, 3], type=pa.uint64()),
            }
        )
    )
    csr = tmp_path / "p.csr"
    build_csr(edges, num_vertices=4, out_path=csr, with_eids=False)
    return NodeScanOperator(nodes, columns=["nid"]), CsrReader(csr)


def test_var_path_one_hop_matches_expand(tmp_path: Path) -> None:
    seeds, csr = _seed(tmp_path)
    op = VarPathOperator(seeds, forward=csr, hop_min=1, hop_max=1)
    batch = list(run_pipeline(op))[0]
    pairs = sorted(
        zip(batch.column("src").to_pylist(), batch.column("dst").to_pylist(), strict=False)
    )
    assert pairs == [(0, 1), (0, 2), (1, 2), (2, 3)]


def test_var_path_two_hops_includes_transitive_reach(tmp_path: Path) -> None:
    seeds, csr = _seed(tmp_path)
    op = VarPathOperator(seeds, forward=csr, hop_min=1, hop_max=2)
    batch = list(run_pipeline(op))[0]
    pairs = set(zip(batch.column("src").to_pylist(), batch.column("dst").to_pylist(), strict=False))
    # 0 reaches 1 (hop1), 2 (hop1), 3 (via 2 hop2)
    assert (0, 3) in pairs
    # 1 reaches 2 (hop1), 3 (hop2)
    assert (1, 3) in pairs


def test_var_path_dedups_visited_pairs(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "g")
    nodes = open_node_store(bundle, class_iri="http://x/V", local_name="V", create=True)
    nodes.append(pa.record_batch({"label": pa.array(["a", "b"])}))
    edges = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    # cycle: 0 ↔ 1
    edges.append(
        pa.record_batch(
            {"src": pa.array([0, 1], type=pa.uint64()), "dst": pa.array([1, 0], type=pa.uint64())}
        )
    )
    csr = tmp_path / "p.csr"
    build_csr(edges, num_vertices=2, out_path=csr, with_eids=False)
    op = VarPathOperator(
        NodeScanOperator(nodes, columns=["nid"]), forward=CsrReader(csr), hop_min=1, hop_max=5
    )
    batch = list(run_pipeline(op))[0]
    # Only (0,1), (1,0), (0,0), (1,1) possible reachable pairs after dedup.
    pairs = set(zip(batch.column("src").to_pylist(), batch.column("dst").to_pylist(), strict=False))
    assert pairs == {(0, 1), (1, 0), (0, 0), (1, 1)}


def test_var_path_rejects_invalid_range(tmp_path: Path) -> None:
    seeds, csr = _seed(tmp_path)
    with pytest.raises(CaracalError) as exc:
        VarPathOperator(seeds, forward=csr, hop_min=2, hop_max=1)
    assert exc.value.code == "CDB-6031"
