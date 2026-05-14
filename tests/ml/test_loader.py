from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.graph import build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml import NeighborLoader, NeighborLoaderConfig, Subgraph
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed(tmp_path: Path):
    bundle = create_bundle(tmp_path / "g")
    nodes = open_node_store(bundle, class_iri="http://x/V", local_name="V", create=True)
    nodes.append(pa.record_batch({"label": pa.array([f"n{i}" for i in range(10)])}))
    p = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    p.append(
        pa.record_batch(
            {
                "src": pa.array(list(range(10)), type=pa.uint64()),
                "dst": pa.array([(i + 1) % 10 for i in range(10)], type=pa.uint64()),
            }
        )
    )
    csr_path = tmp_path / "p.csr"
    build_csr(p, num_vertices=10, out_path=csr_path, with_eids=True)
    return bundle, CsrReader(csr_path)


def test_neighbor_loader_yields_subgraphs(tmp_path: Path) -> None:
    bundle, csr = _seed(tmp_path)
    cfg = NeighborLoaderConfig(
        layers=[2, 1],
        edge_readers={"http://x/p": csr},
        seed_class_iri="http://x/V",
        seed_local_name="V",
        batch_size=4,
        backend="arrow",
        seed=0,
    )
    loader = NeighborLoader(bundle, cfg)
    batches = list(iter(loader))
    # 10 seeds in batches of 4 → 3 batches.
    assert len(batches) == 3
    assert all(isinstance(b, Subgraph) for b in batches)
    # Each subgraph must contain at least one edge table.
    assert all(b.num_edges() > 0 for b in batches)


def test_neighbor_loader_node_features_attaches_columns(tmp_path: Path) -> None:
    bundle, csr = _seed(tmp_path)
    cfg = NeighborLoaderConfig(
        layers=[1],
        edge_readers={"http://x/p": csr},
        seed_class_iri="http://x/V",
        seed_local_name="V",
        batch_size=10,
        node_features={"http://x/V": ["label"]},
    )
    loader = NeighborLoader(bundle, cfg)
    sg = next(iter(loader))
    assert "http://x/V" in sg.nodes
    assert "label" in sg.nodes["http://x/V"].column_names


def test_neighbor_loader_accepts_lynxes_backend_when_installed(tmp_path: Path) -> None:
    pytest.importorskip("lynxes")
    bundle, csr = _seed(tmp_path)
    cfg = NeighborLoaderConfig(
        layers=[1],
        edge_readers={"http://x/p": csr},
        seed_class_iri="http://x/V",
        seed_local_name="V",
        batch_size=10,
        backend="lynxes",
    )
    first = next(iter(NeighborLoader(bundle, cfg)))
    assert hasattr(first, "nodes") and hasattr(first, "edges")


def test_neighbor_loader_rejects_dgl_backend(tmp_path: Path) -> None:
    bundle, csr = _seed(tmp_path)
    cfg = NeighborLoaderConfig(
        layers=[1],
        edge_readers={"http://x/p": csr},
        seed_class_iri="http://x/V",
        seed_local_name="V",
        backend="dgl",
    )
    with pytest.raises(CaracalError) as exc:
        NeighborLoader(bundle, cfg)
    assert exc.value.code == "CDB-6120"
    assert "DGL backend is not supported" in exc.value.message


def test_neighbor_loader_rejects_lynxes_backend_when_missing(tmp_path: Path) -> None:
    try:
        import lynxes  # noqa: F401
    except ImportError:
        bundle, csr = _seed(tmp_path)
        cfg = NeighborLoaderConfig(
            layers=[1],
            edge_readers={"http://x/p": csr},
            seed_class_iri="http://x/V",
            seed_local_name="V",
            batch_size=10,
            backend="lynxes",
        )
        with pytest.raises(CaracalError) as exc:
            next(iter(NeighborLoader(bundle, cfg)))
        assert exc.value.code == "CDB-6113"
