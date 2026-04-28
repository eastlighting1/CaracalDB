"""Case B fixture: a small Account / Transaction graph plus a kNN embedding.

Mirrors 03 §B's fintech scenario at unit-test scale. Five accounts each carry
a tiny embedding; transactions form a sparse graph whose 1-hop fan-out we
aggregate to compute a per-account "refresh feature".
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from caracaldb.graph import build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.graph.hnsw import HnswConfig, HnswIndex
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store
from caracaldb.storage.snapshot import create_snapshot
from caracaldb.storage.wal import Wal
from caracaldb.tx import TransactionManager


def _embeddings(n: int, dim: int = 8) -> np.ndarray:
    rng = np.random.default_rng(42)
    return rng.standard_normal((n, dim)).astype(np.float32)


@pytest.fixture
def case_b(tmp_path: Path):
    bundle = create_bundle(tmp_path / "fintech")
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/Account", local_name="Account")
    save_catalog(bundle, catalog)

    n = 5
    embeddings = _embeddings(n)
    accounts = open_node_store(
        bundle, class_iri="http://x/Account", local_name="Account", create=True
    )
    accounts.append(
        pa.record_batch(
            {
                "name": pa.array([f"acct_{i}" for i in range(n)]),
                "balance": pa.array([100.0 * (i + 1) for i in range(n)]),
            }
        )
    )

    tx_store = open_edge_store(
        bundle, property_iri="http://x/transferredTo", local_name="transferredTo", create=True
    )
    # 0 → 1 → 2 → 0 (cycle), 3 → 4
    tx_store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1, 2, 3], type=pa.uint64()),
                "dst": pa.array([1, 2, 0, 4], type=pa.uint64()),
                "amount": pa.array([10.0, 20.0, 30.0, 5.0]),
            }
        )
    )
    csr_path = bundle.path / "transferredTo.csr"
    build_csr(tx_store, num_vertices=n, out_path=csr_path, with_eids=True)

    # Build the HNSW index over account embeddings.
    cfg = HnswConfig(dim=8, max_elements=n)
    idx = HnswIndex(cfg)
    idx.add(np.arange(n, dtype=np.uint64), embeddings)

    wal = Wal(bundle.path / "wal")
    return {
        "bundle": bundle,
        "accounts": accounts,
        "tx_store": tx_store,
        "csr": CsrReader(csr_path),
        "embeddings": embeddings,
        "hnsw": idx,
        "wal": wal,
        "tx_manager": TransactionManager(wal),
        "snapshot": create_snapshot(bundle, "v1"),
    }
