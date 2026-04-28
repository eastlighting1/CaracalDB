"""Case C fixture: tiny User × Item bipartite graph + tower embeddings.

Mirrors 03 §C's e-commerce recommendation case at unit-test scale: 4 users,
4 items, ``viewed`` and ``purchased`` edges, plus 8-dim user/item embeddings
that allow a two-tower similarity check.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from caracaldb.graph import build_csc, build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.graph.hnsw import HnswConfig, HnswIndex
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


@pytest.fixture
def case_c(tmp_path: Path):
    bundle = create_bundle(tmp_path / "ec")
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/User", local_name="User")
    catalog.register_class(iri="http://x/Item", local_name="Item")
    save_catalog(bundle, catalog)

    rng = np.random.default_rng(42)
    user_emb = rng.standard_normal((4, 8)).astype(np.float32)
    item_emb = rng.standard_normal((4, 8)).astype(np.float32)
    # Embed the "obvious match": user 0 and item 0 share the same vector.
    item_emb[0] = user_emb[0]

    users = open_node_store(bundle, class_iri="http://x/User", local_name="User", create=True)
    users.append(pa.record_batch({"name": pa.array([f"u{i}" for i in range(4)])}))
    items = open_node_store(bundle, class_iri="http://x/Item", local_name="Item", create=True)
    items.append(pa.record_batch({"name": pa.array([f"i{i}" for i in range(4)])}))

    viewed = open_edge_store(
        bundle, property_iri="http://x/viewed", local_name="viewed", create=True
    )
    viewed.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2, 3], type=pa.uint64()),
                "dst": pa.array([1, 2, 0, 1, 3], type=pa.uint64()),
            }
        )
    )
    purchased = open_edge_store(
        bundle, property_iri="http://x/purchased", local_name="purchased", create=True
    )
    purchased.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1, 2], type=pa.uint64()),
                "dst": pa.array([0, 1, 2], type=pa.uint64()),
            }
        )
    )

    viewed_csr = bundle.path / "viewed.csr"
    viewed_csc = bundle.path / "viewed.csc"
    build_csr(viewed, num_vertices=4, out_path=viewed_csr, with_eids=True)
    build_csc(viewed, num_vertices=4, out_path=viewed_csc, with_eids=True)

    cfg = HnswConfig(dim=8, max_elements=4)
    user_idx = HnswIndex(cfg)
    user_idx.add(np.arange(4, dtype=np.uint64), user_emb)
    item_idx = HnswIndex(cfg)
    item_idx.add(np.arange(4, dtype=np.uint64), item_emb)

    return {
        "bundle": bundle,
        "users": users,
        "items": items,
        "viewed_csr": CsrReader(viewed_csr),
        "viewed_csc": CsrReader(viewed_csc),
        "user_emb": user_emb,
        "item_emb": item_emb,
        "user_hnsw": user_idx,
        "item_hnsw": item_idx,
    }
