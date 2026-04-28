"""Shared fixture: build a small Gene/Tissue graph and a CSR for `interactsWith`.

The fixture mirrors 03 §A's biomedical case at a small enough scale to stay
deterministic. The schema is intentionally narrow so the goldens can pin
exact values rather than ranges.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.graph import build_csc, build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


@pytest.fixture
def case_a_bundle(tmp_path: Path):
    bundle = create_bundle(tmp_path / "bio")
    catalog = Catalog.empty()
    catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
    catalog.register_class(iri="http://example.org/Tissue", local_name="Tissue")
    save_catalog(bundle, catalog)

    genes = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    genes.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2", "BRCA1", "EGFR", "KRAS"]),
                "chromosome": pa.array(["17", "12", "17", "7", "12"]),
            }
        )
    )

    tissues = open_node_store(
        bundle, class_iri="http://example.org/Tissue", local_name="Tissue", create=True
    )
    tissues.append(pa.record_batch({"name": pa.array(["liver", "lung", "brain"])}))

    iw = open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        create=True,
    )
    iw.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2, 3, 4], type=pa.uint64()),
                "dst": pa.array([1, 2, 3, 0, 4, 0], type=pa.uint64()),
            }
        )
    )

    expressed = open_edge_store(
        bundle,
        property_iri="http://example.org/expressedIn",
        local_name="expressedIn",
        create=True,
    )
    expressed.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2, 3, 4], type=pa.uint64()),
                "dst": pa.array([0, 1, 0, 0, 1, 2], type=pa.uint64()),
            }
        )
    )

    fwd_iw = bundle.path / "interactsWith.csr"
    rev_iw = bundle.path / "interactsWith.csc"
    fwd_ex = bundle.path / "expressedIn.csr"
    build_csr(iw, num_vertices=5, out_path=fwd_iw, with_eids=True)
    build_csc(iw, num_vertices=5, out_path=rev_iw, with_eids=True)
    build_csr(expressed, num_vertices=5, out_path=fwd_ex, with_eids=True)

    return {
        "bundle_path": bundle.path,
        "iw_csr": CsrReader(fwd_iw),
        "iw_csc": CsrReader(rev_iw),
        "expressed_csr": CsrReader(fwd_ex),
    }
