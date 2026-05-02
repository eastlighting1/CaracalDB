"""End-to-end tests for ``AS_OF SNAPSHOT 'name'`` through the public query path.

These cover snapshot resolution through the single-class and multi-hop
pattern compilers, missing snapshot errors, and row-level visibility for
node and edge rows inserted after a named snapshot.
"""

from pathlib import Path

import pyarrow as pa
import pytest

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed_two_class_graph(tmp_path: Path) -> Path:
    bundle_path = tmp_path / "bio"
    bundle = create_bundle(bundle_path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/Gene", local_name="Gene")
    catalog.register_class(iri="http://x/Tissue", local_name="Tissue")
    catalog.register_property(iri="http://x/expressed_in", local_name="expressed_in")
    save_catalog(bundle, catalog)
    g = open_node_store(bundle, class_iri="http://x/Gene", local_name="Gene", create=True)
    g.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([0, 1], type=pa.uint64()),
                "symbol": pa.array(["TP53", "BRCA1"]),
            }
        )
    )
    t = open_node_store(bundle, class_iri="http://x/Tissue", local_name="Tissue", create=True)
    t.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([2, 3], type=pa.uint64()),
                "name": pa.array(["liver", "lung"]),
            }
        )
    )
    e = open_edge_store(
        bundle, property_iri="http://x/expressed_in", local_name="expressed_in", create=True
    )
    e.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1], type=pa.uint64()),
                "dst": pa.array([2, 3, 2], type=pa.uint64()),
            }
        )
    )
    return bundle_path


def test_as_of_named_snapshot_returns_rows(tmp_path: Path) -> None:
    """A valid named snapshot resolves and returns rows visible at that LSN."""
    db = cdb.connect(tmp_path / "demo.crcl")
    db.define_class("Gene")
    db.insert_nodes("Gene", [{"symbol": "TP53"}, {"symbol": "BRCA1"}])
    db.create_snapshot("v1")

    rows = db.sql("MATCH (g:Gene) AS_OF SNAPSHOT 'v1' RETURN g.symbol").rows()
    assert sorted(r["symbol"] for r in rows) == ["BRCA1", "TP53"]


def test_as_of_missing_snapshot_raises_cdb_8013(tmp_path: Path) -> None:
    """A missing snapshot name surfaces ``CDB-8013`` from the public query
    path, instead of being silently ignored."""
    db = cdb.connect(tmp_path / "demo.crcl")
    db.define_class("Gene")
    db.insert_nodes("Gene", [{"symbol": "TP53"}])

    with pytest.raises(CaracalError) as exc:
        db.sql("MATCH (g:Gene) AS_OF SNAPSHOT 'missing' RETURN g.symbol").rows()
    assert exc.value.code == "CDB-8013"
    assert "missing" in exc.value.message


def test_as_of_pattern_query_resolves_snapshot(tmp_path: Path) -> None:
    """Multi-hop pattern queries also resolve and accept ``AS_OF SNAPSHOT``."""
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    db.create_snapshot("v1")

    rows = db.sql(
        "MATCH (g:Gene)-[:expressed_in]->(t:Tissue) AS_OF SNAPSHOT 'v1' " "RETURN g.symbol, t.name"
    ).rows()
    pairs = sorted((r["symbol"], r["name"]) for r in rows)
    assert pairs == [("BRCA1", "liver"), ("TP53", "liver"), ("TP53", "lung")]


def test_as_of_pattern_query_missing_snapshot_raises(tmp_path: Path) -> None:
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")

    with pytest.raises(CaracalError) as exc:
        db.sql(
            "MATCH (g:Gene)-[:expressed_in]->(t:Tissue) AS_OF SNAPSHOT 'nope' " "RETURN g.symbol"
        ).rows()
    assert exc.value.code == "CDB-8013"


def test_database_snapshot_lifecycle(tmp_path: Path) -> None:
    """``Database`` exposes create/list/release for snapshots so users don't
    need to import from ``caracaldb.storage.snapshot``."""
    db = cdb.connect(tmp_path / "demo.crcl")
    db.define_class("Gene")
    db.insert_nodes("Gene", [{"symbol": "TP53"}])

    snap = db.create_snapshot("v1")
    assert snap.name == "v1"

    listing = db.list_snapshots()
    assert [s.name for s in listing] == ["v1"]

    assert db.release_snapshot("v1") is True
    assert db.list_snapshots() == []


def test_as_of_hides_rows_inserted_after_snapshot(tmp_path: Path) -> None:
    """Rows inserted after the snapshot should not be visible via AS_OF."""
    db = cdb.connect(tmp_path / "demo.crcl")
    db.define_class("Gene")
    db.insert_nodes("Gene", [{"symbol": "TP53"}])
    db.create_snapshot("v1")
    db.insert_nodes("Gene", [{"symbol": "BRCA1"}])

    rows = db.sql("MATCH (g:Gene) AS_OF SNAPSHOT 'v1' RETURN g.symbol").rows()
    assert sorted(r["symbol"] for r in rows) == ["TP53"]


def test_as_of_hides_edges_inserted_after_snapshot(tmp_path: Path) -> None:
    db = cdb.connect(tmp_path / "demo.crcl")
    db.insert_node_table(
        [
            {"node_id": "gene/TP53", "type": "Gene", "symbol": "TP53"},
            {"node_id": "tissue/liver", "type": "Tissue", "name": "liver"},
            {"node_id": "tissue/lung", "type": "Tissue", "name": "lung"},
        ]
    )
    db.insert_edge_table([{"src": "gene/TP53", "dst": "tissue/liver", "type": "expressed_in"}])
    db.create_snapshot("v1")
    db.insert_edge_table([{"src": "gene/TP53", "dst": "tissue/lung", "type": "expressed_in"}])

    rows = db.sql(
        "MATCH (g:Gene)-[:expressed_in]->(t:Tissue) AS_OF SNAPSHOT 'v1' " "RETURN g.symbol, t.name"
    ).rows()
    assert rows == [{"symbol": "TP53", "name": "liver"}]


def test_as_of_visibility_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "demo.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene")
        db.insert_nodes("Gene", [{"symbol": "TP53"}])
        db.create_snapshot("v1")

    with cdb.connect(path) as db:
        db.insert_nodes("Gene", [{"symbol": "BRCA1"}])
        rows = db.sql("MATCH (g:Gene) AS_OF SNAPSHOT 'v1' RETURN g.symbol").rows()

    assert rows == [{"symbol": "TP53"}]


@pytest.mark.xfail(
    reason="Catalog definitions are not yet versioned by snapshot LSN.",
    strict=True,
)
def test_as_of_hides_class_defined_after_snapshot(tmp_path: Path) -> None:
    """A class defined after a snapshot should not be queryable via AS_OF.

    Snapshot-aware catalog visibility is still separate from node/edge row
    visibility because the catalog is mutated in place today.
    """
    db = cdb.connect(tmp_path / "demo.crcl")
    db.define_class("Gene")
    db.insert_nodes("Gene", [{"symbol": "TP53"}])
    db.create_snapshot("v1")
    db.define_class("Tissue")
    db.insert_nodes("Tissue", [{"name": "liver"}])

    with pytest.raises(CaracalError):
        db.sql("MATCH (t:Tissue) AS_OF SNAPSHOT 'v1' RETURN t.name").rows()
