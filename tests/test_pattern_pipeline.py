"""End-to-end tests for the multi-hop MATCH pipeline (CDB-045 wiring).

These tests cover the M2 carry-over recorded in docs/milestones/M2-gate.md and
M5-gate.md "Carry-overs into v0.2.0": the pattern compiler producing logical
plans that are now wired through ``Connection.sql`` into a real
``NodeScan → Expand → HashJoin`` pipeline.
"""

from pathlib import Path

import pyarrow as pa
import pytest

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import Catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed_two_class_graph(tmp_path: Path) -> Path:
    """Build a graph with two classes (Gene, Tissue) and an `expressed_in`
    relation, using the global-gid layout that ``insert_edge_table`` produces.
    """
    bundle_path = tmp_path / "bio"
    bundle = create_bundle(bundle_path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/Gene", local_name="Gene")
    catalog.register_class(iri="http://x/Tissue", local_name="Tissue")
    catalog.register_property(iri="http://x/expressed_in", local_name="expressed_in")
    from caracaldb.onto.catalog import save_catalog

    save_catalog(bundle, catalog)

    gene_store = open_node_store(
        bundle, class_iri="http://x/Gene", local_name="Gene", create=True
    )
    # gids 0..2 for genes
    gene_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([0, 1, 2], type=pa.uint64()),
                "symbol": pa.array(["TP53", "BRCA1", "EGFR"]),
            }
        )
    )

    tissue_store = open_node_store(
        bundle, class_iri="http://x/Tissue", local_name="Tissue", create=True
    )
    # gids 3..4 for tissues
    tissue_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([3, 4], type=pa.uint64()),
                "name": pa.array(["liver", "lung"]),
            }
        )
    )

    edges = open_edge_store(
        bundle,
        property_iri="http://x/expressed_in",
        local_name="expressed_in",
        create=True,
    )
    # TP53 -> liver, TP53 -> lung, BRCA1 -> liver
    edges.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1], type=pa.uint64()),
                "dst": pa.array([3, 4, 3], type=pa.uint64()),
            }
        )
    )
    return bundle_path


def test_one_hop_pattern_returns_joined_columns(tmp_path: Path) -> None:
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        "MATCH (g:Gene)-[:expressed_in]->(t:Tissue) RETURN g.symbol, t.name"
    ).rows()
    pairs = sorted((r["symbol"], r["name"]) for r in rows)
    assert pairs == [("BRCA1", "liver"), ("TP53", "liver"), ("TP53", "lung")]


def test_one_hop_pattern_with_where_filter(tmp_path: Path) -> None:
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        "MATCH (g:Gene)-[:expressed_in]->(t:Tissue) "
        "WHERE t.name = 'liver' RETURN g.symbol"
    ).rows()
    symbols = sorted(r["symbol"] for r in rows)
    assert symbols == ["BRCA1", "TP53"]


def test_one_hop_pattern_with_limit(tmp_path: Path) -> None:
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        "MATCH (g:Gene)-[:expressed_in]->(t:Tissue) RETURN g.symbol, t.name LIMIT 1"
    ).rows()
    assert len(rows) == 1


def test_unknown_relation_raises(tmp_path: Path) -> None:
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    with pytest.raises(CaracalError) as exc:
        db.sql(
            "MATCH (g:Gene)-[:no_such_rel]->(t:Tissue) RETURN g.symbol"
        ).rows()
    assert exc.value.code == "CDB-6023"


def test_two_hop_pattern_returns_joined_columns(tmp_path: Path) -> None:
    bundle_path = tmp_path / "two-hop"
    bundle = create_bundle(bundle_path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/A", local_name="A")
    catalog.register_class(iri="http://x/B", local_name="B")
    catalog.register_class(iri="http://x/C", local_name="C")
    catalog.register_property(iri="http://x/r1", local_name="r1")
    catalog.register_property(iri="http://x/r2", local_name="r2")
    from caracaldb.onto.catalog import save_catalog

    save_catalog(bundle, catalog)

    a_store = open_node_store(bundle, class_iri="http://x/A", local_name="A", create=True)
    a_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([0, 1], type=pa.uint64()),
                "label": pa.array(["a0", "a1"]),
            }
        )
    )
    b_store = open_node_store(bundle, class_iri="http://x/B", local_name="B", create=True)
    b_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([2, 3], type=pa.uint64()),
                "label": pa.array(["b0", "b1"]),
            }
        )
    )
    c_store = open_node_store(bundle, class_iri="http://x/C", local_name="C", create=True)
    c_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([4, 5], type=pa.uint64()),
                "label": pa.array(["c0", "c1"]),
            }
        )
    )
    r1 = open_edge_store(
        bundle, property_iri="http://x/r1", local_name="r1", create=True
    )
    # a0 -> b0, a1 -> b1
    r1.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1], type=pa.uint64()),
                "dst": pa.array([2, 3], type=pa.uint64()),
            }
        )
    )
    r2 = open_edge_store(
        bundle, property_iri="http://x/r2", local_name="r2", create=True
    )
    # b0 -> c0, b1 -> c1, b1 -> c0
    r2.append(
        pa.record_batch(
            {
                "src": pa.array([2, 3, 3], type=pa.uint64()),
                "dst": pa.array([4, 5, 4], type=pa.uint64()),
            }
        )
    )

    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        "MATCH (a:A)-[:r1]->(b:B)-[:r2]->(c:C) "
        "RETURN a.label, b.label, c.label"
    ).rows()
    # Note: RETURN columns are aliased by their tail field name 'label' three
    # times — pyarrow accepts duplicate column names but rows() collapses to
    # the last value, so use the test below as the canonical 2-hop assertion.
    assert len(rows) == 3


def test_two_hop_pattern_distinguishes_columns_via_alias(tmp_path: Path) -> None:
    """Use AS aliases so each column lands under a distinct name."""
    bundle_path = tmp_path / "two-hop-alias"
    bundle = create_bundle(bundle_path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/A", local_name="A")
    catalog.register_class(iri="http://x/B", local_name="B")
    catalog.register_class(iri="http://x/C", local_name="C")
    catalog.register_property(iri="http://x/r1", local_name="r1")
    catalog.register_property(iri="http://x/r2", local_name="r2")
    from caracaldb.onto.catalog import save_catalog

    save_catalog(bundle, catalog)

    a_store = open_node_store(bundle, class_iri="http://x/A", local_name="A", create=True)
    a_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([0], type=pa.uint64()),
                "label": pa.array(["a0"]),
            }
        )
    )
    b_store = open_node_store(bundle, class_iri="http://x/B", local_name="B", create=True)
    b_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([1], type=pa.uint64()),
                "label": pa.array(["b0"]),
            }
        )
    )
    c_store = open_node_store(bundle, class_iri="http://x/C", local_name="C", create=True)
    c_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([2], type=pa.uint64()),
                "label": pa.array(["c0"]),
            }
        )
    )
    r1 = open_edge_store(
        bundle, property_iri="http://x/r1", local_name="r1", create=True
    )
    r1.append(
        pa.record_batch(
            {
                "src": pa.array([0], type=pa.uint64()),
                "dst": pa.array([1], type=pa.uint64()),
            }
        )
    )
    r2 = open_edge_store(
        bundle, property_iri="http://x/r2", local_name="r2", create=True
    )
    r2.append(
        pa.record_batch(
            {
                "src": pa.array([1], type=pa.uint64()),
                "dst": pa.array([2], type=pa.uint64()),
            }
        )
    )

    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        "MATCH (a:A)-[:r1]->(b:B)-[:r2]->(c:C) "
        "RETURN a.label AS al, b.label AS bl, c.label AS cl"
    ).rows()
    assert rows == [{"al": "a0", "bl": "b0", "cl": "c0"}]


def test_rel_type_union_merges_both_relations(tmp_path: Path) -> None:
    """``-[:r1|r2]->`` should fan out to both edge stores and union the pairs."""
    bundle_path = tmp_path / "rel-union"
    bundle = create_bundle(bundle_path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/A", local_name="A")
    catalog.register_class(iri="http://x/B", local_name="B")
    catalog.register_property(iri="http://x/r1", local_name="r1")
    catalog.register_property(iri="http://x/r2", local_name="r2")
    from caracaldb.onto.catalog import save_catalog

    save_catalog(bundle, catalog)

    a_store = open_node_store(bundle, class_iri="http://x/A", local_name="A", create=True)
    a_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([0, 1], type=pa.uint64()),
                "label": pa.array(["a0", "a1"]),
            }
        )
    )
    b_store = open_node_store(bundle, class_iri="http://x/B", local_name="B", create=True)
    b_store.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([2, 3], type=pa.uint64()),
                "label": pa.array(["b0", "b1"]),
            }
        )
    )
    r1 = open_edge_store(bundle, property_iri="http://x/r1", local_name="r1", create=True)
    # r1: a0->b0
    r1.append(
        pa.record_batch(
            {
                "src": pa.array([0], type=pa.uint64()),
                "dst": pa.array([2], type=pa.uint64()),
            }
        )
    )
    r2 = open_edge_store(bundle, property_iri="http://x/r2", local_name="r2", create=True)
    # r2: a0->b1, a1->b0 (b0=gid 2, b1=gid 3)
    r2.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1], type=pa.uint64()),
                "dst": pa.array([3, 2], type=pa.uint64()),
            }
        )
    )

    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        "MATCH (a:A)-[:r1|r2]->(b:B) RETURN a.label AS al, b.label AS bl"
    ).rows()
    pairs = sorted((r["al"], r["bl"]) for r in rows)
    assert pairs == [("a0", "b0"), ("a0", "b1"), ("a1", "b0")]


def test_rel_type_union_unknown_relation_raises(tmp_path: Path) -> None:
    """Every alternative in a union must exist; missing one raises CDB-6023."""
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    with pytest.raises(CaracalError) as exc:
        db.sql(
            "MATCH (g:Gene)-[:expressed_in|nonexistent]->(t:Tissue) RETURN g.symbol"
        ).rows()
    assert exc.value.code == "CDB-6023"


def test_degree_builtin_returns_per_node_out_degree(tmp_path: Path) -> None:
    """`degree(alias, "rel")` returns the forward-CSR out-degree per node."""
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql(
        'MATCH (g:Gene)-[:expressed_in]->(t:Tissue) '
        'RETURN g.symbol, degree(g, "expressed_in") AS d'
    ).rows()
    # TP53 expressed in liver+lung (deg 2), BRCA1 in liver (deg 1)
    by_sym: dict[str, int] = {}
    for row in rows:
        by_sym[row["symbol"]] = int(row["d"])
    assert by_sym == {"TP53": 2, "BRCA1": 1}


def test_degree_builtin_unknown_relation_raises(tmp_path: Path) -> None:
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    with pytest.raises(CaracalError) as exc:
        db.sql(
            'MATCH (g:Gene)-[:expressed_in]->(t:Tissue) '
            'RETURN g.symbol, degree(g, "no_such_rel") AS d'
        ).rows()
    assert exc.value.code == "CDB-6023"


def test_unsupported_graph_builtin_raises(tmp_path: Path) -> None:
    """`neighbors`/`shortest_path`/`k_hop` remain a documented carry-over."""
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    with pytest.raises(CaracalError) as exc:
        db.sql(
            'MATCH (g:Gene)-[:expressed_in]->(t:Tissue) '
            'RETURN neighbors(g, "expressed_in") AS ns'
        ).rows()
    assert exc.value.code == "CDB-6020"


def test_single_class_match_still_uses_legacy_path(tmp_path: Path) -> None:
    """The single-(alias:Class) shortcut must keep working unchanged."""
    bundle_path = _seed_two_class_graph(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    rows = db.sql("MATCH (g:Gene) RETURN g.symbol").rows()
    assert sorted(r["symbol"] for r in rows) == ["BRCA1", "EGFR", "TP53"]
