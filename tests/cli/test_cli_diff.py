"""Tests for ``caracal diff`` — the embedded-friendly governance command.

The diff is exercised against three scenarios that cover the governance
use cases the CLI is meant to support:

1. Two bundles with no differences should exit 0 and report nothing.
2. A bundle with an added node and an added edge should exit 1 with the
   correct counts on both sides.
3. A bundle with a renamed class (added + removed) should surface both
   catalog deltas.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from caracaldb.cli.app import cmd_diff
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.diff import diff_bundles
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed(path: Path, *, gene_gids: list[int], edges: list[tuple[int, int]]) -> Path:
    bundle = create_bundle(path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://x/Gene", local_name="Gene")
    catalog.register_class(iri="http://x/Tissue", local_name="Tissue")
    catalog.register_property(iri="http://x/expressed_in", local_name="expressed_in")
    save_catalog(bundle, catalog)

    gene = open_node_store(
        bundle, class_iri="http://x/Gene", local_name="Gene", create=True
    )
    gene.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array(gene_gids, type=pa.uint64()),
                "symbol": pa.array([f"G{i}" for i in gene_gids]),
            }
        )
    )
    tissue = open_node_store(
        bundle, class_iri="http://x/Tissue", local_name="Tissue", create=True
    )
    tissue.append(
        pa.record_batch(
            {
                "_cdb_gid": pa.array([100, 101], type=pa.uint64()),
                "name": pa.array(["liver", "lung"]),
            }
        )
    )
    if edges:
        store = open_edge_store(
            bundle,
            property_iri="http://x/expressed_in",
            local_name="expressed_in",
            create=True,
        )
        store.append(
            pa.record_batch(
                {
                    "src": pa.array([s for s, _ in edges], type=pa.uint64()),
                    "dst": pa.array([d for _, d in edges], type=pa.uint64()),
                }
            )
        )
    return bundle.path


def test_diff_identical_bundles_returns_zero(tmp_path: Path) -> None:
    a = _seed(tmp_path / "a", gene_gids=[1, 2], edges=[(1, 100)])
    b = _seed(tmp_path / "b", gene_gids=[1, 2], edges=[(1, 100)])
    diff = diff_bundles(a, b)
    assert diff.is_empty()
    assert cmd_diff(a, b, json_out=False) == 0


def test_diff_detects_added_node_and_edge(tmp_path: Path) -> None:
    a = _seed(tmp_path / "a", gene_gids=[1, 2], edges=[(1, 100)])
    b = _seed(tmp_path / "b", gene_gids=[1, 2, 3], edges=[(1, 100), (3, 101)])
    diff = diff_bundles(a, b)
    assert not diff.is_empty()
    gene_change = next(c for c in diff.class_changes if c.local_name == "Gene")
    assert gene_change.only_in_a == 0
    assert gene_change.only_in_b == 1
    assert gene_change.in_both == 2
    rel = next(r for r in diff.relation_changes if r.local_name == "expressed_in")
    assert rel.only_in_a == 0
    assert rel.only_in_b == 1
    assert cmd_diff(a, b, json_out=False) == 1


def test_diff_json_output_is_valid(tmp_path: Path, capsys) -> None:
    a = _seed(tmp_path / "a", gene_gids=[1, 2], edges=[(1, 100)])
    b = _seed(tmp_path / "b", gene_gids=[1, 2, 3], edges=[(1, 100)])
    rc = cmd_diff(a, b, json_out=True)
    assert rc == 1
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["a"].endswith("a.crcl")
    assert payload["b"].endswith("b.crcl")
    gene = next(c for c in payload["class_changes"] if c["local_name"] == "Gene")
    assert gene["only_in_b"] == 1


def test_diff_detects_class_rename(tmp_path: Path) -> None:
    """A renamed class shows up as one removed + one added catalog entry."""
    bundle_a = create_bundle(tmp_path / "a")
    cat_a = Catalog.empty()
    cat_a.register_class(iri="http://x/Old", local_name="Old")
    save_catalog(bundle_a, cat_a)

    bundle_b = create_bundle(tmp_path / "b")
    cat_b = Catalog.empty()
    cat_b.register_class(iri="http://x/New", local_name="New")
    save_catalog(bundle_b, cat_b)

    diff = diff_bundles(bundle_a.path, bundle_b.path)
    assert "Old" in diff.classes_removed
    assert "New" in diff.classes_added
