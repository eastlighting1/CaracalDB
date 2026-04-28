from pathlib import Path

import pyarrow as pa

from caracaldb.exec.expr import compile_expr
from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import FilterOperator, NodeScanOperator, ProjectOperator
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store


def _seed_store(tmp_path: Path) -> NodeScanOperator:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2", "BRCA1", "EGFR"]),
                "chromosome": pa.array(["17", "12", "17", "7"]),
            }
        )
    )
    return NodeScanOperator(store)


def test_node_scan_streams_all_rows(tmp_path: Path) -> None:
    op = _seed_store(tmp_path)
    batches = list(run_pipeline(op))
    assert sum(b.num_rows for b in batches) == 4
    syms = [v for b in batches for v in b.column("symbol").to_pylist()]
    assert syms == ["TP53", "MDM2", "BRCA1", "EGFR"]


def test_node_scan_with_pushed_columns(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(bundle, class_iri="http://x/Gene", local_name="Gene", create=True)
    store.append(
        pa.record_batch(
            {"symbol": pa.array(["TP53", "MDM2"]), "chromosome": pa.array(["17", "12"])}
        )
    )
    op = NodeScanOperator(store, columns=["symbol"])
    out = list(run_pipeline(op))[0]
    assert out.schema.names == ["symbol"]


def test_filter_operator_drops_rows(tmp_path: Path) -> None:
    scan = _seed_store(tmp_path)
    pred = compile_expr(("eq", ("col", "chromosome"), ("lit", "17")))
    op = FilterOperator(scan, pred)
    batches = list(run_pipeline(op))
    rows = [v for b in batches for v in b.column("symbol").to_pylist()]
    assert sorted(rows) == ["BRCA1", "TP53"]


def test_project_operator_aliases_columns(tmp_path: Path) -> None:
    scan = _seed_store(tmp_path)
    op = ProjectOperator(scan, [(compile_expr(("col", "symbol")), "gene_symbol")])
    out = list(run_pipeline(op))
    assert out[0].schema.names == ["gene_symbol"]
    assert out[0].column("gene_symbol").to_pylist() == ["TP53", "MDM2", "BRCA1", "EGFR"]


def test_node_scan_predicate_pushdown_short_path(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(bundle, class_iri="http://x/Gene", local_name="Gene", create=True)
    store.append(
        pa.record_batch(
            {"symbol": pa.array(["A", "B", "C"]), "chromosome": pa.array(["17", "12", "17"])}
        )
    )
    pred = compile_expr(("eq", ("col", "chromosome"), ("lit", "17")))
    op = NodeScanOperator(store, predicate=pred)
    out = list(run_pipeline(op))
    assert out[0].column("symbol").to_pylist() == ["A", "C"]
