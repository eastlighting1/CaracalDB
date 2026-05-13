from __future__ import annotations

import json

import pyarrow as pa
import pytest

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import parse_tuft
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.planner import lower_node_scan, lower_project, lower_topk
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store


def test_python_planner_lowers_stable_rust_node_scan_json() -> None:
    plan = lower_node_scan(
        class_iri="http://example.org/Gene",
        local_name="Gene",
        snapshot_lsn=7,
    )
    assert plan.to_json() == (
        '{"class_iri":"http://example.org/Gene","local_name":"Gene",'
        '"op":"node_scan","snapshot_lsn":7}'
    )


def test_dual_planner_shape_for_topk_is_stable() -> None:
    plan = lower_topk(
        lower_node_scan(class_iri="http://example.org/Gene", local_name="Gene"),
        order_by="score",
        skip=2,
        limit=5,
    )
    payload = json.loads(plan.to_json())
    assert payload["op"] == "top_k"
    assert payload["skip"] == 2
    assert payload["limit"] == 5
    assert payload["input"]["op"] == "node_scan"


def test_dual_planner_compare_python_and_rust_generated_plan() -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    python_plan = lower_project(
        lower_node_scan(class_iri="http://example.org/Gene", local_name="Gene"),
        ["symbol"],
    )
    rust_plan = rust.lower_tuft_plan("MATCH (g:Gene) RETURN g.symbol")
    assert json.loads(rust_plan) == json.loads(python_plan.to_json())


def test_rust_parser_diagnostic_matches_python_code_and_span() -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    text = "MATCH (g:Gene) RETURN"
    with pytest.raises(CaracalError) as exc_info:
        parse_tuft(text)
    python_diag = exc_info.value.diagnostic()
    rust_diag = rust.tuft_diagnostic(text)

    assert rust_diag["code"] == python_diag.code
    assert rust_diag["span_start"] is None
    assert rust_diag["span_end"] is None
    assert python_diag.span is None


def test_dual_engine_node_scan_result_matches_python_reference(tmp_path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "dual")
    store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2"], type=pa.string()),
                "score": pa.array([2, 1], type=pa.uint64()),
            }
        )
    )

    python_table = store.to_table()
    streams = rust.scan_node_store(str(bundle.path), "http://example.org/Gene", "Gene")
    rust_table = pa.concat_tables(
        [pa.ipc.open_stream(pa.BufferReader(stream)).read_all() for stream in streams]
    ).select(python_table.schema.names)

    assert rust_table.to_pylist() == python_table.to_pylist()


def test_rust_execution_plan_consumes_lowered_topk_and_matches_reference(tmp_path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "dual-topk")
    store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["A", "B", "C", "D"], type=pa.string()),
                "score": pa.array([30, 10, 20, None], type=pa.uint64()),
            }
        )
    )

    plan = lower_topk(
        lower_node_scan(class_iri="http://example.org/Gene", local_name="Gene"),
        order_by="score",
        skip=1,
        limit=2,
    )
    rust_table = _rust_plan_table(rust, bundle.path, plan.to_json()).select(
        store.to_table().schema.names
    )

    assert rust_table.to_pylist() == [
        {"nid": 2, "symbol": "C", "score": 20},
        {"nid": 0, "symbol": "A", "score": 30},
    ]


def test_rust_execution_plan_filter_matches_reference_null_and_numeric_behavior(tmp_path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "dual-filter")
    store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["A", "B", "C"], type=pa.string()),
                "score": pa.array([10, None, 10], type=pa.uint64()),
            }
        )
    )

    plan = {
        "op": "filter_eq_u64",
        "column": "score",
        "value": 10,
        "input": {
            "op": "node_scan",
            "class_iri": "http://example.org/Gene",
            "local_name": "Gene",
            "snapshot_lsn": None,
        },
    }
    rust_table = _rust_plan_table(rust, bundle.path, json.dumps(plan)).select(
        store.to_table().schema.names
    )

    assert rust_table.to_pylist() == [
        {"nid": 0, "symbol": "A", "score": 10},
        {"nid": 2, "symbol": "C", "score": 10},
    ]


def test_dual_engine_query_result_matches_python_reference(tmp_path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "dual-query")
    catalog = Catalog.empty()
    catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
    save_catalog(bundle, catalog)
    store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2"], type=pa.string()),
                "score": pa.array([2, 1], type=pa.uint64()),
            }
        )
    )

    with cdb.connect(bundle.path, format="bundle") as db:
        python_table = db.sql("MATCH (g:Gene) RETURN g.symbol").arrow()

    plan = {
        "op": "project",
        "columns": ["symbol"],
        "input": {
            "op": "node_scan",
            "class_iri": "http://example.org/Gene",
            "local_name": "Gene",
        },
    }
    rust_table = _rust_plan_table(rust, bundle.path, json.dumps(plan))

    assert rust_table.to_pylist() == python_table.to_pylist()


def _rust_plan_table(rust, bundle_path, plan_json: str) -> pa.Table:
    streams = rust.execute_plan(str(bundle_path), plan_json)
    assert streams
    return pa.concat_tables(
        [pa.ipc.open_stream(pa.BufferReader(stream)).read_all() for stream in streams]
    )
