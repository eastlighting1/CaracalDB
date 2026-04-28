from pathlib import Path

import pyarrow as pa

from caracaldb.exec.expr import compile_expr
from caracaldb.exec.operators import FilterOperator, NodeScanOperator, ProjectOperator
from caracaldb.observability import (
    ProfileReport,
    Tracer,
    explain_logical,
    get_tracer,
    profile_pipeline,
    render_explain,
    set_tracer,
)
from caracaldb.plan.cost import CatalogStats
from caracaldb.plan.logical import LLimit, LNodeScan, LProject, LSelection
from caracaldb.plan.pattern_compiler import LExpand, LJoin
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store


def _logical_plan() -> LLimit:
    scan = LNodeScan(class_iri="Gene", local_name="Gene", alias="g")
    sel = LSelection(child=scan, predicate=("eq", ("col", "chromosome"), ("lit", "17")))
    proj = LProject(child=sel, projections=((("col", "symbol"), "symbol"),))
    return LLimit(child=proj, limit=10)


def test_explain_logical_renders_indented_tree() -> None:
    plan = _logical_plan()
    tree = explain_logical(plan, CatalogStats(class_rows={"Gene": 30_000}))
    text = render_explain(tree)
    assert "Limit" in text and "NodeScan" in text
    assert "rows≈" in text  # cardinality annotation present


def test_explain_handles_pattern_compiler_nodes() -> None:
    expand = LExpand(
        child=LNodeScan(class_iri="Gene", local_name="Gene", alias="g"),
        property_iri="interactsWith",
        direction="out",
        src_alias="g.nid",
        dst_alias="t.nid",
        edge_alias=None,
    )
    join = LJoin(
        left=expand,
        right=LNodeScan(class_iri="Tissue", local_name="Tissue", alias="t"),
        left_key="t.nid",
        right_key="nid",
    )
    text = render_explain(explain_logical(join))
    assert "Join" in text and "Expand" in text


def test_profile_pipeline_collects_per_operator_metrics(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(bundle, class_iri="http://x/Gene", local_name="Gene", create=True)
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2", "BRCA1", "EGFR"]),
                "chromosome": pa.array(["17", "12", "17", "7"]),
            }
        )
    )
    pred = compile_expr(("eq", ("col", "chromosome"), ("lit", "17")))
    op = ProjectOperator(
        FilterOperator(NodeScanOperator(store), pred),
        [(compile_expr(("col", "symbol")), "symbol")],
    )
    iterator, report = profile_pipeline(op)
    out = pa.Table.from_batches(list(iterator))
    assert isinstance(report, ProfileReport)
    assert out.num_rows == 2
    assert report.wall_ms > 0
    by_name = {p.name: p for p in report.operators}
    assert {"Project", "Filter", "NodeScan"} <= set(by_name.keys())
    assert by_name["Project"].rows == 2
    text = report.to_text()
    assert "Project" in text and "ms" in text


def test_tracer_records_spans_and_swaps() -> None:
    fresh = Tracer()
    previous = set_tracer(fresh)
    try:
        with get_tracer().span("plan.compile", query="MATCH (g:Gene)") as span:
            span.set_attribute("alias", "g")
        with get_tracer().span("exec.run"):
            pass
        records = get_tracer().spans
        assert [r.name for r in records] == ["plan.compile", "exec.run"]
        assert records[0].attributes == {"query": "MATCH (g:Gene)", "alias": "g"}
        assert all(r.duration_ms >= 0 for r in records)
    finally:
        set_tracer(previous)


def test_disabled_tracer_does_not_record() -> None:
    silent = Tracer(enabled=False)
    previous = set_tracer(silent)
    try:
        with get_tracer().span("noop"):
            pass
        assert get_tracer().spans == []
    finally:
        set_tracer(previous)
