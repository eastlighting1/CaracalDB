"""Case A goldens: 03 §A TP53 patterns realised through M2 operators.

The MVP planner's pattern-compilation hook is still maturing (CDB-045 is
logical-only); the goldens here exercise the *physical* operator stack —
NodeScan + Filter + Project + Expand + HashJoin + HashAggregate + TopK —
through hand-wired pipelines that mirror what the planner will produce.
That keeps the goldens stable while CDB-052's gate uses the same operator
chain end-to-end.
"""

from __future__ import annotations

import pyarrow as pa

from caracaldb.exec.expr import compile_expr
from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import (
    ExpandOperator,
    FilterOperator,
    HashAggregateOperator,
    HashJoinOperator,
    NodeScanOperator,
    ProjectOperator,
    TopKOperator,
    VarPathOperator,
)
from caracaldb.storage import open_bundle
from caracaldb.storage.node_store import open_node_store


def _gene_scan(bundle):
    store = open_node_store(bundle, class_iri="http://example.org/Gene", local_name="Gene")
    return NodeScanOperator(store)


def test_q1_genes_on_chromosome_17(case_a_bundle) -> None:
    bundle = open_bundle(case_a_bundle["bundle_path"])
    pred = compile_expr(("eq", ("col", "chromosome"), ("lit", "17")))
    op = ProjectOperator(
        FilterOperator(_gene_scan(bundle), pred),
        [(compile_expr(("col", "symbol")), "symbol")],
    )
    table = pa.Table.from_batches(list(run_pipeline(op)))
    assert sorted(table["symbol"].to_pylist()) == ["BRCA1", "TP53"]


def test_q2_tp53_one_hop_neighbours_via_expand(case_a_bundle) -> None:
    bundle = open_bundle(case_a_bundle["bundle_path"])
    # Filter to TP53 first (nid=0).
    pred = compile_expr(("eq", ("col", "symbol"), ("lit", "TP53")))
    seeds = FilterOperator(_gene_scan(bundle), pred)
    expand = ExpandOperator(seeds, forward=case_a_bundle["iw_csr"], direction="out")
    out = pa.Table.from_batches(list(run_pipeline(expand)))
    assert sorted(out["dst"].to_pylist()) == [1, 2]


def test_q3_two_hop_var_path_from_tp53(case_a_bundle) -> None:
    bundle = open_bundle(case_a_bundle["bundle_path"])
    seeds = FilterOperator(
        _gene_scan(bundle), compile_expr(("eq", ("col", "symbol"), ("lit", "TP53")))
    )
    walker = VarPathOperator(seeds, forward=case_a_bundle["iw_csr"], hop_min=1, hop_max=2)
    out = pa.Table.from_batches(list(run_pipeline(walker)))
    reached = set(out["dst"].to_pylist())
    # TP53 (0) → {1, 2}; 1→3, 2→0. Within 2 hops TP53 reaches {1, 2, 3, 0}.
    assert {1, 2, 3} <= reached


def test_q4_neighbour_count_per_seed(case_a_bundle) -> None:
    bundle = open_bundle(case_a_bundle["bundle_path"])
    expand = ExpandOperator(_gene_scan(bundle), forward=case_a_bundle["iw_csr"], direction="out")
    agg = HashAggregateOperator(expand, group_keys=["src"], aggregates=[(None, "count_star", "n")])
    out = pa.Table.from_batches(list(run_pipeline(agg)))
    counts = dict(zip(out["src"].to_pylist(), out["n"].to_pylist(), strict=False))
    assert counts == {0: 2, 1: 1, 2: 1, 3: 1, 4: 1}


def test_q5_top3_genes_by_out_degree(case_a_bundle) -> None:
    bundle = open_bundle(case_a_bundle["bundle_path"])
    expand = ExpandOperator(_gene_scan(bundle), forward=case_a_bundle["iw_csr"], direction="out")
    agg = HashAggregateOperator(
        expand, group_keys=["src"], aggregates=[(None, "count_star", "deg")]
    )
    top = TopKOperator(agg, keys=[("deg", True), ("src", False)], limit=3)
    out = pa.Table.from_batches(list(run_pipeline(top)))
    assert out["src"].to_pylist()[0] == 0  # TP53 has deg 2
    assert out["deg"].to_pylist() == [2, 1, 1]


def test_q6_join_back_to_gene_symbols(case_a_bundle) -> None:
    bundle = open_bundle(case_a_bundle["bundle_path"])
    # Build a scan and use it as the hash-join build side; expand emits dst nids.
    build = _gene_scan(bundle)  # rows: (nid, symbol, chromosome)
    seeds = FilterOperator(
        _gene_scan(bundle), compile_expr(("eq", ("col", "symbol"), ("lit", "TP53")))
    )
    probe = ExpandOperator(seeds, forward=case_a_bundle["iw_csr"], direction="out")
    join = HashJoinOperator(build, probe, build_key="nid", probe_key="dst")
    out = pa.Table.from_batches(list(run_pipeline(join)))
    assert sorted(out["symbol"].to_pylist()) == ["BRCA1", "MDM2"]
