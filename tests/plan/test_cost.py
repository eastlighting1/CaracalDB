from caracaldb.plan.cost import CatalogStats, estimate
from caracaldb.plan.logical import LNodeScan, LProject, LSelection
from caracaldb.plan.pattern_compiler import LExpand, LJoin


def test_node_scan_cost_uses_class_stats() -> None:
    stats = CatalogStats(class_rows={"Gene": 30_000})
    plan = LNodeScan(class_iri="Gene", local_name="Gene", alias="g")
    est = estimate(plan, stats)
    assert est.rows == 30_000
    assert est.total > 0


def test_selection_reduces_cardinality() -> None:
    stats = CatalogStats(class_rows={"Gene": 1_000})
    base = LNodeScan(class_iri="Gene", local_name="Gene", alias="g")
    sel = LSelection(child=base, predicate=("eq", ("col", "x"), ("lit", 1)))
    assert estimate(sel, stats).rows < estimate(base, stats).rows


def test_expand_uses_avg_degree_and_hops() -> None:
    stats = CatalogStats(class_rows={"G": 100}, avg_degree={"p": 5.0})
    plan = LExpand(
        child=LNodeScan(class_iri="G", local_name="G", alias="a"),
        property_iri="p",
        direction="out",
        src_alias="a.nid",
        dst_alias="b.nid",
        edge_alias=None,
        hop_min=2,
        hop_max=2,
    )
    est = estimate(plan, stats)
    # 100 seeds × 5^2 = 2500.
    assert est.rows == 2_500


def test_join_cost_uses_min_of_inputs() -> None:
    stats = CatalogStats(class_rows={"G": 100, "H": 10_000})
    left = LNodeScan(class_iri="G", local_name="G", alias="a")
    right = LNodeScan(class_iri="H", local_name="H", alias="b")
    join = LJoin(left=left, right=right, left_key="x", right_key="y")
    est = estimate(join, stats)
    assert est.rows == 100  # min(100, 10000)


def test_project_preserves_rows() -> None:
    stats = CatalogStats(class_rows={"G": 50})
    base = LNodeScan(class_iri="G", local_name="G", alias="a")
    proj = LProject(child=base, projections=((("col", "nid"), "nid"),))
    assert estimate(proj, stats).rows == estimate(base, stats).rows
