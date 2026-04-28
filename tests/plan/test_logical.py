from caracaldb.plan.logical import (
    LLimit,
    LNodeScan,
    LProject,
    LSelection,
    walk,
)


def _scan() -> LNodeScan:
    return LNodeScan(
        class_iri="http://example.org/Gene",
        local_name="Gene",
        alias="n",
        columns=("nid", "symbol"),
    )


def test_logical_plan_has_no_children_by_default() -> None:
    scan = _scan()
    assert scan.children() == ()
    assert list(walk(scan)) == [scan]


def test_logical_plan_walks_in_preorder() -> None:
    scan = _scan()
    sel = LSelection(child=scan, predicate=("eq", "n.chromosome", "17"))
    proj = LProject(child=sel, projections=(("n.symbol", "symbol"),))
    limit = LLimit(child=proj, limit=10)
    order = [type(node).__name__ for node in walk(limit)]
    assert order == ["LLimit", "LProject", "LSelection", "LNodeScan"]


def test_logical_ops_are_value_equal() -> None:
    a = _scan()
    b = _scan()
    assert a == b
    assert hash(a) == hash(b)
