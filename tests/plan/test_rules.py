from caracaldb.plan.logical import LLimit, LNodeScan, LProject, LSelection
from caracaldb.plan.rules import predicate_pushdown, projection_pruning, run_rules


def _scan(
    columns: tuple[str, ...] | None = ("nid", "symbol", "chromosome", "biotype"),
) -> LNodeScan:
    return LNodeScan(
        class_iri="http://example.org/Gene",
        local_name="Gene",
        alias="g",
        columns=columns,
    )


def test_projection_pruning_restricts_scan_columns() -> None:
    plan = LProject(
        child=_scan(),
        projections=(
            (("col", "symbol"), "symbol"),
            (("col", "chromosome"), "chromosome"),
        ),
    )
    pruned = projection_pruning(plan)
    assert isinstance(pruned, LProject)
    assert isinstance(pruned.child, LNodeScan)
    assert pruned.child.columns == ("chromosome", "symbol")


def test_predicate_pushdown_widens_scan_columns_to_cover_predicate() -> None:
    pred = ("eq", ("col", "chromosome"), ("lit", "17"))
    plan = LSelection(child=_scan(columns=("nid", "symbol")), predicate=pred)
    pushed = predicate_pushdown(plan)
    assert isinstance(pushed, LSelection)
    assert pushed.child.columns == ("chromosome", "nid", "symbol")


def test_run_rules_combines_pushdown_and_pruning() -> None:
    pred = ("eq", ("col", "chromosome"), ("lit", "17"))
    plan = LLimit(
        child=LProject(
            child=LSelection(child=_scan(), predicate=pred),
            projections=((("col", "symbol"), "symbol"),),
        ),
        limit=10,
    )
    result = run_rules(plan)
    # Final scan should keep only symbol + chromosome (referenced by either layer).
    project = result.plan.child  # type: ignore[attr-defined]
    selection = project.child
    scan = selection.child
    assert scan.columns == ("chromosome", "symbol")
    assert "predicate_pushdown" in result.applied or "projection_pruning" in result.applied


def test_run_rules_reaches_fixpoint() -> None:
    plan = _scan()
    result = run_rules(plan)
    assert result.plan == plan
    assert result.iterations <= 2
