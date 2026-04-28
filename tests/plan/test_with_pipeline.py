from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import parse_tuft
from caracaldb.plan.with_pipeline import (
    collect_aliases,
    is_aggregate,
    split_pipelines,
)


def _query(text: str) -> ta.Query:
    program = parse_tuft(text)
    stmt = program.statements[0]
    assert isinstance(stmt, ta.QueryStmt) and stmt.query is not None
    return stmt.query


def test_split_pipelines_no_with_returns_single_segment() -> None:
    q = _query("MATCH (g:Gene) RETURN g")
    segments = split_pipelines(q)
    assert len(segments) == 1
    assert segments[0].boundary is None


def test_split_pipelines_with_clause_creates_boundary() -> None:
    q = _query("MATCH (g:Gene) WITH g.symbol AS sym WHERE sym = 'TP53' RETURN sym")
    segments = split_pipelines(q)
    assert len(segments) == 2
    assert segments[0].boundary is not None
    assert collect_aliases(segments[0].boundary) == ("sym",)
    assert segments[1].boundary is None


def test_with_boundary_carries_filter() -> None:
    q = _query("MATCH (g:Gene) WITH g.symbol AS sym WHERE sym = 'TP53' RETURN sym")
    segments = split_pipelines(q)
    assert segments[0].boundary is not None
    assert segments[0].boundary.where is not None


def test_is_aggregate_detects_count_in_with() -> None:
    q = _query("MATCH (g:Gene) WITH count(g) AS n RETURN n")
    segments = split_pipelines(q)
    assert is_aggregate(segments[0].boundary)
