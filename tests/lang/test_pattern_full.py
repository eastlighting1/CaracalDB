"""Golden coverage for the M2 pattern surface.

The grammar already accepts the full pattern syntax; this test pins the
transformer output so future grammar refactors can not silently change the
AST shape.
"""

from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import parse_tuft


def _query(text: str) -> ta.Query:
    program = parse_tuft(text)
    stmt = program.statements[0]
    assert isinstance(stmt, ta.QueryStmt) and stmt.query is not None
    return stmt.query


def test_pattern_arrow_directions() -> None:
    q = _query("MATCH (a)-[:p]->(b), (c)<-[:q]-(d), (e)-[:r]-(f) RETURN a")
    patterns = q.clauses[0].patterns  # type: ignore[attr-defined]
    dirs = [
        elem.direction
        for pattern in patterns
        for elem in pattern.elements
        if isinstance(elem, ta.RelPattern)
    ]
    assert dirs == [ta.Direction.OUT, ta.Direction.IN, ta.Direction.BOTH]


def test_pattern_label_union_and_prop_map() -> None:
    q = _query("MATCH (g:Gene & :Drug {symbol: 'TP53', score: 0.9}) RETURN g")
    elem = q.clauses[0].patterns[0].elements[0]  # type: ignore[attr-defined]
    assert isinstance(elem, ta.NodePattern)
    assert len(elem.labels) == 2
    assert elem.props is not None and len(elem.props.entries) == 2


def test_pattern_rel_type_union() -> None:
    q = _query("MATCH (a)-[r:p|q|r]->(b) RETURN a")
    rel = q.clauses[0].patterns[0].elements[1]  # type: ignore[attr-defined]
    assert isinstance(rel, ta.RelPattern)
    assert len(rel.types) == 3


def test_pattern_hop_range_variants() -> None:
    q = _query("MATCH (a)-[*1..3]->(b), (c)-[*..5]->(d), (e)-[*]->(f) RETURN a")
    rels = [
        elem
        for pattern in q.clauses[0].patterns  # type: ignore[attr-defined]
        for elem in pattern.elements
        if isinstance(elem, ta.RelPattern)
    ]
    assert (rels[0].hop_range.min_hops, rels[0].hop_range.max_hops) == (1, 3)
    # *..5 → min_hops None or 1, max_hops 5; the transformer mirrors the parsed numbers.
    assert rels[1].hop_range.max_hops == 5
    # *  → unbounded — both fields stay None.
    assert rels[2].hop_range.min_hops is None and rels[2].hop_range.max_hops is None


def test_pattern_rel_prop_map() -> None:
    q = _query("MATCH (a)-[r:p {weight: 0.5}]->(b) RETURN a")
    rel = q.clauses[0].patterns[0].elements[1]  # type: ignore[attr-defined]
    assert isinstance(rel, ta.RelPattern)
    assert rel.props is not None
    assert rel.props.entries[0].key.name == "weight"
