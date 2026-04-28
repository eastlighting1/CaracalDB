import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import parse_tuft
from caracaldb.plan.logical import LNodeScan
from caracaldb.plan.pattern_compiler import LExpand, LJoin, compile_pattern


def _pattern(text: str) -> ta.Pattern:
    program = parse_tuft(text)
    stmt = program.statements[0]
    assert isinstance(stmt, ta.QueryStmt) and stmt.query is not None
    match = stmt.query.clauses[0]
    assert isinstance(match, ta.MatchClause)
    return match.patterns[0]


def test_compile_single_node_pattern_emits_node_scan() -> None:
    pat = _pattern("MATCH (g:Gene) RETURN g")
    plan = compile_pattern(pat)
    assert isinstance(plan, LNodeScan)
    assert plan.class_iri == "Gene"
    assert plan.alias == "g"


def test_compile_two_hop_pattern_emits_expand_and_join() -> None:
    pat = _pattern("MATCH (g:Gene)-[:interactsWith]->(t:Tissue) RETURN g")
    plan = compile_pattern(pat)
    assert isinstance(plan, LJoin)
    expand = plan.left
    target_scan = plan.right
    assert isinstance(expand, LExpand)
    assert isinstance(target_scan, LNodeScan)
    assert expand.property_iri == "interactsWith"
    assert expand.src_alias == "g.nid"
    assert expand.dst_alias == "t.nid"
    assert plan.left_key == "t.nid"
    assert plan.right_key == "nid"
    assert target_scan.alias == "t"


def test_compile_three_segment_chain_stacks_joins() -> None:
    pat = _pattern("MATCH (a:A)-[:p]->(b:B)-[:q]->(c:C) RETURN a")
    plan = compile_pattern(pat)
    assert isinstance(plan, LJoin)
    inner = plan.left
    assert isinstance(inner, LExpand)
    assert isinstance(inner.child, LJoin)


def test_compile_pattern_rejects_missing_label() -> None:
    pat = _pattern("MATCH (g) RETURN g")
    with pytest.raises(CaracalError) as exc:
        compile_pattern(pat)
    assert exc.value.code == "CDB-6050"


def test_compile_pattern_rejects_missing_rel_type() -> None:
    pat = _pattern("MATCH (g:Gene)-[]->(t:Tissue) RETURN g")
    with pytest.raises(CaracalError) as exc:
        compile_pattern(pat)
    assert exc.value.code == "CDB-6050"
