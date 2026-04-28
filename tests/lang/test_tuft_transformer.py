from caracaldb.lang.tuft import parse_tuft
from caracaldb.lang.tuft.ast import (
    BinOp,
    DdlStmt,
    DmlStmt,
    Literal,
    MatchClause,
    QueryStmt,
    ReturnClause,
    TypeRef,
    WhereClause,
)


def test_transform_create_class() -> None:
    program = parse_tuft(
        "CREATE CLASS bio:Gene SUBCLASSOF bio:BiologicalEntity "
        "PROPERTIES (symbol STRING REQUIRED UNIQUE, embedding VECTOR<F32,768>);"
    )

    stmt = program.statements[0]
    assert isinstance(stmt, DdlStmt)
    assert stmt.op == "create_class"
    props = stmt.payload["properties"]
    assert props[0]["constraints"] == ("REQUIRED", "UNIQUE")
    assert isinstance(props[1]["type"], TypeRef)
    assert props[1]["type"].name == "VECTOR"


def test_transform_match_where_return_limit() -> None:
    program = parse_tuft(
        "MATCH (g:Gene {symbol:'TP53'}) " "WHERE g.symbol = 'TP53' " "RETURN g.symbol LIMIT 10;"
    )

    stmt = program.statements[0]
    assert isinstance(stmt, QueryStmt)
    assert stmt.query is not None
    assert isinstance(stmt.query.clauses[0], MatchClause)
    assert isinstance(stmt.query.clauses[1], WhereClause)
    assert isinstance(stmt.query.clauses[1].predicate, BinOp)
    assert stmt.query.clauses[1].predicate.op == "="
    assert isinstance(stmt.query.clauses[2], ReturnClause)
    assert isinstance(stmt.query.modifiers.limit, Literal)
    assert stmt.query.modifiers.limit.value == 10


def test_transform_insert_triples() -> None:
    program = parse_tuft("INSERT TRIPLES { :TP53 a bio:Gene . :TP53 bio:symbol 'TP53' . };")

    stmt = program.statements[0]
    assert isinstance(stmt, DmlStmt)
    assert stmt.op == "insert_triples"
    assert len(stmt.payload["triples"]) == 2
