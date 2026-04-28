import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import (
    BoundProgram,
    Nullability,
    TypeChecker,
    bind_program,
    check_types,
    parse_tuft,
)
from caracaldb.lang.tuft.ast import QueryStmt, ReturnClause
from caracaldb.lang.tuft.typer import TuftType
from caracaldb.onto.catalog import (
    Catalog,
    ConstraintDef,
    ConstraintKind,
    FieldDef,
    TypeKind,
    TypeRef,
)


def _catalog() -> Catalog:
    catalog = Catalog.empty(catalog_id="bio")
    catalog.register_class(
        "https://example.test/bio/Gene",
        fields=(
            FieldDef(
                "symbol",
                TypeRef(TypeKind.STRING, nullable=False),
                constraints=(ConstraintDef(ConstraintKind.REQUIRED),),
            ),
            FieldDef("score", TypeRef(TypeKind.FLOAT64, nullable=True)),
            FieldDef("active", TypeRef(TypeKind.BOOL, nullable=True)),
        ),
    )
    return catalog


def _bind(source: str) -> tuple[BoundProgram, Catalog]:
    catalog = _catalog()
    bound = bind_program(
        parse_tuft(source),
        catalog,
        prefixes={"": "https://example.test/bio/"},
        source_text=source,
    )
    return bound, catalog


def test_type_checker_accepts_numeric_and_boolean_where_expression() -> None:
    source = "MATCH (g:Gene {symbol:'TP53'}) WHERE g.score + 1.5 > 3.0 AND g.active RETURN g;"
    bound, catalog = _bind(source)

    typed = check_types(bound, catalog, source_text=source)

    assert typed.expr_types


def test_type_checker_records_nullable_numeric_result() -> None:
    program = parse_tuft("MATCH (g:Gene) RETURN 1 + 2.0;")
    stmt = program.statements[0]
    assert isinstance(stmt, QueryStmt)
    assert stmt.query is not None
    clause = stmt.query.clauses[1]
    assert isinstance(clause, ReturnClause)

    checker = TypeChecker(_catalog())
    typ = checker._check_expr(clause.projections[0].expr)

    assert typ == TuftType(TypeKind.FLOAT64, Nullability.REQUIRED)


def test_type_checker_rejects_required_property_missing() -> None:
    source = "MATCH (g:Gene {score: 1.0}) RETURN g;"
    bound, catalog = _bind(source)

    with pytest.raises(CaracalError) as exc:
        check_types(bound, catalog, source_text=source)

    assert exc.value.code == "TF-9501"


def test_type_checker_rejects_required_property_null() -> None:
    source = "MATCH (g:Gene {symbol: null}) RETURN g;"
    bound, catalog = _bind(source)

    with pytest.raises(CaracalError) as exc:
        check_types(bound, catalog, source_text=source)

    assert exc.value.code in {"TF-4010", "TF-9501"}


def test_type_checker_rejects_property_type_mismatch() -> None:
    source = "MATCH (g:Gene {symbol: 42}) RETURN g;"
    bound, catalog = _bind(source)

    with pytest.raises(CaracalError) as exc:
        check_types(bound, catalog, source_text=source)

    assert exc.value.code == "TF-4001"


def test_type_checker_rejects_non_bool_where() -> None:
    source = "MATCH (g:Gene {symbol:'TP53'}) WHERE g.symbol RETURN g;"
    bound, catalog = _bind(source)

    with pytest.raises(CaracalError) as exc:
        check_types(bound, catalog, source_text=source)

    assert exc.value.code == "TF-4001"
