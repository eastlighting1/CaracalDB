"""Dataclass AST nodes for the Tuft query language.

The AST intentionally stays close to the language specification. Binding,
type-checking, and planning attach richer semantic information in later phases.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


@dataclass(frozen=True, slots=True)
class Span:
    """Source span used by diagnostics."""

    start: int
    end: int
    file_id: str | None = None


@dataclass(frozen=True, slots=True)
class Ident:
    name: str
    span: Span | None = None
    escaped: bool = False


@dataclass(frozen=True, slots=True)
class QName:
    value: str
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class Iri:
    value: str
    span: Span | None = None


NameRef = QName | Iri


class StatementKind(StrEnum):
    DDL = "ddl"
    DML = "dml"
    DQL = "dql"
    TX = "tx"
    UTIL = "util"


@dataclass(frozen=True, slots=True)
class Stmt:
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class DdlStmt(Stmt):
    op: str = ""
    target: NameRef | Ident | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DmlStmt(Stmt):
    op: str = ""
    query: Query | None = None
    pattern: Pattern | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueryStmt(Stmt):
    query: Query | None = None


@dataclass(frozen=True, slots=True)
class TxStmt(Stmt):
    op: str = ""


@dataclass(frozen=True, slots=True)
class UtilStmt(Stmt):
    op: str = ""
    query: Query | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Program:
    statements: tuple[Stmt, ...]
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class PrefixDecl:
    prefix: str
    iri: Iri
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class Modifiers:
    order_by: tuple[OrderItem, ...] = ()
    skip: Expr | None = None
    limit: Expr | None = None


@dataclass(frozen=True, slots=True)
class Query:
    clauses: tuple[Clause, ...]
    prefixes: tuple[PrefixDecl, ...] = ()
    modifiers: Modifiers = field(default_factory=Modifiers)
    span: Span | None = None


class ClauseKind(StrEnum):
    MATCH = "match"
    OPTIONAL_MATCH = "optional_match"
    WITH = "with"
    WHERE = "where"
    UNWIND = "unwind"
    RETURN = "return"
    CALL = "call"
    INSERT_TRIPLES = "insert_triples"
    INSERT_PATTERN = "insert_pattern"
    SET = "set"
    DELETE = "delete"
    MERGE = "merge"
    INFER = "infer"
    EXPORT = "export"


@dataclass(frozen=True, slots=True)
class Clause:
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class MatchClause(Clause):
    patterns: tuple[Pattern, ...] = ()
    optional: bool = False
    as_of: AsOf | None = None


@dataclass(frozen=True, slots=True)
class WhereClause(Clause):
    predicate: Expr | None = None


@dataclass(frozen=True, slots=True)
class WithClause(Clause):
    projections: tuple[Projection, ...] = ()
    where: Expr | None = None


@dataclass(frozen=True, slots=True)
class ReturnClause(Clause):
    projections: tuple[Projection, ...] = ()
    distinct: bool = False


@dataclass(frozen=True, slots=True)
class UnwindClause(Clause):
    expr: Expr | None = None
    alias: Ident | None = None


@dataclass(frozen=True, slots=True)
class CallClause(Clause):
    function: NameRef | None = None
    args: tuple[Expr, ...] = ()
    yield_items: tuple[Ident, ...] = ()


@dataclass(frozen=True, slots=True)
class SetClause(Clause):
    items: tuple[SetItem, ...] = ()


@dataclass(frozen=True, slots=True)
class DeleteClause(Clause):
    exprs: tuple[Expr, ...] = ()
    detach: bool = False


@dataclass(frozen=True, slots=True)
class MergeClause(Clause):
    pattern: Pattern | None = None
    on_create: tuple[SetItem, ...] = ()
    on_match: tuple[SetItem, ...] = ()


@dataclass(frozen=True, slots=True)
class InferClause(Clause):
    rules: tuple[Ident, ...] = ()
    graph: Ident | None = None


@dataclass(frozen=True, slots=True)
class ExportClause(Clause):
    format: str = ""
    target: str = ""
    body: dict[str, Any] = field(default_factory=dict)


class Direction(StrEnum):
    OUT = "out"
    IN = "in"
    BOTH = "both"


@dataclass(frozen=True, slots=True)
class HopRange:
    min_hops: int | None = None
    max_hops: int | None = None


@dataclass(frozen=True, slots=True)
class Pattern:
    elements: tuple[PatternElem, ...]
    binding: Ident | None = None
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class PatternElem:
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class NodePattern(PatternElem):
    var: Ident | None = None
    labels: tuple[NameRef, ...] = ()
    props: PropMap | None = None


@dataclass(frozen=True, slots=True)
class RelPattern(PatternElem):
    var: Ident | None = None
    types: tuple[NameRef, ...] = ()
    direction: Direction = Direction.OUT
    hop_range: HopRange = field(default_factory=HopRange)
    props: PropMap | None = None


@dataclass(frozen=True, slots=True)
class PathPattern(PatternElem):
    var: Ident | None = None
    inner: tuple[PatternElem, ...] = ()


@dataclass(frozen=True, slots=True)
class PropMap:
    entries: tuple[PropEntry, ...] = ()
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class PropEntry:
    key: Ident
    value: Expr
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class Projection:
    expr: Expr
    alias: Ident | None = None
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class OrderItem:
    expr: Expr
    descending: bool = False
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class SetItem:
    target: Expr
    value: Expr
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class AsOf:
    kind: str
    value: str
    span: Span | None = None


class LiteralKind(StrEnum):
    BOOL = "bool"
    INT = "int"
    FLOAT = "float"
    DECIMAL = "decimal"
    STRING = "string"
    NULL = "null"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    DURATION = "duration"
    IRI = "iri"
    TYPED_STRING = "typed_string"
    LANG_STRING = "lang_string"


@dataclass(frozen=True, slots=True)
class Expr:
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class Literal(Expr):
    kind: LiteralKind = LiteralKind.NULL
    value: Any = None
    datatype: NameRef | None = None
    lang: str | None = None


@dataclass(frozen=True, slots=True)
class Var(Expr):
    name: Ident | None = None


@dataclass(frozen=True, slots=True)
class PathExpr(Expr):
    root: Ident | None = None
    steps: tuple[Ident, ...] = ()


@dataclass(frozen=True, slots=True)
class FnCall(Expr):
    name: NameRef | None = None
    args: tuple[Expr, ...] = ()


@dataclass(frozen=True, slots=True)
class BinOp(Expr):
    op: str = ""
    left: Expr | None = None
    right: Expr | None = None


@dataclass(frozen=True, slots=True)
class UnaryOp(Expr):
    op: str = ""
    operand: Expr | None = None


@dataclass(frozen=True, slots=True)
class Case(Expr):
    arms: tuple[tuple[Expr, Expr], ...] = ()
    default: Expr | None = None


@dataclass(frozen=True, slots=True)
class Cast(Expr):
    expr: Expr | None = None
    type_ref: TypeRef | None = None


@dataclass(frozen=True, slots=True)
class Subquery(Expr):
    query: Query | None = None


@dataclass(frozen=True, slots=True)
class Exists(Expr):
    pattern: Pattern | None = None


@dataclass(frozen=True, slots=True)
class ListExpr(Expr):
    items: tuple[Expr, ...] = ()


@dataclass(frozen=True, slots=True)
class MapExpr(Expr):
    entries: tuple[tuple[str, Expr], ...] = ()


@dataclass(frozen=True, slots=True)
class Subscript(Expr):
    target: Expr | None = None
    index: Expr | None = None


@dataclass(frozen=True, slots=True)
class TypeRef:
    name: str | NameRef
    params: tuple[TypeRef | int, ...] = ()
    span: Span | None = None


@dataclass(frozen=True, slots=True)
class TriplePattern:
    subject: Expr | NameRef
    predicate: NameRef | str
    object: Expr | NameRef
    span: Span | None = None


__all__ = [
    "AsOf",
    "BinOp",
    "CallClause",
    "Case",
    "Cast",
    "Clause",
    "ClauseKind",
    "DeleteClause",
    "Direction",
    "DdlStmt",
    "DmlStmt",
    "Exists",
    "ExportClause",
    "Expr",
    "FnCall",
    "HopRange",
    "Ident",
    "InferClause",
    "Iri",
    "ListExpr",
    "Literal",
    "LiteralKind",
    "MapExpr",
    "MatchClause",
    "MergeClause",
    "Modifiers",
    "NameRef",
    "NodePattern",
    "OrderItem",
    "PathExpr",
    "PathPattern",
    "Pattern",
    "PatternElem",
    "PrefixDecl",
    "Program",
    "Projection",
    "PropEntry",
    "PropMap",
    "QName",
    "Query",
    "QueryStmt",
    "RelPattern",
    "ReturnClause",
    "SetClause",
    "SetItem",
    "Span",
    "StatementKind",
    "Stmt",
    "Subquery",
    "Subscript",
    "TriplePattern",
    "TxStmt",
    "TypeRef",
    "UnaryOp",
    "UnwindClause",
    "UtilStmt",
    "Var",
    "WhereClause",
    "WithClause",
]
