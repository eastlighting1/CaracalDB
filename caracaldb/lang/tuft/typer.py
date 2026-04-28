"""Basic Tuft type checker."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft.binder import BoundProgram
from caracaldb.onto.catalog import Catalog, ConstraintKind, FieldDef, TypeKind, TypeRef


class Nullability(StrEnum):
    REQUIRED = "required"
    NULLABLE = "nullable"
    NULL = "null"


@dataclass(frozen=True, slots=True)
class TuftType:
    kind: TypeKind
    nullable: Nullability = Nullability.NULLABLE
    params: tuple[TuftType, ...] = ()
    width: int | None = None

    def with_nullability(self, nullable: Nullability) -> TuftType:
        return TuftType(kind=self.kind, nullable=nullable, params=self.params, width=self.width)


@dataclass(frozen=True, slots=True)
class TypedProgram:
    bound: BoundProgram
    expr_types: dict[int, TuftType]


@dataclass(slots=True)
class TypeChecker:
    catalog: Catalog
    source_name: str | None = None
    source_text: str | None = None
    _expr_types: dict[int, TuftType] = field(default_factory=dict)
    _var_classes: dict[str, str] = field(default_factory=dict)

    def check(self, bound: BoundProgram) -> TypedProgram:
        for stmt in bound.program.statements:
            self._check_stmt(stmt, bound.prefixes)
        return TypedProgram(bound=bound, expr_types=dict(self._expr_types))

    def _check_stmt(self, stmt: ta.Stmt, prefixes: dict[str, str]) -> None:
        if isinstance(stmt, ta.QueryStmt) and stmt.query is not None:
            self._check_query(stmt.query, prefixes)
        elif isinstance(stmt, ta.DmlStmt):
            if stmt.query is not None:
                self._check_query(stmt.query, prefixes)
            if stmt.pattern is not None:
                self._check_pattern(stmt.pattern, prefixes)
            for pattern in stmt.payload.get("patterns", ()):
                if isinstance(pattern, ta.Pattern):
                    self._check_pattern(pattern, prefixes)

    def _check_query(self, query: ta.Query, prefixes: dict[str, str]) -> None:
        for clause in query.clauses:
            if isinstance(clause, ta.MatchClause):
                for pattern in clause.patterns:
                    self._check_pattern(pattern, prefixes)
            elif isinstance(clause, ta.WhereClause) and clause.predicate is not None:
                self._require_bool(self._check_expr(clause.predicate), clause.predicate.span)
            elif isinstance(clause, ta.WithClause):
                for projection in clause.projections:
                    self._check_expr(projection.expr)
                if clause.where is not None:
                    self._require_bool(self._check_expr(clause.where), clause.where.span)
            elif isinstance(clause, ta.ReturnClause):
                for projection in clause.projections:
                    self._check_expr(projection.expr)

    def _check_pattern(self, pattern: ta.Pattern, prefixes: dict[str, str]) -> None:
        for elem in pattern.elements:
            if isinstance(elem, ta.NodePattern):
                class_iri = self._first_resolved(elem.labels, prefixes)
                if elem.var is not None and class_iri is not None:
                    self._var_classes[elem.var.name] = class_iri
                if class_iri is not None and elem.props is not None:
                    self._check_required_props(class_iri, elem.props)
                    self._check_prop_map(class_iri, elem.props)
            elif isinstance(elem, ta.RelPattern) and elem.props is not None:
                for entry in elem.props.entries:
                    self._check_expr(entry.value)

    def _check_prop_map(self, class_iri: str, prop_map: ta.PropMap) -> None:
        fields = self._fields_by_name(class_iri)
        for entry in prop_map.entries:
            field = fields.get(entry.key.name)
            actual = self._check_expr(entry.value)
            if field is None:
                continue
            expected = self._from_catalog_type(field.type)
            self._require_assignable(expected, actual, entry.value.span)
            if self._is_required(field) and actual.nullable == Nullability.NULL:
                raise self._error(
                    "TF-9501",
                    f"REQUIRED property `{entry.key.name}` cannot be null",
                    span=entry.value.span,
                )

    def _check_required_props(self, class_iri: str, prop_map: ta.PropMap) -> None:
        provided = {entry.key.name for entry in prop_map.entries}
        for field_def in self._fields_by_name(class_iri).values():
            if self._is_required(field_def) and field_def.name not in provided:
                raise self._error(
                    "TF-9501",
                    f"REQUIRED property `{field_def.name}` is missing",
                    span=prop_map.span,
                )

    def _check_expr(self, expr: ta.Expr) -> TuftType:
        if isinstance(expr, ta.Literal):
            return self._record(expr, self._literal_type(expr))
        if isinstance(expr, ta.Var):
            if expr.name is not None and expr.name.name.lower() == "null":
                return self._record(expr, TuftType(TypeKind.UNKNOWN, Nullability.NULL))
            return self._record(expr, TuftType(TypeKind.UNKNOWN))
        if isinstance(expr, ta.PathExpr):
            return self._record(expr, self._path_type(expr))
        if isinstance(expr, ta.BinOp):
            return self._record(expr, self._binop_type(expr))
        if isinstance(expr, ta.UnaryOp):
            inner = (
                self._check_expr(expr.operand)
                if expr.operand is not None
                else TuftType(TypeKind.UNKNOWN)
            )
            if expr.op.upper() == "NOT":
                self._require_bool(inner, expr.span)
                return self._record(expr, TuftType(TypeKind.BOOL, _nullable_union(inner)))
            if expr.op in {"+", "-"}:
                self._require_numeric(inner, expr.span)
                return self._record(expr, inner)
        if isinstance(expr, ta.Cast):
            if expr.expr is not None:
                self._check_expr(expr.expr)
            if expr.type_ref is not None:
                return self._record(expr, self._from_ast_type(expr.type_ref))
        if isinstance(expr, ta.ListExpr):
            item_types = tuple(self._check_expr(item) for item in expr.items)
            item_type = item_types[0] if item_types else TuftType(TypeKind.UNKNOWN)
            return self._record(expr, TuftType(TypeKind.LIST, params=(item_type,)))
        return self._record(expr, TuftType(TypeKind.UNKNOWN))

    def _binop_type(self, expr: ta.BinOp) -> TuftType:
        left = self._check_expr(expr.left) if expr.left is not None else TuftType(TypeKind.UNKNOWN)
        right = (
            self._check_expr(expr.right)
            if isinstance(expr.right, ta.Expr)
            else TuftType(TypeKind.UNKNOWN)
        )
        op = expr.op.upper()
        if op in {"+", "-", "*", "/", "%"}:
            self._require_numeric(left, expr.span)
            self._require_numeric(right, expr.span)
            return _promote_numeric(left, right)
        if op in {"AND", "OR", "XOR"}:
            self._require_bool(left, expr.span)
            self._require_bool(right, expr.span)
            return TuftType(TypeKind.BOOL, _nullable_union(left, right))
        if op in {"=", "!=", "<>", "<", "<=", ">", ">=", "IN", "IS NULL", "IS NOT NULL"}:
            if not _can_compare(left, right):
                raise self._error(
                    "TF-4001",
                    f"type mismatch: cannot compare {left.kind.name} and {right.kind.name}",
                    span=expr.span,
                )
            return TuftType(TypeKind.BOOL, _nullable_union(left, right))
        return TuftType(TypeKind.UNKNOWN)

    def _literal_type(self, expr: ta.Literal) -> TuftType:
        mapping = {
            ta.LiteralKind.BOOL: TypeKind.BOOL,
            ta.LiteralKind.INT: TypeKind.INT64,
            ta.LiteralKind.FLOAT: TypeKind.FLOAT64,
            ta.LiteralKind.DECIMAL: TypeKind.DECIMAL,
            ta.LiteralKind.STRING: TypeKind.STRING,
            ta.LiteralKind.DATE: TypeKind.DATE,
            ta.LiteralKind.TIME: TypeKind.TIME,
            ta.LiteralKind.DATETIME: TypeKind.DATETIME,
            ta.LiteralKind.DURATION: TypeKind.DURATION,
            ta.LiteralKind.IRI: TypeKind.IRI,
            ta.LiteralKind.TYPED_STRING: TypeKind.STRING,
            ta.LiteralKind.LANG_STRING: TypeKind.STRING,
        }
        if expr.kind == ta.LiteralKind.NULL:
            return TuftType(TypeKind.UNKNOWN, Nullability.NULL)
        return TuftType(mapping[expr.kind], Nullability.REQUIRED)

    def _path_type(self, expr: ta.PathExpr) -> TuftType:
        if expr.root is None or not expr.steps:
            return TuftType(TypeKind.UNKNOWN)
        class_iri = self._var_classes.get(expr.root.name)
        if class_iri is None:
            return TuftType(TypeKind.UNKNOWN)
        field = self._fields_by_name(class_iri).get(expr.steps[0].name)
        if field is None:
            return TuftType(TypeKind.UNKNOWN)
        return self._from_catalog_type(field.type)

    def _from_catalog_type(self, type_ref: TypeRef) -> TuftType:
        return TuftType(
            type_ref.kind,
            Nullability.NULLABLE if type_ref.nullable else Nullability.REQUIRED,
            params=tuple(self._from_catalog_type(item) for item in type_ref.params),
        )

    def _from_ast_type(self, type_ref: ta.TypeRef) -> TuftType:
        name = type_ref.name
        if isinstance(name, str):
            kind = _TYPE_NAMES.get(name.upper(), TypeKind.UNKNOWN)
            return TuftType(kind, Nullability.NULLABLE)
        return TuftType(TypeKind.UNKNOWN)

    def _require_assignable(
        self,
        expected: TuftType,
        actual: TuftType,
        span: ta.Span | None,
    ) -> None:
        if actual.nullable == Nullability.NULL:
            if expected.nullable == Nullability.REQUIRED:
                raise self._error("TF-4010", "cannot assign NULL to REQUIRED field", span=span)
            return
        if expected.kind == actual.kind or _is_numeric_widening(actual.kind, expected.kind):
            return
        raise self._error(
            "TF-4001",
            f"type mismatch: expected {expected.kind.name}, got {actual.kind.name}",
            span=span,
        )

    def _require_numeric(self, typ: TuftType, span: ta.Span | None) -> None:
        if typ.kind not in _NUMERIC:
            raise self._error("TF-4001", f"expected numeric type, got {typ.kind.name}", span=span)

    def _require_bool(self, typ: TuftType, span: ta.Span | None) -> None:
        if typ.kind not in {TypeKind.BOOL, TypeKind.UNKNOWN}:
            raise self._error("TF-4001", f"expected BOOL, got {typ.kind.name}", span=span)

    def _record(self, expr: ta.Expr, typ: TuftType) -> TuftType:
        self._expr_types[id(expr)] = typ
        return typ

    def _fields_by_name(self, class_iri: str) -> dict[str, FieldDef]:
        class_def = self.catalog.class_by_iri(class_iri)
        if class_def is None:
            return {}
        return {field.name: field for field in class_def.fields}

    def _is_required(self, field: FieldDef) -> bool:
        return (not field.type.nullable) or any(
            constraint.kind == ConstraintKind.REQUIRED for constraint in field.constraints
        )

    def _first_resolved(self, refs: tuple[ta.NameRef, ...], prefixes: dict[str, str]) -> str | None:
        if not refs:
            return None
        ref = refs[0]
        if isinstance(ref, ta.Iri):
            return ref.value
        if ":" not in ref.value:
            return f"{prefixes.get('', '')}{ref.value}"
        prefix, local = ref.value.split(":", 1)
        return f"{prefixes.get(prefix, '')}{local}"

    def _error(self, code: str, message: str, *, span: ta.Span | None) -> CaracalError:
        return CaracalError(
            code=code,
            message=message,
            span=span,
            source_name=self.source_name,
            source_text=self.source_text,
        )


def check_types(
    bound: BoundProgram,
    catalog: Catalog,
    *,
    source_name: str | None = None,
    source_text: str | None = None,
) -> TypedProgram:
    checker = TypeChecker(catalog=catalog, source_name=source_name, source_text=source_text)
    return checker.check(bound)


_NUMERIC = {
    TypeKind.INT8,
    TypeKind.INT16,
    TypeKind.INT32,
    TypeKind.INT64,
    TypeKind.UINT8,
    TypeKind.UINT16,
    TypeKind.UINT32,
    TypeKind.UINT64,
    TypeKind.FLOAT32,
    TypeKind.FLOAT64,
    TypeKind.DECIMAL,
}
_TYPE_NAMES = {kind.name: kind for kind in TypeKind}
_TYPE_NAMES.update({"INT": TypeKind.INT64, "F32": TypeKind.FLOAT32, "F64": TypeKind.FLOAT64})


def _promote_numeric(left: TuftType, right: TuftType) -> TuftType:
    if TypeKind.FLOAT64 in {left.kind, right.kind}:
        kind = TypeKind.FLOAT64
    elif TypeKind.FLOAT32 in {left.kind, right.kind}:
        kind = TypeKind.FLOAT32
    elif TypeKind.DECIMAL in {left.kind, right.kind}:
        kind = TypeKind.DECIMAL
    else:
        kind = TypeKind.INT64
    return TuftType(kind, _nullable_union(left, right))


def _nullable_union(*types: TuftType) -> Nullability:
    if any(item.nullable == Nullability.NULL for item in types):
        return Nullability.NULL
    if any(item.nullable == Nullability.NULLABLE for item in types):
        return Nullability.NULLABLE
    return Nullability.REQUIRED


def _can_compare(left: TuftType, right: TuftType) -> bool:
    if TypeKind.UNKNOWN in {left.kind, right.kind}:
        return True
    if left.kind == right.kind:
        return True
    return left.kind in _NUMERIC and right.kind in _NUMERIC


def _is_numeric_widening(actual: TypeKind, expected: TypeKind) -> bool:
    if actual not in _NUMERIC or expected not in _NUMERIC:
        return False
    return _numeric_rank(actual) <= _numeric_rank(expected)


def _numeric_rank(kind: TypeKind) -> int:
    ranks = {
        TypeKind.INT8: 1,
        TypeKind.UINT8: 1,
        TypeKind.INT16: 2,
        TypeKind.UINT16: 2,
        TypeKind.INT32: 3,
        TypeKind.UINT32: 3,
        TypeKind.INT64: 4,
        TypeKind.UINT64: 4,
        TypeKind.FLOAT32: 5,
        TypeKind.FLOAT64: 6,
        TypeKind.DECIMAL: 7,
    }
    return ranks.get(kind, 100)


__all__ = ["Nullability", "TuftType", "TypeChecker", "TypedProgram", "check_types"]
