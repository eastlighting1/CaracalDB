"""Semantic binder for Tuft names and ontology references."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.onto.catalog import Catalog


@dataclass(frozen=True, slots=True)
class BoundName:
    original: ta.NameRef
    iri: str


@dataclass(frozen=True, slots=True)
class BoundProgram:
    program: ta.Program
    prefixes: dict[str, str]
    classes: tuple[BoundName, ...] = ()
    properties: tuple[BoundName, ...] = ()


@dataclass(slots=True)
class Binder:
    catalog: Catalog
    prefixes: dict[str, str] = field(default_factory=dict)
    source_name: str | None = None
    source_text: str | None = None
    _classes: list[BoundName] = field(default_factory=list)
    _properties: list[BoundName] = field(default_factory=list)

    def bind(self, program: ta.Program) -> BoundProgram:
        for stmt in program.statements:
            self._bind_stmt(stmt)
        return BoundProgram(
            program=program,
            prefixes=dict(self.prefixes),
            classes=tuple(self._classes),
            properties=tuple(self._properties),
        )

    def resolve_name(self, name: ta.NameRef) -> str:
        if isinstance(name, ta.Iri):
            return name.value

        value = name.value
        if ":" not in value:
            if "" not in self.prefixes:
                raise self._error(
                    "TF-3001",
                    f"undefined default prefix for name `{value}`",
                    span=name.span,
                    hint=(
                        "declare a default prefix through binder prefixes or use an "
                        "explicit prefixed name"
                    ),
                )
            return f"{self.prefixes['']}{value}"

        prefix, local = value.split(":", 1)
        if prefix not in self.prefixes:
            raise self._error(
                "TF-3001",
                f"undefined prefix `{prefix}`",
                span=name.span,
                hint=f"declare PREFIX {prefix}: <...> before using `{value}`",
            )
        return f"{self.prefixes[prefix]}{local}"

    def bind_class(self, name: ta.NameRef) -> BoundName:
        iri = self.resolve_name(name)
        if self.catalog.class_by_iri(iri) is None:
            raise self._error(
                "TF-3004",
                f"unknown class `{iri}`",
                span=name.span,
                hint="create the class in the catalog before referencing it",
            )
        bound = BoundName(original=name, iri=iri)
        self._classes.append(bound)
        return bound

    def bind_property(self, name: ta.NameRef) -> BoundName:
        iri = self.resolve_name(name)
        if self.catalog.property_by_iri(iri) is None:
            raise self._error(
                "TF-3005",
                f"unknown property `{iri}`",
                span=name.span,
                hint="create the property in the catalog before referencing it",
            )
        bound = BoundName(original=name, iri=iri)
        self._properties.append(bound)
        return bound

    def _bind_stmt(self, stmt: ta.Stmt) -> None:
        if isinstance(stmt, ta.DdlStmt):
            self._bind_ddl(stmt)
        elif isinstance(stmt, ta.QueryStmt) and stmt.query is not None:
            self._bind_query(stmt.query)
        elif isinstance(stmt, ta.DmlStmt):
            self._bind_dml(stmt)
        elif isinstance(stmt, ta.UtilStmt) and stmt.query is not None:
            self._bind_query(stmt.query)

    def _bind_ddl(self, stmt: ta.DdlStmt) -> None:
        if stmt.op == "prefix":
            decl = stmt.payload.get("decl")
            if isinstance(decl, ta.PrefixDecl):
                self.prefixes[decl.prefix] = decl.iri.value
            return

        if stmt.op == "create_class":
            self._bind_target(stmt.target)
            for item in stmt.payload.get("subclasses_of", ()):
                if isinstance(item, ta.QName | ta.Iri):
                    self.bind_class(item)
            return

        if stmt.op == "create_property":
            self._bind_target(stmt.target)
            for item in stmt.payload.get("domain", ()):
                if isinstance(item, ta.QName | ta.Iri):
                    self.bind_class(item)
            self._bind_type_refs(stmt.payload.get("range", ()))
            return

        if stmt.op == "create_graph":
            for item in stmt.payload.get("ontologies", ()):
                if isinstance(item, ta.QName | ta.Iri):
                    self.resolve_name(item)

    def _bind_dml(self, stmt: ta.DmlStmt) -> None:
        if stmt.query is not None:
            self._bind_query(stmt.query)
        if stmt.pattern is not None:
            self._bind_pattern(stmt.pattern)
        for pattern in stmt.payload.get("patterns", ()):
            if isinstance(pattern, ta.Pattern):
                self._bind_pattern(pattern)
        for triple in stmt.payload.get("triples", ()):
            if isinstance(triple, ta.TriplePattern):
                self._bind_triple(triple)

    def _bind_query(self, query: ta.Query) -> None:
        for decl in query.prefixes:
            self.prefixes[decl.prefix] = decl.iri.value
        for clause in query.clauses:
            self._bind_clause(clause)

    def _bind_clause(self, clause: ta.Clause) -> None:
        if isinstance(clause, ta.MatchClause):
            for pattern in clause.patterns:
                self._bind_pattern(pattern)
        elif isinstance(clause, ta.CallClause) and clause.function is not None:
            self.resolve_name(clause.function)
        elif isinstance(clause, ta.SetClause):
            for item in clause.items:
                self._bind_expr(item.value)
        elif isinstance(clause, ta.WhereClause) and clause.predicate is not None:
            self._bind_expr(clause.predicate)
        elif isinstance(clause, ta.WithClause):
            self._bind_projections(clause.projections)
            if clause.where is not None:
                self._bind_expr(clause.where)
        elif isinstance(clause, ta.ReturnClause):
            self._bind_projections(clause.projections)

    def _bind_pattern(self, pattern: ta.Pattern) -> None:
        for elem in pattern.elements:
            if isinstance(elem, ta.NodePattern):
                for label in elem.labels:
                    self.bind_class(label)
                if elem.props is not None:
                    self._bind_prop_map(elem.props)
            elif isinstance(elem, ta.RelPattern):
                for rel_type in elem.types:
                    self.bind_property(rel_type)
                if elem.props is not None:
                    self._bind_prop_map(elem.props)
            elif isinstance(elem, ta.PathPattern):
                for inner in elem.inner:
                    self._bind_pattern(ta.Pattern(elements=(inner,)))

    def _bind_triple(self, triple: ta.TriplePattern) -> None:
        if isinstance(triple.predicate, ta.QName | ta.Iri):
            self.bind_property(triple.predicate)
        for value in (triple.subject, triple.object):
            if isinstance(value, ta.Expr):
                self._bind_expr(value)
            elif isinstance(value, ta.QName | ta.Iri):
                self.resolve_name(value)

    def _bind_expr(self, expr: ta.Expr) -> None:
        if isinstance(expr, ta.BinOp):
            if expr.left is not None:
                self._bind_expr(expr.left)
            if isinstance(expr.right, ta.Expr):
                self._bind_expr(expr.right)
        elif isinstance(expr, ta.UnaryOp) and expr.operand is not None:
            self._bind_expr(expr.operand)
        elif isinstance(expr, ta.FnCall):
            if expr.name is not None:
                self.resolve_name(expr.name)
            for arg in expr.args:
                self._bind_expr(arg)
        elif isinstance(expr, ta.Literal) and expr.datatype is not None:
            self.resolve_name(expr.datatype)
        elif isinstance(expr, ta.ListExpr):
            for item in expr.items:
                self._bind_expr(item)
        elif isinstance(expr, ta.MapExpr):
            for _, item in expr.entries:
                self._bind_expr(item)
        elif isinstance(expr, ta.Subscript):
            if expr.target is not None:
                self._bind_expr(expr.target)
            if expr.index is not None:
                self._bind_expr(expr.index)
        elif isinstance(expr, ta.Cast) and expr.type_ref is not None:
            self._bind_type_ref(expr.type_ref)

    def _bind_projections(self, projections: tuple[ta.Projection, ...]) -> None:
        for projection in projections:
            self._bind_expr(projection.expr)

    def _bind_prop_map(self, prop_map: ta.PropMap) -> None:
        for entry in prop_map.entries:
            self._bind_expr(entry.value)

    def _bind_type_refs(self, value: Any) -> None:
        for item in value:
            if isinstance(item, ta.TypeRef):
                self._bind_type_ref(item)

    def _bind_type_ref(self, type_ref: ta.TypeRef) -> None:
        if isinstance(type_ref.name, ta.QName | ta.Iri):
            self.resolve_name(type_ref.name)
        for param in type_ref.params:
            if isinstance(param, ta.TypeRef):
                self._bind_type_ref(param)

    def _bind_target(self, target: ta.NameRef | ta.Ident | None) -> None:
        if isinstance(target, ta.QName | ta.Iri):
            self.resolve_name(target)

    def _error(
        self,
        code: str,
        message: str,
        *,
        span: ta.Span | None,
        hint: str | None = None,
    ) -> CaracalError:
        return CaracalError(
            code=code,
            message=message,
            span=span,
            hint=hint,
            source_name=self.source_name,
            source_text=self.source_text,
        )


def bind_program(
    program: ta.Program,
    catalog: Catalog,
    *,
    prefixes: dict[str, str] | None = None,
    source_name: str | None = None,
    source_text: str | None = None,
) -> BoundProgram:
    return Binder(
        catalog=catalog,
        prefixes=dict(prefixes or {}),
        source_name=source_name,
        source_text=source_text,
    ).bind(program)


__all__ = ["Binder", "BoundName", "BoundProgram", "bind_program"]
