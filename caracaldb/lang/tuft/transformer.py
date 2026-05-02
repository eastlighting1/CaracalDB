"""Lark parse-tree to Tuft AST transformer."""

from __future__ import annotations

import ast as py_ast
from pathlib import Path
from typing import Any, cast

from lark import Token, Transformer

from caracaldb.lang.tuft import ast as ta


def _tuple(items: list[Any]) -> tuple[Any, ...]:
    return tuple(item for item in items if item is not None)


def _text(value: Any) -> str:
    if isinstance(value, Token):
        return str(value)
    if isinstance(value, ta.Ident):
        return value.name
    if isinstance(value, ta.QName | ta.Iri):
        return value.value
    return str(value)


def _strip_qname(value: ta.NameRef | ta.Ident) -> str:
    if isinstance(value, ta.Ident):
        return value.name
    return value.value


def _span(token: Token | None) -> ta.Span | None:
    if token is None:
        return None
    return ta.Span(start=token.start_pos or 0, end=token.end_pos or 0)


def _load_grammar() -> str:
    return Path(__file__).with_name("tuft.lark").read_text(encoding="utf-8")


class TuftTransformer(Transformer[Any, Any]):
    """Build a lightweight, binder-ready AST from the Tuft parse tree."""

    def start(self, items: list[Any]) -> ta.Program:
        return cast(ta.Program, items[0])

    def stmt_list(self, items: list[Any]) -> ta.Program:
        statements: list[ta.Stmt] = []
        for item in items:
            if isinstance(item, ta.Query):
                statements.append(ta.QueryStmt(query=item))
            elif isinstance(item, ta.Stmt):
                statements.append(item)
            elif isinstance(item, ta.PrefixDecl):
                statements.append(ta.DdlStmt(op="prefix", payload={"decl": item}))
        return ta.Program(statements=tuple(statements))

    # ------------------------------------------------------------------
    # Tokens and names

    def IDENT(self, token: Token) -> ta.Ident:
        return ta.Ident(str(token), span=_span(token))

    def ESCAPED_IDENT(self, token: Token) -> ta.Ident:
        return ta.Ident(str(token)[1:-1], span=_span(token), escaped=True)

    def PREFIXED_NAME(self, token: Token) -> ta.QName:
        return ta.QName(str(token), span=_span(token))

    def IRI(self, token: Token) -> ta.Iri:
        return ta.Iri(str(token)[1:-1], span=_span(token))

    def SPARQL_VAR(self, token: Token) -> ta.Var:
        name = str(token)[1:]
        return ta.Var(name=ta.Ident(name, span=_span(token)), span=_span(token))

    def STRING(self, token: Token) -> str:
        return cast(str, py_ast.literal_eval(str(token)))

    def INT(self, token: Token) -> int:
        text = str(token).replace("_", "")
        return int(text, 0)

    def FLOAT(self, token: Token) -> float:
        text = str(token).replace("_", "")
        if text.endswith("f32"):
            text = text[:-3]
        return float(text)

    def DECIMAL(self, token: Token) -> str:
        return str(token).replace("_", "")

    def LANGTAG(self, token: Token) -> str:
        return str(token)[1:]

    def BOOL_LIT(self, token: Token) -> bool:
        return str(token).lower() == "true"

    def NULL_LIT(self, _token: Token) -> None:
        return None

    def PRIMITIVE_TYPE(self, token: Token) -> str:
        return str(token)

    def CONSTRAINT(self, token: Token) -> str:
        return str(token)

    def PROPERTY_KIND(self, token: Token) -> str:
        return str(token)

    def CHARACTERISTIC(self, token: Token) -> str:
        return str(token)

    def INDEX_KIND(self, token: Token) -> str:
        return str(token)

    def SOURCE_KIND(self, token: Token) -> str:
        return str(token)

    def TX_OP(self, token: Token) -> str:
        return str(token)

    def EXPORT_FORMAT(self, token: Token) -> str:
        return str(token)

    def ORDER_DIR(self, token: Token) -> str:
        return str(token)

    def EQUAL(self, token: Token) -> str:
        return str(token)

    def COMP_OP(self, token: Token) -> str:
        return str(token)

    def IN_OP(self, token: Token) -> str:
        return str(token)

    def OR_OP(self, token: Token) -> str:
        return str(token)

    def AND_OP(self, token: Token) -> str:
        return str(token)

    def NOT_OP(self, token: Token) -> str:
        return str(token)

    def ADD_OP(self, token: Token) -> str:
        return str(token)

    def MUL_OP(self, token: Token) -> str:
        return str(token)

    def SIGN_OP(self, token: Token) -> str:
        return str(token)

    def A_PRED(self, token: Token) -> str:
        return str(token)

    def ident(self, items: list[Any]) -> ta.Ident:
        return cast(ta.Ident, items[0])

    def prefix_name(self, items: list[Any]) -> str:
        return _text(items[0])

    def iri_ref(self, items: list[Any]) -> ta.Iri:
        return cast(ta.Iri, items[0])

    def default_qname(self, items: list[Any]) -> ta.QName:
        return ta.QName(f":{_text(items[0])}")

    def comp_operator(self, items: list[Any]) -> str:
        return cast(str, items[0])

    def qname(self, items: list[Any]) -> ta.QName:
        item = items[0]
        if isinstance(item, ta.QName):
            return item
        return ta.QName(_text(item))

    def ref(self, items: list[Any]) -> ta.NameRef:
        return cast(ta.NameRef, items[0])

    def class_ref(self, items: list[Any]) -> ta.NameRef:
        return cast(ta.NameRef, items[0])

    def prop_ref(self, items: list[Any]) -> ta.NameRef:
        return cast(ta.NameRef, items[0])

    def label_ref(self, items: list[Any]) -> ta.NameRef:
        return cast(ta.NameRef, items[0])

    def ident_list(self, items: list[Any]) -> tuple[ta.Ident, ...]:
        return tuple(items)

    def qname_list(self, items: list[Any]) -> tuple[ta.QName, ...]:
        return tuple(items)

    def class_ref_list(self, items: list[Any]) -> tuple[ta.NameRef, ...]:
        return tuple(items)

    def type_ref_list(self, items: list[Any]) -> tuple[ta.TypeRef, ...]:
        return tuple(items)

    # ------------------------------------------------------------------
    # DDL / DML / utility statements

    def prefix_decl(self, items: list[Any]) -> ta.DdlStmt:
        decl = ta.PrefixDecl(prefix=items[0], iri=items[1])
        return ta.DdlStmt(op="prefix", payload={"decl": decl})

    def base_decl(self, items: list[Any]) -> ta.DdlStmt:
        return ta.DdlStmt(op="base", target=items[0])

    def create_ontology(self, items: list[Any]) -> ta.DdlStmt:
        payload: dict[str, Any] = {}
        if len(items) > 1:
            payload["base"] = items[1]
        return ta.DdlStmt(op="create_ontology", target=items[0], payload=payload)

    def create_graph(self, items: list[Any]) -> ta.DdlStmt:
        target = items[0]
        payload = {"ontologies": items[1] if len(items) > 1 else ()}
        return ta.DdlStmt(op="create_graph", target=target, payload=payload)

    def create_class(self, items: list[Any]) -> ta.DdlStmt:
        payload: dict[str, Any] = {}
        for item in items[1:]:
            if isinstance(item, tuple) and item and isinstance(item[0], ta.NameRef):
                payload["subclasses_of"] = item
            elif isinstance(item, list | tuple):
                payload["properties"] = tuple(item)
        return ta.DdlStmt(op="create_class", target=items[0], payload=payload)

    def subclass_clause(self, items: list[Any]) -> tuple[ta.NameRef, ...]:
        return cast(tuple[ta.NameRef, ...], items[0])

    def class_props(self, items: list[Any]) -> tuple[dict[str, Any], ...]:
        return tuple(items)

    def property_field(self, items: list[Any]) -> dict[str, Any]:
        return {"name": items[0], "type": items[1], "constraints": tuple(items[2:])}

    def property_constraint(self, items: list[Any]) -> Any:
        return items[0]

    def default_clause(self, items: list[Any]) -> dict[str, Any]:
        return {"default": items[0]}

    def check_clause(self, items: list[Any]) -> dict[str, Any]:
        return {"check": items[0]}

    def create_property(self, items: list[Any]) -> ta.DdlStmt:
        payload: dict[str, Any] = {"kind": items[1]}
        for item in items[2:]:
            if isinstance(item, tuple) and item:
                tag = item[0]
                if tag == "domain":
                    payload["domain"] = item[1]
                elif tag == "range":
                    payload["range"] = item[1]
                elif tag == "properties":
                    payload["properties"] = item[1]
                elif tag == "characteristics":
                    payload["characteristics"] = item[1]
        return ta.DdlStmt(op="create_property", target=items[0], payload=payload)

    def property_kind(self, items: list[Any]) -> str:
        return cast(str, items[0])

    def property_domain(self, items: list[Any]) -> tuple[str, tuple[ta.NameRef, ...]]:
        return ("domain", cast(tuple[ta.NameRef, ...], items[0]))

    def property_range(self, items: list[Any]) -> tuple[str, tuple[ta.TypeRef, ...]]:
        return ("range", cast(tuple[ta.TypeRef, ...], items[0]))

    def property_props(self, items: list[Any]) -> tuple[str, tuple[dict[str, Any], ...]]:
        return ("properties", tuple(items))

    def property_characteristics(self, items: list[Any]) -> tuple[str, tuple[str, ...]]:
        return ("characteristics", tuple(items))

    def characteristic(self, items: list[Any]) -> str:
        return cast(str, items[0])

    def tx(self, items: list[Any]) -> ta.TxStmt:
        return ta.TxStmt(op=items[0].lower())

    def infer_stmt(self, items: list[Any]) -> ta.UtilStmt:
        return ta.UtilStmt(op="infer_closure", payload={"rules": items[0], "graph": items[1]})

    def snapshot_stmt(self, items: list[Any]) -> ta.UtilStmt:
        return ta.UtilStmt(op="create_snapshot", payload={"name": items[0]})

    def explain_stmt(self, items: list[Any]) -> ta.UtilStmt:
        return ta.UtilStmt(op="explain", query=items[0])

    def profile_stmt(self, items: list[Any]) -> ta.UtilStmt:
        return ta.UtilStmt(op="profile", query=items[0])

    def insert_triples(self, items: list[Any]) -> ta.DmlStmt:
        return ta.DmlStmt(op="insert_triples", payload={"triples": items[0]})

    def insert_stmt(self, items: list[Any]) -> ta.DmlStmt:
        return cast(ta.DmlStmt, items[0])

    def insert_pattern(self, items: list[Any]) -> ta.DmlStmt:
        patterns = items[0]
        return ta.DmlStmt(op="insert_pattern", pattern=patterns[0], payload={"patterns": patterns})

    # ------------------------------------------------------------------
    # Query clauses

    def dql(self, items: list[Any]) -> ta.Query:
        clauses = items[0]
        modifiers = next(
            (item for item in items[1:] if isinstance(item, ta.Modifiers)), ta.Modifiers()
        )
        return ta.Query(clauses=clauses, modifiers=modifiers)

    def query_body(self, items: list[Any]) -> tuple[ta.Clause, ...]:
        return tuple(items)

    def match_clause(self, items: list[Any]) -> ta.MatchClause:
        patterns = items[0]
        as_of = items[1] if len(items) > 1 else None
        return ta.MatchClause(patterns=patterns, as_of=as_of)

    def optional_match_clause(self, items: list[Any]) -> ta.MatchClause:
        patterns = items[0]
        as_of = items[1] if len(items) > 1 else None
        return ta.MatchClause(patterns=patterns, optional=True, as_of=as_of)

    def where_clause(self, items: list[Any]) -> ta.WhereClause:
        return ta.WhereClause(predicate=items[0])

    def with_clause(self, items: list[Any]) -> ta.WithClause:
        where = (
            items[1].predicate if len(items) > 1 and isinstance(items[1], ta.WhereClause) else None
        )
        return ta.WithClause(projections=items[0], where=where)

    def return_clause(self, items: list[Any]) -> ta.ReturnClause:
        return ta.ReturnClause(projections=items[-1])

    def projection_list(self, items: list[Any]) -> tuple[ta.Projection, ...]:
        return tuple(items)

    def projection(self, items: list[Any]) -> ta.Projection:
        alias = items[1] if len(items) > 1 else None
        return ta.Projection(expr=items[0], alias=alias)

    def set_clause(self, items: list[Any]) -> ta.SetClause:
        return ta.SetClause(items=tuple(items))

    def set_item(self, items: list[Any]) -> ta.SetItem:
        return ta.SetItem(target=items[0], value=items[1])

    def call_clause(self, items: list[Any]) -> ta.CallClause:
        fn = items[0]
        args: tuple[ta.Expr, ...] = ()
        yield_items: tuple[ta.Ident, ...] = ()
        for item in items[1:]:
            if isinstance(item, tuple) and all(isinstance(x, ta.Ident) for x in item):
                yield_items = item
            elif isinstance(item, tuple):
                args = item
        return ta.CallClause(function=fn, args=args, yield_items=yield_items)

    def yield_clause(self, items: list[Any]) -> tuple[ta.Ident, ...]:
        return cast(tuple[ta.Ident, ...], items[0])

    def unwind_clause(self, items: list[Any]) -> ta.UnwindClause:
        return ta.UnwindClause(expr=items[0], alias=items[1])

    def delete_clause(self, items: list[Any]) -> ta.DeleteClause:
        return ta.DeleteClause(exprs=items[-1])

    def as_of(self, items: list[Any]) -> ta.AsOf:
        node = items[0]
        # Both ``string_lit`` and ``datetime_lit`` produce ``ta.Literal``; the
        # underlying ``.value`` is the unquoted payload (snapshot name or
        # ISO-8601 datetime). We carry that scalar through verbatim so engine
        # consumers don't have to re-parse a Literal repr.
        if isinstance(node, ta.Literal):
            kind = "datetime" if node.kind is ta.LiteralKind.DATETIME else "snapshot"
            return ta.AsOf(kind=kind, value=str(node.value))
        return ta.AsOf(kind="snapshot" if isinstance(node, str) else "datetime", value=_text(node))

    def modifiers(self, items: list[Any]) -> ta.Modifiers:
        order_by: tuple[ta.OrderItem, ...] = ()
        skip: ta.Expr | None = None
        limit: ta.Expr | None = None
        for item in items:
            if isinstance(item, tuple) and item and isinstance(item[0], ta.OrderItem):
                order_by = item
            elif isinstance(item, tuple) and item[0] == "skip":
                skip = item[1]
            elif isinstance(item, tuple) and item[0] == "limit":
                limit = item[1]
        return ta.Modifiers(order_by=order_by, skip=skip, limit=limit)

    def order_clause(self, items: list[Any]) -> tuple[ta.OrderItem, ...]:
        return tuple(items)

    def order_item(self, items: list[Any]) -> ta.OrderItem:
        descending = len(items) > 1 and str(items[1]).upper() == "DESC"
        return ta.OrderItem(expr=items[0], descending=descending)

    def skip_clause(self, items: list[Any]) -> tuple[str, ta.Expr]:
        return ("skip", items[0])

    def limit_clause(self, items: list[Any]) -> tuple[str, ta.Expr]:
        return ("limit", items[0])

    # ------------------------------------------------------------------
    # Patterns

    def pattern_list(self, items: list[Any]) -> tuple[ta.Pattern, ...]:
        return tuple(items)

    def pattern(self, items: list[Any]) -> ta.Pattern:
        binding = items[0] if items and isinstance(items[0], ta.Ident) else None
        chain = items[1] if binding is not None else items[0]
        return ta.Pattern(binding=binding, elements=tuple(chain))

    def path_binding(self, items: list[Any]) -> ta.Ident:
        return cast(ta.Ident, items[0])

    def pattern_chain(self, items: list[Any]) -> tuple[ta.PatternElem, ...]:
        return tuple(items)

    def pattern_elem(self, items: list[Any]) -> ta.PatternElem:
        return cast(ta.PatternElem, items[0])

    def node_pattern(self, items: list[Any]) -> ta.NodePattern:
        var: ta.Ident | None = None
        labels: tuple[ta.NameRef, ...] = ()
        props: ta.PropMap | None = None
        for item in items:
            if isinstance(item, ta.Ident):
                var = item
            elif isinstance(item, tuple):
                labels = item
            elif isinstance(item, ta.PropMap):
                props = item
        return ta.NodePattern(var=var, labels=labels, props=props)

    def label_list(self, items: list[Any]) -> tuple[ta.NameRef, ...]:
        return tuple(items)

    def rel_type_list(self, items: list[Any]) -> tuple[ta.NameRef, ...]:
        return tuple(items)

    def left_arrow(self, _items: list[Any]) -> str:
        return "<"

    def right_arrow(self, _items: list[Any]) -> str:
        return ">"

    def rel(self, items: list[Any]) -> ta.RelPattern:
        rel = next(item for item in items if isinstance(item, ta.RelPattern))
        left = "<" in items
        right = ">" in items
        direction = ta.Direction.BOTH
        if right:
            direction = ta.Direction.OUT
        elif left:
            direction = ta.Direction.IN
        return ta.RelPattern(
            var=rel.var,
            types=rel.types,
            direction=direction,
            hop_range=rel.hop_range,
            props=rel.props,
        )

    def rel_pattern(self, items: list[Any]) -> ta.RelPattern:
        var: ta.Ident | None = None
        rel_types: tuple[ta.NameRef, ...] = ()
        hop_range = ta.HopRange()
        props: ta.PropMap | None = None
        for item in items:
            if isinstance(item, ta.Ident):
                var = item
            elif isinstance(item, tuple):
                rel_types = item
            elif isinstance(item, ta.HopRange):
                hop_range = item
            elif isinstance(item, ta.PropMap):
                props = item
        return ta.RelPattern(var=var, types=rel_types, hop_range=hop_range, props=props)

    def hop_range(self, items: list[Any]) -> ta.HopRange:
        if not items:
            return ta.HopRange()
        if len(items) == 1:
            return ta.HopRange(min_hops=items[0], max_hops=items[0])
        return ta.HopRange(min_hops=items[0], max_hops=items[1])

    def prop_map(self, items: list[Any]) -> ta.PropMap:
        return ta.PropMap(entries=tuple(items))

    def prop_entry(self, items: list[Any]) -> ta.PropEntry:
        return ta.PropEntry(key=items[0], value=items[1])

    # ------------------------------------------------------------------
    # Expressions

    def expr_list(self, items: list[Any]) -> tuple[ta.Expr, ...]:
        return tuple(items)

    def path_expr(self, items: list[Any]) -> ta.Expr:
        if len(items) == 1:
            return ta.Var(name=items[0])
        return ta.PathExpr(root=items[0], steps=tuple(items[1:]))

    def function_call(self, items: list[Any]) -> ta.FnCall:
        args = items[1] if len(items) > 1 else ()
        return ta.FnCall(name=items[0], args=args)

    def bin_op(self, items: list[Any]) -> ta.BinOp:
        return ta.BinOp(left=items[0], op=str(items[1]), right=items[2])

    def unary_op(self, items: list[Any]) -> ta.UnaryOp:
        return ta.UnaryOp(op=str(items[0]), operand=items[1])

    def is_null(self, items: list[Any]) -> ta.BinOp:
        return ta.BinOp(left=items[0], op="IS NULL", right=ta.Literal())

    def is_not_null(self, items: list[Any]) -> ta.BinOp:
        return ta.BinOp(left=items[0], op="IS NOT NULL", right=ta.Literal())

    def subclassof_star(self, items: list[Any]) -> ta.BinOp:
        return ta.BinOp(left=items[0], op="SUBCLASSOF*", right=items[1])

    def subpropertyof_star(self, items: list[Any]) -> ta.BinOp:
        return ta.BinOp(left=items[0], op="SUBPROPERTYOF*", right=items[1])

    def attr(self, items: list[Any]) -> ta.PathExpr:
        target, step = items
        if isinstance(target, ta.Var) and target.name is not None:
            return ta.PathExpr(root=target.name, steps=(step,))
        if isinstance(target, ta.PathExpr) and target.root is not None:
            return ta.PathExpr(root=target.root, steps=target.steps + (step,))
        return ta.PathExpr(root=ta.Ident(_text(target)), steps=(step,))

    def subscript(self, items: list[Any]) -> ta.Subscript:
        return ta.Subscript(target=items[0], index=items[1])

    def cast(self, items: list[Any]) -> ta.Cast:
        return ta.Cast(expr=items[0], type_ref=items[1])

    def list_lit(self, items: list[Any]) -> ta.ListExpr:
        if len(items) == 1 and isinstance(items[0], tuple):
            return ta.ListExpr(items=items[0])
        return ta.ListExpr(items=tuple(items))

    def map_lit(self, items: list[Any]) -> ta.MapExpr:
        return ta.MapExpr(entries=tuple(items))

    def map_entry(self, items: list[Any]) -> tuple[str, ta.Expr]:
        return (_text(items[0]), items[1])

    def number(self, items: list[Any]) -> ta.Literal:
        value = items[0]
        kind = ta.LiteralKind.INT
        if isinstance(value, float):
            kind = ta.LiteralKind.FLOAT
        elif isinstance(value, str):
            kind = ta.LiteralKind.DECIMAL
        return ta.Literal(kind=kind, value=value)

    def string_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.STRING, value=items[0])

    def bool_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.BOOL, value=items[0])

    def null_lit(self, _items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.NULL, value=None)

    def date_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.DATE, value=items[0])

    def time_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.TIME, value=items[0])

    def datetime_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.DATETIME, value=items[0])

    def duration_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.DURATION, value=items[0])

    def typed_string_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.TYPED_STRING, value=items[0], datatype=items[1])

    def lang_string_lit(self, items: list[Any]) -> ta.Literal:
        return ta.Literal(kind=ta.LiteralKind.LANG_STRING, value=items[0], lang=items[1])

    # ------------------------------------------------------------------
    # Types

    def primitive_type(self, items: list[Any]) -> ta.TypeRef:
        params = tuple(items[1]) if len(items) > 1 else ()
        return ta.TypeRef(name=items[0], params=params)

    def decimal_params(self, items: list[Any]) -> tuple[int, int]:
        return (items[0], items[1])

    def list_type(self, items: list[Any]) -> ta.TypeRef:
        return ta.TypeRef(name="LIST", params=(items[0],))

    def map_type(self, items: list[Any]) -> ta.TypeRef:
        return ta.TypeRef(name="MAP", params=(items[0], items[1]))

    def vector_type(self, items: list[Any]) -> ta.TypeRef:
        return ta.TypeRef(name="VECTOR", params=(items[0], items[1]))

    def matrix_type(self, items: list[Any]) -> ta.TypeRef:
        return ta.TypeRef(name="MATRIX", params=(items[0], items[1], items[2]))

    def struct_field(self, items: list[Any]) -> tuple[str, ta.TypeRef]:
        return (_text(items[0]), items[1])

    def struct_type(self, items: list[Any]) -> ta.TypeRef:
        return ta.TypeRef(name="STRUCT", params=tuple(items))

    def union_type(self, items: list[Any]) -> ta.TypeRef:
        return ta.TypeRef(name="UNION", params=tuple(items))

    # ------------------------------------------------------------------
    # Triples

    def triple_block(self, items: list[Any]) -> tuple[ta.TriplePattern, ...]:
        return tuple(items)

    def triple_pattern(self, items: list[Any]) -> ta.TriplePattern:
        return ta.TriplePattern(subject=items[0], predicate=items[1], object=items[2])

    def triple_subject(self, items: list[Any]) -> Any:
        return items[0]

    def triple_predicate(self, items: list[Any]) -> Any:
        return items[0]

    def triple_object(self, items: list[Any]) -> Any:
        return items[0]

    def sparql_var(self, items: list[Any]) -> ta.Var:
        return cast(ta.Var, items[0])


__all__ = ["TuftTransformer", "_load_grammar"]
