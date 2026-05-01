"""Tiny expression evaluator for the M1 vertical slice.

Expressions are encoded as nested tuples — see ``caracaldb.plan.rules`` — so
the executor and the rule layer share one wire format. ``compile_expr`` turns
a tuple into a callable ``(pa.RecordBatch) -> pa.Array`` that returns either a
boolean mask (for predicates) or a typed value column (for projections).

Supported forms (M1):
    ("col", name)                         column reference
    ("lit", value)                        scalar literal
    ("eq" | "ne" | "lt" | "le" | "gt" | "ge", left, right)
    ("and" | "or", left, right)
    ("not", operand)
    ("in", left, ("lit", [v1, v2, ...]))   membership test

Higher-order machinery (function calls, casts, vector functions) lands in M2
alongside the binder + builtin registry.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from caracaldb.lang.diagnostics import CaracalError

ExprFn = Callable[[pa.RecordBatch], pa.Array]

_BIN_OPS: dict[str, Callable[[Any, Any], pa.Array]] = {
    "eq": pc.equal,
    "ne": pc.not_equal,
    "lt": pc.less,
    "le": pc.less_equal,
    "gt": pc.greater,
    "ge": pc.greater_equal,
}


def compile_expr(expr: object) -> ExprFn:
    if not isinstance(expr, tuple) or not expr:
        raise CaracalError(code="CDB-6010", message=f"unsupported expression: {expr!r}")
    head = expr[0]
    if head == "col":
        if len(expr) != 2 or not isinstance(expr[1], str):
            raise CaracalError(code="CDB-6010", message=f"malformed column ref: {expr!r}")
        name = expr[1]
        return lambda batch: batch.column(name)
    if head == "lit":
        if len(expr) != 2:
            raise CaracalError(code="CDB-6010", message=f"malformed literal: {expr!r}")
        scalar = pa.scalar(expr[1])
        return lambda batch, _s=scalar: pa.array([_s.as_py()] * batch.num_rows)
    if head in _BIN_OPS:
        if len(expr) != 3:
            raise CaracalError(code="CDB-6010", message=f"binary op needs 2 operands: {expr!r}")
        left = compile_expr(expr[1])
        right = compile_expr(expr[2])
        op = _BIN_OPS[head]
        return lambda batch: op(left(batch), right(batch))
    if head in ("and", "or"):
        if len(expr) != 3:
            raise CaracalError(code="CDB-6010", message=f"{head} needs 2 operands: {expr!r}")
        left = compile_expr(expr[1])
        right = compile_expr(expr[2])
        join = pc.and_ if head == "and" else pc.or_
        return lambda batch: join(left(batch), right(batch))
    if head == "not":
        if len(expr) != 2:
            raise CaracalError(code="CDB-6010", message=f"not needs 1 operand: {expr!r}")
        inner = compile_expr(expr[1])
        return lambda batch: pc.invert(inner(batch))
    if head == "py_unary":
        # ("py_unary", callable, child_expr) — apply a pre-bound Python
        # function to a single column. Used by graph built-ins like
        # ``degree(alias, "rel")`` where the planner pre-computes a
        # gid-indexed lookup array and binds it via closure.
        if len(expr) != 3 or not callable(expr[1]):
            raise CaracalError(code="CDB-6010", message=f"malformed py_unary: {expr!r}")
        fn = expr[1]
        child = compile_expr(expr[2])
        return lambda batch: fn(child(batch))
    if head == "in":
        if (
            len(expr) != 3
            or not isinstance(expr[2], tuple)
            or expr[2][0] != "lit"
            or not isinstance(expr[2][1], (list, tuple))
        ):
            raise CaracalError(code="CDB-6010", message=f"malformed 'in': {expr!r}")
        left = compile_expr(expr[1])
        value_set = pa.array(list(expr[2][1]))
        return lambda batch: pc.is_in(left(batch), value_set=value_set)
    raise CaracalError(code="CDB-6010", message=f"unknown expression head: {head!r}")


__all__ = ["ExprFn", "compile_expr"]
