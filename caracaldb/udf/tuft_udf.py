"""Pure Tuft UDFs (`DEFINE FUNCTION ... AS <expr>`).

A Tuft UDF is a parameterised expression compiled through the same tuple-IR
pipeline that powers ``compile_expr``. Calling a UDF substitutes the bound
arguments into the expression body and evaluates the result against the
provided ``RecordBatch``. Pure Tuft UDFs MUST NOT call into Python — that
path belongs to ``py_udf.PyUdf``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from caracaldb.exec.expr import compile_expr
from caracaldb.lang.diagnostics import CaracalError


def _substitute(expr: Any, mapping: dict[str, Any]) -> Any:
    if isinstance(expr, tuple) and expr:
        head = expr[0]
        if head == "param" and len(expr) >= 2 and isinstance(expr[1], str):
            if expr[1] not in mapping:
                raise CaracalError(code="CDB-6130", message=f"unbound UDF parameter: {expr[1]!r}")
            return mapping[expr[1]]
        return tuple(_substitute(child, mapping) for child in expr)
    return expr


@dataclass(slots=True)
class TuftUdf:
    name: str
    params: tuple[str, ...]
    body: Any  # tuple-IR expression with ``("param", name)`` placeholders

    def __call__(self, batch: pa.RecordBatch, *args: Any) -> pa.Array:
        if len(args) != len(self.params):
            raise CaracalError(
                code="CDB-6130",
                message=f"{self.name}() expects {len(self.params)} args, got {len(args)}",
            )
        mapping = dict(zip(self.params, args, strict=True))
        compiled = compile_expr(_substitute(self.body, mapping))
        return compiled(batch)


def define_tuft_udf(name: str, params: Sequence[str], body: Any) -> TuftUdf:
    return TuftUdf(name=name, params=tuple(params), body=body)


__all__ = ["TuftUdf", "define_tuft_udf"]
