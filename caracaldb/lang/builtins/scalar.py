"""Scalar / string / time built-ins (01 §8.1-8.3 subset)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from caracaldb.lang.diagnostics import CaracalError


@dataclass(frozen=True, slots=True)
class BuiltinFn:
    name: str
    arity: int | tuple[int, int]  # exact arity or (min, max)
    kind: str  # "scalar" | "agg" | "graph"
    dispatch: Callable[[list[pa.Array]], pa.Array]

    def check_arity(self, n: int) -> None:
        if isinstance(self.arity, int):
            if n != self.arity:
                raise CaracalError(
                    code="CDB-6060",
                    message=f"{self.name}() expects {self.arity} arg(s), got {n}",
                )
        else:
            lo, hi = self.arity
            if not (lo <= n <= hi):
                raise CaracalError(
                    code="CDB-6060",
                    message=f"{self.name}() expects {lo}..{hi} args, got {n}",
                )


def _make(
    name: str, arity: int | tuple[int, int], fn: Callable[[list[pa.Array]], pa.Array]
) -> BuiltinFn:
    return BuiltinFn(name=name, arity=arity, kind="scalar", dispatch=fn)


# Numeric -------------------------------------------------------------------
def _abs(args):
    return pc.abs(args[0])


def _round(args):
    return pc.round(args[0])


def _ceil(args):
    return pc.ceil(args[0])


def _floor(args):
    return pc.floor(args[0])


# String --------------------------------------------------------------------
def _length(args):
    return pc.utf8_length(args[0])


def _upper(args):
    return pc.utf8_upper(args[0])


def _lower(args):
    return pc.utf8_lower(args[0])


def _starts_with(args):
    pattern = args[1].to_pylist()[0] if hasattr(args[1], "to_pylist") else args[1]
    return pc.starts_with(args[0], pattern=pattern)


def _ends_with(args):
    pattern = args[1].to_pylist()[0] if hasattr(args[1], "to_pylist") else args[1]
    return pc.ends_with(args[0], pattern=pattern)


def _contains(args):
    pattern = args[1].to_pylist()[0] if hasattr(args[1], "to_pylist") else args[1]
    return pc.match_substring(args[0], pattern=pattern)


def _coalesce(args):
    out = args[0]
    for arr in args[1:]:
        out = pc.coalesce(out, arr)
    return out


# Time ----------------------------------------------------------------------
def _year(args):
    return pc.year(args[0])


def _month(args):
    return pc.month(args[0])


def _day(args):
    return pc.day(args[0])


SCALAR_FUNCTIONS: dict[str, BuiltinFn] = {
    fn.name: fn
    for fn in [
        _make("abs", 1, _abs),
        _make("round", 1, _round),
        _make("ceil", 1, _ceil),
        _make("floor", 1, _floor),
        _make("length", 1, _length),
        _make("upper", 1, _upper),
        _make("lower", 1, _lower),
        _make("starts_with", 2, _starts_with),
        _make("ends_with", 2, _ends_with),
        _make("contains", 2, _contains),
        _make("coalesce", (1, 8), _coalesce),
        _make("year", 1, _year),
        _make("month", 1, _month),
        _make("day", 1, _day),
    ]
}


__all__ = ["BuiltinFn", "SCALAR_FUNCTIONS"]
