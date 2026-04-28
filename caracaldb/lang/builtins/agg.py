"""Aggregate / collection built-ins (01 §8.5-8.6 subset).

Aggregate functions are recognised here for binding/typing; their actual
dispatch is performed by ``HashAggregateOperator`` (CDB-042) which calls the
matching Arrow group-aggregate kernel.
"""

from __future__ import annotations

import pyarrow.compute as pc

from caracaldb.lang.builtins.scalar import BuiltinFn


def _size(args):
    return pc.list_value_length(args[0])


def _head(args):
    # Take first element of each list, return null when empty.
    return pc.list_element(args[0], 0)


def _last(args):
    n = pc.list_value_length(args[0])
    indices = pc.subtract(n, 1)
    return pc.list_element(args[0], indices)


def _sort_list(args):
    return pc.list_sort(args[0])


def _agg_unsupported(args):  # pragma: no cover
    raise NotImplementedError("aggregate dispatch happens via HashAggregateOperator")


def _make(name: str, arity, kind: str, fn) -> BuiltinFn:
    return BuiltinFn(name=name, arity=arity, kind=kind, dispatch=fn)


AGG_FUNCTIONS: dict[str, BuiltinFn] = {
    fn.name: fn
    for fn in [
        _make("size", 1, "scalar", _size),
        _make("head", 1, "scalar", _head),
        _make("last", 1, "scalar", _last),
        _make("sort", 1, "scalar", _sort_list),
        # The following are recognised at bind time; their dispatch happens
        # in HashAggregateOperator (CDB-042). The placeholder lambda asserts.
        _make("count", (0, 1), "agg", _agg_unsupported),
        _make("sum", 1, "agg", _agg_unsupported),
        _make("avg", 1, "agg", _agg_unsupported),
        _make("mean", 1, "agg", _agg_unsupported),
        _make("min", 1, "agg", _agg_unsupported),
        _make("max", 1, "agg", _agg_unsupported),
        _make("collect", 1, "agg", _agg_unsupported),
        _make("stddev", 1, "agg", _agg_unsupported),
        _make("percentile", 2, "agg", _agg_unsupported),
    ]
}


__all__ = ["AGG_FUNCTIONS"]
