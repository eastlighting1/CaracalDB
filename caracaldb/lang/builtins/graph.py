"""Graph topology built-ins.

These functions are bound at parse time; runtime dispatch needs the active
``CsrReader`` and is performed by the executor through a context-aware
``compile_expr`` extension. Right now we only register their names + arities
so the binder accepts ``degree(n)`` / ``neighbors(n)`` / ``shortest_path(a,b)``
/ ``k_hop(n, k)`` calls without flagging them as unknown.
"""

from __future__ import annotations

from caracaldb.lang.builtins.scalar import BuiltinFn


def _ctx_only(_args):  # pragma: no cover
    raise NotImplementedError("graph topology dispatch needs a CsrReader-bound exec context (M3)")


def _ctx_only_with_csr(args):  # pragma: no cover - wired up at exec layer
    raise NotImplementedError("graph topology dispatch needs a CsrReader-bound exec context")


def _make(name: str, arity, fn=_ctx_only) -> BuiltinFn:
    return BuiltinFn(name=name, arity=arity, kind="graph", dispatch=fn)


GRAPH_FUNCTIONS: dict[str, BuiltinFn] = {
    fn.name: fn
    for fn in [
        _make("degree", (1, 2)),
        _make("neighbors", (1, 2)),
        _make("shortest_path", (2, 3)),
        _make("k_hop", 2),
    ]
}


__all__ = ["GRAPH_FUNCTIONS"]
