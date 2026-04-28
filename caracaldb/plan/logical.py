"""Logical plan tree.

Logical operators are pure data — they describe *what* the engine should
compute, not *how*. The M1 surface covers the vertical slice needed for
``MATCH (n:Class) WHERE ... RETURN ... LIMIT k``: NodeScan, Selection,
Projection, Aggregate, OrderBy, Limit. Pattern compilation
(NodeScan + Expand + Join) lands in M2 (CDB-045).

Predicates and projection items are kept opaque (``object``) at this layer; the
binder/typer/expr-compiler decide what they mean. The execution engine consumes
the tree via ``children()`` and pattern-matching, so adding new ops only
requires extending the ``LogicalOp`` hierarchy.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class LogicalOp:
    """Marker base; subclasses define their own ``children()``."""

    def children(self) -> tuple[LogicalOp, ...]:
        return ()


@dataclass(frozen=True, slots=True)
class LNodeScan(LogicalOp):
    class_iri: str
    local_name: str
    alias: str = "n"
    columns: tuple[str, ...] | None = None  # None = all columns


@dataclass(frozen=True, slots=True)
class LSelection(LogicalOp):
    child: LogicalOp
    predicate: object  # Expr; kept opaque for layering

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.child,)


@dataclass(frozen=True, slots=True)
class LProject(LogicalOp):
    child: LogicalOp
    projections: tuple[tuple[object, str], ...]  # (expr, output_name)

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.child,)


@dataclass(frozen=True, slots=True)
class LAggregate(LogicalOp):
    child: LogicalOp
    group_keys: tuple[object, ...] = ()
    aggregates: tuple[tuple[str, object, str], ...] = ()  # (fn, expr, output)

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.child,)


@dataclass(frozen=True, slots=True)
class LOrderBy(LogicalOp):
    child: LogicalOp
    keys: tuple[tuple[object, bool], ...] = field(default_factory=tuple)  # (expr, descending)

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.child,)


@dataclass(frozen=True, slots=True)
class LLimit(LogicalOp):
    child: LogicalOp
    limit: int
    offset: int = 0

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.child,)


def walk(plan: LogicalOp) -> Iterator[LogicalOp]:
    """Pre-order traversal of a logical plan tree."""
    yield plan
    for child in plan.children():
        yield from walk(child)


__all__ = [
    "LAggregate",
    "LLimit",
    "LNodeScan",
    "LOrderBy",
    "LProject",
    "LSelection",
    "LogicalOp",
    "walk",
]
