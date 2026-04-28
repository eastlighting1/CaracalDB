"""``WITH`` clause materialisation helper.

Tuft's ``WITH proj1, proj2 [WHERE …]`` clause splits a query into a
``sub-pipeline`` whose output becomes the input to the next clause. The
helper here takes a list of ``Clause`` objects and produces an alternating
sequence of pipeline boundaries, each carrying the (renamed) projection list
and an optional follow-up filter. The planner then materialises each
sub-pipeline as if it were a standalone query.

The helper is grammar-shape only — it does not bind names; binding remains
the binder's job (CDB-018). The aim is to give the planner a stable handle
on "where one pipeline stops and the next begins" so future rules
(predicate pushdown across boundaries) have a target to operate on.
"""

from __future__ import annotations

from dataclasses import dataclass

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta


@dataclass(frozen=True, slots=True)
class WithBoundary:
    projections: tuple[ta.Projection, ...]
    where: ta.Expr | None
    span: ta.Span | None


@dataclass(frozen=True, slots=True)
class PipelineSegment:
    clauses: tuple[ta.Clause, ...]
    boundary: WithBoundary | None  # boundary at the *end* of the segment


def split_pipelines(query: ta.Query) -> tuple[PipelineSegment, ...]:
    """Split a query's clause list at every ``WITH`` boundary.

    The returned tuple is non-empty as long as the input has at least one
    clause; the final segment's ``boundary`` is None unless the query ends
    with a ``WITH`` (which is a degenerate but legal shape).
    """
    if not query.clauses:
        raise CaracalError(code="CDB-6051", message="query has no clauses")

    segments: list[PipelineSegment] = []
    buf: list[ta.Clause] = []
    for clause in query.clauses:
        if isinstance(clause, ta.WithClause):
            boundary = WithBoundary(
                projections=clause.projections,
                where=clause.where,
                span=clause.span,
            )
            segments.append(PipelineSegment(clauses=tuple(buf), boundary=boundary))
            buf = []
        else:
            buf.append(clause)
    segments.append(PipelineSegment(clauses=tuple(buf), boundary=None))
    return tuple(segments)


def collect_aliases(boundary: WithBoundary) -> tuple[str, ...]:
    """Project aliases that downstream segments can reference."""
    out: list[str] = []
    for proj in boundary.projections:
        if proj.alias is not None:
            out.append(proj.alias.name)
        else:
            # Fall back to the dotted/identifier form.
            if isinstance(proj.expr, ta.PathExpr) and proj.expr.steps:
                out.append(proj.expr.steps[-1].name)
            elif isinstance(proj.expr, ta.Var) and proj.expr.name is not None:
                out.append(proj.expr.name.name)
    return tuple(out)


def is_aggregate(boundary: WithBoundary) -> bool:
    """Heuristic: any aggregate-style function call in the projection list."""
    aggregate_names = {"count", "sum", "avg", "mean", "min", "max", "collect", "list"}
    for proj in boundary.projections:
        if isinstance(proj.expr, ta.FnCall) and proj.expr.name is not None:
            name = proj.expr.name.value.lower() if hasattr(proj.expr.name, "value") else ""
            if name in aggregate_names:
                return True
    return False


__all__ = [
    "PipelineSegment",
    "WithBoundary",
    "collect_aliases",
    "is_aggregate",
    "split_pipelines",
]
