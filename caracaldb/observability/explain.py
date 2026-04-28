"""``EXPLAIN`` plan-tree renderer.

Walks any ``LogicalOp`` tree (including the M2 ``LExpand``/``LJoin``) and
produces a human-readable indented tree with per-node estimated cardinality
when ``CatalogStats`` are supplied. The output is intentionally text-only —
the M5 OTLP exporter (CDB-082) wraps the same tree in span events for the
profiler to enrich at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from caracaldb.plan.cost import CatalogStats, estimate
from caracaldb.plan.logical import LogicalOp


@dataclass(slots=True)
class ExplainNode:
    label: str
    rows_estimate: float | None = None
    detail: str | None = None
    children: list[ExplainNode] = field(default_factory=list)


def _node_label(op: LogicalOp) -> str:
    cls = type(op).__name__
    parts: list[str] = [cls.removeprefix("L")]
    extras: list[str] = []
    for attr in ("class_iri", "property_iri", "alias", "direction", "limit"):
        if hasattr(op, attr):
            value = getattr(op, attr)
            if value is None:
                continue
            extras.append(f"{attr}={value}")
    if extras:
        parts.append("(" + ", ".join(extras) + ")")
    return " ".join(parts)


def explain_logical(plan: LogicalOp, stats: CatalogStats | None = None) -> ExplainNode:
    """Convert a logical plan tree into an ``ExplainNode`` tree."""
    rows: float | None = None
    if stats is not None:
        rows = estimate(plan, stats).rows
    node = ExplainNode(label=_node_label(plan), rows_estimate=rows)
    for child in plan.children():
        node.children.append(explain_logical(child, stats))
    return node


def render_explain(node: ExplainNode, *, indent: int = 0) -> str:
    head = "│  " * max(0, indent - 1) + ("└─ " if indent else "")
    suffix = ""
    if node.rows_estimate is not None:
        suffix = f"   [rows≈{node.rows_estimate:.0f}]"
    if node.detail:
        suffix += f"   {node.detail}"
    line = f"{head}{node.label}{suffix}"
    out_lines = [line]
    for child in node.children:
        out_lines.append(render_explain(child, indent=indent + 1))
    return "\n".join(out_lines)


__all__ = ["ExplainNode", "explain_logical", "render_explain"]
