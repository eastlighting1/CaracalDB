"""Pattern → logical plan compiler.

Translates a (binder-resolved) Tuft ``Pattern`` into a sequence of logical
operators::

    NodeScan(Class₀) →
        [Expand(rel₁) → HashJoin(NodeScan(Class₁))] →
        [Expand(rel₂) → HashJoin(NodeScan(Class₂))] → …

The compiler does not know about physical CSR readers — it produces logical
nodes (``LNodeScan``, ``LExpand``, ``LJoin``) that the planner replaces with
their physical counterparts when bound to a database. The intent is exactly
the M2 §10 sketch: every additional ``-[rel]->(node)`` segment becomes an
Expand on top of the running pipeline plus a Join back to the freshly scanned
target node table on the destination nid.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.plan.logical import LNodeScan, LogicalOp

ExpandDirection = Literal["out", "in", "both"]


@dataclass(frozen=True, slots=True)
class LExpand(LogicalOp):
    child: LogicalOp
    property_iri: str
    direction: ExpandDirection
    src_alias: str
    dst_alias: str
    edge_alias: str | None
    hop_min: int = 1
    hop_max: int = 1

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.child,)


@dataclass(frozen=True, slots=True)
class LJoin(LogicalOp):
    left: LogicalOp
    right: LogicalOp
    left_key: str
    right_key: str
    kind: Literal["inner", "left"] = "inner"
    left_prefix: str | None = None
    right_prefix: str | None = None

    def children(self) -> tuple[LogicalOp, ...]:
        return (self.left, self.right)


def _resolve_label(elem: ta.NodePattern) -> str:
    if not elem.labels:
        raise CaracalError(code="CDB-6050", message="every node pattern must carry a class label")
    label = elem.labels[0]
    if isinstance(label, ta.Iri):
        return label.value
    return label.value


def _resolve_rel_type(rel: ta.RelPattern) -> str:
    if not rel.types:
        raise CaracalError(
            code="CDB-6050", message="every rel pattern must carry an explicit property"
        )
    label = rel.types[0]
    if isinstance(label, ta.Iri):
        return label.value
    return label.value


def compile_pattern(pattern: ta.Pattern) -> LogicalOp:
    """Lower a pattern element list into a NodeScan + Expand + Join chain."""
    if not pattern.elements:
        raise CaracalError(code="CDB-6050", message="empty pattern")

    first = pattern.elements[0]
    if not isinstance(first, ta.NodePattern):
        raise CaracalError(code="CDB-6050", message="pattern must start with a node element")

    head_alias = first.var.name if first.var is not None else "n0"
    plan: LogicalOp = LNodeScan(
        class_iri=_resolve_label(first),
        local_name=_local_name(_resolve_label(first)),
        alias=head_alias,
    )

    cursor_alias = head_alias
    rel: ta.RelPattern | None = None
    for elem in pattern.elements[1:]:
        if isinstance(elem, ta.RelPattern):
            rel = elem
            continue
        if not isinstance(elem, ta.NodePattern):
            raise CaracalError(
                code="CDB-6050",
                message=f"unsupported pattern element kind: {type(elem).__name__}",
            )
        if rel is None:
            raise CaracalError(
                code="CDB-6050",
                message="adjacent node patterns require a connecting -[rel]- element",
            )

        next_alias = elem.var.name if elem.var is not None else f"n{len(pattern.elements)}"
        property_iri = _resolve_rel_type(rel)
        direction: ExpandDirection
        if rel.direction == ta.Direction.OUT:
            direction = "out"
        elif rel.direction == ta.Direction.IN:
            direction = "in"
        else:
            direction = "both"

        edge_alias = rel.var.name if rel.var is not None else None
        hop_min = rel.hop_range.min_hops or 1
        hop_max = rel.hop_range.max_hops or hop_min
        src_alias = f"{cursor_alias}.nid"
        dst_alias = f"{next_alias}.nid"
        expansion = LExpand(
            child=plan,
            property_iri=property_iri,
            direction=direction,
            src_alias=src_alias,
            dst_alias=dst_alias,
            edge_alias=edge_alias,
            hop_min=hop_min,
            hop_max=hop_max,
        )
        target_scan = LNodeScan(
            class_iri=_resolve_label(elem),
            local_name=_local_name(_resolve_label(elem)),
            alias=next_alias,
        )
        plan = LJoin(
            left=expansion,
            right=target_scan,
            left_key=dst_alias,
            right_key="nid",
            left_prefix=None,
            right_prefix=next_alias,
        )
        cursor_alias = next_alias
        rel = None

    return plan


def _local_name(iri: str) -> str:
    return iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1].rsplit(":", 1)[-1]


__all__ = ["ExpandDirection", "LExpand", "LJoin", "compile_pattern"]
