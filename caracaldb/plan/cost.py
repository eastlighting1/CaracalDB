"""Basic cardinality and cost estimation.

Catalog statistics keyed by class / property IRI feed simple node-and-edge
formulas:

    cardinality(NodeScan(C))   = stats.class_rows[C]
    cardinality(Selection(p))  = cardinality(child) * selectivity(p)
    cardinality(Expand(prop))  = cardinality(child) * stats.avg_degree[prop]
    cardinality(Join)          = min(left, right) * 1.0    -- conservative

The cost model returns ``(rows, io_cost, cpu_cost)``. ``io_cost`` is dominated
by the read pages the operator pulls from disk; ``cpu_cost`` scales with the
estimated row count. Costs are pure floats — the planner uses them only to
compare alternative plans, not to predict wall time.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from caracaldb.plan.logical import LAggregate, LNodeScan, LogicalOp, LProject, LSelection
from caracaldb.plan.pattern_compiler import LExpand, LJoin


@dataclass(slots=True)
class CatalogStats:
    class_rows: dict[str, int] = field(default_factory=dict)
    avg_degree: dict[str, float] = field(default_factory=dict)
    histograms: dict[tuple[str, str], dict[object, int]] = field(default_factory=dict)
    default_class_rows: int = 1_000
    default_avg_degree: float = 4.0


@dataclass(frozen=True, slots=True)
class CostEstimate:
    rows: float
    io_cost: float
    cpu_cost: float

    @property
    def total(self) -> float:
        return self.io_cost + self.cpu_cost


_PAGE_SIZE_ROWS = 65_536  # rows per Arrow batch — roughly one page of work
_DEFAULT_SELECTIVITY = 0.1


def estimate(plan: LogicalOp, stats: CatalogStats | None = None) -> CostEstimate:
    s = stats or CatalogStats()
    return _estimate_node(plan, s)


def _estimate_node(node: LogicalOp, s: CatalogStats) -> CostEstimate:
    if isinstance(node, LNodeScan):
        rows = float(s.class_rows.get(node.class_iri, s.default_class_rows))
        io = rows / _PAGE_SIZE_ROWS
        return CostEstimate(rows=rows, io_cost=io, cpu_cost=rows * 0.001)
    if isinstance(node, LSelection):
        child = _estimate_node(node.child, s)
        rows = child.rows * _DEFAULT_SELECTIVITY
        return CostEstimate(
            rows=rows, io_cost=child.io_cost, cpu_cost=child.cpu_cost + rows * 0.0005
        )
    if isinstance(node, LProject):
        child = _estimate_node(node.child, s)
        return CostEstimate(
            rows=child.rows, io_cost=child.io_cost, cpu_cost=child.cpu_cost + child.rows * 0.0001
        )
    if isinstance(node, LAggregate):
        child = _estimate_node(node.child, s)
        # Conservative: groups ≈ sqrt(rows).
        groups = max(1.0, child.rows**0.5)
        return CostEstimate(
            rows=groups, io_cost=child.io_cost, cpu_cost=child.cpu_cost + child.rows * 0.002
        )
    if isinstance(node, LExpand):
        child = _estimate_node(node.child, s)
        deg = s.avg_degree.get(node.property_iri, s.default_avg_degree)
        # Variable-length scales degree per hop, capped softly.
        hops = max(node.hop_max, node.hop_min, 1)
        rows = child.rows * (deg**hops)
        return CostEstimate(
            rows=rows,
            io_cost=child.io_cost + rows / _PAGE_SIZE_ROWS,
            cpu_cost=child.cpu_cost + rows * 0.0008,
        )
    if isinstance(node, LJoin):
        left = _estimate_node(node.left, s)
        right = _estimate_node(node.right, s)
        rows = min(left.rows, right.rows)
        return CostEstimate(
            rows=rows,
            io_cost=left.io_cost + right.io_cost,
            cpu_cost=left.cpu_cost + right.cpu_cost + (left.rows + right.rows) * 0.0006,
        )
    if not node.children():
        return CostEstimate(rows=float(s.default_class_rows), io_cost=1.0, cpu_cost=0.0)
    children = [_estimate_node(c, s) for c in node.children()]
    rows = max(c.rows for c in children)
    return CostEstimate(
        rows=rows,
        io_cost=sum(c.io_cost for c in children),
        cpu_cost=sum(c.cpu_cost for c in children),
    )


__all__ = ["CatalogStats", "CostEstimate", "estimate"]
