"""Rule-based logical plan rewrites.

The M1 surface is two rules — predicate pushdown and projection pruning — both
of which only need to recognise simple "column reference" predicates and
projections. Predicates and projections stay opaque to higher layers; rules
inspect them via a small ``ColumnRefHints`` protocol that the binder emits
after CDB-018. For the M1 vertical slice we accept tuples of the form
``("col", "name")`` as a stand-in so the rules can be exercised in isolation.

The runner applies rules to fixpoint with a cycle break at ``max_iterations``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from dataclasses import replace as dc_replace

from caracaldb.plan.logical import (
    LAggregate,
    LLimit,
    LNodeScan,
    LogicalOp,
    LOrderBy,
    LProject,
    LSelection,
)

Rule = Callable[[LogicalOp], LogicalOp]


@dataclass(frozen=True, slots=True)
class RewriteResult:
    plan: LogicalOp
    iterations: int
    applied: tuple[str, ...]


def _columns_used(node: object) -> set[str]:
    """Best-effort column extraction from opaque expression placeholders.

    The runtime binder will populate richer metadata; for unit-testability we
    accept the conventions used by tests: ``("col", name)`` for a column
    reference, and tuples whose head is in ``{"and", "or", "eq", "fn", ...}``
    for nested expressions whose tail is a list of sub-expressions.
    """
    if isinstance(node, tuple) and node:
        head = node[0]
        if head == "col" and len(node) >= 2 and isinstance(node[1], str):
            return {node[1]}
        # Tagged tuple (head is a string operator): recurse over operands only.
        # Untagged tuple (e.g. an expression list): recurse over every item.
        items = node[1:] if isinstance(head, str) else node
        result: set[str] = set()
        for child in items:
            result |= _columns_used(child)
        return result
    return set()


def predicate_pushdown(plan: LogicalOp) -> LogicalOp:
    """Push ``LSelection`` predicates into immediate ``LNodeScan`` columns where
    possible.

    The M1 implementation only collapses ``Selection(NodeScan(...))`` by
    leaving the selection in place but ensuring the underlying scan keeps
    every column referenced by the predicate (so projection pruning above does
    not strip them).
    """
    match plan:
        case LSelection(child=LNodeScan() as scan, predicate=pred):
            referenced = _columns_used(pred)
            if scan.columns is None:
                return plan  # no pruning yet → nothing to widen
            keep = tuple(sorted(set(scan.columns) | referenced))
            if keep == scan.columns:
                return plan
            return LSelection(child=dc_replace(scan, columns=keep), predicate=pred)
        case _:
            return _recurse(plan, predicate_pushdown)


def projection_pruning(plan: LogicalOp) -> LogicalOp:
    """Restrict ``LNodeScan`` columns to those a parent ``LProject`` actually
    keeps. Conservative: only triggers when ``LProject`` directly wraps an
    ``LNodeScan`` (or a ``Selection(NodeScan)``)."""
    match plan:
        case LProject(child=LNodeScan() as scan, projections=projs):
            wanted = _columns_used(tuple(p[0] for p in projs))
            return LProject(
                child=dc_replace(scan, columns=tuple(sorted(wanted)) or scan.columns),
                projections=projs,
            )
        case LProject(
            child=LSelection(child=LNodeScan() as scan, predicate=pred),
            projections=projs,
        ):
            wanted = _columns_used(tuple(p[0] for p in projs)) | _columns_used(pred)
            return LProject(
                child=LSelection(
                    child=dc_replace(scan, columns=tuple(sorted(wanted)) or scan.columns),
                    predicate=pred,
                ),
                projections=projs,
            )
        case _:
            return _recurse(plan, projection_pruning)


def _recurse(plan: LogicalOp, rule: Rule) -> LogicalOp:
    """Apply ``rule`` to all children of ``plan`` and rebuild if any changed."""
    if not plan.children():
        return plan
    new_children = tuple(rule(c) for c in plan.children())
    if new_children == plan.children():
        return plan
    # Rebuild known compound ops.
    match plan:
        case LSelection(predicate=p):
            return LSelection(child=new_children[0], predicate=p)
        case LProject(projections=projs):
            return LProject(child=new_children[0], projections=projs)
        case LAggregate(group_keys=g, aggregates=a):
            return LAggregate(child=new_children[0], group_keys=g, aggregates=a)
        case LOrderBy(keys=k):
            return LOrderBy(child=new_children[0], keys=k)
        case LLimit(limit=lim, offset=off):
            return LLimit(child=new_children[0], limit=lim, offset=off)
        case _:
            return plan


def run_rules(
    plan: LogicalOp,
    rules: Iterable[Rule] | None = None,
    *,
    max_iterations: int = 8,
) -> RewriteResult:
    rules = list(rules) if rules is not None else [predicate_pushdown, projection_pruning]
    applied: list[str] = []
    current = plan
    for i in range(max_iterations):
        changed = False
        for rule in rules:
            new = rule(current)
            if new != current:
                applied.append(getattr(rule, "__name__", "rule"))
                current = new
                changed = True
        if not changed:
            return RewriteResult(plan=current, iterations=i + 1, applied=tuple(applied))
    return RewriteResult(plan=current, iterations=max_iterations, applied=tuple(applied))


__all__ = [
    "RewriteResult",
    "Rule",
    "predicate_pushdown",
    "projection_pruning",
    "run_rules",
]
