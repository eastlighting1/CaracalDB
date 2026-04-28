"""Forward-chaining reasoner: ``INFER CLOSURE`` materialisation.

The M3 surface covers two characteristics:

* ``SYMMETRIC p``  →  for every ``(s, p, o)`` in the edge store, also persist
  ``(o, p, s)`` if not already present.
* ``TRANSITIVE p`` →  iterative closure: if ``p(a, b)`` and ``p(b, c)`` are
  present, persist ``p(a, c)``. Iteration stops when no new edges are added
  in a pass (or when an optional ``max_iterations`` cap is hit).

Each materialisation pass appends a ``WalRecord(kind="INFER_CLOSURE", payload=...)``
to the active WAL so recovery (CDB-026) can replay or skip the inferred
edges deterministically. New edges flow through the same ``EdgeStore.append``
path as user inserts; a ``ReasonerReport`` summarises additions per rule.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.edge_store import (
    DST_COLUMN,
    SRC_COLUMN,
    EdgeStore,
)
from caracaldb.storage.wal import Wal

InferRule = Literal["SYMMETRIC", "TRANSITIVE"]
INFER_CLOSURE_KIND = "INFER_CLOSURE"


@dataclass(slots=True)
class ReasonerReport:
    rule: InferRule
    property_iri: str
    added_edges: int = 0
    iterations: int = 0
    skipped_existing: int = 0


def _existing_edges(store: EdgeStore) -> set[tuple[int, int]]:
    table = store.to_table()
    if table.num_rows == 0:
        return set()
    src = table[SRC_COLUMN].to_pylist()
    dst = table[DST_COLUMN].to_pylist()
    return {(int(s), int(d)) for s, d in zip(src, dst, strict=True)}


def _append_edges(
    store: EdgeStore,
    pairs: list[tuple[int, int]],
) -> None:
    if not pairs:
        return
    src = pa.array([p[0] for p in pairs], type=pa.uint64())
    dst = pa.array([p[1] for p in pairs], type=pa.uint64())
    store.append(pa.RecordBatch.from_arrays([src, dst], names=[SRC_COLUMN, DST_COLUMN]))


def _wal_log(
    wal: Wal | None,
    rule: InferRule,
    property_iri: str,
    added: int,
    iterations: int,
) -> None:
    if wal is None:
        return
    payload = json.dumps(
        {
            "rule": rule,
            "property_iri": property_iri,
            "added": added,
            "iterations": iterations,
        }
    ).encode("utf-8")
    wal.append(INFER_CLOSURE_KIND, payload)


def infer_symmetric(
    store: EdgeStore,
    *,
    property_iri: str,
    wal: Wal | None = None,
) -> ReasonerReport:
    """Append ``(o, s)`` for every ``(s, o)`` whose reverse is missing."""
    existing = _existing_edges(store)
    additions: list[tuple[int, int]] = []
    skipped = 0
    for s, d in list(existing):
        if (d, s) in existing:
            skipped += 1
            continue
        additions.append((d, s))
        existing.add((d, s))
    _append_edges(store, additions)
    _wal_log(wal, "SYMMETRIC", property_iri, len(additions), 1)
    return ReasonerReport(
        rule="SYMMETRIC",
        property_iri=property_iri,
        added_edges=len(additions),
        iterations=1,
        skipped_existing=skipped,
    )


def infer_transitive(
    store: EdgeStore,
    *,
    property_iri: str,
    max_iterations: int = 16,
    triple_budget: int | None = None,
    wal: Wal | None = None,
) -> ReasonerReport:
    if max_iterations <= 0:
        raise CaracalError(code="CDB-9510", message="max_iterations must be positive")
    edges = _existing_edges(store)
    total_new = 0
    iteration = 0
    while iteration < max_iterations:
        iteration += 1
        # Build adjacency from current edge set.
        out_adj: dict[int, set[int]] = {}
        for s, d in edges:
            out_adj.setdefault(s, set()).add(d)
        additions: list[tuple[int, int]] = []
        for s, d in list(edges):
            for d2 in out_adj.get(d, ()):
                pair = (s, d2)
                if pair in edges:
                    continue
                additions.append(pair)
                edges.add(pair)
                if triple_budget is not None and total_new + len(additions) >= triple_budget:
                    raise CaracalError(
                        code="TF-6012",
                        message=(
                            f"transitive closure budget exhausted at "
                            f"{triple_budget} new edges for {property_iri!r}"
                        ),
                    )
        if not additions:
            break
        _append_edges(store, additions)
        total_new += len(additions)
    _wal_log(wal, "TRANSITIVE", property_iri, total_new, iteration)
    return ReasonerReport(
        rule="TRANSITIVE",
        property_iri=property_iri,
        added_edges=total_new,
        iterations=iteration,
    )


@dataclass(slots=True)
class InferClosurePlan:
    targets: list[tuple[InferRule, str, EdgeStore]] = field(default_factory=list)


def infer_closure(
    plan: InferClosurePlan,
    *,
    wal: Wal | None = None,
    triple_budget: int | None = None,
) -> list[ReasonerReport]:
    reports: list[ReasonerReport] = []
    for rule, iri, store in plan.targets:
        if rule == "SYMMETRIC":
            reports.append(infer_symmetric(store, property_iri=iri, wal=wal))
        elif rule == "TRANSITIVE":
            reports.append(
                infer_transitive(store, property_iri=iri, wal=wal, triple_budget=triple_budget)
            )
        else:  # pragma: no cover
            raise CaracalError(code="CDB-9510", message=f"unsupported rule: {rule}")
    return reports


__all__ = [
    "INFER_CLOSURE_KIND",
    "InferClosurePlan",
    "InferRule",
    "ReasonerReport",
    "infer_closure",
    "infer_symmetric",
    "infer_transitive",
]
