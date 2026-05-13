"""Stable Python-to-Rust planning boundary.

This module intentionally stays small: Python remains the reference parser and
planner, while Rust consumes a stable JSON shape for the engine subset that is
ready for dual execution.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RustPlan:
    physical: dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(self.physical, sort_keys=True, separators=(",", ":"))


def lower_node_scan(
    *,
    class_iri: str,
    local_name: str,
    snapshot_lsn: int | None = None,
) -> RustPlan:
    return RustPlan(
        {
            "op": "node_scan",
            "class_iri": class_iri,
            "local_name": local_name,
            "snapshot_lsn": snapshot_lsn,
        }
    )


def lower_topk(
    input_plan: RustPlan,
    *,
    order_by: str,
    skip: int = 0,
    limit: int | None = None,
) -> RustPlan:
    return RustPlan(
        {
            "op": "top_k",
            "order_by": [
                {
                    "expr": {"kind": "column", "name": order_by},
                    "descending": False,
                    "nulls_last": True,
                }
            ],
            "skip": skip,
            "limit": limit,
            "input": input_plan.physical,
        }
    )


def lower_project(input_plan: RustPlan, columns: list[str]) -> RustPlan:
    return RustPlan(
        {
            "op": "project",
            "columns": columns,
            "input": input_plan.physical,
        }
    )


__all__ = ["RustPlan", "lower_node_scan", "lower_project", "lower_topk"]
