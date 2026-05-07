"""Public vector distance helpers.

These helpers are intentionally small and dependency-light. They accept any
1-D sequence of numeric values, coerce to float32, and raise CaracalDB errors
for the shape mistakes that graph-retrieval adapters need to diagnose clearly.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np

from caracaldb.lang.diagnostics import CaracalError


def _as_vector(value: Sequence[float] | np.ndarray, label: str) -> np.ndarray:
    vector = np.asarray(value, dtype=np.float32)
    if vector.ndim != 1:
        raise CaracalError(code="CDB-6061", message=f"{label} must be a 1-D vector")
    return vector


def _pair(
    a: Sequence[float] | np.ndarray,
    b: Sequence[float] | np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    left = _as_vector(a, "left operand")
    right = _as_vector(b, "right operand")
    if left.shape != right.shape:
        raise CaracalError(
            code="CDB-6061",
            message=f"vector dimension mismatch: {left.shape[0]} != {right.shape[0]}",
        )
    return left, right


def cosine_similarity(a: Sequence[float] | np.ndarray, b: Sequence[float] | np.ndarray) -> float:
    """Return cosine similarity.

    If either vector has zero norm, CaracalDB returns ``0.0``. This keeps the
    function total and makes zero-vector behavior explicit for ranking code.
    """

    left, right = _pair(a, b)
    denom = float(np.linalg.norm(left) * np.linalg.norm(right))
    if denom == 0.0:
        return 0.0
    return float(np.dot(left, right) / denom)


def cosine_distance(a: Sequence[float] | np.ndarray, b: Sequence[float] | np.ndarray) -> float:
    """Return ``1 - cosine_similarity(a, b)``."""

    return float(1.0 - cosine_similarity(a, b))


def dot_product(a: Sequence[float] | np.ndarray, b: Sequence[float] | np.ndarray) -> float:
    """Return dot product for equal-dimension vectors."""

    left, right = _pair(a, b)
    return float(np.dot(left, right))


def l2_distance(a: Sequence[float] | np.ndarray, b: Sequence[float] | np.ndarray) -> float:
    """Return Euclidean distance for equal-dimension vectors."""

    left, right = _pair(a, b)
    return float(np.linalg.norm(left - right))


def score_from_distance(
    metric: str,
    distance: float,
    query: Any = None,
    vector: Any = None,
) -> float:
    """Convert a distance into a descending score for public result rows."""

    if metric == "cosine":
        return float(1.0 - distance)
    if metric == "l2":
        return float(-distance)
    if metric in {"ip", "dot", "dot_product"}:
        if query is not None and vector is not None:
            return dot_product(query, vector)
        return float(1.0 - distance)
    raise CaracalError(code="CDB-7090", message=f"unsupported vector metric: {metric!r}")


__all__ = [
    "cosine_distance",
    "cosine_similarity",
    "dot_product",
    "l2_distance",
    "score_from_distance",
]
