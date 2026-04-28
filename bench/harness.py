"""Small benchmark harness used by CI and the ``caracal bench`` command."""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import numpy as np

BenchResult = dict[str, Any]
BenchRunner = Callable[[], BenchResult]


def _elapsed_ms(fn: Callable[[], object]) -> float:
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0


def bench_1hop(n: int = 10_000, degree: int = 8) -> BenchResult:
    edges = np.arange(n * degree, dtype=np.uint64).reshape(n, degree) % n
    seeds = np.arange(0, n, max(1, n // 256), dtype=np.uint64)

    def run() -> int:
        return int(edges[seeds].sum())

    return {"scenario": "1hop", "metric": "ms", "value": _elapsed_ms(run), "n": n}


def bench_2hop(n: int = 10_000, degree: int = 8) -> BenchResult:
    edges = np.arange(n * degree, dtype=np.uint64).reshape(n, degree) % n
    seeds = np.arange(0, n, max(1, n // 128), dtype=np.uint64)

    def run() -> int:
        first = edges[seeds].ravel()
        second = edges[first].ravel()
        return int(second.sum())

    return {"scenario": "2hop", "metric": "ms", "value": _elapsed_ms(run), "n": n}


def bench_knn(n: int = 2_000, dim: int = 32, k: int = 10) -> BenchResult:
    rng = np.random.default_rng(42)
    vectors = rng.normal(size=(n, dim)).astype(np.float32)
    query = vectors[0]

    def run() -> np.ndarray[Any, Any]:
        distances = np.linalg.norm(vectors - query, axis=1)
        return np.argpartition(distances, k)[:k]

    return {"scenario": "knn", "metric": "ms", "value": _elapsed_ms(run), "n": n}


def bench_neighbor_sample(n: int = 10_000, degree: int = 16, fanout: int = 4) -> BenchResult:
    rng = random.Random(42)
    adjacency = [tuple((i * degree + j) % n for j in range(degree)) for i in range(n)]
    seeds = list(range(0, n, max(1, n // 128)))

    def run() -> int:
        total = 0
        for seed in seeds:
            total += sum(rng.sample(adjacency[seed], fanout))
        return total

    return {
        "scenario": "neighbor_sample",
        "metric": "ms",
        "value": _elapsed_ms(run),
        "n": n,
    }


RUNNERS: dict[str, BenchRunner] = {
    "1hop": bench_1hop,
    "2hop": bench_2hop,
    "knn": bench_knn,
    "neighbor_sample": bench_neighbor_sample,
}


def run_all() -> list[BenchResult]:
    return [runner() for runner in RUNNERS.values()]


def write_results(path: Path, results: Iterable[BenchResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(results), indent=2), encoding="utf-8")


def compare_against_baseline(
    latest: Iterable[BenchResult],
    baseline: Iterable[BenchResult],
    *,
    tolerance: float,
) -> list[str]:
    latest_by_name = {str(item["scenario"]): item for item in latest}
    regressions: list[str] = []
    for base in baseline:
        scenario = str(base["scenario"])
        current = latest_by_name.get(scenario)
        if current is None:
            regressions.append(f"{scenario}: missing latest result")
            continue
        base_value = float(base["value"])
        current_value = float(current["value"])
        if base_value <= 0:
            continue
        max_allowed = base_value * (1.0 + tolerance)
        if current_value > max_allowed:
            regressions.append(
                f"{scenario}: {current_value:.3f} ms > {max_allowed:.3f} ms "
                f"(baseline {base_value:.3f} ms)"
            )
    return regressions
