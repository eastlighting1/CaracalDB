import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from bench.harness import (  # noqa: E402  (path bootstrap above)
    RUNNERS,
    bench_knn,
    compare_against_baseline,
    write_results,
)


def test_runners_registry_is_complete() -> None:
    assert {"1hop", "2hop", "knn", "neighbor_sample"} <= set(RUNNERS)


def test_bench_knn_returns_well_formed_result() -> None:
    result = bench_knn(n=200, dim=8, k=5)
    assert result["scenario"] == "knn"
    assert result["metric"] == "ms"
    assert isinstance(result["value"], float) and result["value"] >= 0.0


def test_write_and_read_baseline_round_trip(tmp_path: Path) -> None:
    target = tmp_path / "results" / "out.json"
    payload = [{"scenario": "1hop", "metric": "ms", "value": 1.0, "n": 10}]
    write_results(target, payload)
    assert json.loads(target.read_text(encoding="utf-8")) == payload


def test_compare_against_baseline_flags_regressions() -> None:
    baseline = [{"scenario": "1hop", "metric": "ms", "value": 100.0, "n": 1000}]
    fast = [{"scenario": "1hop", "metric": "ms", "value": 110.0, "n": 1000}]
    slow = [{"scenario": "1hop", "metric": "ms", "value": 200.0, "n": 1000}]
    assert compare_against_baseline(fast, baseline, tolerance=0.30) == []
    assert compare_against_baseline(slow, baseline, tolerance=0.30)


def test_baseline_file_lives_next_to_harness() -> None:
    repo = Path(__file__).resolve().parents[2]
    assert (repo / "bench" / "results" / "baseline.json").is_file()
