import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from bench.harness import (  # noqa: E402  (path bootstrap above)
    RUNNERS,
    bench_graph_ecosystem,
    bench_knn,
    compare_against_baseline,
    write_results,
)


def test_runners_registry_is_complete() -> None:
    assert {"1hop", "2hop", "knn", "neighbor_sample", "graph_ecosystem"} <= set(RUNNERS)


def test_bench_knn_returns_well_formed_result() -> None:
    result = bench_knn(n=200, dim=8, k=5)
    assert result["scenario"] == "knn"
    assert result["metric"] == "ms"
    assert isinstance(result["value"], float) and result["value"] >= 0.0


def test_graph_ecosystem_bench_reports_native_modes() -> None:
    result = bench_graph_ecosystem(n_nodes=200, n_edges=600, dim=8, top_k=4)
    assert result["scenario"] == "graph_ecosystem"
    assert result["semantic_entry_mode"] == "caracal_hnsw"
    assert result["semantic_reentry_mode"] == "native_result_nodes"
    assert result["relation_expand_mode"] == "neighbors_api"
    assert result["fallback_flags"] == []
    assert result["vector_index_used"] == "chunk_embedding_hnsw"
    assert result["result_count"] == 4


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
