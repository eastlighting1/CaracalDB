from pathlib import Path

import numpy as np
import pytest

from caracaldb.graph.hnsw import HnswConfig, HnswIndex
from caracaldb.lang.diagnostics import CaracalError


def _normalised(vec: np.ndarray) -> np.ndarray:
    return vec / max(np.linalg.norm(vec), 1e-9)


def test_hnsw_add_and_search_finds_nearest(tmp_path: Path) -> None:
    rng = np.random.default_rng(0)
    cfg = HnswConfig(dim=8, max_elements=64)
    idx = HnswIndex(cfg)
    vecs = rng.standard_normal((20, 8)).astype(np.float32)
    idx.add(np.arange(20, dtype=np.uint64), vecs)

    labels, dists = idx.search(vecs[5], k=3)
    # The closest match for vec[5] should be itself (distance ≈ 0).
    assert labels[0][0] == 5
    assert dists[0][0] < 1e-3


def test_hnsw_resizes_on_overflow() -> None:
    cfg = HnswConfig(dim=4, max_elements=2)
    idx = HnswIndex(cfg)
    rng = np.random.default_rng(1)
    idx.add([0, 1], rng.standard_normal((2, 4)).astype(np.float32))
    # Adding a third triggers a resize_index call internally.
    idx.add([2], rng.standard_normal((1, 4)).astype(np.float32))
    assert len(idx) == 3


def test_hnsw_round_trips_to_disk(tmp_path: Path) -> None:
    cfg = HnswConfig(dim=4, max_elements=8)
    idx = HnswIndex(cfg)
    rng = np.random.default_rng(2)
    vecs = rng.standard_normal((5, 4)).astype(np.float32)
    idx.add(np.arange(5, dtype=np.uint64), vecs)
    target = tmp_path / "vec.hnsw"
    idx.save(target)

    loaded = HnswIndex.load(target, config=cfg)
    assert len(loaded) == 5
    labels, _ = loaded.search(vecs[0], k=1)
    assert labels[0][0] == 0


def test_hnsw_rejects_bad_dim() -> None:
    cfg = HnswConfig(dim=4, max_elements=4)
    idx = HnswIndex(cfg)
    with pytest.raises(CaracalError) as exc:
        idx.add([0], np.array([[1.0, 2.0]], dtype=np.float32))
    assert exc.value.code == "CDB-7091"


def test_hnsw_rejects_invalid_config() -> None:
    with pytest.raises(CaracalError) as exc:
        HnswIndex(HnswConfig(dim=0))
    assert exc.value.code == "CDB-7090"
