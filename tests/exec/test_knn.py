import numpy as np
import pytest

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import KnnOperator
from caracaldb.graph.hnsw import HnswConfig, HnswIndex
from caracaldb.lang.builtins import VECTOR_FUNCTIONS
from caracaldb.lang.diagnostics import CaracalError


def _index() -> HnswIndex:
    rng = np.random.default_rng(0)
    cfg = HnswConfig(dim=8, max_elements=64)
    idx = HnswIndex(cfg)
    vecs = rng.standard_normal((30, 8)).astype(np.float32)
    idx.add(np.arange(30, dtype=np.uint64), vecs)
    return idx


def test_knn_returns_top_k_labels() -> None:
    idx = _index()
    rng = np.random.default_rng(0)
    query = rng.standard_normal((1, 8)).astype(np.float32)
    op = KnnOperator(idx, query=query, k=5)
    out = list(run_pipeline(op))[0]
    assert out.num_rows == 5
    # Distances are non-decreasing.
    dists = out.column("distance").to_pylist()
    assert dists == sorted(dists)


def test_knn_metadata_filter_drops_disallowed_ids() -> None:
    idx = _index()
    rng = np.random.default_rng(0)
    query = rng.standard_normal((1, 8)).astype(np.float32)
    only_even = lambda labels: labels % 2 == 0  # noqa: E731
    op = KnnOperator(idx, query=query, k=10, metadata_filter=only_even)
    out = list(run_pipeline(op))[0]
    assert all(int(x) % 2 == 0 for x in out.column("nid").to_pylist())


def test_knn_rejects_non_positive_k() -> None:
    with pytest.raises(CaracalError) as exc:
        KnnOperator(_index(), query=np.zeros(8, dtype=np.float32), k=0)
    assert exc.value.code == "CDB-6090"


def test_vector_similarity_matches_manual_cosine() -> None:
    import pyarrow as pa

    a = pa.FixedSizeListArray.from_arrays(pa.array([1.0, 0.0, 0.0, 1.0], type=pa.float32()), 2)
    b = pa.FixedSizeListArray.from_arrays(pa.array([1.0, 0.0, 0.0, 1.0], type=pa.float32()), 2)
    out = VECTOR_FUNCTIONS["similarity"].dispatch([a, b])
    # Cosine of identical unit vectors == 1.
    assert all(abs(v - 1.0) < 1e-5 for v in out.to_pylist())


def test_vector_normalize_returns_unit_vectors() -> None:
    import pyarrow as pa

    src = pa.FixedSizeListArray.from_arrays(pa.array([3.0, 4.0, 0.0, 0.0], type=pa.float32()), 2)
    out = VECTOR_FUNCTIONS["vec_normalize"].dispatch([src])
    rows = out.to_pylist()
    # First row: (3,4) / 5 = (0.6, 0.8).
    assert abs(rows[0][0] - 0.6) < 1e-5 and abs(rows[0][1] - 0.8) < 1e-5
