from pathlib import Path

import pyarrow as pa

from caracaldb.exec.operator import PhysicalOperator, run_pipeline
from caracaldb.exec.operators import RandomWalkOperator
from caracaldb.graph import build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store


class _Seeds(PhysicalOperator):
    def __init__(self, seeds: list[int]) -> None:
        super().__init__()
        self._batch = pa.record_batch({"nid": pa.array(seeds, type=pa.uint64())})
        self._done = False

    def _next_batch(self):
        if self._done:
            return None
        self._done = True
        return self._batch


def _path_csr(tmp_path: Path) -> CsrReader:
    bundle = create_bundle(tmp_path / "g")
    p = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    # Linear chain 0→1→2→3→4 plus 4→0 cycle.
    p.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1, 2, 3, 4], type=pa.uint64()),
                "dst": pa.array([1, 2, 3, 4, 0], type=pa.uint64()),
            }
        )
    )
    out = tmp_path / "p.csr"
    build_csr(p, num_vertices=5, out_path=out, with_eids=False)
    return CsrReader(out)


def test_random_walk_basic_length(tmp_path: Path) -> None:
    csr = _path_csr(tmp_path)
    op = RandomWalkOperator(_Seeds([0]), forward=csr, length=4, num_walks=1, seed=0)
    out = list(run_pipeline(op))[0]
    # Single walk of length 4 → 4 rows for walk_id 0, steps [0,1,2,3].
    assert out["walk_id"].to_pylist() == [0, 0, 0, 0]
    assert out["step"].to_pylist() == [0, 1, 2, 3]


def test_random_walk_multiple_walks_per_seed(tmp_path: Path) -> None:
    csr = _path_csr(tmp_path)
    op = RandomWalkOperator(_Seeds([0, 2]), forward=csr, length=3, num_walks=2, seed=1)
    out = list(run_pipeline(op))[0]
    walk_ids = sorted(set(out["walk_id"].to_pylist()))
    # 2 seeds × 2 walks each = 4 distinct walk ids.
    assert walk_ids == [0, 1, 2, 3]


def test_random_walk_node2vec_p_q_runs(tmp_path: Path) -> None:
    csr = _path_csr(tmp_path)
    op = RandomWalkOperator(_Seeds([0]), forward=csr, length=5, num_walks=1, p=0.5, q=2.0, seed=42)
    out = list(run_pipeline(op))[0]
    assert len(out["nid"].to_pylist()) >= 1
