"""Case C goldens: heterogeneous sampling + two-tower similarity."""

from __future__ import annotations

import pyarrow as pa

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import (
    KnnOperator,
    NeighborSampleOperator,
    NodeScanOperator,
)
from caracaldb.exec.operators.export_arrow import (
    export_subgraph_to_arrow,
    import_subgraph_from_arrow,
)
from caracaldb.ml import NeighborLoader, NeighborLoaderConfig, Subgraph


def test_q1_heterogeneous_neighbour_sample(case_c) -> None:
    seeds = NodeScanOperator(case_c["users"], columns=["nid"])
    op = NeighborSampleOperator(
        seeds,
        edge_readers={"http://x/viewed": case_c["viewed_csr"]},
        layers=[2, 1],
        seed=0,
    )
    batches = list(run_pipeline(op))
    layers = sorted({lv for b in batches for lv in b.column("layer").to_pylist()})
    assert layers == [0, 1]


def test_q2_two_tower_user_item_match_via_knn(case_c) -> None:
    # User 0's tower embedding equals Item 0's tower embedding by construction.
    user0 = case_c["user_emb"][0]
    op = KnnOperator(case_c["item_hnsw"], query=user0, k=1)
    out = list(run_pipeline(op))[0]
    assert int(out["nid"].to_pylist()[0]) == 0


def test_q3_neighbor_loader_yields_pyg_or_arrow(case_c) -> None:
    cfg = NeighborLoaderConfig(
        layers=[1],
        edge_readers={"http://x/viewed": case_c["viewed_csr"]},
        seed_class_iri="http://x/User",
        seed_local_name="User",
        batch_size=2,
    )
    loader = NeighborLoader(case_c["bundle"], cfg)
    batches = list(iter(loader))
    assert len(batches) == 2
    assert all(isinstance(b, Subgraph) for b in batches)


def test_q4_export_subgraph_round_trip(case_c, tmp_path) -> None:
    cfg = NeighborLoaderConfig(
        layers=[1],
        edge_readers={"http://x/viewed": case_c["viewed_csr"]},
        seed_class_iri="http://x/User",
        seed_local_name="User",
        batch_size=4,
    )
    loader = NeighborLoader(case_c["bundle"], cfg)
    sg = next(iter(loader))
    target = tmp_path / "case_c_batch.arrow"
    export_subgraph_to_arrow(sg, target)
    restored = import_subgraph_from_arrow(target)
    assert restored.num_edges() == sg.num_edges()


def test_q5_random_walk_on_user_views(case_c) -> None:
    # We piggyback on RandomWalkOperator to emit short walks of length 3 from user 0.
    from caracaldb.exec.operator import PhysicalOperator
    from caracaldb.exec.operators import RandomWalkOperator

    class _Seeds(PhysicalOperator):
        name = "Seeds"

        def __init__(self) -> None:
            super().__init__()
            self._done = False

        def _next_batch(self):
            if self._done:
                return None
            self._done = True
            return pa.record_batch({"nid": pa.array([0], type=pa.uint64())})

    op = RandomWalkOperator(_Seeds(), forward=case_c["viewed_csr"], length=3, num_walks=2, seed=11)
    out = list(run_pipeline(op))[0]
    assert sorted(set(out["walk_id"].to_pylist())) == [0, 1]
    assert out.num_rows >= 2
