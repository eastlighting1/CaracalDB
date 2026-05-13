from pathlib import Path

import numpy as np

import caracaldb as cdb


def _seed_graph(path: Path):
    db = cdb.connect(path, format="bundle")
    db.insert_node_table(
        [
            {"node_id": "a", "type": "Entity", "split": "train"},
            {"node_id": "b", "type": "Entity", "split": "train"},
            {"node_id": "c", "type": "Entity", "split": "valid"},
            {"node_id": "d", "type": "Entity", "split": "train"},
            {"node_id": "iso", "type": "Entity", "split": "train"},
        ]
    )
    db.insert_edge_table(
        [
            {"src": "a", "dst": "b", "type": "REL"},
            {"src": "a", "dst": "c", "type": "REL"},
            {"src": "a", "dst": "d", "type": "REL"},
            {"src": "b", "dst": "d", "type": "REL"},
            {"src": "c", "dst": "d", "type": "REL"},
        ]
    )
    return db


def test_sample_gnn_subgraph_returns_pyg_local_ids_and_global_n_id(tmp_path: Path) -> None:
    with _seed_graph(tmp_path / "gnn") as db:
        edge_index, n_id = db.sample_gnn_subgraph(
            seeds=["a"],
            fanouts=[2],
            edge_types=["REL"],
            strategy="first",
        )

    assert n_id.tolist() == [0, 1, 2]
    assert edge_index.shape == (2, 2)
    assert edge_index.tolist() == [[0, 0], [1, 2]]


def test_sample_gnn_subgraph_preserves_isolated_seed(tmp_path: Path) -> None:
    with _seed_graph(tmp_path / "isolated") as db:
        edge_index, n_id = db.sample_gnn_subgraph(
            seeds=["iso"],
            fanouts=[2],
            edge_types=["REL"],
        )

    assert edge_index.shape == (2, 0)
    assert n_id.tolist() == [4]


def test_sample_gnn_subgraph_is_deterministic_with_seed(tmp_path: Path) -> None:
    with _seed_graph(tmp_path / "deterministic") as db:
        left = db.sample_gnn_subgraph(
            seeds=["a"],
            fanouts=[2, 1],
            edge_types=["REL"],
            seed=17,
        )
        right = db.sample_gnn_subgraph(
            seeds=["a"],
            fanouts=[2, 1],
            edge_types=["REL"],
            seed=17,
        )

    assert np.array_equal(left[0], right[0])
    assert np.array_equal(left[1], right[1])


def test_query_nodes_uses_property_index_compatible_semantics(tmp_path: Path) -> None:
    with _seed_graph(tmp_path / "query") as db:
        db.create_property_index(name="entity_split_idx", node_type="Entity", property="split")
        ids = db.query_nodes("Entity", "split = 'train'")

    assert ids.tolist() == ["a", "b", "d", "iso"]


def test_neighbor_loader_batches_filtered_seed_nodes(tmp_path: Path) -> None:
    with _seed_graph(tmp_path / "loader") as db:
        loader = db.neighbor_loader(
            "Entity",
            fanouts=[1],
            edge_types=["REL"],
            batch_size=2,
            shuffle=False,
            filter="split = 'train'",
            strategy="first",
        )
        batches = list(loader)

    assert len(batches) == 2
    assert batches[0][0].shape[0] == 2
    assert batches[0][1].tolist() == [0, 1, 3]
    assert batches[1][1].tolist() == [3, 4]
