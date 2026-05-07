from pathlib import Path

import pyarrow as pa
import pytest

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError


def _embedding_array(values: list[float], dim: int) -> pa.FixedSizeListArray:
    return pa.FixedSizeListArray.from_arrays(pa.array(values, type=pa.float32()), dim)


def test_vector_index_lifecycle_search_filter_and_reopen(tmp_path: Path) -> None:
    path = tmp_path / "vec-db"
    with cdb.connect(path, format="bundle") as db:
        db.insert_node_table_arrow(
            pa.table(
                {
                    "node_id": pa.array(["c1", "c2", "c3"]),
                    "type": pa.array(["Chunk", "Chunk", "Chunk"]),
                    "source_type": pa.array(["document", "note", "document"]),
                    "text": pa.array(["alpha", "beta", "near alpha"]),
                    "embedding": _embedding_array([1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.9, 0.1, 0.0], 3),
                }
            )
        )

        meta = db.create_vector_index(
            name="chunk_embedding_hnsw",
            node_type="Chunk",
            property="embedding",
            dimension=3,
            metric="cosine",
            algorithm="hnsw",
            options={"ef_search": 16},
        )
        rows = db.vector_search(
            index="chunk_embedding_hnsw",
            query_vector=[1.0, 0.0, 0.0],
            top_k=2,
            filters={"source_type": "document"},
            return_properties=["text"],
        ).rows()

    assert meta["status"] == "ready"
    assert meta["count"] == 3
    assert [row["node_id"] for row in rows] == ["c1", "c3"]
    assert rows[0]["rank"] == 1
    assert rows[0]["matched_property"] == "embedding"
    assert rows[0]["selected_properties"] == {"text": "alpha"}

    with cdb.connect(path, format="bundle") as db:
        indexes = db.list_vector_indexes()
        reopened_rows = db.vector_search(
            index="chunk_embedding_hnsw",
            query_vector=[1.0, 0.0, 0.0],
            top_k=1,
            return_properties=["text"],
        ).rows()
        rebuilt = db.rebuild_vector_index("chunk_embedding_hnsw")
        dropped = db.drop_vector_index("chunk_embedding_hnsw")

    assert indexes[0]["name"] == "chunk_embedding_hnsw"
    assert reopened_rows[0]["node_id"] == "c1"
    assert rebuilt["count"] == 3
    assert dropped is True


def test_vector_dimension_mismatch_and_empty_index(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "vec-errors", format="bundle") as db:
        db.insert_node_table_arrow(
            pa.table(
                {
                    "node_id": pa.array(["c1"]),
                    "type": pa.array(["Chunk"]),
                    "embedding": _embedding_array([1.0, 0.0, 0.0], 3),
                }
            )
        )

        with pytest.raises(CaracalError) as exc:
            db.create_vector_index(
                name="bad_dim",
                node_type="Chunk",
                property="embedding",
                dimension=2,
            )

    assert exc.value.code == "CDB-7091"
    assert "dimension mismatch" in exc.value.message

    with cdb.connect(tmp_path / "empty-vec", format="bundle") as db:
        db.insert_node_table_arrow(
            pa.table(
                {
                    "node_id": pa.array(["c1"]),
                    "type": pa.array(["Chunk"]),
                    "embedding": pa.array([None], type=pa.list_(pa.float32())),
                }
            )
        )
        db.create_vector_index(
            name="empty_idx",
            node_type="Chunk",
            property="embedding",
            dimension=3,
            options={"allow_null_vectors": True},
        )
        result = db.vector_search(index="empty_idx", query_vector=[1.0, 0.0, 0.0], top_k=5)

    assert result.rows() == []
    assert result.arrow().column_names[:7] == [
        "node_id",
        "node_type",
        "internal_id",
        "score",
        "distance",
        "rank",
        "matched_property",
    ]


def test_neighbors_and_k_hop_support_typed_weighted_traversal(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "traversal", format="bundle") as db:
        db.insert_node_table(
            [
                {"node_id": "a", "type": "Entity", "name": "A"},
                {"node_id": "b", "type": "Entity", "name": "B"},
                {"node_id": "c", "type": "Chunk", "name": "C"},
            ]
        )
        db.insert_edge_table(
            [
                {"src": "a", "dst": "b", "type": "RELATED_TO", "weight": 0.9},
                {"src": "b", "dst": "c", "type": "EVIDENCED_BY", "weight": 0.7},
                {"src": "c", "dst": "a", "type": "RELATED_TO", "weight": 0.1},
            ]
        )

        neighbors = db.neighbors(
            seed_node_ids=["a"],
            edge_types=["RELATED_TO", "EVIDENCED_BY"],
            direction="out",
            depth=2,
            edge_filters={"weight_gte": 0.5},
            return_paths=True,
        ).rows()
        subgraph = db.k_hop(
            seeds=["a"],
            depth=2,
            edge_types=["RELATED_TO", "EVIDENCED_BY"],
            direction="out",
        )

    assert [row["node_id"] for row in neighbors] == ["b", "c"]
    assert neighbors[1]["depth"] == 2
    assert neighbors[1]["path_node_ids"] == ["a", "b", "c"]
    assert neighbors[1]["path_edge_types"] == ["RELATED_TO", "EVIDENCED_BY"]
    assert [row["node_id"] for row in subgraph["nodes"].to_pylist()] == ["a", "b", "c"]
    assert subgraph["edges"].num_rows == 2


def test_bounded_paths_shortest_path_and_weighted_scoring(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "paths", format="bundle") as db:
        db.insert_node_table(
            [
                {"node_id": "a", "type": "Entity"},
                {"node_id": "b", "type": "Entity"},
                {"node_id": "c", "type": "Entity"},
            ]
        )
        db.insert_edge_table(
            [
                {"src": "a", "dst": "b", "type": "RELATED_TO", "weight": 0.5},
                {"src": "b", "dst": "c", "type": "RELATED_TO", "weight": 0.8},
                {"src": "a", "dst": "c", "type": "RELATED_TO", "weight": 0.2},
            ]
        )

        scored = db.paths(
            source="a",
            target="c",
            edge_types=["RELATED_TO"],
            max_depth=2,
            score="sum",
            score_property="weight",
        ).rows()
        shortest = db.shortest_path(source="a", target="c", edge_types=["RELATED_TO"])
        top_neighbor = db.neighbors(
            seed_node_ids=["a"],
            edge_types=["RELATED_TO"],
            weight_property="weight",
            top_edges_per_node=1,
        ).rows()

    assert scored[0]["node_ids"] == ["a", "b", "c"]
    assert scored[0]["edge_ids"] == [0, 1]
    assert scored[0]["relation_types"] == ["RELATED_TO", "RELATED_TO"]
    assert scored[0]["directions"] == ["out", "out"]
    assert scored[0]["path_score"] == pytest.approx(1.3)
    assert shortest is not None
    assert shortest["node_ids"] == ["a", "c"]
    assert shortest["depth"] == 1
    assert [row["node_id"] for row in top_neighbor] == ["b"]


def test_tuft_vector_search_call_and_vector_projection_ordering(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "tuft-vector", format="bundle") as db:
        db.insert_node_table_arrow(
            pa.table(
                {
                    "node_id": pa.array(["c1", "c2", "c3"]),
                    "type": pa.array(["Chunk", "Chunk", "Chunk"]),
                    "embedding": _embedding_array([1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.8, 0.2, 0.0], 3),
                }
            )
        )
        db.create_vector_index(
            name="chunk_embedding_hnsw",
            node_type="Chunk",
            property="embedding",
            dimension=3,
        )

        call_rows = db.sql("""
            CALL vector.search('chunk_embedding_hnsw', [1.0, 0.0, 0.0], 3)
            YIELD node_id, score
            RETURN node_id, score
            ORDER BY score DESC
            LIMIT 2
            """).rows()
        projected_rows = db.sql("""
            MATCH (c:Chunk)
            RETURN c.node_id, cosine_similarity(c.embedding, [1.0, 0.0, 0.0]) AS score
            ORDER BY score DESC
            LIMIT 2
            """).rows()
        profile = db.profile("CALL vector.search('chunk_embedding_hnsw', [1.0, 0.0, 0.0], 1)")

    assert [row["node_id"] for row in call_rows] == ["c1", "c3"]
    assert [row["node_id"] for row in projected_rows] == ["c1", "c3"]
    assert profile["vector_index_used"] == "chunk_embedding_hnsw"
    assert profile["fallback_flags"] == []


def test_tuft_variable_length_paths_return_path_objects(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "tuft-vpath", format="bundle") as db:
        db.insert_node_table(
            [
                {"node_id": "a", "type": "Entity", "name": "A"},
                {"node_id": "b", "type": "Entity", "name": "B"},
                {"node_id": "c", "type": "Entity", "name": "C"},
            ]
        )
        db.insert_edge_table(
            [
                {"src": "a", "dst": "b", "type": "RELATED_TO", "weight": 0.5},
                {"src": "b", "dst": "c", "type": "RELATED_TO", "weight": 0.8},
                {"src": "a", "dst": "c", "type": "RELATED_TO", "weight": 0.2},
            ]
        )

        rows = db.sql("""
            MATCH p = (a:Entity)-[:RELATED_TO*1..2]->(c:Entity)
            WHERE a.node_id = 'a' AND c.node_id = 'c'
            RETURN p, length(p) AS hops, c.node_id AS target
            ORDER BY hops DESC
            LIMIT 2
            """).rows()
        profile = db.profile("""
            MATCH p = (a:Entity)-[:RELATED_TO*1..2]->(c:Entity)
            RETURN p
            LIMIT 1
            """)
        caps = db.capabilities()

    assert [row["hops"] for row in rows] == [2, 1]
    assert rows[0]["p"]["node_ids"] == ["a", "b", "c"]
    assert rows[0]["p"]["edge_ids"] == [0, 1]
    assert rows[0]["target"] == "c"
    assert profile["physical_plan"] == "VariableLengthPath"
    assert profile["fallback_flags"] == []
    assert caps["tuft.variable_length_paths"] is True


def test_batch_upsert_nodes_edges_property_index_and_capabilities(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "upsert", format="bundle") as db:
        first = db.upsert_node_table_arrow(
            pa.table({"node_id": ["a", "b"], "type": ["Entity", "Entity"], "name": ["A", "B"]})
        )
        second = db.upsert_node_table_arrow(
            pa.table({"node_id": ["a", "c"], "type": ["Entity", "Entity"], "name": ["A2", "C"]})
        )
        edge_first = db.upsert_edge_table_arrow(
            pa.table(
                {
                    "edge_id": ["e1"],
                    "src": ["a"],
                    "dst": ["b"],
                    "type": ["RELATED_TO"],
                    "weight": [0.1],
                }
            )
        )
        edge_second = db.upsert_edge_table_arrow(
            pa.table(
                {
                    "edge_id": ["e1", "e2"],
                    "src": ["a", "b"],
                    "dst": ["c", "c"],
                    "type": ["RELATED_TO", "RELATED_TO"],
                    "weight": [0.9, 0.3],
                }
            )
        )
        prop_idx = db.create_property_index(
            name="entity_name_idx", node_type="Entity", property="name"
        )
        caps = db.capabilities()
        profile = db.profile("MATCH (e:Entity) RETURN e.name LIMIT 2")
        nodes = db.node_table("Entity").to_pylist()
        edges = db.edge_table("RELATED_TO").to_pylist()

    assert first == {"inserted": 2, "updated": 0, "skipped": 0, "failed": 0}
    assert second == {"inserted": 1, "updated": 1, "skipped": 0, "failed": 0}
    assert edge_first["inserted"] == 1
    assert edge_second == {"inserted": 1, "updated": 1, "skipped": 0, "failed": 0}
    assert [row["name"] for row in nodes] == ["A2", "B", "C"]
    assert [row["edge_id"] for row in edges] == ["e1", "e2"]
    assert edges[0]["weight"] == 0.9
    assert prop_idx["status"] == "ready"
    assert caps["vector_search"] is True
    assert caps["traversal.k_hop"] is True
    assert caps["traversal.shortest_path"] is True
    assert caps["tuft.vector_search"] is True
    assert profile["result_count"] == 2
    assert "operator_timings" in profile


def test_public_vector_distance_helpers() -> None:
    assert cdb.cosine_similarity([1.0, 0.0], [1.0, 0.0]) == 1.0
    assert cdb.cosine_distance([1.0, 0.0], [1.0, 0.0]) == 0.0
    assert cdb.dot_product([1.0, 2.0], [3.0, 4.0]) == 11.0
    assert cdb.l2_distance([0.0, 0.0], [3.0, 4.0]) == 5.0
    with pytest.raises(CaracalError):
        cdb.cosine_similarity([1.0], [1.0, 2.0])
