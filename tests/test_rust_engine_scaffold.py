from __future__ import annotations

import importlib
import json
from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.engine import resolve_engine
from caracaldb.graph.csc_builder import build_csc as build_python_csc
from caracaldb.graph.csr_builder import build_csr as build_python_csr
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle
from caracaldb.storage.column_store import read_column_segment, write_column_segment
from caracaldb.storage.edge_store import DST_COLUMN, SRC_COLUMN, open_edge_store
from caracaldb.storage.node_store import open_node_store


def _table_to_ipc_stream(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def test_engine_defaults_to_python_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CARACALDB_ENGINE", raising=False)
    selection = resolve_engine()
    assert selection.requested == "python"
    assert selection.active == "python"


def test_auto_mode_keeps_python_reference_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CARACALDB_ENGINE", "auto")
    selection = resolve_engine()
    assert selection.requested == "auto"
    assert selection.active == "python"


def test_invalid_engine_mode_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CARACALDB_ENGINE", "bogus")
    with pytest.raises(CaracalError, match="CDB-9007"):
        resolve_engine()


def test_rust_extension_smoke_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = rust.create_bundle(str(tmp_path / "demo"), False)
    assert bundle["path"].endswith(".crcl")
    assert bundle["format_version"] == 1
    opened = rust.open_bundle(str(tmp_path / "demo"))
    assert opened["path"] == bundle["path"]


def test_rust_csr_round_trip_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "graph.csr"
    built = rust.build_csr(str(path), 3, [1, 0, 1], [2, 1, 0], [11, 10, 12])
    assert built["offsets"] == [0, 1, 3, 3]
    assert built["neighbors"] == [1, 2, 0]
    assert built["eids"] == [10, 11, 12]
    assert importlib.import_module("caracaldb._caracaldb_rust").read_csr(str(path)) == built


def test_rust_csr_writer_matches_python_bytes_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    rust_path = tmp_path / "rust.csr"
    python_path = tmp_path / "python.csr"
    edges = pa.table(
        {
            "eid": pa.array([11, 10, 12], type=pa.uint64()),
            SRC_COLUMN: pa.array([1, 0, 1], type=pa.uint64()),
            DST_COLUMN: pa.array([2, 1, 0], type=pa.uint64()),
        }
    )

    rust.build_csr(str(rust_path), 3, [1, 0, 1], [2, 1, 0], [11, 10, 12])
    build_python_csr(edges, num_vertices=3, out_path=python_path, with_eids=True)
    assert rust_path.read_bytes() == python_path.read_bytes()


def test_rust_csr_neighbors_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "graph.csr"
    rust.build_csr(str(path), 3, [1, 0, 1], [2, 1, 0], [11, 10, 12])

    result = rust.csr_neighbors(str(path), 1)
    assert result == {"vertex": 1, "neighbors": [2, 0], "eids": [11, 12]}

    with pytest.raises(Exception, match="CDB-7083"):
        rust.csr_neighbors(str(path), 3)


def test_rust_graph_traversal_bindings_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "graph.csr"
    rust.build_csr(str(path), 4, [0, 0, 1, 2], [1, 2, 3, 3], [10, 11, 12, 13])

    assert rust.csr_k_hop_rows(str(path), [0], 1, 2) == [
        {"seed": 0, "node": 1, "depth": 1, "path_nodes": [0, 1], "path_eids": [10]},
        {"seed": 0, "node": 2, "depth": 1, "path_nodes": [0, 2], "path_eids": [11]},
        {"seed": 0, "node": 3, "depth": 2, "path_nodes": [0, 1, 3], "path_eids": [10, 12]},
    ]
    assert rust.csr_shortest_path_row(str(path), 0, 3, 2) == {
        "nodes": [0, 1, 3],
        "eids": [10, 12],
    }
    assert rust.csr_typed_neighbors(str(path), "RELATED_TO", 0) == [
        {"edge_type": "RELATED_TO", "dst": 1, "eid": 10},
        {"edge_type": "RELATED_TO", "dst": 2, "eid": 11},
    ]
    assert rust.csr_neighbor_sample_rows(str(path), [0], 1, False) == [
        {"src": 0, "dst": 1, "eid": 10}
    ]
    assert rust.hnsw_boundary("vec_idx", "embedding")["storage_boundary"] == "manifest-only"


def test_rust_csc_writer_matches_python_bytes_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    rust_path = tmp_path / "rust.csc"
    python_path = tmp_path / "python.csc"
    edges = pa.table(
        {
            "eid": pa.array([11, 10, 12], type=pa.uint64()),
            SRC_COLUMN: pa.array([1, 0, 1], type=pa.uint64()),
            DST_COLUMN: pa.array([2, 1, 0], type=pa.uint64()),
        }
    )

    rust.build_csc(str(rust_path), 3, [1, 0, 1], [2, 1, 0], [11, 10, 12])
    build_python_csc(edges, num_vertices=3, out_path=python_path, with_eids=True)
    assert rust_path.read_bytes() == python_path.read_bytes()


def test_rust_reads_python_store_manifests_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    open_node_store(bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True)
    open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        create=True,
    )

    assert rust.list_node_stores(str(bundle.path)) == ["Gene"]
    assert rust.list_edge_stores(str(bundle.path)) == ["interactsWith"]
    node = rust.open_node_store(str(bundle.path), "http://example.org/Gene", "Gene")
    edge = rust.open_edge_store(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
    )
    assert node["next_nid"] == 0
    assert edge["next_eid"] == 0


def test_python_reads_rust_store_manifests_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = rust.create_bundle(str(tmp_path / "compat"), False)
    rust.open_node_store(bundle["path"], "http://example.org/Gene", "Gene", True)
    rust.open_edge_store(bundle["path"], "http://example.org/interactsWith", "interactsWith", True)

    py_bundle = create_bundle(tmp_path / "compat", exist_ok=True)
    node = open_node_store(py_bundle, class_iri="http://example.org/Gene", local_name="Gene")
    edge = open_edge_store(
        py_bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
    )
    assert node.next_nid == 0
    assert edge.next_eid == 0


def test_rust_appends_node_batch_and_python_reads_it_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    first = pa.table(
        {
            "symbol": pa.array(["TP53", "MDM2"], type=pa.string()),
            "alias": pa.array(["P53", None], type=pa.string()),
        }
    )
    second = pa.table(
        {
            "symbol": pa.array(["BRCA1"], type=pa.string()),
            "alias": pa.array([None], type=pa.string()),
        }
    )

    result_1 = rust.append_node_batch(
        str(bundle.path),
        "http://example.org/Gene",
        "Gene",
        _table_to_ipc_stream(first),
        7,
    )
    result_2 = rust.append_node_batch(
        str(bundle.path),
        "http://example.org/Gene",
        "Gene",
        _table_to_ipc_stream(second),
        8,
    )

    assert result_1["start_nid"] == 0
    assert result_1["end_nid"] == 2
    assert result_2["start_nid"] == 2
    assert result_2["end_nid"] == 3

    py_store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
    )
    table = py_store.to_table()
    assert table["nid"].to_pylist() == [0, 1, 2]
    assert table["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1"]
    assert table["alias"].to_pylist() == ["P53", None, None]
    assert table.schema.field("alias").nullable
    internal = py_store.schema
    assert internal.field("_created_lsn").type == pa.uint64()
    assert internal.field("_deleted_lsn").type == pa.uint64()
    assert internal.field("_deleted_lsn").nullable
    assert py_store.next_nid == 3


def test_rust_appends_edge_batch_and_python_reads_it_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        create=True,
    )
    first = pa.table(
        {
            SRC_COLUMN: pa.array([0, 1], type=pa.uint64()),
            DST_COLUMN: pa.array([1, 2], type=pa.uint64()),
            "score": pa.array([0.9, 0.7], type=pa.float64()),
        }
    )
    second = pa.table(
        {
            SRC_COLUMN: pa.array([2], type=pa.uint64()),
            DST_COLUMN: pa.array([3], type=pa.uint64()),
            "score": pa.array([0.1], type=pa.float64()),
        }
    )

    result_1 = rust.append_edge_batch(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
        _table_to_ipc_stream(first),
        7,
    )
    result_2 = rust.append_edge_batch(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
        _table_to_ipc_stream(second),
        8,
    )

    assert result_1["start_eid"] == 0
    assert result_1["end_eid"] == 2
    assert result_2["start_eid"] == 2
    assert result_2["end_eid"] == 3

    py_store = open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
    )
    table = py_store.to_table()
    assert table["eid"].to_pylist() == [0, 1, 2]
    assert table["src"].to_pylist() == [0, 1, 2]
    assert table["dst"].to_pylist() == [1, 2, 3]
    assert table["score"].to_pylist() == [0.9, 0.7, 0.1]
    internal = py_store.schema
    assert internal.field("_created_lsn").type == pa.uint64()
    assert internal.field("_deleted_lsn").type == pa.uint64()
    assert internal.field("_deleted_lsn").nullable
    assert py_store.next_eid == 3


def test_rust_append_rejects_empty_and_schema_drift_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    rust.append_node_batch(
        str(bundle.path),
        "http://example.org/Gene",
        "Gene",
        _table_to_ipc_stream(pa.table({"symbol": pa.array(["TP53"], type=pa.string())})),
    )

    with pytest.raises(Exception, match="CDB-7011"):
        rust.append_node_batch(
            str(bundle.path),
            "http://example.org/Gene",
            "Gene",
            _table_to_ipc_stream(pa.table({"symbol": pa.array([], type=pa.string())})),
        )
    with pytest.raises(Exception, match="CDB-7011"):
        rust.append_node_batch(
            str(bundle.path),
            "http://example.org/Gene",
            "Gene",
            _table_to_ipc_stream(pa.table({"name": pa.array(["BRCA1"], type=pa.string())})),
        )

    open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        create=True,
    )
    rust.append_edge_batch(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
        _table_to_ipc_stream(
            pa.table(
                {
                    SRC_COLUMN: pa.array([0], type=pa.uint64()),
                    DST_COLUMN: pa.array([1], type=pa.uint64()),
                    "score": pa.array([0.9], type=pa.float64()),
                }
            )
        ),
    )
    with pytest.raises(Exception, match="CDB-7021"):
        rust.append_edge_batch(
            str(bundle.path),
            "http://example.org/interactsWith",
            "interactsWith",
            _table_to_ipc_stream(
                pa.table(
                    {
                        SRC_COLUMN: pa.array([], type=pa.uint64()),
                        DST_COLUMN: pa.array([], type=pa.uint64()),
                        "score": pa.array([], type=pa.float64()),
                    }
                )
            ),
        )
    with pytest.raises(Exception, match="CDB-7021"):
        rust.append_edge_batch(
            str(bundle.path),
            "http://example.org/interactsWith",
            "interactsWith",
            _table_to_ipc_stream(
                pa.table(
                    {
                        SRC_COLUMN: pa.array([1], type=pa.uint64()),
                        DST_COLUMN: pa.array([2], type=pa.uint64()),
                        "weight": pa.array([0.8], type=pa.float64()),
                    }
                )
            ),
        )


def test_rust_reads_python_column_segment_footer_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "00000000.col"
    batch = pa.record_batch(
        {
            "nid": pa.array([0, 1], type=pa.uint64()),
            "symbol": pa.array(["TP53", "MDM2"], type=pa.string()),
        }
    )
    footer = write_column_segment(path, [batch])

    info = rust.read_column_segment_info(str(path))
    assert info["format_version"] == footer.format_version
    assert info["codec"] == footer.codec
    assert info["row_count"] == footer.row_count
    assert info["batch_count"] == footer.batch_count
    assert info["payload_size"] == footer.payload_size
    assert info["payload_offset"] == 24

    decoded = rust.decode_column_segment(str(path))
    assert decoded["field_names"] == ["nid", "symbol"]
    assert decoded["info"]["row_count"] == 2
    assert decoded["batches"] == [{"row_count": 2, "column_count": 2}]


def test_rust_decodes_python_zstd_column_segment_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "00000000.col"
    batch = pa.record_batch(
        {
            "nid": pa.array([0, 1, 2], type=pa.uint64()),
            "symbol": pa.array(["TP53", "MDM2", "BRCA1"], type=pa.string()),
        }
    )
    footer = write_column_segment(path, [batch], codec="zstd")

    decoded = rust.decode_column_segment(str(path))
    assert decoded["info"]["codec"] == "zstd"
    assert decoded["info"]["row_count"] == footer.row_count
    assert decoded["field_names"] == ["nid", "symbol"]
    assert decoded["batches"] == [{"row_count": 3, "column_count": 2}]


def test_rust_decodes_python_lz4_column_segment_if_installed(tmp_path: Path) -> None:
    pytest.importorskip("lz4.frame")
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "00000000.col"
    batch = pa.record_batch(
        {
            "nid": pa.array([0, 1, 2], type=pa.uint64()),
            "symbol": pa.array(["TP53", "MDM2", "BRCA1"], type=pa.string()),
        }
    )
    footer = write_column_segment(path, [batch], codec="lz4")

    decoded = rust.decode_column_segment(str(path))
    assert decoded["info"]["codec"] == "lz4"
    assert decoded["info"]["row_count"] == footer.row_count
    assert decoded["field_names"] == ["nid", "symbol"]
    assert decoded["batches"] == [{"row_count": 3, "column_count": 2}]


def test_python_reads_rust_written_column_segment_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "rust.col"
    table = pa.table(
        {
            "nid": pa.array([0, 1, 2], type=pa.uint64()),
            "symbol": pa.array(["TP53", "MDM2", "BRCA1"], type=pa.string()),
        }
    )
    ipc_stream = _table_to_ipc_stream(table)

    info = rust.write_column_segment_from_ipc(str(path), ipc_stream)
    assert info["codec"] == "none"
    assert info["row_count"] == 3
    assert info["batch_count"] == 1

    decoded = rust.decode_column_segment(str(path))
    assert decoded["field_names"] == ["nid", "symbol"]
    assert decoded["batches"] == [{"row_count": 3, "column_count": 2}]

    read_back = read_column_segment(path)
    assert read_back["nid"].to_pylist() == [0, 1, 2]
    assert read_back["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1"]


def test_rust_column_writer_preserves_arrow_schema_subset_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "schema-subset.col"
    table = pa.table(
        {
            "u": pa.array([1, 2], type=pa.uint64()),
            "i": pa.array([-1, None], type=pa.int64()),
            "f": pa.array([1.5, None], type=pa.float64()),
            "b": pa.array([True, False], type=pa.bool_()),
            "s": pa.array(["x", None], type=pa.string()),
        }
    )

    rust.write_column_segment_from_ipc(str(path), _table_to_ipc_stream(table))
    read_back = read_column_segment(path)

    assert read_back.schema == table.schema
    assert read_back.to_pylist() == table.to_pylist()


@pytest.mark.parametrize("codec", ["zstd", "lz4"])
def test_python_reads_rust_written_compressed_column_segment_if_installed(
    tmp_path: Path, codec: str
) -> None:
    if codec == "lz4":
        pytest.importorskip("lz4.frame")
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / f"rust-{codec}.col"
    table = pa.table(
        {
            "nid": pa.array([0, 1, 2], type=pa.uint64()),
            "symbol": pa.array(["TP53", "MDM2", "BRCA1"], type=pa.string()),
        }
    )
    info = rust.write_column_segment_from_ipc(str(path), _table_to_ipc_stream(table), codec)
    assert info["codec"] == codec
    assert info["row_count"] == 3

    read_back = read_column_segment(path)
    assert read_back["nid"].to_pylist() == [0, 1, 2]
    assert read_back["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1"]


def test_rust_column_writer_tolerates_leftover_temp_file_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    path = tmp_path / "rust.col"
    path.with_name("rust.col.tmp").write_bytes(b"partial stale temp segment")
    table = pa.table({"nid": pa.array([0, 1], type=pa.uint64())})

    rust.write_column_segment_from_ipc(str(path), _table_to_ipc_stream(table))

    read_back = read_column_segment(path)
    assert read_back["nid"].to_pylist() == [0, 1]
    assert not path.with_name("rust.col.tmp").exists()


def test_rust_rejects_partial_store_manifest_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    root = bundle.path / "nodes" / "Gene"
    root.mkdir(parents=True)
    (root / "_manifest.json").write_text(
        json.dumps({"class_iri": "http://example.org/Gene", "local_name": "Gene"})[:-2],
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="CDB-9000"):
        rust.open_node_store(str(bundle.path), "http://example.org/Gene", "Gene")


def test_rust_scans_python_node_store_summary_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    batch = pa.record_batch(
        {
            "symbol": pa.array(["TP53", "MDM2"], type=pa.string()),
            "chromosome": pa.array(["17", "12"], type=pa.string()),
        }
    )
    store.append(batch)

    summary = rust.scan_node_store_summary(
        str(bundle.path),
        "http://example.org/Gene",
        "Gene",
    )
    assert summary["chunk_count"] == 1
    assert summary["batch_count"] == 1
    assert summary["row_count"] == 2
    assert summary["field_names"] == ["nid", "symbol", "chromosome", "_created_lsn", "_deleted_lsn"]

    streams = rust.scan_node_store(
        str(bundle.path),
        "http://example.org/Gene",
        "Gene",
    )
    tables = [pa.ipc.open_stream(pa.BufferReader(stream)).read_all() for stream in streams]
    table = pa.concat_tables(tables)
    assert table["nid"].to_pylist() == [0, 1]
    assert table["symbol"].to_pylist() == ["TP53", "MDM2"]


def test_rust_node_scan_applies_snapshot_visibility_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    store = open_node_store(
        bundle,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        create=True,
    )
    store.append(
        pa.record_batch({"symbol": pa.array(["TP53", "MDM2"], type=pa.string())}),
        created_lsn=1,
    )
    store.append(
        pa.record_batch({"symbol": pa.array(["BRCA1"], type=pa.string())}),
        created_lsn=3,
    )

    streams = rust.scan_node_store(
        str(bundle.path),
        "http://example.org/Gene",
        "Gene",
        2,
    )
    tables = [pa.ipc.open_stream(pa.BufferReader(stream)).read_all() for stream in streams]
    table = pa.concat_tables(tables)
    assert table["symbol"].to_pylist() == ["TP53", "MDM2"]


def test_rust_scans_python_edge_store_summary_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    store = open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        create=True,
    )
    batch = pa.record_batch(
        {
            SRC_COLUMN: pa.array([0, 1], type=pa.uint64()),
            DST_COLUMN: pa.array([1, 2], type=pa.uint64()),
            "score": pa.array([0.9, 0.7], type=pa.float64()),
        }
    )
    store.append(batch)

    summary = rust.scan_edge_store_summary(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
    )
    assert summary["chunk_count"] == 1
    assert summary["batch_count"] == 1
    assert summary["row_count"] == 2
    assert summary["field_names"] == ["eid", "src", "dst", "score", "_created_lsn", "_deleted_lsn"]

    streams = rust.scan_edge_store(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
    )
    tables = [pa.ipc.open_stream(pa.BufferReader(stream)).read_all() for stream in streams]
    table = pa.concat_tables(tables)
    assert table["eid"].to_pylist() == [0, 1]
    assert table["src"].to_pylist() == [0, 1]
    assert table["dst"].to_pylist() == [1, 2]
    assert table["score"].to_pylist() == [0.9, 0.7]


def test_rust_edge_scan_applies_snapshot_visibility_if_installed(tmp_path: Path) -> None:
    rust = pytest.importorskip("caracaldb._caracaldb_rust")
    bundle = create_bundle(tmp_path / "compat")
    store = open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        create=True,
    )
    store.append(
        pa.record_batch(
            {
                SRC_COLUMN: pa.array([0, 1], type=pa.uint64()),
                DST_COLUMN: pa.array([1, 2], type=pa.uint64()),
                "score": pa.array([0.9, 0.7], type=pa.float64()),
            }
        ),
        created_lsn=1,
    )
    store.append(
        pa.record_batch(
            {
                SRC_COLUMN: pa.array([2], type=pa.uint64()),
                DST_COLUMN: pa.array([3], type=pa.uint64()),
                "score": pa.array([0.1], type=pa.float64()),
            }
        ),
        created_lsn=3,
    )

    streams = rust.scan_edge_store(
        str(bundle.path),
        "http://example.org/interactsWith",
        "interactsWith",
        2,
    )
    tables = [pa.ipc.open_stream(pa.BufferReader(stream)).read_all() for stream in streams]
    table = pa.concat_tables(tables)
    assert table["src"].to_pylist() == [0, 1]
    assert table["dst"].to_pylist() == [1, 2]
    assert table["score"].to_pylist() == [0.9, 0.7]
