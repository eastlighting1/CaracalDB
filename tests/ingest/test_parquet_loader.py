from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from caracaldb.ingest import ingest_edges_from_parquet, ingest_nodes_from_parquet
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle


def _write_parquet(path: Path, table: pa.Table) -> Path:
    pq.write_table(table, str(path))
    return path


def test_ingest_nodes_streams_chunks_and_assigns_nids(tmp_path: Path) -> None:
    parquet_path = _write_parquet(
        tmp_path / "genes.parquet",
        pa.table(
            {
                "symbol": ["TP53", "MDM2", "BRCA1", "EGFR"],
                "chromosome": ["17", "12", "17", "7"],
            }
        ),
    )
    bundle = create_bundle(tmp_path / "bio")
    store, report = ingest_nodes_from_parquet(
        bundle,
        parquet_path=parquet_path,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        chunksize=2,
    )

    assert report.rows_read == 4
    assert report.rows_written == 4
    assert report.chunks == 2
    assert store.next_nid == 4
    table = store.to_table()
    assert table["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1", "EGFR"]


def test_ingest_nodes_renames_columns_and_drops_inbound_nid(tmp_path: Path) -> None:
    parquet_path = _write_parquet(
        tmp_path / "genes.parquet",
        pa.table({"nid": [42, 7], "gene_symbol": ["TP53", "MDM2"]}),
    )
    bundle = create_bundle(tmp_path / "bio")
    store, report = ingest_nodes_from_parquet(
        bundle,
        parquet_path=parquet_path,
        class_iri="http://x/Gene",
        local_name="Gene",
        column_map={"gene_symbol": "symbol"},
    )
    assert report.rows_written == 2
    table = store.to_table()
    assert table.column_names == ["nid", "symbol"]
    assert table["nid"].to_pylist() == [0, 1]


def test_ingest_edges_quarantines_bad_chunk(tmp_path: Path) -> None:
    parquet_path = _write_parquet(
        tmp_path / "edges.parquet",
        pa.table(
            {
                "src": pa.array([1, 2, 3], type=pa.int64()),
                "dst": pa.array([10, 11, 12], type=pa.int64()),
                "score": [0.1, 0.2, 0.3],
            }
        ),
    )
    bundle = create_bundle(tmp_path / "bio")
    store, report = ingest_edges_from_parquet(
        bundle,
        parquet_path=parquet_path,
        property_iri="http://x/p",
        local_name="p",
    )
    assert report.rows_read == 3
    assert report.rows_written == 3
    assert store.to_table()["src"].to_pylist() == [1, 2, 3]


def test_ingest_edges_requires_src_dst(tmp_path: Path) -> None:
    parquet_path = _write_parquet(
        tmp_path / "edges.parquet",
        pa.table({"from_": [1], "to_": [2]}),
    )
    bundle = create_bundle(tmp_path / "bio")
    with pytest.raises(CaracalError) as exc:
        ingest_edges_from_parquet(
            bundle,
            parquet_path=parquet_path,
            property_iri="http://x/p",
            local_name="p",
        )
    assert exc.value.code == "CDB-7031"


def test_ingest_edges_with_negative_uint_quarantines(tmp_path: Path) -> None:
    parquet_path = _write_parquet(
        tmp_path / "edges.parquet",
        pa.table(
            {
                "src": pa.array([-1, 2], type=pa.int64()),
                "dst": pa.array([3, 4], type=pa.int64()),
            }
        ),
    )
    bundle = create_bundle(tmp_path / "bio")
    _, report = ingest_edges_from_parquet(
        bundle,
        parquet_path=parquet_path,
        property_iri="http://x/p",
        local_name="p",
    )
    assert report.rows_quarantined == 2
    assert report.rows_written == 0
    assert report.quarantined and report.quarantined[0]["code"] == "CDB-7031"
