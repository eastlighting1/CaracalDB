from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import (
    ColumnReader,
    ColumnWriter,
    read_column_segment,
    write_column_segment,
)


def _sample_batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "nid": pa.array([1, 2, 3], type=pa.uint64()),
            "symbol": pa.array(["TP53", "MDM2", "BRCA1"], type=pa.string()),
        }
    )


def test_column_segment_round_trips_arrow_table(tmp_path: Path) -> None:
    path = tmp_path / "Gene.col"
    footer = write_column_segment(path, [_sample_batch()])

    table = read_column_segment(path)

    assert footer.row_count == 3
    assert footer.batch_count == 1
    assert table.column_names == ["nid", "symbol"]
    assert table["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1"]


def test_column_writer_rejects_schema_mismatch(tmp_path: Path) -> None:
    writer = ColumnWriter(tmp_path / "bad.col")
    writer.append(_sample_batch())

    with pytest.raises(CaracalError) as exc:
        writer.append(pa.record_batch({"nid": pa.array([1], type=pa.uint64())}))

    assert exc.value.code == "CDB-7001"


def test_column_reader_rejects_bad_magic(tmp_path: Path) -> None:
    path = tmp_path / "bad.col"
    path.write_bytes(b"not a caracal segment")

    with pytest.raises(CaracalError) as exc:
        ColumnReader(path)

    assert exc.value.code == "CDB-7001"


def test_column_segment_zstd_round_trip_when_available(tmp_path: Path) -> None:
    pytest.importorskip("zstandard")

    path = tmp_path / "Gene.zstd.col"
    footer = write_column_segment(path, [_sample_batch()], codec="zstd")

    assert footer.codec == "zstd"
    assert read_column_segment(path).num_rows == 3
