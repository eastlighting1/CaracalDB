from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import (
    DST_COLUMN,
    EID_COLUMN,
    SRC_COLUMN,
    list_edge_stores,
    open_edge_store,
)


def _interaction_batch(pairs: list[tuple[int, int]], scores: list[float]) -> pa.RecordBatch:
    src = pa.array([p[0] for p in pairs], type=pa.uint64())
    dst = pa.array([p[1] for p in pairs], type=pa.uint64())
    score = pa.array(scores, type=pa.float64())
    return pa.RecordBatch.from_arrays([src, dst, score], names=[SRC_COLUMN, DST_COLUMN, "score"])


def test_edge_store_assigns_eids_and_round_trips(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_edge_store(
        bundle,
        property_iri="http://example.org/interactsWith",
        local_name="interactsWith",
        src_class_iri="http://example.org/Gene",
        dst_class_iri="http://example.org/Gene",
        create=True,
    )
    store.append(_interaction_batch([(0, 1), (1, 2)], [0.9, 0.7]))
    store.append(_interaction_batch([(2, 0)], [0.5]))

    assert store.next_eid == 3
    table = store.to_table()
    assert table.num_rows == 3
    assert table.column_names[0] == EID_COLUMN
    assert table[EID_COLUMN].to_pylist() == [0, 1, 2]
    assert table[SRC_COLUMN].to_pylist() == [0, 1, 2]
    assert table[DST_COLUMN].to_pylist() == [1, 2, 0]
    assert "interactsWith" in list_edge_stores(bundle)


def test_edge_store_persists_across_open(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_edge_store(
        bundle,
        property_iri="http://example.org/p",
        local_name="p",
        create=True,
    )
    store.append(_interaction_batch([(0, 1)], [0.1]))

    reopened = open_edge_store(bundle, property_iri="http://example.org/p", local_name="p")
    assert reopened.next_eid == 1
    assert reopened.to_table()[SRC_COLUMN].to_pylist() == [0]


def test_edge_store_rejects_missing_required_columns(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    bad = pa.record_batch({SRC_COLUMN: pa.array([0], type=pa.uint64())})
    with pytest.raises(CaracalError) as exc:
        store.append(bad)
    assert exc.value.code == "CDB-7021"


def test_edge_store_rejects_bad_nid_type(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    bad = pa.record_batch(
        {
            SRC_COLUMN: pa.array([1], type=pa.int64()),
            DST_COLUMN: pa.array([2], type=pa.int64()),
        }
    )
    with pytest.raises(CaracalError) as exc:
        store.append(bad)
    assert exc.value.code == "CDB-7021"


def test_edge_store_rejects_property_mismatch(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    with pytest.raises(CaracalError) as exc:
        open_edge_store(bundle, property_iri="http://x/q", local_name="p")
    assert exc.value.code == "CDB-7023"
