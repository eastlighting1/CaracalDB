from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import (
    NID_COLUMN,
    list_node_stores,
    open_node_store,
)


def _gene_batch(symbols: list[str]) -> pa.RecordBatch:
    return pa.record_batch(
        {
            "symbol": pa.array(symbols, type=pa.string()),
            "chromosome": pa.array(["17"] * len(symbols), type=pa.string()),
        }
    )


def test_node_store_assigns_nids_and_round_trips(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )

    ref_a = store.append(_gene_batch(["TP53", "MDM2"]))
    ref_b = store.append(_gene_batch(["BRCA1"]))

    assert ref_a.start_nid == 0 and ref_a.end_nid == 2
    assert ref_b.start_nid == 2 and ref_b.end_nid == 3
    assert store.next_nid == 3

    table = store.to_table()
    assert table.num_rows == 3
    assert table.column_names[0] == NID_COLUMN
    assert table[NID_COLUMN].to_pylist() == [0, 1, 2]
    assert table["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1"]
    assert "Gene" in list_node_stores(bundle)


def test_node_store_persists_across_open(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    store.append(_gene_batch(["TP53"]))

    reopened = open_node_store(bundle, class_iri="http://example.org/Gene", local_name="Gene")
    assert reopened.next_nid == 1
    assert reopened.to_table()["symbol"].to_pylist() == ["TP53"]


def test_node_store_rejects_class_mismatch(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    open_node_store(bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True)
    with pytest.raises(CaracalError) as exc:
        open_node_store(bundle, class_iri="http://example.org/Other", local_name="Gene")
    assert exc.value.code == "CDB-7013"


def test_node_store_rejects_existing_nid_column(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    bad = pa.record_batch(
        {"nid": pa.array([5], type=pa.uint64()), "symbol": pa.array(["X"], type=pa.string())}
    )
    with pytest.raises(CaracalError) as exc:
        store.append(bad)
    assert exc.value.code == "CDB-7011"


def test_node_store_rejects_schema_drift(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "bio")
    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    store.append(_gene_batch(["TP53"]))
    with pytest.raises(CaracalError) as exc:
        store.append(pa.record_batch({"symbol": pa.array(["MDM2"], type=pa.string())}))
    assert exc.value.code == "CDB-7011"
