from pathlib import Path

import pyarrow as pa
import pytest

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store


def _seed_bundle(tmp_path: Path) -> Path:
    bundle_path = tmp_path / "bio"
    bundle = create_bundle(bundle_path)
    catalog = bundle and __import__("caracaldb.onto.catalog", fromlist=["Catalog"]).Catalog.empty()
    catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
    save_catalog(bundle, catalog)

    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2", "BRCA1", "EGFR"]),
                "chromosome": pa.array(["17", "12", "17", "7"]),
            }
        )
    )
    return bundle_path


def test_connect_and_select_returns_arrow(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    conn = db.cursor()
    table = conn.sql("MATCH (g:Gene) RETURN g.symbol").arrow()
    assert table.column_names == ["symbol"]
    assert table["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1", "EGFR"]


def test_where_filter_applies(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    conn = cdb.connect(bundle_path, format="bundle").cursor()
    table = conn.sql("MATCH (g:Gene) WHERE g.chromosome = '17' RETURN g.symbol").arrow()
    assert sorted(table["symbol"].to_pylist()) == ["BRCA1", "TP53"]


def test_limit_clips_result(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    conn = cdb.connect(bundle_path, format="bundle").cursor()
    table = conn.sql("MATCH (g:Gene) RETURN g.symbol LIMIT 2").arrow()
    assert table.num_rows == 2


def test_unknown_class_raises(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    conn = cdb.connect(bundle_path, format="bundle").cursor()
    with pytest.raises(CaracalError) as exc:
        conn.sql("MATCH (x:Unknown) RETURN x.foo").arrow()
    assert exc.value.code in {"CDB-6021", "TF-3004"}
