from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import ClosureScanOperator
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.onto.closure import ClassClosureIndex
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store


def _bundle(tmp_path: Path):
    bundle = create_bundle(tmp_path / "bio")
    catalog = Catalog.empty()
    organ = catalog.register_class(iri="http://x/Organ", local_name="Organ")
    catalog.register_class(iri="http://x/Liver", local_name="Liver", superclass_iris=(organ.iri,))
    catalog.register_class(iri="http://x/Lung", local_name="Lung", superclass_iris=(organ.iri,))
    save_catalog(bundle, catalog)

    organ_store = open_node_store(
        bundle, class_iri="http://x/Organ", local_name="Organ", create=True
    )
    organ_store.append(pa.record_batch({"name": pa.array(["generic_organ"])}))
    liver = open_node_store(bundle, class_iri="http://x/Liver", local_name="Liver", create=True)
    liver.append(pa.record_batch({"name": pa.array(["liver_a", "liver_b"])}))
    lung = open_node_store(bundle, class_iri="http://x/Lung", local_name="Lung", create=True)
    lung.append(pa.record_batch({"name": pa.array(["lung_a"])}))

    closure = ClassClosureIndex.from_catalog(catalog)
    return bundle, closure


def test_closure_scan_unions_subclass_rows(tmp_path: Path) -> None:
    bundle, closure = _bundle(tmp_path)
    op = ClosureScanOperator(bundle, closure, base_iri="http://x/Organ")
    table = pa.Table.from_batches(list(run_pipeline(op)))
    assert sorted(table["name"].to_pylist()) == [
        "generic_organ",
        "liver_a",
        "liver_b",
        "lung_a",
    ]
    assert set(table["class_iri"].to_pylist()) == {
        "http://x/Organ",
        "http://x/Liver",
        "http://x/Lung",
    }


def test_closure_scan_strict_excludes_self(tmp_path: Path) -> None:
    bundle, closure = _bundle(tmp_path)
    op = ClosureScanOperator(bundle, closure, base_iri="http://x/Organ", include_self=False)
    table = pa.Table.from_batches(list(run_pipeline(op)))
    assert "generic_organ" not in table["name"].to_pylist()


def test_closure_scan_rejects_unknown_class(tmp_path: Path) -> None:
    bundle, closure = _bundle(tmp_path)
    with pytest.raises(CaracalError) as exc:
        ClosureScanOperator(bundle, closure, base_iri="http://x/Unknown")
    assert exc.value.code == "CDB-6070"
