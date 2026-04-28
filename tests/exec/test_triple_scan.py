from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import TriplePatternStep, TripleScanOperator
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store


def _seed(tmp_path: Path):
    bundle = create_bundle(tmp_path / "g")
    store = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2], type=pa.uint64()),
                "dst": pa.array([1, 2, 2, 0], type=pa.uint64()),
            }
        )
    )
    return store


def test_triple_scan_var_var(tmp_path: Path) -> None:
    store = _seed(tmp_path)
    step = TriplePatternStep(
        subject_var="s",
        subject_const=None,
        predicate_iri="http://x/p",
        object_var="o",
        object_const=None,
    )
    out = pa.Table.from_batches(list(run_pipeline(TripleScanOperator(store, step))))
    pairs = list(zip(out["s"].to_pylist(), out["o"].to_pylist(), strict=True))
    assert sorted(pairs) == [(0, 1), (0, 2), (1, 2), (2, 0)]


def test_triple_scan_const_subject(tmp_path: Path) -> None:
    store = _seed(tmp_path)
    step = TriplePatternStep(
        subject_var=None,
        subject_const=0,
        predicate_iri="http://x/p",
        object_var="o",
        object_const=None,
    )
    out = pa.Table.from_batches(list(run_pipeline(TripleScanOperator(store, step))))
    assert sorted(out["o"].to_pylist()) == [1, 2]


def test_triple_scan_const_const_emits_found(tmp_path: Path) -> None:
    store = _seed(tmp_path)
    step = TriplePatternStep(
        subject_var=None,
        subject_const=0,
        predicate_iri="http://x/p",
        object_var=None,
        object_const=1,
    )
    out = pa.Table.from_batches(list(run_pipeline(TripleScanOperator(store, step))))
    assert out["found"].to_pylist() == [True]


def test_triple_scan_rejects_unbound_subject(tmp_path: Path) -> None:
    store = _seed(tmp_path)
    with pytest.raises(CaracalError) as exc:
        TripleScanOperator(
            store,
            TriplePatternStep(
                subject_var=None,
                subject_const=None,
                predicate_iri="http://x/p",
                object_var="o",
                object_const=None,
            ),
        )
    assert exc.value.code == "CDB-6080"
