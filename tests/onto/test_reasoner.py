from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.reasoner import (
    INFER_CLOSURE_KIND,
    InferClosurePlan,
    infer_closure,
    infer_symmetric,
    infer_transitive,
)
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store
from caracaldb.storage.wal import Wal, iter_all_records


def _seed(tmp_path: Path):
    bundle = create_bundle(tmp_path / "g")
    store = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    return bundle, store


def test_symmetric_adds_missing_reverse_edges(tmp_path: Path) -> None:
    bundle, store = _seed(tmp_path)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1], type=pa.uint64()),
                "dst": pa.array([1, 2], type=pa.uint64()),
            }
        )
    )
    report = infer_symmetric(store, property_iri="http://x/p")
    assert report.added_edges == 2
    edges = {
        (s, d)
        for s, d in zip(
            store.to_table()["src"].to_pylist(),
            store.to_table()["dst"].to_pylist(),
            strict=True,
        )
    }
    assert (1, 0) in edges and (2, 1) in edges


def test_symmetric_skips_already_present_pairs(tmp_path: Path) -> None:
    bundle, store = _seed(tmp_path)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1], type=pa.uint64()),
                "dst": pa.array([1, 0], type=pa.uint64()),
            }
        )
    )
    report = infer_symmetric(store, property_iri="http://x/p")
    assert report.added_edges == 0


def test_transitive_closes_chain(tmp_path: Path) -> None:
    bundle, store = _seed(tmp_path)
    # 0 → 1 → 2 → 3
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1, 2], type=pa.uint64()),
                "dst": pa.array([1, 2, 3], type=pa.uint64()),
            }
        )
    )
    report = infer_transitive(store, property_iri="http://x/p")
    edges = {
        (s, d)
        for s, d in zip(
            store.to_table()["src"].to_pylist(),
            store.to_table()["dst"].to_pylist(),
            strict=True,
        )
    }
    # Expected closure: (0,1),(0,2),(0,3),(1,2),(1,3),(2,3) = 6.
    assert {(0, 2), (0, 3), (1, 3)} <= edges
    assert report.added_edges == 3


def test_transitive_triple_budget_raises(tmp_path: Path) -> None:
    bundle, store = _seed(tmp_path)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 1, 2, 3], type=pa.uint64()),
                "dst": pa.array([1, 2, 3, 4], type=pa.uint64()),
            }
        )
    )
    with pytest.raises(CaracalError) as exc:
        infer_transitive(store, property_iri="http://x/p", triple_budget=1)
    assert exc.value.code == "TF-6012"


def test_infer_closure_logs_to_wal(tmp_path: Path) -> None:
    bundle, store = _seed(tmp_path)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0], type=pa.uint64()),
                "dst": pa.array([1], type=pa.uint64()),
            }
        )
    )
    with Wal(bundle.path / "wal") as wal:
        plan = InferClosurePlan(targets=[("SYMMETRIC", "http://x/p", store)])
        reports = infer_closure(plan, wal=wal)
    assert reports[0].added_edges == 1
    kinds = [r.kind for r in iter_all_records(bundle.path / "wal")]
    assert INFER_CLOSURE_KIND in kinds
