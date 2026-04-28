"""Case B goldens: refresh_account_features procedure shapes."""

from __future__ import annotations

import pyarrow as pa
import pytest

from caracaldb.exec.operator import run_pipeline
from caracaldb.exec.operators import (
    ExpandOperator,
    HashAggregateOperator,
    KnnOperator,
    NodeScanOperator,
)
from caracaldb.lang.diagnostics import CaracalError


def test_q1_account_balance_sum_via_aggregate(case_b) -> None:
    scan = NodeScanOperator(case_b["accounts"])
    agg = HashAggregateOperator(scan, group_keys=[], aggregates=[("balance", "sum", "total")])
    out = pa.Table.from_batches(list(run_pipeline(agg)))
    # 100 + 200 + 300 + 400 + 500 = 1500
    assert out["total"].to_pylist() == [1500.0]


def test_q2_per_account_outdegree(case_b) -> None:
    scan = NodeScanOperator(case_b["accounts"], columns=["nid"])
    expand = ExpandOperator(scan, forward=case_b["csr"], direction="out")
    agg = HashAggregateOperator(expand, group_keys=["src"], aggregates=[(None, "count_star", "n")])
    out = pa.Table.from_batches(list(run_pipeline(agg)))
    rows = dict(zip(out["src"].to_pylist(), out["n"].to_pylist(), strict=True))
    assert rows == {0: 1, 1: 1, 2: 1, 3: 1}


def test_q3_knn_lookup_against_account_embeddings(case_b) -> None:
    embeddings = case_b["embeddings"]
    op = KnnOperator(case_b["hnsw"], query=embeddings[2], k=3)
    out = list(run_pipeline(op))[0]
    assert out["nid"].to_pylist()[0] == 2  # nearest to itself


def test_q4_transaction_w_w_conflict_signals_8002(case_b) -> None:
    mgr = case_b["tx_manager"]
    a = mgr.begin()
    b = mgr.begin()
    a.record_write("Account", 0)
    b.record_write("Account", 0)
    mgr.commit(a)
    with pytest.raises(CaracalError) as exc:
        mgr.commit(b)
    assert exc.value.code == "CDB-8002"


def test_q5_snapshot_pins_view_at_creation(case_b) -> None:
    snap = case_b["snapshot"]
    # Snapshot has lsn_high == 0 because no writes occurred before its creation.
    assert snap.lsn_high == 0
    # After committing some Tx, the snapshot's lsn_high stays fixed.
    mgr = case_b["tx_manager"]
    with mgr.transaction() as tx:
        tx.record_write("Account", 1)
    assert snap.lsn_high == 0
