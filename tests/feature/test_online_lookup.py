from pathlib import Path

import numpy as np
import pyarrow as pa

from caracaldb.feature import OnlineFeatureView
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store


def _seed(tmp_path: Path):
    bundle = create_bundle(tmp_path / "f")
    store = open_node_store(bundle, class_iri="http://x/A", local_name="A", create=True)
    store.append(
        pa.record_batch(
            {
                "balance": pa.array([100.0, 200.0, 300.0]),
                "tier": pa.array(["bronze", "silver", "gold"]),
            }
        )
    )
    return bundle


def test_online_lookup_single(tmp_path: Path) -> None:
    bundle = _seed(tmp_path)
    view = OnlineFeatureView(
        bundle, class_iri="http://x/A", local_name="A", feature_columns=["balance", "tier"]
    )
    row = view.lookup(1)
    assert float(row["balance"]) == 200.0 and row["tier"] == "silver"


def test_online_lookup_many_returns_aligned_table(tmp_path: Path) -> None:
    bundle = _seed(tmp_path)
    view = OnlineFeatureView(
        bundle, class_iri="http://x/A", local_name="A", feature_columns=["balance"]
    )
    out = view.lookup_many(np.array([0, 99, 2], dtype=np.uint64))
    bal = out["balance"].to_pylist()
    assert bal[0] == 100.0
    assert bal[1] is None  # unknown nid
    assert bal[2] == 300.0


def test_online_lookup_p99_under_5ms_for_small_table(tmp_path: Path) -> None:
    bundle = _seed(tmp_path)
    view = OnlineFeatureView(
        bundle, class_iri="http://x/A", local_name="A", feature_columns=["balance"]
    )
    for nid in (0, 1, 2) * 200:
        view.lookup(nid)
    stats = view.stats()
    # On a 3-row in-memory table the p99 should be comfortably below 5 ms.
    assert stats.p99_ms < 5.0, stats
