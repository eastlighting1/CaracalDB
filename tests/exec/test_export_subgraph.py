from pathlib import Path

import pyarrow as pa
import pytest

from caracaldb.exec.operators.export_arrow import (
    export_subgraph_to_arrow,
    import_subgraph_from_arrow,
)
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml import Subgraph


def _toy() -> Subgraph:
    sg = Subgraph(meta={"snapshot": "v1"})
    sg.add_nodes(
        "Account",
        pa.table(
            {
                "nid": pa.array([0, 1, 2], type=pa.uint64()),
                "balance": pa.array([10.0, 20.0, 30.0]),
            }
        ),
    )
    sg.add_edges(
        "transferredTo",
        pa.table(
            {
                "src": pa.array([0, 1], type=pa.uint64()),
                "dst": pa.array([1, 2], type=pa.uint64()),
            }
        ),
    )
    return sg


def test_export_and_import_round_trip(tmp_path: Path) -> None:
    target = tmp_path / "subgraph.arrow"
    export_subgraph_to_arrow(_toy(), target)
    sg = import_subgraph_from_arrow(target)
    assert "Account" in sg.nodes and sg.nodes["Account"].num_rows == 3
    assert "transferredTo" in sg.edges and sg.edges["transferredTo"].num_rows == 2


def test_export_empty_subgraph_raises(tmp_path: Path) -> None:
    with pytest.raises(CaracalError) as exc:
        export_subgraph_to_arrow(Subgraph(), tmp_path / "empty.arrow")
    assert exc.value.code == "CDB-6160"
