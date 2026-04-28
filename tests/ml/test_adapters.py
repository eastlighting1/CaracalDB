import numpy as np
import pyarrow as pa
import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml import Subgraph
from caracaldb.ml.dgl_adapter import to_dgl_block
from caracaldb.ml.jraph_adapter import to_graphs_tuple
from caracaldb.ml.pyg_adapter import to_pyg_data


def _toy_subgraph() -> Subgraph:
    sg = Subgraph(meta={"snapshot": "v1"})
    sg.add_nodes(
        "Account",
        pa.table(
            {
                "nid": pa.array([0, 1, 2], type=pa.uint64()),
                "embedding": pa.array(
                    np.eye(3, dtype=np.float32).tolist(),
                    type=pa.list_(pa.float32(), 3),
                ),
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


def test_subgraph_basic_counts() -> None:
    sg = _toy_subgraph()
    assert sg.num_nodes() == 3 and sg.num_edges() == 2


def test_pyg_adapter_or_skip() -> None:
    pytest.importorskip("torch")
    pytest.importorskip("torch_geometric")
    data = to_pyg_data(_toy_subgraph())
    assert data["Account"].num_nodes == 3


def test_dgl_adapter_or_skip() -> None:
    pytest.importorskip("dgl")
    pytest.importorskip("torch")
    g = to_dgl_block(_toy_subgraph())
    assert g.num_edges() == 2


def test_jraph_adapter_or_skip() -> None:
    pytest.importorskip("jraph")
    gt = to_graphs_tuple(_toy_subgraph())
    assert int(gt.n_edge[0]) == 2


def test_pyg_adapter_raises_actionable_error_when_missing() -> None:
    try:
        import torch_geometric  # noqa: F401
    except ImportError:
        with pytest.raises(CaracalError) as exc:
            to_pyg_data(_toy_subgraph())
        assert exc.value.code == "CDB-6110"


def test_dgl_adapter_raises_actionable_error_when_missing() -> None:
    try:
        import dgl  # noqa: F401
    except ImportError:
        with pytest.raises(CaracalError) as exc:
            to_dgl_block(_toy_subgraph())
        assert exc.value.code == "CDB-6111"


def test_jraph_adapter_raises_actionable_error_when_missing() -> None:
    try:
        import jraph  # noqa: F401
    except ImportError:
        with pytest.raises(CaracalError) as exc:
            to_graphs_tuple(_toy_subgraph())
        assert exc.value.code == "CDB-6112"
