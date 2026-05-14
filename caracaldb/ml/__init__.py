"""ML / GNN integration: Subgraph + framework adapters."""

from caracaldb.ml.jraph_adapter import to_graphs_tuple
from caracaldb.ml.loader import NeighborLoader, NeighborLoaderConfig
from caracaldb.ml.lynxes_adapter import from_graphframe, to_graphframe
from caracaldb.ml.pyg_adapter import to_pyg_data
from caracaldb.ml.subgraph import Subgraph

__all__ = [
    "NeighborLoader",
    "NeighborLoaderConfig",
    "Subgraph",
    "from_graphframe",
    "to_graphframe",
    "to_graphs_tuple",
    "to_pyg_data",
]
