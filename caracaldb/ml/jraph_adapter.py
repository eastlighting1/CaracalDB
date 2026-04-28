"""jraph adapter: Subgraph → ``GraphsTuple``.

jraph requires homogeneous senders / receivers in NumPy form. We flatten all
node tables into a single ``(num_nodes, feat_dim)`` matrix when feasible and
concatenate edge endpoints across properties (with per-edge type ids stored
under ``edges`` so callers can split downstream).
"""

from __future__ import annotations

import numpy as np

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml.subgraph import Subgraph


def to_graphs_tuple(subgraph: Subgraph, *, feature_column: str = "embedding"):
    try:
        import jraph
    except ImportError as exc:  # pragma: no cover
        raise CaracalError(
            code="CDB-6112",
            message="jraph not installed; jraph adapter unavailable",
            hint="pip install jax jraph",
        ) from exc

    senders_chunks: list[np.ndarray] = []
    receivers_chunks: list[np.ndarray] = []
    etype_chunks: list[np.ndarray] = []
    etype_ids = {prop: i for i, prop in enumerate(sorted(subgraph.edges))}
    for prop, tbl in subgraph.edges.items():
        s = tbl.column("src").combine_chunks().to_numpy(zero_copy_only=False).astype("int32")
        d = tbl.column("dst").combine_chunks().to_numpy(zero_copy_only=False).astype("int32")
        senders_chunks.append(s)
        receivers_chunks.append(d)
        etype_chunks.append(np.full(s.shape, etype_ids[prop], dtype="int32"))

    senders = np.concatenate(senders_chunks) if senders_chunks else np.empty(0, dtype="int32")
    receivers = np.concatenate(receivers_chunks) if receivers_chunks else np.empty(0, dtype="int32")
    etypes = np.concatenate(etype_chunks) if etype_chunks else np.empty(0, dtype="int32")

    node_features = None
    n_nodes = 0
    for tbl in subgraph.nodes.values():
        n_nodes += tbl.num_rows
        if feature_column in tbl.column_names and node_features is None:
            node_features = (
                tbl.column(feature_column).combine_chunks().to_numpy(zero_copy_only=False)
            )

    return jraph.GraphsTuple(
        nodes=node_features,
        edges=etypes,
        senders=senders,
        receivers=receivers,
        n_node=np.array([n_nodes], dtype="int32"),
        n_edge=np.array([senders.size], dtype="int32"),
        globals=None,
    )


__all__ = ["to_graphs_tuple"]
