"""DGL adapter: Subgraph → per-layer ``DGLBlock``.

For the M4 surface we expose a single ``to_dgl_block()`` that produces a
heterogeneous DGL graph; layered fan-out is the caller's job (typically the
``NeighborLoader`` / ``DataLoader`` wraps it). Same dependency-graceful path
as the PyG adapter.
"""

from __future__ import annotations

from collections.abc import Mapping

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml.subgraph import Subgraph


def to_dgl_block(
    subgraph: Subgraph,
    *,
    feature_column: str = "embedding",
    edge_endpoints: Mapping[str, tuple[str, str]] | None = None,
):
    try:
        import dgl
        import torch
    except (ImportError, OSError) as exc:  # pragma: no cover
        raise CaracalError(
            code="CDB-6111",
            message="dgl / torch not installed; DGL adapter unavailable",
            hint="pip install dgl torch",
        ) from exc

    homogeneous_cls = next(iter(subgraph.nodes)) if len(subgraph.nodes) == 1 else None
    edge_dict: dict[tuple[str, str, str], tuple] = {}
    for prop, tbl in subgraph.edges.items():
        if edge_endpoints is not None and prop in edge_endpoints:
            src_cls, dst_cls = edge_endpoints[prop]
        elif homogeneous_cls is not None:
            src_cls = dst_cls = homogeneous_cls
        else:
            raise CaracalError(
                code="CDB-6111",
                message=(
                    f"edge_endpoints[{prop!r}] is required when the subgraph carries "
                    "more than one node class"
                ),
            )
        src = tbl.column("src").combine_chunks().to_numpy(zero_copy_only=False)
        dst = tbl.column("dst").combine_chunks().to_numpy(zero_copy_only=False)
        edge_dict[(src_cls, prop, dst_cls)] = (
            torch.from_numpy(src.astype("int64")),
            torch.from_numpy(dst.astype("int64")),
        )
    g = dgl.heterograph(edge_dict)
    for cls, tbl in subgraph.nodes.items():
        if feature_column in tbl.column_names and cls in g.ntypes:
            g.nodes[cls].data["x"] = torch.from_numpy(
                tbl.column(feature_column).combine_chunks().to_numpy(zero_copy_only=False)
            )
    return g


__all__ = ["to_dgl_block"]
