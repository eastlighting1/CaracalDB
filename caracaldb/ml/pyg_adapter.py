"""PyG adapter: Subgraph → ``HeteroData``.

The conversion path is intentionally narrow: every node table contributes
``data[<class>].x`` from the configured feature column (default
``embedding``) and ``data[<class>].num_nodes``; every edge table contributes
``data[(src_cls, prop, dst_cls), 'edge_index']``. Edge endpoints are emitted
as 2×E int64 tensors via ``torch.from_numpy`` (zero-copy on most platforms).

If ``torch_geometric`` / ``torch`` is missing the conversion raises
``CaracalError(CDB-6110)`` so callers can surface a single, actionable error
rather than a generic ImportError.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml.subgraph import Subgraph


def _arrow_col_to_numpy(col: pa.ChunkedArray) -> np.ndarray:
    """Convert an Arrow column to a numeric numpy array.

    For scalar columns this is a simple ``to_numpy``.  For list-typed columns
    (``list`` / ``fixed_size_list``) the flat values are extracted and reshaped
    into a 2-D ``(num_rows, list_width)`` array so that ``torch.from_numpy``
    receives a contiguous numeric buffer instead of an object array.
    """
    arr = col.combine_chunks()
    if pa.types.is_fixed_size_list(arr.type):
        width = arr.type.list_size
        flat = arr.values.to_numpy(zero_copy_only=False).copy()
        return flat.reshape(-1, width)
    if pa.types.is_list(arr.type) or pa.types.is_large_list(arr.type):
        flat = arr.values.to_numpy(zero_copy_only=False)
        n = len(arr)
        if n == 0:
            return flat.reshape(0, 0)
        width = len(flat) // n
        return np.ascontiguousarray(flat.reshape(n, width))
    result = arr.to_numpy(zero_copy_only=False)
    if not result.flags.writeable:
        result = result.copy()
    return result


def to_pyg_data(
    subgraph: Subgraph,
    *,
    feature_column: str = "embedding",
    edge_endpoints: Mapping[str, tuple[str, str]] | None = None,
):
    """Convert ``Subgraph`` to ``torch_geometric.data.HeteroData``.

    ``edge_endpoints`` maps ``property_iri → (src_class_iri, dst_class_iri)``;
    when omitted, the adapter assumes a single homogeneous class equal to the
    only key in ``subgraph.nodes`` (raising on ambiguity).
    """
    try:
        import torch
        from torch_geometric.data import HeteroData
    except ImportError as exc:  # pragma: no cover - exercised only when torch missing
        raise CaracalError(
            code="CDB-6110",
            message="torch_geometric / torch not installed; PyG adapter unavailable",
            hint="pip install torch torch-geometric",
        ) from exc

    data = HeteroData()
    for cls, tbl in subgraph.nodes.items():
        data[cls].num_nodes = tbl.num_rows
        if feature_column in tbl.column_names:
            data[cls].x = torch.from_numpy(
                _arrow_col_to_numpy(tbl.column(feature_column))
            )

    homogeneous_cls = next(iter(subgraph.nodes)) if len(subgraph.nodes) == 1 else None
    for prop, tbl in subgraph.edges.items():
        if edge_endpoints is not None and prop in edge_endpoints:
            src_cls, dst_cls = edge_endpoints[prop]
        elif homogeneous_cls is not None:
            src_cls = dst_cls = homogeneous_cls
        else:
            raise CaracalError(
                code="CDB-6110",
                message=(
                    f"edge_endpoints[{prop!r}] is required when the subgraph carries "
                    "more than one node class"
                ),
            )
        src = tbl.column("src").combine_chunks().to_numpy(zero_copy_only=False)
        dst = tbl.column("dst").combine_chunks().to_numpy(zero_copy_only=False)
        edge_index = torch.stack(
            [torch.from_numpy(src.astype("int64")), torch.from_numpy(dst.astype("int64"))]
        )
        data[(src_cls, prop, dst_cls)].edge_index = edge_index
    return data


__all__ = ["to_pyg_data"]
