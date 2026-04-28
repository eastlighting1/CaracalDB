"""CSC adjacency builder.

CSC is just CSR over swapped ``(src, dst)`` columns: it stores the in-edge
adjacency keyed by destination vertex. Building it via the same writer keeps
the on-disk format identical, so a single ``CsrReader`` can read both
``forward.csr`` and ``reverse.csc`` files.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from caracaldb.graph.csr_builder import CSRBuildResult, build_csr
from caracaldb.storage.edge_store import DST_COLUMN, EID_COLUMN, SRC_COLUMN, EdgeStore


def build_csc(
    edges: EdgeStore | pa.Table,
    *,
    num_vertices: int,
    out_path: str | Path,
    with_eids: bool = True,
) -> CSRBuildResult:
    """Build the reverse adjacency by swapping src/dst before delegating to
    ``build_csr``.
    """
    table = edges.to_table() if isinstance(edges, EdgeStore) else edges
    if table.num_rows == 0:
        return build_csr(table, num_vertices=num_vertices, out_path=out_path, with_eids=False)

    # Rename src↔dst so the CSR builder treats each in-edge as an out-edge.
    columns: dict[str, pa.Array] = {}
    columns[SRC_COLUMN] = table[DST_COLUMN].combine_chunks()
    columns[DST_COLUMN] = table[SRC_COLUMN].combine_chunks()
    if EID_COLUMN in table.column_names:
        columns[EID_COLUMN] = table[EID_COLUMN].combine_chunks()
    swapped = pa.table(columns)
    return build_csr(swapped, num_vertices=num_vertices, out_path=out_path, with_eids=with_eids)


__all__ = ["build_csc"]
