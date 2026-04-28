"""CSR adjacency builder.

The builder consumes ``(src, dst, eid)`` columns drawn from an edge store
chunk, sorts by source, and emits the canonical CSR layout::

    offsets[i+1] - offsets[i] = degree(i)
    neighbors[offsets[i]:offsets[i+1]] = sorted dsts of vertex i
    eids[offsets[i]:offsets[i+1]]      = matching edge ids (optional)

Algorithm: ``np.argsort(kind='stable')`` on src, then ``np.bincount`` +
``np.cumsum`` to compute offsets in O(n). Pure NumPy, GIL-free in the heavy
loops.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa

from caracaldb.graph.csr_format import write_csr
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.edge_store import DST_COLUMN, EID_COLUMN, SRC_COLUMN, EdgeStore


@dataclass(frozen=True, slots=True)
class CSRBuildResult:
    path: Path
    num_vertices: int
    num_edges: int
    has_eids: bool


def _to_uint64(table: pa.Table, name: str) -> np.ndarray:
    column = table[name].combine_chunks()
    if column.type != pa.uint64():
        column = column.cast(pa.uint64())
    arr = column.to_numpy(zero_copy_only=False)
    return np.ascontiguousarray(arr, dtype=np.uint64)


def build_csr_arrays(
    src: np.ndarray,
    dst: np.ndarray,
    eids: np.ndarray | None,
    num_vertices: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
    if src.shape != dst.shape:
        raise CaracalError(code="CDB-7082", message="src/dst length mismatch")
    if eids is not None and eids.shape != src.shape:
        raise CaracalError(code="CDB-7082", message="eids length mismatch")
    if num_vertices < 0:
        raise CaracalError(code="CDB-7082", message="num_vertices must be >= 0")
    src_u = src.astype(np.uint64, copy=False)
    dst_u = dst.astype(np.uint64, copy=False)

    if src_u.size and int(src_u.max()) >= num_vertices:
        raise CaracalError(
            code="CDB-7082",
            message=(
                f"src index {int(src_u.max())} exceeds num_vertices={num_vertices}; "
                "increase num_vertices or filter out-of-range edges"
            ),
        )

    order = np.argsort(src_u, kind="stable")
    src_s = src_u[order]
    dst_s = dst_u[order]
    eids_s = eids[order].astype(np.uint64, copy=False) if eids is not None else None

    counts = np.bincount(src_s, minlength=num_vertices).astype(np.uint64, copy=False)
    offsets = np.zeros(num_vertices + 1, dtype=np.uint64)
    np.cumsum(counts, out=offsets[1:])
    return offsets, dst_s, eids_s


def build_csr(
    edges: EdgeStore | pa.Table,
    *,
    num_vertices: int,
    out_path: str | Path,
    with_eids: bool = True,
) -> CSRBuildResult:
    table = edges.to_table() if isinstance(edges, EdgeStore) else edges
    if table.num_rows == 0 and num_vertices == 0:
        # Allow an empty (0×0) graph for round-trip parity.
        offsets = np.zeros(1, dtype=np.uint64)
        write_csr(out_path, offsets=offsets, neighbors=np.empty(0, dtype=np.uint64))
        return CSRBuildResult(path=Path(out_path), num_vertices=0, num_edges=0, has_eids=False)

    src = _to_uint64(table, SRC_COLUMN)
    dst = _to_uint64(table, DST_COLUMN)
    eids = _to_uint64(table, EID_COLUMN) if with_eids and EID_COLUMN in table.column_names else None
    offsets, neighbors, eid_arr = build_csr_arrays(src, dst, eids, num_vertices)

    path = write_csr(out_path, offsets=offsets, neighbors=neighbors, eids=eid_arr)
    return CSRBuildResult(
        path=path,
        num_vertices=num_vertices,
        num_edges=int(neighbors.shape[0]),
        has_eids=eid_arr is not None,
    )


__all__ = ["CSRBuildResult", "build_csr", "build_csr_arrays"]
