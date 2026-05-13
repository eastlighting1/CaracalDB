"""Lynxes / GraphFrame bridge.

Both directions go through Arrow:

* ``to_graphframe(subgraph)`` → ``lynxes.GraphFrame`` over a vertices table
  (concatenated from ``subgraph.nodes``) and an edges table (concatenated
  from ``subgraph.edges``). The bridge preserves the original class / property
  IRIs as ``class_iri`` / ``property_iri`` columns so downstream Lynxes
  pipelines can split them again.
* ``from_graphframe(gf, ...)`` → ``Subgraph`` by reading the same two tables
  back. Callers supply ``class_map`` / ``prop_map`` to route rows to the
  right class / property IRI.

When the optional ``lynxes`` package is unavailable, both directions raise
``CaracalError(CDB-6113)`` with a one-line install hint.
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml.subgraph import Subgraph


def _require_lynxes():
    try:
        import lynxes

        return lynxes
    except ImportError as exc:  # pragma: no cover
        raise CaracalError(
            code="CDB-6113",
            message="lynxes is not installed; the GraphFrame bridge is unavailable",
            hint="pip install lynxes  # eastl/Graphframe",
        ) from exc


def to_graphframe(subgraph: Subgraph):
    """Convert a :class:`Subgraph` to a ``lynxes.GraphFrame``.

    Maps each node class IRI to a ``_label`` and generates sequential
    ``_id`` values. Edge tables are converted with ``_src``, ``_dst``,
    ``_type``, and ``_direction`` (always ``0`` = outgoing).
    """
    lynxes = _require_lynxes()
    node_frame = _build_node_frame(lynxes, subgraph.nodes)
    edge_frame = _build_edge_frame(lynxes, subgraph.edges)
    return lynxes.GraphFrame.from_frames(node_frame, edge_frame)


def from_graphframe(
    gf,
    *,
    class_map: Mapping[str, str] | None = None,
    prop_map: Mapping[str, str] | None = None,
) -> Subgraph:
    _require_lynxes()
    sg = Subgraph()
    vertices = gf.nodes.to_pyarrow()
    if "_label" in vertices.column_names:
        label_col = vertices.column("_label")
        seen_labels: set[str] = set()
        for i in range(len(label_col)):
            for lbl in label_col[i].as_py():
                seen_labels.add(lbl)
        for cls in sorted(seen_labels):
            mask = pa.array([cls in row.as_py() for row in label_col], type=pa.bool_())
            sub = vertices.filter(mask)
            drop = [c for c in ("_id", "_label") if c in sub.column_names]
            if drop:
                sub = sub.drop_columns(drop)
            sg.add_nodes(_resolve(class_map, cls), sub)
    edges = gf.edges.to_pyarrow()
    if "_type" in edges.column_names:
        for prop in sorted({*edges.column("_type").to_pylist()}):
            mask = pa.compute.equal(edges.column("_type"), prop)
            sub = edges.filter(mask)
            drop = [c for c in ("_src", "_dst", "_type", "_direction") if c in sub.column_names]
            if drop:
                sub = sub.drop_columns(drop)
            sg.add_edges(_resolve(prop_map, prop), sub)
    return sg


def _build_node_frame(lynxes, nodes: dict[str, pa.Table]):
    """Build a ``lynxes.NodeFrame`` from the subgraph's node tables."""
    rows: dict[str, list] = {"_id": [], "_label": []}
    extra_cols: dict[str, list] = {}
    counter = 0
    for cls, tbl in nodes.items():
        for _ in range(tbl.num_rows):
            rows["_id"].append(str(counter))
            rows["_label"].append([cls])
            counter += 1
        for col_name in tbl.column_names:
            if col_name not in ("_id", "_label"):
                extra_cols.setdefault(col_name, []).extend(tbl.column(col_name).to_pylist())
    data = {**rows, **extra_cols}
    return lynxes.NodeFrame.from_dict(data)


def _build_edge_frame(lynxes, edges: dict[str, pa.Table]):
    """Build a ``lynxes.EdgeFrame`` from the subgraph's edge tables."""
    rows: dict[str, list] = {"_src": [], "_dst": [], "_type": [], "_direction": []}
    extra_cols: dict[str, list] = {}
    for prop, tbl in edges.items():
        n = tbl.num_rows
        src_col = "src" if "src" in tbl.column_names else "_src"
        dst_col = "dst" if "dst" in tbl.column_names else "_dst"
        rows["_src"].extend(str(v) for v in tbl.column(src_col).to_pylist())
        rows["_dst"].extend(str(v) for v in tbl.column(dst_col).to_pylist())
        rows["_type"].extend([prop] * n)
        rows["_direction"].extend([0] * n)
        for col_name in tbl.column_names:
            if col_name not in (src_col, dst_col, "_type", "_direction"):
                extra_cols.setdefault(col_name, []).extend(tbl.column(col_name).to_pylist())
    data = {**rows, **extra_cols}
    return lynxes.EdgeFrame.from_dict(data)


def _resolve(mapping: Mapping[str, str] | None, value: str) -> str:
    if mapping is None:
        return value
    return mapping.get(value, value)


__all__ = ["from_graphframe", "to_graphframe"]
