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
    lynxes = _require_lynxes()
    vertices = _concat_with_iri(subgraph.nodes, "class_iri")
    edges = _concat_with_iri(subgraph.edges, "property_iri")
    return lynxes.GraphFrame(vertices, edges)  # type: ignore[attr-defined]


def from_graphframe(
    gf,
    *,
    class_map: Mapping[str, str] | None = None,
    prop_map: Mapping[str, str] | None = None,
) -> Subgraph:
    _require_lynxes()
    sg = Subgraph()
    vertices = gf.vertices  # Arrow Table by Lynxes contract
    if "class_iri" in vertices.column_names:
        for cls in {*vertices["class_iri"].to_pylist()}:
            sub = vertices.filter(pa.compute.equal(vertices["class_iri"], cls))
            sg.add_nodes(_resolve(class_map, cls), sub.drop_columns(["class_iri"]))
    edges = gf.edges
    if "property_iri" in edges.column_names:
        for prop in {*edges["property_iri"].to_pylist()}:
            sub = edges.filter(pa.compute.equal(edges["property_iri"], prop))
            sg.add_edges(_resolve(prop_map, prop), sub.drop_columns(["property_iri"]))
    return sg


def _concat_with_iri(tables: dict[str, pa.Table], iri_column: str) -> pa.Table:
    if not tables:
        return pa.table({iri_column: pa.array([], type=pa.string())})
    pieces: list[pa.Table] = []
    for iri, tbl in tables.items():
        with_iri = tbl.append_column(iri_column, pa.array([iri] * tbl.num_rows, type=pa.string()))
        pieces.append(with_iri)
    schema_names = list(pieces[0].column_names)
    return pa.concat_tables(
        [t.select([n for n in schema_names if n in t.column_names]) for t in pieces],
        promote_options="default",
    )


def _resolve(mapping: Mapping[str, str] | None, value: str) -> str:
    if mapping is None:
        return value
    return mapping.get(value, value)


__all__ = ["from_graphframe", "to_graphframe"]
