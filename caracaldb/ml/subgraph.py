"""``Subgraph`` — the canonical Arrow-native mini-batch container.

Adapters consume ``Subgraph`` and return framework-native graphs. Keeping the
representation Arrow-first means the adapters do at most one zero-copy bridge
each (``torch.from_numpy`` / ``dgl.graph(...)`` / ``jraph.GraphsTuple``), and
the heavy work (sampling, feature gather) is shared.

Layout:
* ``nodes[class_iri] -> pa.Table`` with at least ``nid: UInt64`` plus
  optional feature columns.
* ``edges[property_iri] -> pa.Table`` with ``src: UInt64``, ``dst: UInt64``,
  optional ``eid``/feature columns.
* ``meta[str] -> str`` for free-form annotations (snapshot id, seeds…).

Adapters look up by class / property IRI, so ``Subgraph`` does not depend on
the on-disk catalogue.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass(slots=True)
class Subgraph:
    nodes: dict[str, pa.Table] = field(default_factory=dict)
    edges: dict[str, pa.Table] = field(default_factory=dict)
    meta: dict[str, str] = field(default_factory=dict)

    def num_nodes(self) -> int:
        return sum(t.num_rows for t in self.nodes.values())

    def num_edges(self) -> int:
        return sum(t.num_rows for t in self.edges.values())

    def add_nodes(self, class_iri: str, table: pa.Table) -> None:
        self.nodes[class_iri] = table

    def add_edges(self, property_iri: str, table: pa.Table) -> None:
        self.edges[property_iri] = table


__all__ = ["Subgraph"]
