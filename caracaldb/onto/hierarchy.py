"""Ontology hierarchy DAG utilities."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import Catalog


@dataclass(frozen=True, slots=True)
class HierarchyDAG:
    nodes: tuple[str, ...]
    parents_by_child: dict[str, tuple[str, ...]]
    children_by_parent: dict[str, tuple[str, ...]]
    topological_order: tuple[str, ...]

    @classmethod
    def from_edges(
        cls,
        nodes: tuple[str, ...],
        parents_by_child: dict[str, tuple[str, ...]],
        *,
        relation_name: str,
    ) -> HierarchyDAG:
        node_set = set(nodes)
        normalized_parents: dict[str, tuple[str, ...]] = {node: () for node in nodes}
        children: dict[str, list[str]] = {node: [] for node in nodes}
        indegree: dict[str, int] = {node: 0 for node in nodes}

        for child, parents in parents_by_child.items():
            if child not in node_set:
                raise _unknown_hierarchy_node(relation_name, child)
            deduped = tuple(dict.fromkeys(parents))
            normalized_parents[child] = deduped
            for parent in deduped:
                if parent not in node_set:
                    raise _unknown_hierarchy_node(relation_name, parent)
                children[parent].append(child)
                indegree[child] += 1

        queue = deque(node for node in nodes if indegree[node] == 0)
        ordered: list[str] = []
        while queue:
            node = queue.popleft()
            ordered.append(node)
            for child in children[node]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)

        if len(ordered) != len(nodes):
            raise CaracalError(
                code="CDB-9505",
                message=f"{relation_name} hierarchy contains a cycle",
            )

        return cls(
            nodes=nodes,
            parents_by_child=normalized_parents,
            children_by_parent={node: tuple(children[node]) for node in nodes},
            topological_order=tuple(ordered),
        )

    def parents(self, iri: str) -> tuple[str, ...]:
        self._require_node(iri)
        return self.parents_by_child[iri]

    def children(self, iri: str) -> tuple[str, ...]:
        self._require_node(iri)
        return self.children_by_parent[iri]

    def ancestors(self, iri: str, *, include_self: bool = False) -> tuple[str, ...]:
        self._require_node(iri)
        seen: set[str] = set()
        ordered: list[str] = []
        if include_self:
            seen.add(iri)
            ordered.append(iri)
        self._collect_up(iri, seen, ordered)
        return tuple(ordered)

    def descendants(self, iri: str, *, include_self: bool = False) -> tuple[str, ...]:
        self._require_node(iri)
        seen: set[str] = set()
        ordered: list[str] = []
        if include_self:
            seen.add(iri)
            ordered.append(iri)
        self._collect_down(iri, seen, ordered)
        return tuple(ordered)

    def is_subtype(self, child: str, parent: str, *, reflexive: bool = True) -> bool:
        self._require_node(child)
        self._require_node(parent)
        if reflexive and child == parent:
            return True
        return parent in self.ancestors(child)

    def _collect_up(self, iri: str, seen: set[str], ordered: list[str]) -> None:
        for parent in self.parents_by_child[iri]:
            if parent in seen:
                continue
            seen.add(parent)
            ordered.append(parent)
            self._collect_up(parent, seen, ordered)

    def _collect_down(self, iri: str, seen: set[str], ordered: list[str]) -> None:
        for child in self.children_by_parent[iri]:
            if child in seen:
                continue
            seen.add(child)
            ordered.append(child)
            self._collect_down(child, seen, ordered)

    def _require_node(self, iri: str) -> None:
        if iri not in self.parents_by_child:
            raise _unknown_hierarchy_node("ontology", iri)


@dataclass(frozen=True, slots=True)
class OntologyHierarchy:
    classes: HierarchyDAG
    properties: HierarchyDAG

    @classmethod
    def from_catalog(cls, catalog: Catalog) -> OntologyHierarchy:
        return cls(
            classes=HierarchyDAG.from_edges(
                tuple(item.iri for item in catalog.classes),
                {item.iri: item.superclass_iris for item in catalog.classes},
                relation_name="class",
            ),
            properties=HierarchyDAG.from_edges(
                tuple(item.iri for item in catalog.properties),
                {item.iri: item.superproperty_iris for item in catalog.properties},
                relation_name="property",
            ),
        )


def _unknown_hierarchy_node(relation_name: str, iri: str) -> CaracalError:
    return CaracalError(
        code="CDB-9504",
        message=f"unknown {relation_name} hierarchy node: {iri}",
    )


__all__ = ["HierarchyDAG", "OntologyHierarchy"]
