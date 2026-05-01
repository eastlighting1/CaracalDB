"""Bundle diff — content-addressable comparison between two ``.crcl`` bundles.

Used by ``caracal diff`` to produce a governance-friendly summary of what
changed between two bundle states. The diff is intentionally coarse-grained
because the embedded engine has no notion of "user", "tenant", or "audit
log" — the host application owns those. What the engine *can* offer is a
deterministic answer to "are these two bundles the same, and if not, what
nodes/edges changed?"

The implementation is read-only and works across two arbitrary bundles. It
does not require both bundles to share a lineage; the ``_cdb_gid`` /
``nid`` columns are the join key for node-set comparisons, and ``(src, dst)``
pairs are the key for edge-set comparisons. For graphs without a stable id
scheme (e.g. two independent ingests of the same upstream data), the diff
is still well-defined but will report large adds/removes — that is a true
statement about what the bundles contain, even if the *intent* was the same.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from caracaldb.onto.catalog import load_catalog
from caracaldb.storage.bundle import Bundle, open_bundle
from caracaldb.storage.edge_store import list_edge_stores, open_edge_store
from caracaldb.storage.node_store import list_node_stores, open_node_store


@dataclass(slots=True)
class ClassDiff:
    """Per-class node-set delta."""

    local_name: str
    only_in_a: int = 0
    only_in_b: int = 0
    in_both: int = 0
    schema_changed: bool = False
    a_columns: tuple[str, ...] = ()
    b_columns: tuple[str, ...] = ()


@dataclass(slots=True)
class RelationDiff:
    """Per-relation edge-set delta."""

    local_name: str
    only_in_a: int = 0
    only_in_b: int = 0
    in_both: int = 0


@dataclass(slots=True)
class BundleDiff:
    """Aggregate diff across catalog + node sets + edge sets."""

    classes_added: tuple[str, ...] = ()
    classes_removed: tuple[str, ...] = ()
    properties_added: tuple[str, ...] = ()
    properties_removed: tuple[str, ...] = ()
    class_changes: tuple[ClassDiff, ...] = ()
    relation_changes: tuple[RelationDiff, ...] = ()
    a_path: str = ""
    b_path: str = ""

    def is_empty(self) -> bool:
        return (
            not self.classes_added
            and not self.classes_removed
            and not self.properties_added
            and not self.properties_removed
            and all(
                c.only_in_a == 0 and c.only_in_b == 0 and not c.schema_changed
                for c in self.class_changes
            )
            and all(r.only_in_a == 0 and r.only_in_b == 0 for r in self.relation_changes)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "a": self.a_path,
            "b": self.b_path,
            "classes_added": list(self.classes_added),
            "classes_removed": list(self.classes_removed),
            "properties_added": list(self.properties_added),
            "properties_removed": list(self.properties_removed),
            "class_changes": [
                {
                    "local_name": c.local_name,
                    "only_in_a": c.only_in_a,
                    "only_in_b": c.only_in_b,
                    "in_both": c.in_both,
                    "schema_changed": c.schema_changed,
                    "a_columns": list(c.a_columns),
                    "b_columns": list(c.b_columns),
                }
                for c in self.class_changes
            ],
            "relation_changes": [
                {
                    "local_name": r.local_name,
                    "only_in_a": r.only_in_a,
                    "only_in_b": r.only_in_b,
                    "in_both": r.in_both,
                }
                for r in self.relation_changes
            ],
        }


def _id_column(store_schema: pa.Schema) -> str:
    if "_cdb_gid" in store_schema.names:
        return "_cdb_gid"
    return "nid"


def _node_id_set(bundle: Bundle, local_name: str) -> tuple[set[int], pa.Schema]:
    # Resolve the class IRI from the on-disk store so we don't depend on the
    # catalog mapping (which may have been loaded from a different bundle).
    catalog = load_catalog(bundle)
    cls = next(
        (c for c in catalog.classes if (c.local_name or "") == local_name),
        None,
    )
    if cls is None:
        return set(), pa.schema([])
    store = open_node_store(bundle, class_iri=cls.iri, local_name=local_name)
    schema = store.schema
    if store.num_rows == 0:
        return set(), schema
    id_col = _id_column(schema)
    if id_col not in schema.names:
        # No identity column we can compare on; treat as opaque.
        return set(), schema
    table = store.to_table(columns=[id_col])
    arr = table.column(id_col).to_pylist()
    return set(int(v) for v in arr if v is not None), schema


def _edge_pair_set(bundle: Bundle, local_name: str) -> set[tuple[int, int]]:
    catalog = load_catalog(bundle)
    prop = next(
        (p for p in catalog.properties if (p.local_name or "") == local_name),
        None,
    )
    if prop is None:
        return set()
    store = open_edge_store(bundle, property_iri=prop.iri, local_name=local_name)
    if store.num_rows == 0:
        return set()
    table = store.to_table(columns=["src", "dst"])
    src = table.column("src").to_pylist()
    dst = table.column("dst").to_pylist()
    return {(int(s), int(d)) for s, d in zip(src, dst, strict=False)}


def diff_bundles(a_path: str | Path, b_path: str | Path) -> BundleDiff:
    """Compute a node/edge-level diff between two bundles.

    Both bundles are opened read-only. The diff is symmetric under swapping
    ``a`` and ``b`` — ``only_in_a`` and ``only_in_b`` swap roles, everything
    else stays.
    """
    a = open_bundle(a_path)
    b = open_bundle(b_path)
    a_cat = load_catalog(a)
    b_cat = load_catalog(b)

    a_classes = {(c.local_name or "") for c in a_cat.classes}
    b_classes = {(c.local_name or "") for c in b_cat.classes}
    a_props = {(p.local_name or "") for p in a_cat.properties}
    b_props = {(p.local_name or "") for p in b_cat.properties}

    classes_added = tuple(sorted(b_classes - a_classes))
    classes_removed = tuple(sorted(a_classes - b_classes))
    properties_added = tuple(sorted(b_props - a_props))
    properties_removed = tuple(sorted(a_props - b_props))

    shared_classes = sorted(a_classes & b_classes)
    class_changes: list[ClassDiff] = []
    for name in shared_classes:
        if not name:
            continue
        if name not in list_node_stores(a) or name not in list_node_stores(b):
            continue
        a_ids, a_schema = _node_id_set(a, name)
        b_ids, b_schema = _node_id_set(b, name)
        a_cols = tuple(a_schema.names)
        b_cols = tuple(b_schema.names)
        class_changes.append(
            ClassDiff(
                local_name=name,
                only_in_a=len(a_ids - b_ids),
                only_in_b=len(b_ids - a_ids),
                in_both=len(a_ids & b_ids),
                schema_changed=a_cols != b_cols,
                a_columns=a_cols,
                b_columns=b_cols,
            )
        )

    shared_props = sorted(a_props & b_props)
    relation_changes: list[RelationDiff] = []
    for name in shared_props:
        if not name:
            continue
        if name not in list_edge_stores(a) or name not in list_edge_stores(b):
            continue
        a_edges = _edge_pair_set(a, name)
        b_edges = _edge_pair_set(b, name)
        relation_changes.append(
            RelationDiff(
                local_name=name,
                only_in_a=len(a_edges - b_edges),
                only_in_b=len(b_edges - a_edges),
                in_both=len(a_edges & b_edges),
            )
        )

    return BundleDiff(
        classes_added=classes_added,
        classes_removed=classes_removed,
        properties_added=properties_added,
        properties_removed=properties_removed,
        class_changes=tuple(class_changes),
        relation_changes=tuple(relation_changes),
        a_path=str(a.path),
        b_path=str(b.path),
    )


def render_diff(diff: BundleDiff) -> str:
    """Human-readable rendering of a ``BundleDiff`` for terminal output."""
    lines: list[str] = []
    lines.append(f"--- {diff.a_path}")
    lines.append(f"+++ {diff.b_path}")
    if diff.is_empty():
        lines.append("(no differences)")
        return "\n".join(lines)
    if diff.classes_added or diff.classes_removed:
        lines.append("")
        lines.append("# Catalog: classes")
        for name in diff.classes_removed:
            lines.append(f"- {name}")
        for name in diff.classes_added:
            lines.append(f"+ {name}")
    if diff.properties_added or diff.properties_removed:
        lines.append("")
        lines.append("# Catalog: properties")
        for name in diff.properties_removed:
            lines.append(f"- {name}")
        for name in diff.properties_added:
            lines.append(f"+ {name}")
    changed_classes = [
        c for c in diff.class_changes if c.only_in_a or c.only_in_b or c.schema_changed
    ]
    if changed_classes:
        lines.append("")
        lines.append("# Node sets")
        for c in changed_classes:
            extra = " (schema changed)" if c.schema_changed else ""
            lines.append(
                f"  {c.local_name}: -{c.only_in_a} +{c.only_in_b} ={c.in_both}{extra}"
            )
    changed_rels = [r for r in diff.relation_changes if r.only_in_a or r.only_in_b]
    if changed_rels:
        lines.append("")
        lines.append("# Edge sets")
        for r in changed_rels:
            lines.append(
                f"  {r.local_name}: -{r.only_in_a} +{r.only_in_b} ={r.in_both}"
            )
    return "\n".join(lines)


__all__ = [
    "BundleDiff",
    "ClassDiff",
    "RelationDiff",
    "diff_bundles",
    "render_diff",
]
