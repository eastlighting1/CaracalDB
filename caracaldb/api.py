"""Public API: ``caracaldb.connect`` → ``Connection.sql(...).arrow()``.

This is the MVP wiring for the M1 vertical slice. It supports the single-class
``MATCH (alias:Class) [WHERE expr] RETURN alias.field[, ...] [LIMIT k]`` shape
end-to-end: Tuft text → AST → binder → logical plan → physical plan → Arrow
Table. Anything outside that shape raises ``CDB-6020`` with a clear message
so users see immediately that it's an M1 limitation rather than a silent
mistranslation.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from numbers import Integral
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import pyarrow as pa

if TYPE_CHECKING:
    import numpy as np

from caracaldb.exec.as_of import apply_as_of, resolve_as_of
from caracaldb.exec.expr import compile_expr
from caracaldb.exec.operator import ExecCtx, PhysicalOperator, run_pipeline
from caracaldb.exec.operators import (
    ClosureScanOperator,
    DropColumnsOperator,
    ExpandOperator,
    FilterOperator,
    HashJoinOperator,
    NodeScanOperator,
    ProjectOperator,
    RenameOperator,
    UnionAllOperator,
)
from caracaldb.graph.csc_builder import build_csc
from caracaldb.graph.csr_builder import build_csr
from caracaldb.graph.csr_reader import CsrReader
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import bind_program, parse_tuft
from caracaldb.onto.catalog import Catalog, ClassDef, load_catalog, save_catalog
from caracaldb.onto.closure import ClassClosureIndex
from caracaldb.storage import Bundle, create_bundle, open_bundle
from caracaldb.storage.edge_store import list_edge_stores, open_edge_store
from caracaldb.storage.manifest import MANIFEST_NAME
from caracaldb.storage.mvcc import SnapshotId
from caracaldb.storage.node_store import NodeStore, list_node_stores, open_node_store
from caracaldb.storage.pack import is_packed, pack_bundle
from caracaldb.storage.snapshot import (
    SnapshotEntry,
    create_snapshot,
    list_snapshots,
    release_snapshot,
)

_INTERNAL_IRI_PREFIX = "caracaldb:local:"
_INTERNAL_GID_COLUMN = "_cdb_gid"
_RESOURCE_BASE = "caracaldb://resource/"
_IRI_COLUMN = "_iri"
_LABELS_COLUMN = "_labels"
_PLACEHOLDER_COLUMN = "_placeholder"
_RDF_TYPE_PREDICATES = {
    "a",
    "rdf:type",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    "https://www.w3.org/1999/02/22-rdf-syntax-ns#type",
}


@dataclass(frozen=True, slots=True)
class ResourceRef:
    """Resolved CaracalDB resource identity.

    Examples
    --------
    ```python
    ref = ResourceRef("employee/E12345", 42, "caracaldb://resource/employee/E12345")
    ref.internal_id
    # 42
    ```
    """

    external_id: Any
    internal_id: int
    display_iri: str
    iri: str | None = None
    type: str | None = None


@dataclass(slots=True)
class Result:
    """Materialized result from a CaracalDB query.

    Examples
    --------
    ```python
    result = Result([])
    result.arrow().num_rows
    # 0
    ```
    """

    _batches: list[pa.RecordBatch]

    def arrow(self) -> pa.Table:
        if not self._batches:
            return pa.table({})
        return pa.Table.from_batches(self._batches)

    def rows(self) -> list[dict[str, Any]]:
        return self.arrow().to_pylist()

    def record_batches(self) -> Iterator[pa.RecordBatch]:
        return iter(self._batches)


@dataclass(frozen=True, slots=True)
class NodeQuery:
    """Fluent node table query.

    ``NodeQuery`` is intentionally small: equality predicates are evaluated
    with Arrow kernels, and the result stays as a ``pyarrow.Table`` until the
    caller asks for Python rows.
    """

    _db: Database
    _class_name: str
    _filters: Mapping[str, Any]
    _columns: tuple[str, ...] | None = None

    def where(self, **properties: Any) -> NodeQuery:
        return replace(self, _filters={**self._filters, **properties})

    def select(self, *columns: str) -> NodeQuery:
        return replace(self, _columns=tuple(columns) if columns else None)

    def arrow(self) -> pa.Table:
        needed = set(self._columns or ())
        needed.update(self._filters)
        table = self._db.node_table(
            self._class_name,
            columns=sorted(needed) if self._columns is not None and needed else None,
        )
        for name, value in self._filters.items():
            if name not in table.column_names:
                raise CaracalError(
                    code="CDB-6020",
                    message=f"node filter column missing on {self._class_name!r}: {name!r}",
                )
            column = table[name]
            if value is None:
                mask = pa.compute.is_null(column)
            else:
                scalar = pa.scalar(value, type=table.schema.field(name).type)
                mask = pa.compute.equal(column, scalar)
            table = table.filter(mask)
        if self._columns is not None:
            return table.select(list(self._columns))
        return table

    def rows(self) -> list[dict[str, Any]]:
        return self.arrow().to_pylist()

    def count(self) -> int:
        return self.arrow().num_rows

    def first(self) -> dict[str, Any] | None:
        rows = self.arrow().slice(0, 1).to_pylist()
        return rows[0] if rows else None


class Connection:
    """Query connection bound to an open :class:`Database`.

    Examples
    --------
    ```python
    isinstance(Connection, type)
    # True
    ```
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def catalog(self) -> Catalog:
        return self._db.catalog

    def sql(self, text: str, *, params: dict[str, Any] | None = None) -> Result:
        if params:
            raise CaracalError(
                code="CDB-6020", message="parameter binding lands in M2; pass literals inline"
            )
        program = parse_tuft(text)
        # Binder runs best-effort: M1 MVP allows bare class names without a default prefix.
        # If binding fails because of missing prefix metadata, the planner does a local-name
        # fallback against the catalog. Real binding is enforced once CDB-053+ patterns land.
        try:
            bind_program(program, self._db.catalog)
        except CaracalError as exc:
            if exc.code not in {"TF-3001", "TF-3004"}:
                raise
        if len(program.statements) != 1 or not isinstance(program.statements[0], ta.QueryStmt):
            raise CaracalError(
                code="CDB-6020",
                message="conn.sql() M1 MVP supports a single MATCH/RETURN statement",
            )
        query = program.statements[0].query
        assert query is not None
        if _is_multi_element_pattern(query):
            plan_p = _compile_pattern_query(query, self._db)
            op = _build_pattern_pipeline(plan_p, self._db)
            ctx = apply_as_of(ExecCtx(), plan_p.snapshot)
            batches = list(run_pipeline(op, ctx))
            if plan_p.limit is not None:
                batches = _apply_limit(batches, plan_p.limit)
            return Result(batches)
        plan = _compile_query(query, self._db)
        op = _build_pipeline(plan, self._db)
        ctx = apply_as_of(ExecCtx(), plan.snapshot)
        batches = list(run_pipeline(op, ctx))
        if plan.limit is not None:
            batches = _apply_limit(batches, plan.limit)
        return Result(batches)


class Database:
    """Handle to open CaracalDB database.

    Use as a context manager to ensure packed files are re-packed on exit::

        with cdb.connect("data") as db:
            db.cursor().sql("MATCH ...")

    See Also
    --------
    connect : Function used to instantiate a Database.
    Connection : The context for executing queries.

    Notes
    -----
    The Database object owns the underlying storage bundle and catalog.
    It is recommended to use it as a context manager to ensure proper cleanup,
    especially when working with packed `.crcl` files.

    Examples
    --------
    ```python
    isinstance(Database, type)
    # True
    ```
    """

    def __init__(
        self,
        bundle: Bundle,
        catalog: Catalog,
        *,
        _packed_source: Path | None = None,
        _working_dir: Path | None = None,
        _mode: str = "rw",
    ) -> None:
        self._bundle = bundle
        self._catalog = catalog
        self._packed_source = _packed_source
        self._working_dir = _working_dir
        self._mode = _mode
        self._closed = False
        self._csr_cache: dict[str, dict[str, CsrReader]] = {}

    @property
    def bundle(self) -> Bundle:
        return self._bundle

    @property
    def catalog(self) -> Catalog:
        return self._catalog

    def cursor(self) -> Connection:
        return Connection(self)

    def sql(self, text: str, *, params: dict[str, Any] | None = None) -> Result:
        return self.cursor().sql(text, params=params)

    def nodes(self, class_name: str) -> NodeQuery:
        return NodeQuery(self, class_name, {})

    def define_class(
        self,
        name: str,
        *,
        iri: str | None = None,
        superclass_iris: tuple[str, ...] = (),
    ) -> ClassDef:
        class_iri = iri or _synthetic_iri(name)
        existing = self._catalog.class_by_iri(class_iri)
        if existing is not None:
            return self._merge_class(existing, local_name=name, superclass_iris=superclass_iris)
        for candidate in self._catalog.classes:
            if (candidate.local_name or _local(candidate.iri)) == name:
                return self._merge_class(
                    candidate,
                    local_name=name,
                    superclass_iris=superclass_iris,
                )
        cls = self._catalog.register_class(
            iri=class_iri,
            local_name=name,
            superclass_iris=tuple(superclass_iris),
        )
        save_catalog(self._bundle, self._catalog)
        return cls

    def insert_nodes(
        self,
        class_name: str,
        rows: Mapping[str, Any] | Iterable[Mapping[str, Any]] | pa.Table,
    ) -> Any:
        if isinstance(rows, pa.Table):
            table = rows
        else:
            payload = [dict(rows)] if isinstance(rows, Mapping) else [dict(row) for row in rows]
            table = pa.Table.from_pylist(payload) if payload else pa.table({})
        if table.num_rows == 0:
            raise CaracalError(code="CDB-7011", message="cannot insert an empty node batch")

        cls = self._find_class(class_name)
        store = open_node_store(
            self._bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
            create=True,
        )
        ref = store.append(table, created_lsn=self._next_lsn())
        self._invalidate_graph_indexes()
        return ref

    def insert_node_table(
        self,
        rows: Mapping[str, Any] | Iterable[Mapping[str, Any]] | pa.Table,
        *,
        key_col: str = "node_id",
        type_col: str = "type",
    ) -> dict[str, Any]:
        if isinstance(rows, pa.Table):
            return self.insert_node_table_arrow(rows, key_col=key_col, type_col=type_col)

        payload = [dict(rows)] if isinstance(rows, Mapping) else [dict(row) for row in rows]
        if not payload:
            raise CaracalError(code="CDB-7011", message="cannot insert an empty node table")
        for row in payload:
            _require_columns(row, (key_col, type_col), "node table")
            if _INTERNAL_GID_COLUMN in row:
                raise CaracalError(
                    code="CDB-7011",
                    message=f"node table must not include reserved column {_INTERNAL_GID_COLUMN!r}",
                )

        existing_ids = _external_id_map(self, key_col=key_col)
        next_gid = max(existing_ids.values(), default=-1) + 1
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in payload:
            external_id = row[key_col]
            if external_id in existing_ids:
                gid = existing_ids[external_id]
            else:
                gid = next_gid
                existing_ids[external_id] = gid
                next_gid += 1
            class_name = _coerce_local_name(row[type_col], "node type")
            out = dict(row)
            out[_INTERNAL_GID_COLUMN] = gid
            grouped.setdefault(class_name, []).append(out)

        refs: dict[str, Any] = {}
        for class_name, group in grouped.items():
            self.define_class(class_name)
            refs[class_name] = self.insert_nodes(class_name, group)
        return refs

    def insert_node_table_arrow(
        self,
        table: pa.Table,
        *,
        key_col: str = "node_id",
        type_col: str = "type",
    ) -> dict[str, Any]:
        if table.num_rows == 0:
            raise CaracalError(code="CDB-7011", message="cannot insert an empty node table")
        _require_table_columns(table, (key_col, type_col), "node table")
        if _INTERNAL_GID_COLUMN in table.column_names:
            raise CaracalError(
                code="CDB-7011",
                message=f"node table must not include reserved column {_INTERNAL_GID_COLUMN!r}",
            )

        existing_ids = _external_id_map(self, key_col=key_col)
        key_array = table[key_col].combine_chunks()
        unique_keys = key_array.unique().to_pylist()
        next_gid = max(existing_ids.values(), default=-1) + 1
        for external_id in unique_keys:
            if external_id not in existing_ids:
                existing_ids[external_id] = next_gid
                next_gid += 1

        lookup_keys = unique_keys
        lookup_gids = pa.array([existing_ids[key] for key in lookup_keys], type=pa.uint64())
        key_indices = pa.compute.index_in(key_array, value_set=pa.array(lookup_keys))
        gid_array = pa.compute.take(lookup_gids, key_indices)
        with_gid = table.append_column(_INTERNAL_GID_COLUMN, gid_array)

        refs: dict[str, Any] = {}
        type_array = table[type_col].combine_chunks()
        for raw_type in type_array.unique().to_pylist():
            class_name = _coerce_local_name(raw_type, "node type")
            mask = pa.compute.equal(type_array, pa.scalar(raw_type, type=type_array.type))
            group = with_gid.filter(mask)
            self.define_class(class_name)
            refs[class_name] = self.insert_nodes(class_name, group)
        return refs

    def insert_edge_table(
        self,
        rows: Mapping[str, Any] | Iterable[Mapping[str, Any]] | pa.Table,
        *,
        src_col: str = "src",
        dst_col: str = "dst",
        type_col: str = "type",
        node_key_col: str = "node_id",
    ) -> dict[str, Any]:
        if isinstance(rows, pa.Table):
            return self.insert_edge_table_arrow(
                rows,
                src_col=src_col,
                dst_col=dst_col,
                type_col=type_col,
                node_key_col=node_key_col,
            )

        payload = [dict(rows)] if isinstance(rows, Mapping) else [dict(row) for row in rows]
        if not payload:
            raise CaracalError(code="CDB-7021", message="cannot insert an empty edge table")

        id_map = _external_id_map(self, key_col=node_key_col)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in payload:
            _require_columns(row, (src_col, dst_col, type_col), "edge table")
            if "eid" in row:
                raise CaracalError(
                    code="CDB-7021",
                    message=(
                        "edge table must not include an 'eid' column; "
                        "it is assigned by the store"
                    ),
                )
            relation = _coerce_local_name(row[type_col], "edge type")
            src = _resolve_external_node_id(id_map, row[src_col], src_col)
            dst = _resolve_external_node_id(id_map, row[dst_col], dst_col)
            out = {
                key: value for key, value in row.items() if key not in {src_col, dst_col, type_col}
            }
            out["src"] = src
            out["dst"] = dst
            out[type_col] = row[type_col]
            grouped.setdefault(relation, []).append(out)

        refs: dict[str, Any] = {}
        for relation, group in grouped.items():
            prop = self._define_property(relation)
            store = open_edge_store(
                self._bundle,
                property_iri=prop.iri,
                local_name=prop.local_name or _local(prop.iri),
                create=True,
            )
            refs[relation] = store.append(_edge_table(group), created_lsn=self._next_lsn())
            self._invalidate_graph_indexes(relation)
        return refs

    def insert_edge_table_arrow(
        self,
        table: pa.Table,
        *,
        src_col: str = "src",
        dst_col: str = "dst",
        type_col: str = "type",
        node_key_col: str = "node_id",
    ) -> dict[str, Any]:
        if table.num_rows == 0:
            raise CaracalError(code="CDB-7021", message="cannot insert an empty edge table")
        _require_table_columns(table, (src_col, dst_col, type_col), "edge table")
        if "eid" in table.column_names:
            raise CaracalError(
                code="CDB-7021",
                message="edge table must not include an 'eid' column; it is assigned by the store",
            )

        id_map = _external_id_map(self, key_col=node_key_col)
        src = _resolve_external_node_array(id_map, table[src_col].combine_chunks(), src_col)
        dst = _resolve_external_node_array(id_map, table[dst_col].combine_chunks(), dst_col)

        property_columns = [
            name for name in table.column_names if name not in {src_col, dst_col, type_col}
        ]
        output_columns = [table[name] for name in property_columns]
        output_names = [*property_columns]
        output_columns.extend([src, dst, table[type_col]])
        output_names.extend(["src", "dst", type_col])
        resolved = pa.table(output_columns, names=output_names)

        refs: dict[str, Any] = {}
        type_array = table[type_col].combine_chunks()
        for raw_type in type_array.unique().to_pylist():
            relation = _coerce_local_name(raw_type, "edge type")
            mask = pa.compute.equal(type_array, pa.scalar(raw_type, type=type_array.type))
            group = resolved.filter(mask)
            prop = self._define_property(relation)
            store = open_edge_store(
                self._bundle,
                property_iri=prop.iri,
                local_name=prop.local_name or _local(prop.iri),
                create=True,
            )
            refs[relation] = store.append(group, created_lsn=self._next_lsn())
            self._invalidate_graph_indexes(relation)
        return refs

    def insert_triples(
        self,
        triples: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        subject_col: str = "subject",
        predicate_col: str = "predicate",
        object_col: str = "object",
        policy: str = "warn",
    ) -> dict[str, Any]:
        _validate_import_policy(policy)
        payload = (
            [dict(triples)] if isinstance(triples, Mapping) else [dict(row) for row in triples]
        )
        if not payload:
            raise CaracalError(code="CDB-7011", message="cannot insert an empty triple batch")

        nodes: dict[Any, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []
        property_iris: dict[str, str] = {}
        for triple in payload:
            _require_columns(triple, (subject_col, predicate_col, object_col), "triple")
            subject = _resource_id(triple[subject_col])
            predicate = _predicate_name(triple[predicate_col])
            predicate_iri = _predicate_iri(triple[predicate_col])
            obj = triple[object_col]
            node = nodes.setdefault(subject, _placeholder_node(subject))

            if _is_rdf_type(predicate_iri or predicate):
                class_name = _resource_type(obj)
                node["type"] = class_name
                continue

            if predicate_iri is not None:
                property_iris[predicate] = predicate_iri

            if _is_literal_object(obj):
                node[predicate] = _literal_value(obj)
                continue

            target = _resource_id(obj)
            nodes.setdefault(target, _placeholder_node(target))
            edges.append({"src": subject, "dst": target, "type": predicate})

        refs: dict[str, Any] = {}
        if nodes:
            refs["nodes"] = self.insert_node_table(list(nodes.values()))
        for local_name, iri in property_iris.items():
            self._define_property(local_name, iri=iri)
        if edges:
            refs["edges"] = self.insert_edge_table(edges)
        return refs

    def import_resource(self, obj: Mapping[str, Any], *, policy: str = "warn") -> dict[str, Any]:
        _validate_import_policy(policy)
        shape = _resource_shape(obj)
        if shape == "neo4j":
            return self._import_neo4j_resource(obj, policy=policy)
        if shape == "iri":
            return self._import_iri_resource(obj, policy=policy)
        if shape == "triple":
            return self.insert_triples([_canonical_triple(obj)], policy=policy)
        if shape == "typed_node":
            return {"nodes": self.insert_node_table([obj])}
        if shape == "typed_edge":
            return {"edges": self.insert_edge_table([obj])}
        raise _unsupported_resource_shape()

    def import_resources(
        self, objs: Iterable[Mapping[str, Any]], *, policy: str = "warn"
    ) -> list[dict[str, Any]]:
        _validate_import_policy(policy)
        return [self.import_resource(obj, policy=policy) for obj in objs]

    def resource(self, external_id: Any, *, key_col: str = "node_id") -> ResourceRef:
        found = _find_resource_row(self, external_id, key_col=key_col)
        if found is None:
            raise CaracalError(
                code="CDB-7012",
                message=f"resource not found for {key_col}: {external_id!r}",
            )
        class_name, row = found
        return ResourceRef(
            external_id=external_id,
            internal_id=int(row[_INTERNAL_GID_COLUMN]),
            display_iri=_display_resource_iri(external_id),
            iri=row.get(_IRI_COLUMN),
            type=class_name,
        )

    def export_resource_turtle(
        self,
        external_id: Any,
        *,
        base: str = _RESOURCE_BASE,
        policy: str = "warn",
    ) -> str:
        _validate_import_policy(policy)
        found = _find_resource_row(self, external_id, key_col="node_id")
        if found is None:
            raise CaracalError(
                code="CDB-7012",
                message=f"resource not found for node_id: {external_id!r}",
            )
        class_name, row = found
        subject = _format_iri(_display_resource_iri(external_id, base=base))
        lines = ["@prefix cdb: <caracaldb://resource/> .", ""]
        statements: list[tuple[str, str]] = []
        cls = self._find_class(class_name)
        statements.append(("a", _format_iri(_display_class_iri(cls, base=base))))

        skip = {"nid", "node_id", "type", _INTERNAL_GID_COLUMN, _IRI_COLUMN, _LABELS_COLUMN}
        skip.add(_PLACEHOLDER_COLUMN)
        for name, value in row.items():
            if name in skip or value is None:
                continue
            statements.append(
                (_format_iri(_display_property_iri(self, name, base=base)), _literal_turtle(value))
            )

        gid = int(row[_INTERNAL_GID_COLUMN])
        for relation, edge in _edges_for_gid(self, gid):
            target_id = _external_id_for_gid(self, int(edge["dst"]))
            if target_id is None:
                continue
            statements.append(
                (
                    _format_iri(_display_property_iri(self, relation, base=base)),
                    _format_iri(_display_resource_iri(target_id, base=base)),
                )
            )

        lines.append(f"{subject}")
        for index, (predicate, obj) in enumerate(statements):
            end = " ." if index == len(statements) - 1 else " ;"
            lines.append(f"    {predicate} {obj}{end}")
        return "\n".join(lines) + "\n"

    def _import_neo4j_resource(self, obj: Mapping[str, Any], *, policy: str) -> dict[str, Any]:
        _validate_import_policy(policy)
        labels = obj.get("labels")
        if not isinstance(labels, list) or not labels:
            raise CaracalError(code="CDB-7010", message="Neo4j resource requires non-empty labels")
        external_id = obj["id"]
        properties = obj.get("properties", {})
        if not isinstance(properties, Mapping):
            raise CaracalError(
                code="CDB-7010", message="Neo4j resource properties must be an object"
            )
        node = {
            "node_id": external_id,
            "type": _coerce_local_name(labels[0], "node label"),
            _LABELS_COLUMN: [str(label) for label in labels],
            **dict(properties),
        }
        relationships = obj.get("relationships", {})
        if not isinstance(relationships, Mapping):
            raise CaracalError(
                code="CDB-7010", message="Neo4j resource relationships must be an object"
            )
        targets = [
            _resource_id(target)
            for raw in relationships.values()
            for target in _relationship_targets(raw)
        ]
        existing_ids = _external_id_map(self, key_col="node_id")
        placeholder_nodes = [
            _placeholder_node(target) for target in targets if target not in existing_ids
        ]
        nodes = [node, *placeholder_nodes]
        edges = [
            {"src": external_id, "dst": _resource_id(target), "type": relation}
            for relation, raw in relationships.items()
            for target in _relationship_targets(raw)
        ]
        refs: dict[str, Any] = {"nodes": self.insert_node_table(nodes)}
        if edges:
            refs["edges"] = self.insert_edge_table(edges)
        return refs

    def _import_iri_resource(self, obj: Mapping[str, Any], *, policy: str) -> dict[str, Any]:
        _validate_import_policy(policy)
        iri = str(obj.get("@id") or obj.get("iri"))
        external_id = obj.get("node_id") or obj.get("id") or iri
        class_name = _coerce_local_name(
            obj.get("type") or obj.get("label") or "Resource", "resource type"
        )
        node = {"node_id": external_id, "type": class_name, _IRI_COLUMN: iri}
        for key, value in obj.items():
            if key not in {"@id", "iri", "id", "node_id", "type", "label"}:
                node[str(key)] = value
        return {"nodes": self.insert_node_table([node])}

    def exec(self, text: str) -> None:
        for statement in _split_exec_statements(text):
            upper = statement.upper()
            if upper.startswith("CREATE CLASS "):
                name = statement[len("CREATE CLASS ") :].strip()
                self.define_class(name)
                continue
            if upper.startswith("INSERT "):
                class_name, row = _parse_insert_statement(statement)
                self.insert_nodes(class_name, row)
                continue
            raise CaracalError(
                code="CDB-6020",
                message=(
                    "db.exec() currently supports CREATE CLASS name and "
                    "INSERT name { field: value }"
                ),
            )

    def close(self) -> None:
        """Close the database.

        If the database was opened from (or created as) a packed single
        file, the working directory bundle is re-packed to the original
        file path and the temporary working directory is removed.
        """
        if self._closed:
            return
        self._closed = True
        if self._packed_source is not None and self._mode == "rw":
            # Re-pack the working bundle back to the packed file.
            if self._packed_source.exists():
                self._packed_source.unlink()
            pack_bundle(self._bundle.path, output=self._packed_source)
        if self._working_dir is not None:
            shutil.rmtree(self._working_dir, ignore_errors=True)

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc: object) -> bool:
        self.close()
        return False

    def open_node_store(self, class_iri: str) -> NodeStore:
        cls = self._find_class(class_iri)
        return open_node_store(
            self._bundle, class_iri=cls.iri, local_name=cls.local_name or _local(cls.iri)
        )

    def node_table(
        self,
        class_name: str,
        *,
        columns: list[str] | None = None,
    ) -> pa.Table:
        return self.open_node_store(class_name).to_table(columns=columns)

    def edge_table(
        self,
        property_name: str,
        *,
        columns: list[str] | None = None,
    ) -> pa.Table:
        prop = self._find_property(property_name)
        store = open_edge_store(
            self._bundle,
            property_iri=prop.iri,
            local_name=prop.local_name or _local(prop.iri),
        )
        return store.to_table(columns=columns)

    def out(
        self,
        node: Any | Iterable[Any],
        edge_type: str,
        *,
        node_key_col: str = "node_id",
        return_eids: bool = False,
    ) -> pa.Table:
        """Return outgoing adjacency as an Arrow table with ``src`` and ``dst``."""
        forward, _ = _readers_for_relation(self, edge_type, "out")
        if forward is None:
            return _empty_adjacency_table(return_eids=return_eids)
        return _adjacency_table(
            self,
            forward,
            node,
            direction="out",
            node_key_col=node_key_col,
            return_eids=return_eids,
        )

    def in_(
        self,
        node: Any | Iterable[Any],
        edge_type: str,
        *,
        node_key_col: str = "node_id",
        return_eids: bool = False,
    ) -> pa.Table:
        """Return incoming adjacency as an Arrow table with normalized ``src`` and ``dst``."""
        _, reverse = _readers_for_relation(self, edge_type, "in")
        if reverse is None:
            return _empty_adjacency_table(return_eids=return_eids)
        return _adjacency_table(
            self,
            reverse,
            node,
            direction="in",
            node_key_col=node_key_col,
            return_eids=return_eids,
        )

    def degree(
        self,
        node: Any | Iterable[Any],
        edge_type: str,
        *,
        direction: str = "out",
        node_key_col: str = "node_id",
    ) -> int | pa.Table:
        """Return adjacency degree for one node, or a table for many nodes."""
        if direction not in {"out", "in", "both"}:
            raise CaracalError(
                code="CDB-6020",
                message=f"degree direction must be 'out', 'in', or 'both', got {direction!r}",
            )
        ids, scalar = _resolve_graph_node_ids(self, node, node_key_col=node_key_col)
        forward, reverse = _readers_for_relation(self, edge_type, direction)
        degrees = _degree_array(ids, forward if direction in {"out", "both"} else None)
        if direction in {"in", "both"}:
            degrees = degrees + _degree_array(ids, reverse)
        if scalar:
            return int(degrees[0])
        return pa.table(
            {
                "node_id": pa.array(ids, type=pa.uint64()),
                "degree": pa.array(degrees, type=pa.uint64()),
            }
        )

    def common_neighbors(
        self,
        left: Any,
        right: Any,
        edge_type: str,
        *,
        direction: str = "out",
        node_key_col: str = "node_id",
    ) -> pa.Table:
        """Return the common neighbor ids for two nodes under one edge type."""
        left_id, _ = _resolve_graph_node_ids(self, left, node_key_col=node_key_col)
        right_id, _ = _resolve_graph_node_ids(self, right, node_key_col=node_key_col)
        left_neighbors = _neighbor_ids(self, int(left_id[0]), edge_type, direction=direction)
        right_neighbors = _neighbor_ids(self, int(right_id[0]), edge_type, direction=direction)
        import numpy as np

        common = np.intersect1d(left_neighbors, right_neighbors, assume_unique=False)
        return pa.table({"node_id": pa.array(common, type=pa.uint64())})

    def overlap(
        self,
        node: Any,
        candidates: Iterable[Any],
        edge_type: str,
        *,
        direction: str = "out",
        node_key_col: str = "node_id",
        top_k: int | None = None,
    ) -> pa.Table:
        """Rank candidate nodes by common-neighbor overlap with ``node``."""
        seed_id, _ = _resolve_graph_node_ids(self, node, node_key_col=node_key_col)
        candidate_ids, _ = _resolve_graph_node_ids(self, candidates, node_key_col=node_key_col)
        seed_neighbors = _neighbor_ids(self, int(seed_id[0]), edge_type, direction=direction)
        import numpy as np

        rows: list[tuple[int, int]] = []
        for candidate_id in candidate_ids:
            candidate_neighbors = _neighbor_ids(
                self, int(candidate_id), edge_type, direction=direction
            )
            overlap = np.intersect1d(seed_neighbors, candidate_neighbors, assume_unique=False).size
            rows.append((int(candidate_id), int(overlap)))
        rows.sort(key=lambda item: (-item[1], item[0]))
        if top_k is not None:
            if top_k < 0:
                raise CaracalError(code="CDB-6020", message="top_k must be >= 0")
            rows = rows[:top_k]
        return pa.table(
            {
                "node_id": pa.array([row[0] for row in rows], type=pa.uint64()),
                "overlap": pa.array([row[1] for row in rows], type=pa.uint64()),
            }
        )

    def create_snapshot(self, name: str) -> SnapshotId:
        """Pin a named snapshot at the current bundle LSN.

        The snapshot becomes referenceable from Tuft as
        ``MATCH (...) AS_OF SNAPSHOT 'name' ...``. Node and edge rows
        inserted after the snapshot are hidden from ``AS_OF`` reads.
        """
        return create_snapshot(self._bundle, name)

    def list_snapshots(self) -> list[SnapshotEntry]:
        """Return all named snapshots stored in the bundle, ordered by LSN."""
        return list_snapshots(self._bundle)

    def release_snapshot(self, name: str) -> bool:
        """Decrement a snapshot's refcount; remove it on the final release."""
        return release_snapshot(self._bundle, name)

    def _next_lsn(self) -> int:
        """Advance the bundle's logical write clock and persist it."""
        next_lsn = self._bundle.manifest.last_lsn + 1
        manifest = replace(self._bundle.manifest, last_lsn=next_lsn)
        manifest.write_atomic(self._bundle.path / MANIFEST_NAME)
        object.__setattr__(self._bundle, "manifest", manifest)
        return next_lsn

    def _find_class(self, iri: str) -> ClassDef:
        cls = self._catalog.class_by_iri(iri)
        if cls is None:
            # Fallback: also accept local-name match for the M1 MVP.
            for candidate in self._catalog.classes:
                if (candidate.local_name or _local(candidate.iri)) == iri:
                    return candidate
            raise CaracalError(code="CDB-6021", message=f"class not found in catalog: {iri!r}")
        return cls

    def _find_property(self, iri: str) -> Any:
        prop = self._catalog.property_by_iri(iri)
        if prop is None:
            for candidate in self._catalog.properties:
                if (candidate.local_name or _local(candidate.iri)) == iri:
                    return candidate
            raise CaracalError(code="CDB-6021", message=f"property not found in catalog: {iri!r}")
        return prop

    def _merge_class(
        self,
        cls: ClassDef,
        *,
        local_name: str,
        superclass_iris: tuple[str, ...],
    ) -> ClassDef:
        merged_superclasses = tuple(dict.fromkeys((*cls.superclass_iris, *superclass_iris)))
        merged_local_name = cls.local_name or local_name
        if merged_superclasses == cls.superclass_iris and merged_local_name == cls.local_name:
            return cls

        updated = ClassDef(
            cid=cls.cid,
            iri=cls.iri,
            local_name=merged_local_name,
            superclass_iris=merged_superclasses,
            fields=cls.fields,
            doc=cls.doc,
        )
        self._catalog.classes = tuple(
            updated if item.cid == cls.cid else item for item in self._catalog.classes
        )
        self._catalog._touch()
        save_catalog(self._bundle, self._catalog)
        return updated

    def _define_property(self, name: str, *, iri: str | None = None) -> Any:
        property_iri = iri or _synthetic_iri(name)
        existing = self._catalog.property_by_iri(property_iri)
        if existing is not None:
            return existing
        for candidate in self._catalog.properties:
            if (candidate.local_name or _local(candidate.iri)) == name:
                return candidate
        prop = self._catalog.register_property(iri=property_iri, local_name=name)
        save_catalog(self._bundle, self._catalog)
        return prop

    def _invalidate_graph_indexes(self, relation_local: str | None = None) -> None:
        if relation_local is None:
            self._csr_cache.clear()
            graph_dir = self._bundle.child("graph")
            targets = list(graph_dir.glob("*/*.csr")) + list(graph_dir.glob("*/*.csc"))
            degree_cache = getattr(self, "_degree_cache", None)
            if degree_cache is not None:
                degree_cache.clear()
        else:
            for key in list(self._csr_cache):
                if key == relation_local or key.startswith(f"{relation_local}@"):
                    del self._csr_cache[key]
            graph_dir = self._bundle.child("graph", relation_local)
            targets = list(graph_dir.glob("*.csr")) + list(graph_dir.glob("*.csc"))
            degree_cache = getattr(self, "_degree_cache", None)
            if degree_cache is not None:
                degree_cache.pop(relation_local, None)
        for target in targets:
            target.unlink(missing_ok=True)


def connect(path: str | Path, *, mode: str = "rw", format: str = "auto") -> Database:
    """Open or create a CaracalDB database.

    Parameters
    ----------
    path:
        Database path.  The ``.crcl`` suffix is appended automatically.
    mode:
        ``"rw"`` (default) for read-write, ``"ro"`` for read-only.
    format:
        Storage format — ``"auto"`` (default), ``"packed"``, or
        ``"bundle"``.

        * ``"auto"`` — if the path exists, auto-detect the format
          (packed file vs directory bundle).  For new databases, the
          **packed single file** is the default.
        * ``"packed"`` — force packed single-file format.
        * ``"bundle"`` — force directory bundle format (the engine's
          internal working format).

    Returns
    -------
    Database
        An open database handle.

    See Also
    --------
    Database : The core database object.
    Connection : The context for executing queries.

    Notes
    -----
    ``connect`` is the primary entry point for CaracalDB. It determines whether
    to unpack a single file or open a directory based on the ``format`` parameter.

    Examples
    --------
    ```python
    import tempfile
    root = tempfile.TemporaryDirectory()
    db = connect(Path(root.name) / "demo", format="bundle")
    db.close()
    root.cleanup()
    ```
    """
    if mode not in ("rw", "ro"):
        raise CaracalError(code="CDB-6022", message=f"unsupported mode: {mode}")
    if format not in ("auto", "packed", "bundle"):
        raise CaracalError(code="CDB-6022", message=f"unsupported format: {format}")

    target = Path(path)
    normalized = target if target.suffix == ".crcl" else target.with_suffix(".crcl")

    # --- format="bundle": legacy directory-bundle behaviour ----------------
    if format == "bundle":
        return _connect_bundle(normalized, mode=mode)

    # --- format="auto" or "packed" -----------------------------------------
    if normalized.exists():
        if normalized.is_dir():
            if format == "packed":
                raise CaracalError(
                    code="CDB-6022",
                    message=f"path is a directory but format='packed' was requested: {normalized}",
                    hint="use format='bundle' or pack the directory first",
                )
            # auto + existing directory → open as bundle (no temp dir)
            return _connect_bundle(normalized, mode=mode)
        if normalized.is_file() and is_packed(normalized):
            return _connect_packed(normalized, mode=mode)
        raise CaracalError(
            code="CDB-9003",
            message=f"path exists but is not a valid .crcl bundle or packed file: {normalized}",
        )

    # New database.
    if mode == "ro":
        raise CaracalError(code="CDB-9003", message=f"database not found: {normalized}")
    if format == "bundle":
        return _connect_bundle(normalized, mode=mode)
    # auto / packed → create as packed
    return _connect_new_packed(normalized, mode=mode)


def _connect_bundle(normalized: Path, *, mode: str) -> Database:
    """Open or create a plain directory bundle (legacy behaviour)."""
    bundle = open_bundle(normalized) if normalized.exists() else create_bundle(normalized)
    catalog = load_catalog(bundle)
    return Database(bundle, catalog, _mode=mode)


def _connect_packed(packed_file: Path, *, mode: str) -> Database:
    """Open an existing packed file by unpacking to a temp working dir."""
    working_dir = Path(tempfile.mkdtemp(prefix="caracal_"))
    bundle_dir = working_dir / packed_file.name
    # unpack_bundle validates the file and extracts it.
    from caracaldb.storage.pack import unpack_bundle

    unpack_bundle(packed_file, output=bundle_dir)
    bundle = open_bundle(bundle_dir)
    catalog = load_catalog(bundle)
    return Database(
        bundle,
        catalog,
        _packed_source=packed_file,
        _working_dir=working_dir,
        _mode=mode,
    )


def _connect_new_packed(packed_file: Path, *, mode: str) -> Database:
    """Create a brand-new database that will be packed on close."""
    working_dir = Path(tempfile.mkdtemp(prefix="caracal_"))
    bundle_dir = working_dir / packed_file.name
    bundle = create_bundle(bundle_dir)
    catalog = load_catalog(bundle)
    return Database(
        bundle,
        catalog,
        _packed_source=packed_file,
        _working_dir=working_dir,
        _mode=mode,
    )


def _split_exec_statements(text: str) -> list[str]:
    return [part.strip() for part in text.split(";") if part.strip()]


def _parse_insert_statement(statement: str) -> tuple[str, dict[str, object]]:
    rest = statement[len("INSERT ") :].strip()
    brace = rest.find("{")
    if brace < 0 or not rest.endswith("}"):
        raise CaracalError(
            code="CDB-6020",
            message="INSERT requires the shape: INSERT Class { field: value }",
        )
    class_name = rest[:brace].strip()
    body = rest[brace + 1 : -1].strip()
    if not class_name or not body:
        raise CaracalError(
            code="CDB-6020", message="INSERT requires a class and at least one field"
        )
    row: dict[str, object] = {}
    for item in body.split(","):
        if ":" not in item:
            raise CaracalError(code="CDB-6020", message=f"invalid INSERT field: {item.strip()!r}")
        key, value = item.split(":", 1)
        row[key.strip()] = _parse_exec_literal(value.strip())
    return class_name, row


def _parse_exec_literal(value: str) -> object:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


# ---------------------------------------------------------------------------
# Query → plan
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _CompiledQuery:
    class_iri: str
    local_name: str
    alias: str
    columns: tuple[str, ...]  # node-store columns to read
    predicate: object | None
    projections: tuple[tuple[object, str], ...]
    limit: int | None
    closure_base_iri: str | None = None
    snapshot: SnapshotId | None = None


def _compile_query(query: ta.Query, db: Database) -> _CompiledQuery:
    match_clause = next((c for c in query.clauses if isinstance(c, ta.MatchClause)), None)
    return_clause = next((c for c in query.clauses if isinstance(c, ta.ReturnClause)), None)
    where_clause = next((c for c in query.clauses if isinstance(c, ta.WhereClause)), None)
    if match_clause is None or return_clause is None:
        raise CaracalError(code="CDB-6020", message="MVP requires both MATCH and RETURN clauses")
    if len(match_clause.patterns) != 1 or len(match_clause.patterns[0].elements) != 1:
        raise CaracalError(
            code="CDB-6020",
            message="MVP supports a single (alias:Class) pattern; multi-hop lands in M2",
        )
    elem = match_clause.patterns[0].elements[0]
    if not isinstance(elem, ta.NodePattern):
        raise CaracalError(code="CDB-6020", message="MVP requires a node pattern")
    if not elem.labels:
        raise CaracalError(code="CDB-6020", message="MVP requires a class label")

    label = elem.labels[0]
    class_iri = label.value if isinstance(label, ta.Iri) else _expand(label, query)
    cls = _resolve_class(db.catalog, class_iri)
    alias = elem.var.name if elem.var is not None else "n"

    predicate: object | None = None
    closure_base_iri: str | None = None
    if where_clause is not None and where_clause.predicate is not None:
        closure_base_iri, remaining = _extract_subclassof_predicate(
            where_clause.predicate,
            alias,
            query,
        )
        if remaining is not None:
            predicate = _translate_expr(remaining, alias)

    projections: list[tuple[object, str]] = []
    for proj in return_clause.projections:
        expr = _translate_expr(proj.expr, alias)
        out_name = proj.alias.name if proj.alias is not None else _default_alias(proj.expr, alias)
        projections.append((expr, out_name))

    columns_referenced: set[str] = set()
    for expr, _ in projections:
        columns_referenced |= _collect_cols(expr)
    if predicate is not None:
        columns_referenced |= _collect_cols(predicate)
    columns = tuple(sorted(columns_referenced)) or ("nid",)  # NodeScan has at least nid available

    limit = None
    if query.modifiers.limit is not None:
        limit = _eval_int_literal(query.modifiers.limit, "LIMIT")

    snapshot = resolve_as_of(db.bundle, match_clause.as_of)

    return _CompiledQuery(
        class_iri=cls.iri,
        local_name=cls.local_name or _local(cls.iri),
        alias=alias,
        columns=columns,
        predicate=predicate,
        projections=tuple(projections),
        limit=limit,
        closure_base_iri=closure_base_iri,
        snapshot=snapshot,
    )


def _resolve_class(catalog: Catalog, iri_or_local: str) -> ClassDef:
    cls = catalog.class_by_iri(iri_or_local)
    if cls is not None:
        return cls
    for candidate in catalog.classes:
        if (candidate.local_name or _local(candidate.iri)) == iri_or_local:
            return candidate
    raise CaracalError(code="CDB-6021", message=f"class not found in catalog: {iri_or_local!r}")


def _expand(name: ta.NameRef, query: ta.Query) -> str:
    if isinstance(name, ta.Iri):
        return name.value
    qvalue = name.value
    if ":" in qvalue:
        prefix, local = qvalue.split(":", 1)
        for decl in query.prefixes:
            if decl.prefix == prefix:
                return decl.iri.value + local
    return qvalue


def _translate_expr(expr: ta.Expr | None, alias: str) -> object:
    if expr is None:
        raise CaracalError(code="CDB-6020", message="empty expression")
    if isinstance(expr, ta.PathExpr):
        if expr.root is None or len(expr.steps) != 1:
            raise CaracalError(
                code="CDB-6020",
                message="MVP supports single-step path expressions like alias.field",
            )
        if expr.root.name != alias:
            raise CaracalError(
                code="CDB-6020",
                message=f"unbound variable: {expr.root.name!r} (alias is {alias!r})",
            )
        return ("col", expr.steps[0].name)
    if isinstance(expr, ta.Var):
        if expr.name is None:
            raise CaracalError(code="CDB-6020", message="empty variable")
        # bare alias references in RETURN — emit nid as a stand-in identifier
        if expr.name.name == alias:
            return ("col", "nid")
        return ("col", expr.name.name)
    if isinstance(expr, ta.Literal):
        return ("lit", expr.value)
    if isinstance(expr, ta.BinOp):
        op = _BIN_OP_TO_TUPLE.get(expr.op)
        if op is None:
            raise CaracalError(code="CDB-6020", message=f"unsupported operator: {expr.op}")
        return (op, _translate_expr(expr.left, alias), _translate_expr(expr.right, alias))
    if isinstance(expr, ta.UnaryOp):
        if expr.op.lower() in ("not", "!"):
            return ("not", _translate_expr(expr.operand, alias))
        raise CaracalError(code="CDB-6020", message=f"unsupported unary op: {expr.op}")
    raise CaracalError(
        code="CDB-6020", message=f"unsupported expression node: {type(expr).__name__}"
    )


def _extract_subclassof_predicate(
    expr: ta.Expr,
    alias: str,
    query: ta.Query,
) -> tuple[str | None, ta.Expr | None]:
    if isinstance(expr, ta.BinOp) and expr.op.lower() == "and":
        left_base, left_remaining = _extract_subclassof_predicate(expr.left, alias, query)
        right_base, right_remaining = _extract_subclassof_predicate(expr.right, alias, query)
        if left_base is not None and right_base is not None:
            raise CaracalError(
                code="CDB-6020",
                message="only one SUBCLASSOF* predicate is supported in a query",
            )
        base = left_base or right_base
        remaining_parts = [item for item in (left_remaining, right_remaining) if item is not None]
        if not remaining_parts:
            return base, None
        if len(remaining_parts) == 1:
            return base, remaining_parts[0]
        return base, ta.BinOp(op="AND", left=remaining_parts[0], right=remaining_parts[1])

    if isinstance(expr, ta.BinOp) and expr.op == "SUBCLASSOF*":
        if not _is_alias_class_path(expr.left, alias):
            raise CaracalError(
                code="CDB-6020",
                message="SUBCLASSOF* currently requires the left operand to be alias.class",
            )
        if isinstance(expr.right, ta.Iri):
            return expr.right.value, None
        if isinstance(expr.right, ta.QName):
            return _expand(expr.right, query), None
        raise CaracalError(
            code="CDB-6020",
            message="SUBCLASSOF* currently requires an IRI or qualified class name on the right",
        )

    return None, expr


def _is_alias_class_path(expr: ta.Expr | None, alias: str) -> bool:
    return (
        isinstance(expr, ta.PathExpr)
        and expr.root is not None
        and expr.root.name == alias
        and len(expr.steps) == 1
        and expr.steps[0].name == "class"
    )


_BIN_OP_TO_TUPLE: dict[str, str] = {
    "=": "eq",
    "==": "eq",
    "!=": "ne",
    "<>": "ne",
    "<": "lt",
    "<=": "le",
    ">": "gt",
    ">=": "ge",
    "AND": "and",
    "and": "and",
    "&&": "and",
    "OR": "or",
    "or": "or",
    "||": "or",
}


def _collect_cols(expr: object) -> set[str]:
    if isinstance(expr, tuple) and expr:
        head = expr[0]
        if head == "col" and len(expr) >= 2 and isinstance(expr[1], str):
            return {expr[1]}
        items = expr[1:] if isinstance(head, str) else expr
        result: set[str] = set()
        for item in items:
            result |= _collect_cols(item)
        return result
    return set()


def _eval_int_literal(node: ta.Expr, where: str) -> int:
    if isinstance(node, ta.Literal) and isinstance(node.value, int):
        return int(node.value)
    raise CaracalError(code="CDB-6020", message=f"{where} requires an integer literal")


def _default_alias(expr: ta.Expr, alias: str) -> str:
    if isinstance(expr, ta.PathExpr) and expr.root is not None and len(expr.steps) == 1:
        return expr.steps[0].name
    if isinstance(expr, ta.Var) and expr.name is not None:
        return expr.name.name
    return "expr"


# ---------------------------------------------------------------------------
# Multi-hop pattern compilation
# ---------------------------------------------------------------------------
#
# The single-class shortcut above (``_compile_query`` / ``_build_pipeline``)
# only sees one ``NodePattern`` per ``MATCH`` and falls back to a pure
# ``NodeScan``. The functions below cover the M2 carry-over recorded in
# docs/milestones/M2-gate.md §"Carry-overs into M3":
#
#   "The pattern compiler (CDB-045) produces logical plans, but
#    caracaldb.api.Connection.sql still uses the M1 single-class shortcut.
#    M3 wires the compiler into the public API once LExpand/LJoin have
#    physical translations bound to live CsrReader instances per property."
#
# Strategy: for ``(a:A)-[:rel]->(b:B)`` we build:
#
#     NodeScan(A) ─► Rename(a.) ──┐
#                                  ├─► HashJoin (recover seed properties)
#     NodeScan(A) ─► Rename(a.) ──┘    on a._cdb_gid
#         └─► Expand(rel) ─►  produces (a._cdb_gid, b._cdb_gid)
#
#     ─► HashJoin(NodeScan(B) renamed b.) on b._cdb_gid (recover target props)
#     ─► DropColumns (probe-side duplicate keys)
#     ─► [Filter (WHERE)] ─► Project (RETURN)
#
# We prefer ``_cdb_gid`` as the join key because it is the global node id used
# by ``insert_edge_table``; per-class ``nid`` is fine when the graph is
# single-class (the path falls back to ``nid`` if ``_cdb_gid`` is absent).


def _is_multi_element_pattern(query: ta.Query) -> bool:
    match_clause = next((c for c in query.clauses if isinstance(c, ta.MatchClause)), None)
    if match_clause is None or not match_clause.patterns:
        return False
    return any(len(pattern.elements) > 1 for pattern in match_clause.patterns)


@dataclass(slots=True)
class _PatternHop:
    head_alias: str
    head_class: ClassDef
    next_alias: str
    next_class: ClassDef
    relation_locals: tuple[str, ...]  # length>1 for rel-type union -[:p|q]-
    direction: str  # "out" | "in" | "both"


@dataclass(slots=True)
class _PatternPlan:
    head_alias: str
    head_class: ClassDef
    hops: tuple[_PatternHop, ...]
    alias_columns: dict[str, set[str]]  # alias -> set of property column names needed
    predicate: object | None
    projections: tuple[tuple[object, str], ...]
    limit: int | None
    id_column: str  # "_cdb_gid" or "nid"
    snapshot: SnapshotId | None = None


def _compile_pattern_query(query: ta.Query, db: Database) -> _PatternPlan:
    match_clause = next((c for c in query.clauses if isinstance(c, ta.MatchClause)), None)
    return_clause = next((c for c in query.clauses if isinstance(c, ta.ReturnClause)), None)
    where_clause = next((c for c in query.clauses if isinstance(c, ta.WhereClause)), None)
    if match_clause is None or return_clause is None:
        raise CaracalError(code="CDB-6020", message="multi-hop MATCH requires a RETURN clause")
    if len(match_clause.patterns) != 1:
        raise CaracalError(
            code="CDB-6020",
            message="multi-pattern MATCH (comma-separated) is not yet wired through conn.sql",
        )

    pattern = match_clause.patterns[0]
    elements = list(pattern.elements)
    if not isinstance(elements[0], ta.NodePattern):
        raise CaracalError(code="CDB-6020", message="pattern must start with a node element")

    # Decide the cross-class join key by inspecting the head class's node store.
    head_node = elements[0]
    head_alias = head_node.var.name if head_node.var is not None else "n0"
    head_class = _resolve_pattern_class(db, head_node, query)
    id_column = _detect_id_column(db, head_class)

    hops: list[_PatternHop] = []
    aliases: list[str] = [head_alias]
    cursor_alias = head_alias
    cursor_class = head_class
    pending_rel: ta.RelPattern | None = None
    for elem in elements[1:]:
        if isinstance(elem, ta.RelPattern):
            pending_rel = elem
            continue
        if not isinstance(elem, ta.NodePattern):
            raise CaracalError(
                code="CDB-6020",
                message=f"unsupported pattern element kind: {type(elem).__name__}",
            )
        if pending_rel is None:
            raise CaracalError(
                code="CDB-6020",
                message="adjacent node patterns require a connecting -[rel]- element",
            )
        hop_min = pending_rel.hop_range.min_hops
        hop_max = pending_rel.hop_range.max_hops
        if hop_min not in (None, 1) or hop_max not in (None, 1):
            raise CaracalError(
                code="CDB-6020",
                message=(
                    "variable-length paths *k..m are not yet wired through conn.sql; "
                    "use 1-hop relations or build the pipeline directly"
                ),
            )
        if not pending_rel.types:
            raise CaracalError(code="CDB-6020", message="rel pattern must carry a -[:relation]-")
        # Rel-type unions ``-[:p|q]->`` lower to one Expand per relation,
        # merged via UnionAll before the property-recovery joins.
        relation_locals: list[str] = []
        for rel_label in pending_rel.types:
            iri = rel_label.value if isinstance(rel_label, ta.Iri) else _expand(rel_label, query)
            local = _local(iri) if iri.startswith("http") else iri
            if local not in list_edge_stores(db.bundle):
                raise CaracalError(
                    code="CDB-6023",
                    message=f"edge store missing for relation {local!r}",
                    hint="insert edges with insert_edge_table or open_edge_store before querying",
                )
            relation_locals.append(local)
        next_alias = elem.var.name if elem.var is not None else f"n{len(aliases)}"
        next_class = _resolve_pattern_class(db, elem, query)
        if pending_rel.direction == ta.Direction.OUT:
            direction = "out"
        elif pending_rel.direction == ta.Direction.IN:
            direction = "in"
        else:
            direction = "both"
        hops.append(
            _PatternHop(
                head_alias=cursor_alias,
                head_class=cursor_class,
                next_alias=next_alias,
                next_class=next_class,
                relation_locals=tuple(relation_locals),
                direction=direction,
            )
        )
        aliases.append(next_alias)
        cursor_alias = next_alias
        cursor_class = next_class
        pending_rel = None

    alias_set = set(aliases)
    projections: list[tuple[object, str]] = []
    alias_columns: dict[str, set[str]] = {alias: set() for alias in aliases}
    for proj in return_clause.projections:
        expr_obj, refs = _translate_pattern_expr(proj.expr, alias_set, db)
        out_name = proj.alias.name if proj.alias is not None else _default_pattern_alias(proj.expr)
        projections.append((expr_obj, out_name))
        for alias_name, col in refs:
            alias_columns[alias_name].add(col)

    predicate: object | None = None
    if where_clause is not None and where_clause.predicate is not None:
        predicate, refs = _translate_pattern_expr(where_clause.predicate, alias_set, db)
        for alias_name, col in refs:
            alias_columns[alias_name].add(col)

    limit = None
    if query.modifiers.limit is not None:
        limit = _eval_int_literal(query.modifiers.limit, "LIMIT")

    snapshot = resolve_as_of(db.bundle, match_clause.as_of)

    return _PatternPlan(
        head_alias=head_alias,
        head_class=head_class,
        hops=tuple(hops),
        alias_columns=alias_columns,
        predicate=predicate,
        projections=tuple(projections),
        limit=limit,
        id_column=id_column,
        snapshot=snapshot,
    )


def _resolve_pattern_class(db: Database, node: ta.NodePattern, query: ta.Query) -> ClassDef:
    if not node.labels:
        raise CaracalError(
            code="CDB-6020",
            message="every node pattern must carry a class label in multi-hop MATCH",
        )
    if len(node.labels) > 1:
        raise CaracalError(
            code="CDB-6020",
            message=(
                "multi-label node patterns (a:Foo&:Bar) are not yet wired through conn.sql; "
                "use a single label per node"
            ),
        )
    label = node.labels[0]
    class_iri = label.value if isinstance(label, ta.Iri) else _expand(label, query)
    return _resolve_class(db.catalog, class_iri)


def _detect_id_column(db: Database, head_class: ClassDef) -> str:
    """Pick the join-key column produced by NodeScan for the head class.

    Prefer the global ``_cdb_gid`` (set by ``insert_node_table`` /
    ``insert_edge_table``) so edges built across classes line up with the
    CSR. Fall back to per-class ``nid`` for graphs that were loaded directly
    via ``insert_nodes`` + ``open_edge_store`` (the single-class case).
    """
    try:
        store = open_node_store(
            db.bundle,
            class_iri=head_class.iri,
            local_name=head_class.local_name or _local(head_class.iri),
        )
    except CaracalError:
        return "nid"
    schema_names = list(store.schema.names)
    if _INTERNAL_GID_COLUMN in schema_names:
        return _INTERNAL_GID_COLUMN
    return "nid"


def _build_degree_lookup(db: Database, relation_local: str) -> np.ndarray:
    """Return a uint64 array indexed by gid giving the out-degree under ``relation_local``.

    Memoised on the Database so repeated ``degree(_, "rel")`` calls within a
    session reuse the same lookup. Computed from the forward CSR's offsets,
    which is already built (and cached) by ``_readers_for_relation``.
    """
    cache = getattr(db, "_degree_cache", None)
    if cache is None:
        cache = {}
        db._degree_cache = cache  # type: ignore[attr-defined]
    if relation_local in cache:
        return cache[relation_local]
    forward, _ = _readers_for_relation(db, relation_local, "out")
    if forward is None:
        raise CaracalError(
            code="CDB-6023",
            message=f"degree(): no forward CSR for relation {relation_local!r}",
        )
    import numpy as np

    offsets = np.asarray(forward.offsets, dtype=np.uint64)
    degrees = (offsets[1:] - offsets[:-1]).astype(np.uint64)
    cache[relation_local] = degrees
    return degrees


def _resolve_graph_node_ids(
    db: Database,
    node: Any | Iterable[Any],
    *,
    node_key_col: str,
) -> tuple[list[int], bool]:
    if _is_scalar_node_ref(node):
        values = [node]
        scalar = True
    else:
        values = list(node)
        scalar = False
    id_map = _external_id_map(db, key_col=node_key_col)
    return [_resolve_graph_node_id(id_map, value, node_key_col) for value in values], scalar


def _is_scalar_node_ref(value: Any) -> bool:
    if isinstance(value, ResourceRef | str | bytes):
        return True
    return not isinstance(value, Iterable)


def _resolve_graph_node_id(id_map: Mapping[Any, int], value: Any, node_key_col: str) -> int:
    if isinstance(value, ResourceRef):
        return value.internal_id
    if value in id_map:
        return id_map[value]
    if isinstance(value, Integral) and not isinstance(value, bool) and int(value) >= 0:
        return int(value)
    raise CaracalError(
        code="CDB-7021",
        message=f"unknown graph node reference for {node_key_col!r}: {value!r}",
        hint="pass an internal id, ResourceRef, or an existing node_id value",
    )


def _empty_adjacency_table(*, return_eids: bool = False) -> pa.Table:
    fields = [pa.field("src", pa.uint64()), pa.field("dst", pa.uint64())]
    if return_eids:
        fields.append(pa.field("eid", pa.uint64()))
    return pa.Table.from_batches([], schema=pa.schema(fields))


def _adjacency_table(
    db: Database,
    reader: CsrReader,
    node: Any | Iterable[Any],
    *,
    direction: str,
    node_key_col: str,
    return_eids: bool,
) -> pa.Table:
    ids, _ = _resolve_graph_node_ids(db, node, node_key_col=node_key_col)
    import numpy as np

    seed_ids = np.asarray(ids, dtype=np.uint64)
    if seed_ids.size == 0:
        return _empty_adjacency_table(return_eids=return_eids)
    valid = seed_ids < reader.num_vertices
    if not bool(valid.any()):
        return _empty_adjacency_table(return_eids=return_eids)
    seeds = seed_ids[valid]
    if return_eids:
        src_rep, dst_flat, eid_flat = reader.batch_neighbors(seeds, return_eids=True)
    else:
        src_rep, dst_flat = reader.batch_neighbors(seeds)
        eid_flat = None

    if direction == "out":
        src = src_rep
        dst = dst_flat
    elif direction == "in":
        src = dst_flat
        dst = src_rep
    else:
        raise CaracalError(code="CDB-6020", message=f"unsupported adjacency direction: {direction}")
    if src.size == 0:
        return _empty_adjacency_table(return_eids=return_eids)

    arrays: list[pa.Array] = [pa.array(src, type=pa.uint64()), pa.array(dst, type=pa.uint64())]
    names = ["src", "dst"]
    if return_eids:
        assert eid_flat is not None
        arrays.append(pa.array(eid_flat, type=pa.uint64()))
        names.append("eid")
    return pa.table(arrays, names=names)


def _degree_array(ids: list[int], reader: CsrReader | None) -> np.ndarray:
    import numpy as np

    degrees = np.zeros(len(ids), dtype=np.uint64)
    if reader is None or not ids:
        return degrees
    seed_ids = np.asarray(ids, dtype=np.uint64)
    valid = seed_ids < reader.num_vertices
    if bool(valid.any()):
        degrees[valid] = reader.degrees(seed_ids[valid]).astype(np.uint64)
    return degrees


def _neighbor_ids(
    db: Database,
    node_id: int,
    edge_type: str,
    *,
    direction: str,
) -> np.ndarray:
    import numpy as np

    if direction == "out":
        forward, _ = _readers_for_relation(db, edge_type, "out")
        readers = [forward]
    elif direction == "in":
        _, reverse = _readers_for_relation(db, edge_type, "in")
        readers = [reverse]
    elif direction == "both":
        forward, reverse = _readers_for_relation(db, edge_type, "both")
        readers = [forward, reverse]
    else:
        raise CaracalError(
            code="CDB-6020",
            message=f"direction must be 'out', 'in', or 'both', got {direction!r}",
        )
    pieces: list[np.ndarray] = []
    for reader in readers:
        if reader is None or node_id >= reader.num_vertices:
            continue
        pieces.append(reader.neighbors_of(node_id))
    if not pieces:
        return np.empty(0, dtype=np.uint64)
    return np.unique(np.concatenate(pieces).astype(np.uint64, copy=False))


def _translate_pattern_expr(
    expr: ta.Expr | None,
    aliases: set[str],
    db: Database | None = None,
) -> tuple[object, list[tuple[str, str]]]:
    """Translate an expression that may reference multiple aliases.

    Returns (compiled_expr_tuple, [(alias, column), ...]) where the column
    references list is used to plan which node-store columns we must read.
    Path expressions ``alias.field`` map to ``("col", "alias.field")`` so the
    joined record batch (which carries dotted column names by construction)
    can resolve them directly.
    """
    refs: list[tuple[str, str]] = []
    compiled = _walk_pattern_expr(expr, aliases, refs, db)
    return compiled, refs


def _walk_pattern_expr(
    expr: ta.Expr | None,
    aliases: set[str],
    refs: list[tuple[str, str]],
    db: Database | None = None,
) -> object:
    if expr is None:
        raise CaracalError(code="CDB-6020", message="empty expression")
    if isinstance(expr, ta.PathExpr):
        if expr.root is None or len(expr.steps) != 1:
            raise CaracalError(
                code="CDB-6020",
                message="multi-hop MATCH supports single-step path expressions like alias.field",
            )
        alias_name = expr.root.name
        if alias_name not in aliases:
            raise CaracalError(
                code="CDB-6020",
                message=f"unbound variable: {alias_name!r} (known aliases: {sorted(aliases)})",
            )
        col = expr.steps[0].name
        refs.append((alias_name, col))
        return ("col", f"{alias_name}.{col}")
    if isinstance(expr, ta.Var):
        if expr.name is None:
            raise CaracalError(code="CDB-6020", message="empty variable")
        # Bare alias references resolve to the alias's id column; we'll fix
        # it up when we know the id_column at pipeline build time. Encode as
        # a placeholder now.
        if expr.name.name in aliases:
            return ("alias_id", expr.name.name)
        raise CaracalError(
            code="CDB-6020",
            message=f"unbound variable: {expr.name.name!r} (known aliases: {sorted(aliases)})",
        )
    if isinstance(expr, ta.Literal):
        return ("lit", expr.value)
    if isinstance(expr, ta.BinOp):
        op = _BIN_OP_TO_TUPLE.get(expr.op)
        if op is None:
            raise CaracalError(code="CDB-6020", message=f"unsupported operator: {expr.op}")
        return (
            op,
            _walk_pattern_expr(expr.left, aliases, refs, db),
            _walk_pattern_expr(expr.right, aliases, refs, db),
        )
    if isinstance(expr, ta.UnaryOp):
        if expr.op.lower() in ("not", "!"):
            return ("not", _walk_pattern_expr(expr.operand, aliases, refs, db))
        raise CaracalError(code="CDB-6020", message=f"unsupported unary op: {expr.op}")
    if isinstance(expr, ta.FnCall):
        return _compile_pattern_fncall(expr, aliases, refs, db)
    raise CaracalError(
        code="CDB-6020", message=f"unsupported expression node: {type(expr).__name__}"
    )


def _compile_pattern_fncall(
    expr: ta.FnCall,
    aliases: set[str],
    refs: list[tuple[str, str]],
    db: Database | None,
) -> object:
    """Lower a function call inside a pattern WHERE/RETURN to an expr tuple.

    Currently supports the graph topology built-in ``degree(alias, "rel")``,
    which prebinds the relation's per-vertex out-degree as a numpy lookup
    array. Other built-ins (neighbors / shortest_path / k_hop) still raise
    ``NotImplementedError`` until a vectorised CSR-aware execution context
    lands; the M2 gate doc tracks them as carry-overs.
    """
    if expr.name is None:
        raise CaracalError(code="CDB-6020", message=f"unsupported function call: {expr!r}")
    if isinstance(expr.name, ta.Ident):
        fn_name = expr.name.name
    elif isinstance(expr.name, ta.QName):
        fn_name = expr.name.value
    else:
        raise CaracalError(code="CDB-6020", message=f"unsupported function call: {expr!r}")
    if fn_name == "degree":
        if len(expr.args) != 2:
            raise CaracalError(
                code="CDB-6020",
                message='degree() takes exactly 2 args: degree(alias, "relation")',
            )
        alias_arg, rel_arg = expr.args
        if not isinstance(alias_arg, ta.Var) or alias_arg.name is None:
            raise CaracalError(
                code="CDB-6020",
                message="degree(): first arg must be a node alias",
            )
        alias_name = alias_arg.name.name
        if alias_name not in aliases:
            raise CaracalError(
                code="CDB-6020",
                message=f"degree(): unbound alias {alias_name!r}",
            )
        if not isinstance(rel_arg, ta.Literal) or not isinstance(rel_arg.value, str):
            raise CaracalError(
                code="CDB-6020",
                message="degree(): second arg must be a string literal naming the relation",
            )
        if db is None:
            raise CaracalError(
                code="CDB-6020",
                message="degree(): runtime requires an open database",
            )
        relation_local = rel_arg.value
        lookup = _build_degree_lookup(db, relation_local)
        import numpy as np
        import pyarrow as pa

        def _apply(col: pa.Array, _lookup: np.ndarray = lookup) -> pa.Array:
            ids = np.asarray(col, dtype=np.uint64)
            return pa.array(np.take(_lookup, ids), type=pa.uint64())

        # The id column is resolved later by ``_resolve_alias_id_refs``, so
        # emit the alias-id placeholder for the column ref.
        return ("py_unary", _apply, ("alias_id", alias_name))
    raise CaracalError(
        code="CDB-6020",
        message=(
            f"function {fn_name!r} not yet supported in pattern queries; "
            "graph topology built-ins beyond degree() remain a known M2 carry-over"
        ),
    )


def _resolve_alias_id_refs(expr: object, id_column: str) -> object:
    """Replace ``("alias_id", name)`` placeholders with concrete column refs."""
    if not isinstance(expr, tuple) or not expr:
        return expr
    if expr[0] == "alias_id":
        return ("col", f"{expr[1]}.{id_column}")
    return tuple(_resolve_alias_id_refs(item, id_column) for item in expr)


def _default_pattern_alias(expr: ta.Expr) -> str:
    if isinstance(expr, ta.PathExpr) and expr.root is not None and len(expr.steps) == 1:
        return expr.steps[0].name
    if isinstance(expr, ta.Var) and expr.name is not None:
        return expr.name.name
    return "expr"


# ---------------------------------------------------------------------------
# Pattern plan → physical pipeline
# ---------------------------------------------------------------------------


def _build_pattern_pipeline(plan: _PatternPlan, db: Database) -> PhysicalOperator:
    head_alias = plan.head_alias
    id_column = plan.id_column

    def _scan_for_alias(alias: str, cls: ClassDef) -> PhysicalOperator:
        """Build a fresh prefixed NodeScan for the given alias.

        Called once per *use site* (pull-based operators consume their child
        exactly once), so each call returns an independent operator tree even
        when the same alias appears multiple times in the pipeline DAG.
        """
        wanted = set(plan.alias_columns.get(alias, set()))
        wanted.add(id_column)  # always need the join key
        store = open_node_store(
            db.bundle, class_iri=cls.iri, local_name=cls.local_name or _local(cls.iri)
        )
        available = set(store.schema.names)
        # NodeScan also synthesises "nid" even though it's not in the manifest
        # schema (per node_store.py), so include it without checking.
        cols = sorted({c for c in wanted if c in available or c == "nid"})
        scan = NodeScanOperator(store, columns=cols)
        rename = {name: f"{alias}.{name}" for name in cols}
        return RenameOperator(scan, rename)

    # The pipeline DAG below is "linear with side scans": for each hop we
    # consume the running ``pipeline`` once (as the HashJoin build side that
    # carries seed properties forward) and start a *fresh* scan branch for
    # the Expand seed and for the target-side property join. Because Expand
    # only needs the id column, the seed branch is a tiny parallel scan, not
    # a copy of the running pipeline.
    pipeline: PhysicalOperator = _scan_for_alias(head_alias, plan.head_class)
    head_alias_for_seed = head_alias
    head_class_for_seed = plan.head_class
    head_id_col = f"{head_alias}.{id_column}"

    for hop in plan.hops:
        src_alias_col = head_id_col
        dst_alias_col = f"{hop.next_alias}.{id_column}"
        # Build one Expand per relation in the (possibly unioned) rel-type set,
        # then UnionAll them. Each Expand consumes its own fresh seed branch
        # because PhysicalOperator is pull-based and single-consume.
        expand_branches: list[PhysicalOperator] = []
        snapshot_lsn = plan.snapshot.lsn_high if plan.snapshot is not None else None
        for relation_local in hop.relation_locals:
            forward, reverse = _readers_for_relation(
                db,
                relation_local,
                hop.direction,
                snapshot_lsn=snapshot_lsn,
            )
            seed_branch = _scan_for_alias(head_alias_for_seed, head_class_for_seed)
            seed_for_expand = _ProjectKeyOperator(seed_branch, key=head_id_col)
            expand_branches.append(
                ExpandOperator(
                    seed_for_expand,
                    forward=forward,
                    reverse=reverse,
                    direction=hop.direction,  # type: ignore[arg-type]
                    src_alias=src_alias_col,
                    dst_alias=dst_alias_col,
                    seed_column=head_id_col,
                )
            )
        expand: PhysicalOperator = (
            expand_branches[0]
            if len(expand_branches) == 1
            else UnionAllOperator(tuple(expand_branches))
        )
        # Recover head-side properties: build = the running pipeline (already
        # carries every alias.field accumulated so far), probe = expand pairs.
        head_with_expand = HashJoinOperator(
            build=pipeline,
            probe=expand,
            build_key=head_id_col,
            probe_key=head_id_col,
        )
        head_with_expand = DropColumnsOperator(head_with_expand, drop=(head_id_col,))

        # Bring in target-side properties via a second hash join on the dst id.
        target_scan = _scan_for_alias(hop.next_alias, hop.next_class)
        with_target = HashJoinOperator(
            build=head_with_expand,
            probe=target_scan,
            build_key=dst_alias_col,
            probe_key=dst_alias_col,
        )
        with_target = DropColumnsOperator(with_target, drop=(dst_alias_col,))
        pipeline = with_target
        head_alias_for_seed = hop.next_alias
        head_class_for_seed = hop.next_class
        head_id_col = dst_alias_col  # next hop chains off the target id

    # WHERE / RETURN: at this point columns are dotted (alias.field).
    if plan.predicate is not None:
        predicate = _resolve_alias_id_refs(plan.predicate, id_column)
        pipeline = FilterOperator(pipeline, compile_expr(predicate))
    if plan.projections:
        compiled = [
            (compile_expr(_resolve_alias_id_refs(expr, id_column)), name)
            for expr, name in plan.projections
        ]
        pipeline = ProjectOperator(pipeline, compiled)
    return pipeline


class _ProjectKeyOperator(PhysicalOperator):
    """Emit a single-column view of the upstream batches (for Expand seeds).

    Expand's ``seed_column`` already supports any column name, but materialising
    the entire upstream batch as the Expand seed would force a copy of every
    seed property through the fan-out. This thin wrapper projects to just the
    id column so Expand fans out a compact ``(uint64,)`` batch.
    """

    name = "ProjectKey"

    def __init__(self, child: PhysicalOperator, *, key: str) -> None:
        super().__init__()
        self._child = child
        self._key = key

    def _open(self, ctx: ExecCtx) -> None:
        self._child.open(ctx)

    def _next_batch(self) -> pa.RecordBatch | None:
        batch = self._child.next_batch()
        if batch is None:
            return None
        col = batch.column(self._key)
        return pa.RecordBatch.from_arrays([col], names=[self._key])

    def _close(self) -> None:
        self._child.close()


def _readers_for_relation(
    db: Database,
    relation_local: str,
    direction: str,
    *,
    snapshot_lsn: int | None = None,
) -> tuple[CsrReader | None, CsrReader | None]:
    if direction not in {"out", "in", "both"}:
        raise CaracalError(
            code="CDB-6020",
            message=f"relation reader direction must be 'out', 'in', or 'both', got {direction!r}",
        )
    cache: dict[str, dict[str, CsrReader]] = db._csr_cache  # type: ignore[attr-defined]
    cache_key = f"{relation_local}@{snapshot_lsn}" if snapshot_lsn is not None else relation_local
    entry = cache.setdefault(cache_key, {})
    needs_forward = direction in ("out", "both")
    needs_reverse = direction in ("in", "both")
    if (not needs_forward or "forward" in entry) and (not needs_reverse or "reverse" in entry):
        return entry.get("forward"), entry.get("reverse")

    prop = _find_property_by_local_name(db, relation_local)
    if prop is None:
        raise CaracalError(code="CDB-6023", message=f"property {relation_local!r} not in catalog")
    edge_store = open_edge_store(
        db.bundle, property_iri=prop.iri, local_name=prop.local_name or _local(prop.iri)
    )

    # Vertex space: we use the maximum nid+1 across all node stores so the
    # CSR can hold either per-class nids or global gids — they share the same
    # uint64 space and CSR builder only requires num_vertices to bound them.
    num_vertices = _global_vertex_count(db)

    csr_dir = db.bundle.child("graph", relation_local)
    csr_dir.mkdir(parents=True, exist_ok=True)
    suffix = f".snap{snapshot_lsn}" if snapshot_lsn is not None else ""
    forward_path = csr_dir / f"forward{suffix}.csr"
    reverse_path = csr_dir / f"reverse{suffix}.csc"
    edge_input = (
        edge_store.to_table(snapshot_lsn=snapshot_lsn) if snapshot_lsn is not None else edge_store
    )
    expected_edges = (
        edge_input.num_rows if isinstance(edge_input, pa.Table) else edge_input.num_rows
    )

    forward: CsrReader | None = entry.get("forward")
    reverse: CsrReader | None = entry.get("reverse")
    if needs_forward and forward is None:
        forward = _fresh_csr_reader(
            forward_path, num_vertices=num_vertices, num_edges=expected_edges
        )
        if forward is None:
            build_csr(edge_input, num_vertices=num_vertices, out_path=forward_path, with_eids=True)
            forward = CsrReader(forward_path)
        entry["forward"] = forward
    if needs_reverse and reverse is None:
        reverse = _fresh_csr_reader(
            reverse_path, num_vertices=num_vertices, num_edges=expected_edges
        )
        if reverse is None:
            build_csc(edge_input, num_vertices=num_vertices, out_path=reverse_path, with_eids=True)
            reverse = CsrReader(reverse_path)
        entry["reverse"] = reverse

    return forward, reverse


def _fresh_csr_reader(path: Path, *, num_vertices: int, num_edges: int) -> CsrReader | None:
    if not path.is_file():
        return None
    reader = CsrReader(path)
    if reader.num_vertices == num_vertices and reader.num_edges == num_edges:
        return reader
    path.unlink(missing_ok=True)
    return None


def _global_vertex_count(db: Database) -> int:
    """Upper bound on the largest vertex id touched by any edge.

    For graphs loaded via ``insert_node_table`` / ``insert_edge_table`` the
    edge endpoints live in ``_cdb_gid`` space, so ``max(_cdb_gid)+1`` is the
    right size. For single-class graphs that use ``nid`` directly we still
    end up with at least ``max(nid)+1`` because both id spaces are dense and
    bounded by the per-class store's ``next_nid``. Using the larger of the two
    keeps both regimes working from one CSR.
    """
    n = 0
    for class_name in list_node_stores(db.bundle):
        cls = db._find_class(class_name)
        store = open_node_store(
            db.bundle, class_iri=cls.iri, local_name=cls.local_name or _local(cls.iri)
        )
        if _INTERNAL_GID_COLUMN in store.schema.names:
            table = store.to_table(columns=[_INTERNAL_GID_COLUMN])
            if table.num_rows:
                n = max(n, int(table.column(_INTERNAL_GID_COLUMN).to_pylist()[-1]) + 1)
                # also scan all in case order is not monotonic
                arr = table.column(_INTERNAL_GID_COLUMN).to_pylist()
                if arr:
                    n = max(n, int(max(arr)) + 1)
        n = max(n, store.manifest.next_nid)
    return n


# ---------------------------------------------------------------------------
# Plan → physical pipeline
# ---------------------------------------------------------------------------


def _build_pipeline(plan: _CompiledQuery, db: Database) -> Any:
    # Make sure the underlying class exists on disk.
    if plan.closure_base_iri is not None:
        closure = ClassClosureIndex.from_catalog(db.catalog)
        if not closure.is_subclass(plan.class_iri, plan.closure_base_iri):
            return _EmptyOperator()
        op: Any = ClosureScanOperator(
            db.bundle,
            closure,
            base_iri=plan.class_iri,
            columns=list(plan.columns),
        )
    elif plan.local_name not in list_node_stores(db.bundle):
        raise CaracalError(
            code="CDB-6023",
            message=f"node store missing for class {plan.class_iri!r} (local={plan.local_name!r})",
        )
    else:
        store = open_node_store(db.bundle, class_iri=plan.class_iri, local_name=plan.local_name)
        column_request = list(plan.columns)
        op = NodeScanOperator(store, columns=column_request)

    if plan.predicate is not None:
        op = FilterOperator(op, compile_expr(plan.predicate))
    if plan.projections:
        op = ProjectOperator(op, [(compile_expr(e), name) for e, name in plan.projections])
    return op


class _EmptyOperator(PhysicalOperator):
    name = "Empty"

    def _next_batch(self) -> pa.RecordBatch | None:
        return None


def _apply_limit(batches: list[pa.RecordBatch], limit: int) -> list[pa.RecordBatch]:
    out: list[pa.RecordBatch] = []
    remaining = limit
    for batch in batches:
        if remaining <= 0:
            break
        if batch.num_rows <= remaining:
            out.append(batch)
            remaining -= batch.num_rows
        else:
            out.append(batch.slice(0, remaining))
            remaining = 0
    return out


def _local(iri: str) -> str:
    return iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1].rsplit(":", 1)[-1]


def _synthetic_iri(name: str) -> str:
    return f"{_INTERNAL_IRI_PREFIX}{name}"


def _display_resource_iri(value: Any, *, base: str = _RESOURCE_BASE) -> str:
    return base.rstrip("/") + "/" + quote(str(value), safe="/:@._~-")


def _display_class_iri(cls: ClassDef, *, base: str) -> str:
    if not cls.iri.startswith(_INTERNAL_IRI_PREFIX):
        return cls.iri
    name = cls.local_name or _local(cls.iri)
    return base.rstrip("/") + "/class/" + quote(name, safe="/:@._~-")


def _display_property_iri(db: Database, name: str, *, base: str) -> str:
    for prop in db.catalog.properties:
        if (prop.local_name or _local(prop.iri)) == name:
            if not prop.iri.startswith(_INTERNAL_IRI_PREFIX):
                return prop.iri
            break
    return base.rstrip("/") + "/property/" + quote(name, safe="/:@._~-")


def _require_columns(row: Mapping[str, Any], columns: tuple[str, ...], label: str) -> None:
    missing = [column for column in columns if column not in row]
    if missing:
        raise CaracalError(
            code="CDB-7011",
            message=f"{label} row is missing required column(s): {', '.join(missing)}",
        )


def _coerce_local_name(value: object, label: str) -> str:
    if value is None:
        raise CaracalError(code="CDB-7010", message=f"{label} must not be null")
    name = _local(str(value))
    if not name:
        raise CaracalError(code="CDB-7010", message=f"{label} must not be empty")
    return name


def _validate_import_policy(policy: str) -> None:
    if policy not in {"warn", "strict"}:
        raise CaracalError(
            code="CDB-7010",
            message=f"unsupported import policy: {policy!r}",
            hint="supported policies are 'warn' and 'strict'",
        )


def _resource_shape(obj: Mapping[str, Any]) -> str:
    keys = set(obj)
    if {"id", "labels", "properties", "relationships"} <= keys:
        return "neo4j"
    if "@id" in obj or "iri" in obj:
        return "iri"
    if {"subject", "predicate", "object"} <= keys or {"s", "p", "o"} <= keys:
        return "triple"
    if {"node_id", "type"} <= keys:
        return "typed_node"
    if {"src", "dst", "type"} <= keys:
        return "typed_edge"
    return "unknown"


def _unsupported_resource_shape() -> CaracalError:
    return CaracalError(
        code="CDB-7010",
        message="unsupported resource shape",
        hint=(
            "expected Neo4j keys id/labels/properties/relationships, "
            "IRI keys @id or iri, triple keys subject/predicate/object or s/p/o, "
            "typed node keys node_id/type, or typed edge keys src/dst/type"
        ),
    )


def _canonical_triple(obj: Mapping[str, Any]) -> dict[str, Any]:
    if {"subject", "predicate", "object"} <= set(obj):
        return {"subject": obj["subject"], "predicate": obj["predicate"], "object": obj["object"]}
    return {"subject": obj["s"], "predicate": obj["p"], "object": obj["o"]}


def _resource_id(value: Any) -> Any:
    if isinstance(value, Mapping):
        if "@id" in value:
            return _strip_iri_ref(value["@id"])
        if "iri" in value:
            return _strip_iri_ref(value["iri"])
        if "id" in value:
            return value["id"]
        if "node_id" in value:
            return value["node_id"]
    return _strip_iri_ref(value)


def _strip_iri_ref(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if text.startswith("<") and text.endswith(">"):
        return text[1:-1]
    return text


def _predicate_iri(value: Any) -> str | None:
    if isinstance(value, Mapping):
        raw = value.get("@id") or value.get("iri")
        return str(_strip_iri_ref(raw)) if raw is not None else None
    text = str(_strip_iri_ref(value))
    if "://" in text or text.startswith("urn:"):
        return text
    return None


def _predicate_name(value: Any) -> str:
    if isinstance(value, Mapping):
        raw = value.get("name") or value.get("local_name") or value.get("@id") or value.get("iri")
    else:
        raw = value
    return _coerce_local_name(_strip_iri_ref(raw), "triple predicate")


def _is_rdf_type(predicate: str) -> bool:
    return predicate in _RDF_TYPE_PREDICATES or _local(predicate) == "type"


def _resource_type(value: Any) -> str:
    if isinstance(value, Mapping):
        raw = (
            value.get("type")
            or value.get("label")
            or value.get("@id")
            or value.get("iri")
            or value.get("id")
        )
    else:
        raw = value
    return _coerce_local_name(_strip_iri_ref(raw), "rdf:type object")


def _is_literal_object(value: Any) -> bool:
    if isinstance(value, Mapping):
        return "value" in value or "literal" in value
    if isinstance(value, bool | int | float):
        return True
    if value is None:
        return True
    if not isinstance(value, str):
        return True
    text = value.strip()
    return not (
        text.startswith("<")
        or "://" in text
        or text.startswith("urn:")
        or "/" in text
        or (":" in text and " " not in text)
    )


def _literal_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        if "value" in value:
            return value["value"]
        return value.get("literal")
    return value


def _placeholder_node(external_id: Any) -> dict[str, Any]:
    return {
        "node_id": external_id,
        "type": _type_from_external_id(external_id),
        _PLACEHOLDER_COLUMN: True,
    }


def _relationship_targets(value: Any) -> list[Any]:
    if isinstance(value, list | tuple):
        return list(value)
    return [value]


def _type_from_external_id(external_id: Any) -> str:
    text = str(external_id)
    prefix = text.split("/", 1)[0] if "/" in text else "Resource"
    if not prefix:
        prefix = "Resource"
    return _coerce_local_name(prefix[:1].upper() + prefix[1:], "resource type")


def _external_id_map(db: Database, *, key_col: str) -> dict[Any, int]:
    result: dict[Any, int] = {}
    for class_name in list_node_stores(db.bundle):
        cls = db._find_class(class_name)
        store = open_node_store(
            db.bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
        )
        table = store.to_table()
        if key_col not in table.column_names or _INTERNAL_GID_COLUMN not in table.column_names:
            continue
        keys = table[key_col].to_pylist()
        gids = table[_INTERNAL_GID_COLUMN].to_pylist()
        for key, gid in zip(keys, gids, strict=True):
            result[key] = int(gid)
    return result


def _require_table_columns(table: pa.Table, columns: tuple[str, ...], what: str) -> None:
    missing = [name for name in columns if name not in table.column_names]
    if missing:
        raise CaracalError(
            code="CDB-7011" if what == "node table" else "CDB-7021",
            message=f"{what} is missing required column(s): {', '.join(missing)}",
        )


def _resolve_external_node_id(id_map: Mapping[Any, int], value: Any, column: str) -> int:
    if value not in id_map:
        raise CaracalError(
            code="CDB-7021",
            message=f"edge {column!r} references unknown node_id: {value!r}",
            hint="insert the node table before inserting edges",
        )
    return int(id_map[value])


def _resolve_external_node_array(
    id_map: Mapping[Any, int],
    values: pa.Array | pa.ChunkedArray,
    column: str,
) -> pa.Array:
    unique_values = values.unique().to_pylist()
    missing = [value for value in unique_values if value not in id_map]
    if missing:
        value = missing[0]
        raise CaracalError(
            code="CDB-7021",
            message=f"edge {column!r} references unknown node_id: {value!r}",
            hint="insert the node table before inserting edges",
        )
    lookup_gids = pa.array([id_map[value] for value in unique_values], type=pa.uint64())
    indices = pa.compute.index_in(values, value_set=pa.array(unique_values))
    return pa.compute.take(lookup_gids, indices)


def _edge_table(rows: list[dict[str, Any]]) -> pa.Table:
    table = pa.Table.from_pylist(rows)
    for name in ("src", "dst"):
        index = table.column_names.index(name)
        table = table.set_column(index, name, table[name].cast(pa.uint64()))
    return table


def _find_resource_row(
    db: Database, external_id: Any, *, key_col: str
) -> tuple[str, dict[str, Any]] | None:
    for class_name in list_node_stores(db.bundle):
        cls = db._find_class(class_name)
        store = open_node_store(
            db.bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
        )
        table = store.to_table()
        if key_col not in table.column_names or _INTERNAL_GID_COLUMN not in table.column_names:
            continue
        for row in table.to_pylist():
            if row.get(key_col) == external_id:
                return class_name, row
    return None


def _external_id_for_gid(db: Database, gid: int) -> Any | None:
    for class_name in list_node_stores(db.bundle):
        cls = db._find_class(class_name)
        store = open_node_store(
            db.bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
        )
        table = store.to_table()
        if "node_id" not in table.column_names or _INTERNAL_GID_COLUMN not in table.column_names:
            continue
        for row in table.to_pylist():
            if int(row[_INTERNAL_GID_COLUMN]) == gid:
                return row["node_id"]
    return None


def _edges_for_gid(db: Database, gid: int) -> Iterator[tuple[str, dict[str, Any]]]:
    for relation in list_edge_stores(db.bundle):
        prop = _find_property_by_local_name(db, relation)
        if prop is None:
            continue
        store = open_edge_store(
            db.bundle,
            property_iri=prop.iri,
            local_name=prop.local_name or _local(prop.iri),
        )
        for row in store.to_table().to_pylist():
            if int(row["src"]) == gid:
                yield relation, row


def _find_property_by_local_name(db: Database, name: str) -> Any | None:
    for prop in db.catalog.properties:
        if (prop.local_name or _local(prop.iri)) == name:
            return prop
    return None


def _format_iri(value: str) -> str:
    return f"<{value}>"


def _literal_turtle(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, list):
        return '"' + _escape_turtle_literal(", ".join(str(item) for item in value)) + '"'
    return '"' + _escape_turtle_literal(str(value)) + '"'


def _escape_turtle_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


__all__ = ["Connection", "Database", "NodeQuery", "ResourceRef", "Result", "connect"]
