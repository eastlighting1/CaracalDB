"""Public API: ``caracaldb.connect`` → ``Connection.sql(...).arrow()``.

This is the MVP wiring for the M1 vertical slice. It supports the single-class
``MATCH (alias:Class) [WHERE expr] RETURN alias.field[, ...] [LIMIT k]`` shape
end-to-end: Tuft text → AST → binder → logical plan → physical plan → Arrow
Table. Anything outside that shape raises ``CDB-6020`` with a clear message
so users see immediately that it's an M1 limitation rather than a silent
mistranslation.
"""

from __future__ import annotations

import ast as py_ast
import json
import os
import re
import shutil
import tempfile
import time
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
from caracaldb.graph.hnsw import HnswConfig, HnswIndex
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import bind_program, parse_tuft
from caracaldb.observability.profile import profile_pipeline
from caracaldb.onto.catalog import Catalog, ClassDef, load_catalog, save_catalog
from caracaldb.onto.closure import ClassClosureIndex
from caracaldb.storage import Bundle, create_bundle, open_bundle
from caracaldb.storage.edge_store import list_edge_stores, open_edge_store
from caracaldb.storage.manifest import MANIFEST_NAME, utc_now_iso
from caracaldb.storage.mvcc import SnapshotId
from caracaldb.storage.node_store import NodeStore, list_node_stores, open_node_store
from caracaldb.storage.pack import is_packed, pack_bundle
from caracaldb.storage.snapshot import (
    SnapshotEntry,
    create_snapshot,
    list_snapshots,
    release_snapshot,
)
from caracaldb.vector import (
    cosine_distance,
    cosine_similarity,
    dot_product,
    l2_distance,
    score_from_distance,
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
_VECTOR_INDEX_MANIFEST = "indexes.json"
_PROPERTY_INDEX_MANIFEST = "property_indexes.json"
_INDEX_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.\-]*$")


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

    Examples
    --------
    ```python
    import tempfile
    from pathlib import Path

    import caracaldb as cdb

    root = tempfile.TemporaryDirectory()
    with cdb.connect(Path(root.name) / "demo") as db:
        db.insert_node_table(
            [{"node_id": "person/tom", "type": "Person", "name": "Tom Hanks"}]
        )
        db.nodes("Person").where(name="Tom Hanks").select("node_id").rows()
    # [{'node_id': 'person/tom'}]
    root.cleanup()
    ```
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
        stripped = text.strip()
        if stripped.upper().startswith("CALL VECTOR.SEARCH"):
            return _execute_vector_search_call(self._db, stripped)
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
            if _has_variable_length_pattern(query):
                return _execute_variable_length_pattern_query(self._db, query)
            plan_p = _compile_pattern_query(query, self._db)
            op = _build_pattern_pipeline(plan_p, self._db)
            ctx = apply_as_of(ExecCtx(), plan_p.snapshot)
            batches = list(run_pipeline(op, ctx))
            batches = _apply_modifiers(batches, query.modifiers)
            return Result(batches)
        plan = _compile_query(query, self._db)
        op = _build_pipeline(plan, self._db)
        ctx = apply_as_of(ExecCtx(), plan.snapshot)
        batches = list(run_pipeline(op, ctx))
        batches = _apply_modifiers(batches, query.modifiers)
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

    def upsert_node_table_arrow(
        self,
        table: pa.Table,
        *,
        key_col: str = "node_id",
        type_col: str = "type",
        update_existing: bool = True,
    ) -> dict[str, int]:
        """Idempotently insert or update typed node rows from an Arrow table."""

        return _upsert_node_table_arrow(
            self,
            table,
            key_col=key_col,
            type_col=type_col,
            update_existing=update_existing,
        )

    def upsert_edge_table_arrow(
        self,
        table: pa.Table,
        *,
        edge_key_col: str = "edge_id",
        src_col: str = "src",
        dst_col: str = "dst",
        type_col: str = "type",
        node_key_col: str = "node_id",
        update_existing: bool = True,
    ) -> dict[str, int]:
        """Idempotently insert or update typed edge rows from an Arrow table."""

        return _upsert_edge_table_arrow(
            self,
            table,
            edge_key_col=edge_key_col,
            src_col=src_col,
            dst_col=dst_col,
            type_col=type_col,
            node_key_col=node_key_col,
            update_existing=update_existing,
        )

    def create_vector_index(
        self,
        *,
        name: str,
        node_type: str,
        property: str,
        dimension: int,
        metric: str = "cosine",
        algorithm: str = "hnsw",
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create and persist a vector index over a node vector property."""

        return _create_vector_index(
            self,
            name=name,
            node_type=node_type,
            property_name=property,
            dimension=dimension,
            metric=metric,
            algorithm=algorithm,
            options=dict(options or {}),
        )

    def list_vector_indexes(self) -> list[dict[str, Any]]:
        """Return persisted vector-index metadata ordered by index name."""

        return _list_vector_indexes(self)

    def drop_vector_index(self, name: str) -> bool:
        """Drop vector-index metadata and index files without removing vectors."""

        return _drop_vector_index(self, name)

    def rebuild_vector_index(self, name: str) -> dict[str, Any]:
        """Rebuild a persisted vector index from source node vectors."""

        return _rebuild_vector_index(self, name)

    def vector_search(
        self,
        *,
        index: str,
        query_vector: Iterable[float],
        top_k: int,
        filters: Mapping[str, Any] | None = None,
        return_properties: Iterable[str] | None = None,
    ) -> Result:
        """Search a vector index and return graph-addressable node results."""

        return _vector_search(
            self,
            index=index,
            query_vector=query_vector,
            top_k=top_k,
            filters=dict(filters or {}),
            return_properties=tuple(return_properties or ()),
        )

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

    def neighbors(
        self,
        *,
        seed_node_ids: Iterable[Any],
        edge_types: Iterable[str],
        direction: str = "out",
        depth: int = 1,
        limit: int | None = None,
        node_type_filters: Iterable[str] | None = None,
        edge_filters: Mapping[str, Any] | None = None,
        return_paths: bool = False,
        node_key_col: str = "node_id",
        weight_property: str | None = None,
        top_edges_per_node: int | None = None,
        path_score: str | None = None,
        path_score_property: str = "weight",
        order_by_path_score: str | None = None,
    ) -> Result:
        """Traverse typed relations from seed nodes and return reached nodes."""

        return _neighbors(
            self,
            seed_node_ids=tuple(seed_node_ids),
            edge_types=tuple(edge_types),
            direction=direction,
            depth=depth,
            limit=limit,
            node_type_filters=tuple(node_type_filters or ()),
            edge_filters=dict(edge_filters or {}),
            return_paths=return_paths,
            node_key_col=node_key_col,
            weight_property=weight_property,
            top_edges_per_node=top_edges_per_node,
            path_score=path_score,
            path_score_property=path_score_property,
            order_by_path_score=order_by_path_score,
        )

    def k_hop(
        self,
        *,
        seeds: Iterable[Any],
        depth: int,
        edge_types: Iterable[str],
        direction: str = "out",
        max_nodes: int = 500,
        max_edges: int = 2000,
        node_key_col: str = "node_id",
    ) -> dict[str, pa.Table]:
        """Return a bounded k-hop subgraph as Arrow node and edge tables."""

        return _k_hop(
            self,
            seeds=tuple(seeds),
            depth=depth,
            edge_types=tuple(edge_types),
            direction=direction,
            max_nodes=max_nodes,
            max_edges=max_edges,
            node_key_col=node_key_col,
        )

    def paths(
        self,
        *,
        source: Any,
        target: Any,
        edge_types: Iterable[str],
        max_depth: int,
        limit: int = 20,
        direction: str = "out",
        edge_filters: Mapping[str, Any] | None = None,
        node_key_col: str = "node_id",
        score: str | None = None,
        score_property: str = "weight",
        order: str = "desc",
    ) -> Result:
        """Return deterministic bounded paths between two nodes."""

        return _paths(
            self,
            source=source,
            target=target,
            edge_types=tuple(edge_types),
            max_depth=max_depth,
            limit=limit,
            direction=direction,
            edge_filters=dict(edge_filters or {}),
            node_key_col=node_key_col,
            score=score,
            score_property=score_property,
            order=order,
        )

    def shortest_path(
        self,
        *,
        source: Any,
        target: Any,
        edge_types: Iterable[str],
        max_depth: int | None = None,
        direction: str = "out",
        edge_filters: Mapping[str, Any] | None = None,
        node_key_col: str = "node_id",
    ) -> dict[str, Any] | None:
        """Return one deterministic shortest path, or ``None`` if unreachable."""

        return _shortest_path(
            self,
            source=source,
            target=target,
            edge_types=tuple(edge_types),
            max_depth=max_depth,
            direction=direction,
            edge_filters=dict(edge_filters or {}),
            node_key_col=node_key_col,
        )

    def create_property_index(
        self,
        *,
        name: str,
        node_type: str | None = None,
        property: str,
        edge_type: str | None = None,
    ) -> dict[str, Any]:
        """Persist metadata for a property lookup index."""

        return _create_property_index(
            self,
            name=name,
            node_type=node_type,
            property_name=property,
            edge_type=edge_type,
        )

    def list_property_indexes(self) -> list[dict[str, Any]]:
        """Return persisted property-index metadata ordered by name."""

        return _list_property_indexes(self)

    def capabilities(self) -> dict[str, Any]:
        """Return feature flags without running a query."""

        from caracaldb._version import __version__

        return {
            "version": __version__,
            "vector_property": True,
            "vector_index.hnsw": True,
            "vector_search": True,
            "vector_distance_functions": True,
            "traversal.neighbors": True,
            "traversal.k_hop": True,
            "traversal.paths": True,
            "traversal.shortest_path": True,
            "traversal.weighted_edges": True,
            "tuft.vector_search": True,
            "tuft.variable_length_paths": True,
            "explain": True,
            "profile": True,
            "batch_upsert": True,
            "property_index": True,
        }

    def explain(self, text: str) -> dict[str, Any]:
        """Return a machine-readable explain skeleton for a Tuft query."""

        return _explain_query(self, text)

    def profile(self, text: str) -> dict[str, Any]:
        """Profile a Tuft query and return machine-readable telemetry."""

        return _profile_query(self, text)

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


def _execute_vector_search_call(db: Database, text: str) -> Result:
    spec = _parse_vector_search_call(text)
    result = db.vector_search(
        index=spec["index"],
        query_vector=spec["query_vector"],
        top_k=spec["top_k"],
        filters=spec["filters"],
        return_properties=spec["return_properties"],
    )
    table = result.arrow()
    columns = spec["return_columns"] or spec["yield_columns"]
    if columns:
        missing = [name for name in columns if name not in table.column_names]
        if missing:
            raise CaracalError(
                code="CDB-6020",
                message=f"CALL vector.search requested unknown column: {missing[0]!r}",
            )
        table = table.select(columns)
    order_by = spec["order_by"]
    if order_by is not None and table.num_rows:
        name, descending = order_by
        if name not in table.column_names:
            raise CaracalError(
                code="CDB-6020",
                message=f"ORDER BY column is not projected by vector.search: {name!r}",
            )
        rows = table.to_pylist()
        rows.sort(key=lambda row: row.get(name), reverse=descending)
        table = pa.Table.from_pylist(rows, schema=table.schema)
    limit = spec["limit"]
    if limit is not None:
        table = table.slice(0, limit)
    return Result(_table_to_batches(table))


def _profile_vector_search_call(db: Database, text: str) -> dict[str, Any]:
    start = time.perf_counter()
    spec = _parse_vector_search_call(text)
    result = _execute_vector_search_call(db, text)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    count = result.arrow().num_rows
    return {
        "logical_plan": "vector_search_call",
        "physical_plan": "VectorSearch",
        "indexes_used": [spec["index"]],
        "vector_index_used": spec["index"],
        "node_rows_scanned": 0,
        "edge_rows_scanned": 0,
        "candidate_count": count,
        "result_count": count,
        "elapsed_ms": elapsed_ms,
        "operator_timings": [
            {
                "name": "VectorSearch",
                "rows": count,
                "batches": len(list(result.record_batches())),
                "elapsed_ms": elapsed_ms,
                "peak_bytes": 0,
            }
        ],
        "fallback_flags": [],
    }


def _explain_vector_search_call(db: Database, text: str) -> dict[str, Any]:
    spec = _parse_vector_search_call(text)
    return {
        "logical_plan": "vector_search_call",
        "physical_plan": "VectorSearch",
        "indexes_used": [spec["index"]],
        "vector_index_used": spec["index"],
        "limit": spec["limit"],
        "fallback_flags": [],
    }


def _parse_vector_search_call(text: str) -> dict[str, Any]:
    pattern = re.compile(
        r"""
        ^CALL\s+vector\.search\s*
        \((?P<args>.*?)\)
        (?:\s+YIELD\s+(?P<yield>.*?))?
        (?:\s+RETURN\s+(?P<return>.*?))?
        (?:\s+ORDER\s+BY\s+(?P<order>[A-Za-z_][A-Za-z0-9_]*)(?:\s+(?P<dir>ASC|DESC))?)?
        (?:\s+LIMIT\s+(?P<limit>\d+))?
        \s*;?\s*$
        """,
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    )
    match = pattern.match(text)
    if match is None:
        raise CaracalError(
            code="CDB-6020",
            message=(
                "CALL vector.search supports: "
                "CALL vector.search('index', [vector], k) YIELD ... RETURN ... LIMIT n"
            ),
        )
    args = _split_call_args(match.group("args"))
    if len(args) < 3 or len(args) > 5:
        raise CaracalError(
            code="CDB-6020",
            message="vector.search takes index, query_vector, top_k, optional filters, properties",
        )
    index = _literal_arg(args[0], "vector index name")
    query_vector = _literal_arg(args[1], "query vector")
    top_k = _literal_arg(args[2], "top_k")
    filters = _literal_arg(args[3], "filters") if len(args) >= 4 else {}
    return_properties = _literal_arg(args[4], "return_properties") if len(args) >= 5 else []
    if not isinstance(index, str):
        raise CaracalError(code="CDB-6020", message="vector.search index must be a string literal")
    if not isinstance(query_vector, list):
        raise CaracalError(code="CDB-6020", message="vector.search query vector must be a list")
    if not isinstance(top_k, int):
        raise CaracalError(code="CDB-6020", message="vector.search top_k must be an integer")
    if not isinstance(filters, dict):
        raise CaracalError(code="CDB-6020", message="vector.search filters must be a map/dict")
    if not isinstance(return_properties, list):
        raise CaracalError(
            code="CDB-6020",
            message="vector.search return_properties must be a list of strings",
        )
    yield_columns = _parse_column_list(match.group("yield"))
    return_columns = _parse_column_list(match.group("return"))
    order_name = match.group("order")
    order_by = None
    if order_name is not None:
        order_by = (order_name, (match.group("dir") or "ASC").upper() == "DESC")
    limit = int(match.group("limit")) if match.group("limit") is not None else None
    return {
        "index": index,
        "query_vector": query_vector,
        "top_k": top_k,
        "filters": filters,
        "return_properties": tuple(str(item) for item in return_properties),
        "yield_columns": yield_columns,
        "return_columns": return_columns,
        "order_by": order_by,
        "limit": limit,
    }


def _split_call_args(text: str) -> list[str]:
    args: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    escape = False
    for index, char in enumerate(text):
        if quote is not None:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
        elif char in "[{(":
            depth += 1
        elif char in "]})":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(text[start:index].strip())
            start = index + 1
    tail = text[start:].strip()
    if tail:
        args.append(tail)
    return args


def _literal_arg(text: str, label: str) -> Any:
    try:
        return py_ast.literal_eval(text)
    except (SyntaxError, ValueError) as exc:
        raise CaracalError(code="CDB-6020", message=f"invalid {label}: {text!r}") from exc


def _parse_column_list(text: str | None) -> list[str]:
    if text is None:
        return []
    cleaned = text.strip()
    if not cleaned:
        return []
    return [part.strip() for part in cleaned.split(",") if part.strip()]


def _upsert_node_table_arrow(
    db: Database,
    table: pa.Table,
    *,
    key_col: str,
    type_col: str,
    update_existing: bool,
) -> dict[str, int]:
    if table.num_rows == 0:
        raise CaracalError(code="CDB-7011", message="cannot upsert an empty node table")
    _require_table_columns(table, (key_col, type_col), "node table")
    if _INTERNAL_GID_COLUMN in table.column_names:
        raise CaracalError(
            code="CDB-7011",
            message=f"node upsert table must not include reserved column {_INTERNAL_GID_COLUMN!r}",
        )

    existing: dict[Any, dict[str, Any]] = {}
    touched_types: set[str] = set()
    max_gid = -1
    for class_name in list_node_stores(db.bundle):
        cls = db._find_class(class_name)
        store = open_node_store(
            db.bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
        )
        current = store.to_table()
        if key_col not in current.column_names or type_col not in current.column_names:
            continue
        for row in current.to_pylist():
            if row.get(key_col) is None:
                continue
            clean = _strip_row_columns(row, {"nid", "_created_lsn", "_deleted_lsn"})
            if _INTERNAL_GID_COLUMN in clean:
                max_gid = max(max_gid, int(clean[_INTERNAL_GID_COLUMN]))
            existing[clean[key_col]] = clean
            touched_types.add(_coerce_local_name(clean[type_col], "node type"))

    final = dict(existing)
    inserted = updated = skipped = 0
    next_gid = max_gid + 1
    for raw in table.to_pylist():
        _require_columns(raw, (key_col, type_col), "node table")
        key = raw[key_col]
        class_name = _coerce_local_name(raw[type_col], "node type")
        touched_types.add(class_name)
        if key in existing:
            if not update_existing:
                skipped += 1
                continue
            merged = {**existing[key], **raw}
            merged[_INTERNAL_GID_COLUMN] = existing[key].get(_INTERNAL_GID_COLUMN, next_gid)
            if _INTERNAL_GID_COLUMN not in existing[key]:
                next_gid += 1
            final[key] = merged
            updated += 1
        else:
            out = dict(raw)
            out[_INTERNAL_GID_COLUMN] = next_gid
            next_gid += 1
            final[key] = out
            existing[key] = out
            inserted += 1

    rows_by_type: dict[str, list[dict[str, Any]]] = {name: [] for name in touched_types}
    for row in final.values():
        if type_col not in row:
            continue
        rows_by_type.setdefault(_coerce_local_name(row[type_col], "node type"), []).append(row)

    for class_name in sorted(rows_by_type):
        db.define_class(class_name)
        _replace_node_store_rows(db, class_name, rows_by_type[class_name])
    db._invalidate_graph_indexes()
    return {"inserted": inserted, "updated": updated, "skipped": skipped, "failed": 0}


def _upsert_edge_table_arrow(
    db: Database,
    table: pa.Table,
    *,
    edge_key_col: str,
    src_col: str,
    dst_col: str,
    type_col: str,
    node_key_col: str,
    update_existing: bool,
) -> dict[str, int]:
    if table.num_rows == 0:
        raise CaracalError(code="CDB-7021", message="cannot upsert an empty edge table")
    _require_table_columns(table, (edge_key_col, src_col, dst_col, type_col), "edge table")
    if "eid" in table.column_names:
        raise CaracalError(
            code="CDB-7021",
            message="edge upsert table must not include an 'eid' column; it is assigned",
        )

    existing: dict[Any, dict[str, Any]] = {}
    touched_types: set[str] = set()
    for relation in list_edge_stores(db.bundle):
        prop = _find_property_by_local_name(db, relation)
        if prop is None:
            continue
        store = open_edge_store(
            db.bundle,
            property_iri=prop.iri,
            local_name=prop.local_name or _local(prop.iri),
        )
        current = store.to_table()
        if edge_key_col not in current.column_names:
            continue
        for row in current.to_pylist():
            if row.get(edge_key_col) is None:
                continue
            clean = _strip_row_columns(row, {"eid", "_created_lsn", "_deleted_lsn"})
            existing[clean[edge_key_col]] = clean
            touched_types.add(_coerce_local_name(clean.get(type_col, relation), "edge type"))

    id_map = _external_id_map(db, key_col=node_key_col)
    final = dict(existing)
    inserted = updated = skipped = 0
    for raw in table.to_pylist():
        _require_columns(raw, (edge_key_col, src_col, dst_col, type_col), "edge table")
        key = raw[edge_key_col]
        relation = _coerce_local_name(raw[type_col], "edge type")
        touched_types.add(relation)
        resolved = dict(raw)
        resolved["src"] = _resolve_external_node_id(id_map, raw[src_col], src_col)
        resolved["dst"] = _resolve_external_node_id(id_map, raw[dst_col], dst_col)
        resolved[type_col] = raw[type_col]
        if key in existing:
            if not update_existing:
                skipped += 1
                continue
            final[key] = {**existing[key], **resolved}
            updated += 1
        else:
            final[key] = resolved
            existing[key] = resolved
            inserted += 1

    rows_by_type: dict[str, list[dict[str, Any]]] = {name: [] for name in touched_types}
    for row in final.values():
        relation = _coerce_local_name(row.get(type_col), "edge type")
        rows_by_type.setdefault(relation, []).append(row)

    for relation in sorted(rows_by_type):
        db._define_property(relation)
        _replace_edge_store_rows(db, relation, rows_by_type[relation])
        db._invalidate_graph_indexes(relation)
    return {"inserted": inserted, "updated": updated, "skipped": skipped, "failed": 0}


def _strip_row_columns(row: Mapping[str, Any], names: set[str]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in names}


def _replace_node_store_rows(db: Database, class_name: str, rows: list[dict[str, Any]]) -> None:
    cls = db._find_class(class_name)
    local_name = cls.local_name or _local(cls.iri)
    root = db.bundle.child("nodes", local_name)
    if root.exists():
        shutil.rmtree(root)
    store = open_node_store(
        db.bundle,
        class_iri=cls.iri,
        local_name=local_name,
        create=True,
    )
    if not rows:
        return
    payload = [_strip_row_columns(row, {"nid", "_created_lsn", "_deleted_lsn"}) for row in rows]
    store.append(pa.Table.from_pylist(payload), created_lsn=db._next_lsn())


def _replace_edge_store_rows(db: Database, relation: str, rows: list[dict[str, Any]]) -> None:
    prop = db._find_property(relation)
    local_name = prop.local_name or _local(prop.iri)
    root = db.bundle.child("edges", local_name)
    if root.exists():
        shutil.rmtree(root)
    store = open_edge_store(
        db.bundle,
        property_iri=prop.iri,
        local_name=local_name,
        create=True,
    )
    if not rows:
        return
    payload = [_strip_row_columns(row, {"eid", "_created_lsn", "_deleted_lsn"}) for row in rows]
    store.append(_edge_table(payload), created_lsn=db._next_lsn())


def _create_vector_index(
    db: Database,
    *,
    name: str,
    node_type: str,
    property_name: str,
    dimension: int,
    metric: str,
    algorithm: str,
    options: dict[str, Any],
) -> dict[str, Any]:
    _assert_index_name(name, "vector index")
    if dimension <= 0:
        raise CaracalError(code="CDB-7091", message="vector index dimension must be positive")
    metric = _normalize_vector_metric(metric)
    algorithm = algorithm.lower()
    if algorithm not in {"hnsw", "exact"}:
        raise CaracalError(
            code="CDB-7090",
            message=f"unsupported vector index algorithm: {algorithm!r}",
            hint="supported algorithms are 'hnsw' and 'exact'",
        )
    cls = db._find_class(node_type)
    node_local = cls.local_name or _local(cls.iri)
    metadata = _load_vector_index_manifest(db)
    if name in metadata:
        existing = metadata[name]
        if _same_vector_definition(
            existing,
            node_type=node_local,
            property_name=property_name,
            dimension=dimension,
            metric=metric,
            algorithm=algorithm,
            options=options,
        ):
            return dict(existing)
        raise CaracalError(
            code="CDB-7090",
            message=f"vector index already exists with a different definition: {name!r}",
        )
    table = _node_table_for_local(db, node_local)
    id_column = _vector_id_column(table)
    meta = {
        "name": name,
        "node_type": node_local,
        "property": property_name,
        "dimension": dimension,
        "metric": metric,
        "algorithm": algorithm,
        "options": dict(sorted(options.items())),
        "status": "building",
        "id_column": id_column,
        "count": 0,
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }
    metadata[name] = meta
    _write_vector_index_manifest(db, metadata)
    return _build_vector_index(db, meta)


def _same_vector_definition(
    meta: Mapping[str, Any],
    *,
    node_type: str,
    property_name: str,
    dimension: int,
    metric: str,
    algorithm: str,
    options: Mapping[str, Any],
) -> bool:
    return (
        meta.get("node_type") == node_type
        and meta.get("property") == property_name
        and int(meta.get("dimension", -1)) == dimension
        and meta.get("metric") == metric
        and meta.get("algorithm") == algorithm
        and dict(meta.get("options", {})) == dict(sorted(options.items()))
    )


def _list_vector_indexes(db: Database) -> list[dict[str, Any]]:
    metadata = _load_vector_index_manifest(db)
    return [dict(metadata[name]) for name in sorted(metadata)]


def _drop_vector_index(db: Database, name: str) -> bool:
    metadata = _load_vector_index_manifest(db)
    if name not in metadata:
        return False
    meta = metadata.pop(name)
    index_file = meta.get("index_file")
    if index_file:
        db.bundle.child(str(index_file)).unlink(missing_ok=True)
    _write_vector_index_manifest(db, metadata)
    return True


def _rebuild_vector_index(db: Database, name: str) -> dict[str, Any]:
    metadata = _load_vector_index_manifest(db)
    if name not in metadata:
        raise CaracalError(code="CDB-7092", message=f"vector index not found: {name!r}")
    meta = dict(metadata[name])
    meta["status"] = "building"
    meta["updated_at"] = utc_now_iso()
    metadata[name] = meta
    _write_vector_index_manifest(db, metadata)
    return _build_vector_index(db, meta)


def _build_vector_index(db: Database, meta: dict[str, Any]) -> dict[str, Any]:
    entries, _table = _vector_entries(db, meta)
    meta = dict(meta)
    meta["count"] = len(entries)
    if meta["algorithm"] == "hnsw":
        index_path = _vector_index_file(db, meta["name"])
        if entries:
            import numpy as np

            config = _hnsw_config(meta, max_elements=max(1, len(entries)))
            index = HnswIndex(config)
            vectors = np.vstack([entry["vector"] for entry in entries]).astype(np.float32)
            ids = np.asarray([entry["internal_id"] for entry in entries], dtype=np.uint64)
            index.add(ids, vectors)
            index.save(index_path)
        else:
            index_path.unlink(missing_ok=True)
        meta["index_file"] = str(index_path.relative_to(db.bundle.path)).replace("\\", "/")
    else:
        old_file = meta.get("index_file")
        if old_file:
            db.bundle.child(str(old_file)).unlink(missing_ok=True)
        meta["index_file"] = None
    meta["status"] = "ready"
    meta["updated_at"] = utc_now_iso()
    metadata = _load_vector_index_manifest(db)
    metadata[meta["name"]] = meta
    _write_vector_index_manifest(db, metadata)
    return dict(meta)


def _vector_search(
    db: Database,
    *,
    index: str,
    query_vector: Iterable[float],
    top_k: int,
    filters: dict[str, Any],
    return_properties: tuple[str, ...],
) -> Result:
    if top_k < 0:
        raise CaracalError(code="CDB-6090", message="top_k must be >= 0")
    metadata = _load_vector_index_manifest(db)
    if index not in metadata:
        raise CaracalError(code="CDB-7092", message=f"vector index not found: {index!r}")
    meta = metadata[index]
    import numpy as np

    query = np.asarray(list(query_vector), dtype=np.float32)
    if query.ndim != 1 or int(query.shape[0]) != int(meta["dimension"]):
        got = int(query.shape[0]) if query.ndim == 1 else tuple(query.shape)
        raise CaracalError(
            code="CDB-7091",
            message=(
                f"query dimension mismatch for {index!r}: "
                f"expected {meta['dimension']}, got {got}"
            ),
        )
    entries, table = _vector_entries(db, meta)
    _validate_vector_filters_and_properties(table, filters, return_properties)
    if top_k == 0 or not entries:
        return Result(_table_to_batches(_vector_result_table(meta, table, [], return_properties)))

    if filters or meta["algorithm"] == "exact":
        candidates = [entry for entry in entries if _row_matches_filters(entry["row"], filters)]
        rows = _rank_exact_vector_candidates(meta, query, candidates, top_k, return_properties)
        return Result(_table_to_batches(_vector_result_table(meta, table, rows, return_properties)))

    index_file = meta.get("index_file")
    if not index_file:
        rows = _rank_exact_vector_candidates(meta, query, entries, top_k, return_properties)
        return Result(_table_to_batches(_vector_result_table(meta, table, rows, return_properties)))
    hnsw = HnswIndex.load(db.bundle.child(str(index_file)), config=_hnsw_config(meta))
    labels, distances = hnsw.search(query, k=min(top_k, len(entries)), ef=_ef_search(meta))
    by_internal = {int(entry["internal_id"]): entry for entry in entries}
    rows = []
    for internal_id, distance in zip(labels[0].tolist(), distances[0].tolist(), strict=True):
        entry = by_internal.get(int(internal_id))
        if entry is None:
            continue
        score = score_from_distance(meta["metric"], float(distance), query, entry["vector"])
        rows.append(_vector_result_row(meta, entry, float(distance), score, return_properties))
    rows.sort(key=lambda row: (-float(row["score"]), int(row["internal_id"])))
    rows = rows[:top_k]
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return Result(_table_to_batches(_vector_result_table(meta, table, rows, return_properties)))


def _rank_exact_vector_candidates(
    meta: Mapping[str, Any],
    query: Any,
    entries: list[dict[str, Any]],
    top_k: int,
    return_properties: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows = []
    for entry in entries:
        metric = str(meta["metric"])
        if metric == "cosine":
            distance = cosine_distance(query, entry["vector"])
            score = 1.0 - distance
        elif metric == "l2":
            distance = l2_distance(query, entry["vector"])
            score = -distance
        elif metric in {"ip", "dot", "dot_product"}:
            score = dot_product(query, entry["vector"])
            distance = -score
        else:
            raise CaracalError(code="CDB-7090", message=f"unsupported vector metric: {metric!r}")
        rows.append(
            _vector_result_row(meta, entry, float(distance), float(score), return_properties)
        )
    rows.sort(key=lambda row: (-float(row["score"]), int(row["internal_id"])))
    rows = rows[:top_k]
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
    return rows


def _vector_result_row(
    meta: Mapping[str, Any],
    entry: Mapping[str, Any],
    distance: float,
    score: float,
    return_properties: tuple[str, ...],
) -> dict[str, Any]:
    row = entry["row"]
    return {
        "node_id": row.get("node_id", entry["internal_id"]),
        "node_type": meta["node_type"],
        "internal_id": int(entry["internal_id"]),
        "score": float(score),
        "distance": float(distance),
        "rank": 0,
        "matched_property": meta["property"],
        "selected_properties": {name: row.get(name) for name in return_properties},
    }


def _vector_result_table(
    meta: Mapping[str, Any],
    source: pa.Table,
    rows: list[dict[str, Any]],
    return_properties: tuple[str, ...],
) -> pa.Table:
    node_id_type = (
        source.schema.field("node_id").type if "node_id" in source.column_names else pa.uint64()
    )
    selected_type = pa.struct(
        [
            pa.field(
                name,
                source.schema.field(name).type if name in source.column_names else pa.null(),
            )
            for name in return_properties
        ]
    )
    schema = pa.schema(
        [
            pa.field("node_id", node_id_type),
            pa.field("node_type", pa.string()),
            pa.field("internal_id", pa.uint64()),
            pa.field("score", pa.float32()),
            pa.field("distance", pa.float32()),
            pa.field("rank", pa.uint64()),
            pa.field("matched_property", pa.string()),
            pa.field("selected_properties", selected_type),
        ]
    )
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return pa.table(
        [
            pa.array([row["node_id"] for row in rows], type=node_id_type),
            pa.array([row["node_type"] for row in rows], type=pa.string()),
            pa.array([row["internal_id"] for row in rows], type=pa.uint64()),
            pa.array([row["score"] for row in rows], type=pa.float32()),
            pa.array([row["distance"] for row in rows], type=pa.float32()),
            pa.array([row["rank"] for row in rows], type=pa.uint64()),
            pa.array([row["matched_property"] for row in rows], type=pa.string()),
            pa.array([row["selected_properties"] for row in rows], type=selected_type),
        ],
        schema=schema,
    )


def _vector_entries(
    db: Database,
    meta: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], pa.Table]:
    import numpy as np

    table = _node_table_for_local(db, str(meta["node_type"]))
    property_name = str(meta["property"])
    if property_name not in table.column_names:
        raise CaracalError(
            code="CDB-7091",
            message=f"missing vector property {property_name!r} on node type {meta['node_type']!r}",
        )
    id_column = str(meta.get("id_column") or _vector_id_column(table))
    if id_column not in table.column_names:
        raise CaracalError(
            code="CDB-7091",
            message=f"vector index id column missing from source table: {id_column!r}",
        )
    dimension = int(meta["dimension"])
    allow_null = bool(dict(meta.get("options", {})).get("allow_null_vectors", False))
    entries: list[dict[str, Any]] = []
    for row in table.to_pylist():
        raw_vector = row.get(property_name)
        if raw_vector is None:
            if allow_null:
                continue
            raise CaracalError(
                code="CDB-7091",
                message=(
                    f"null vector in {meta['node_type']!r}.{property_name}; "
                    "set allow_null_vectors to index sparse rows"
                ),
            )
        vector = np.asarray(raw_vector, dtype=np.float32)
        if vector.ndim != 1 or int(vector.shape[0]) != dimension:
            got = int(vector.shape[0]) if vector.ndim == 1 else tuple(vector.shape)
            raise CaracalError(
                code="CDB-7091",
                message=(
                    f"dimension mismatch in {meta['node_type']!r}.{property_name}: "
                    f"expected {dimension}, got {got}"
                ),
            )
        entries.append({"internal_id": int(row[id_column]), "row": row, "vector": vector})
    return entries, table


def _node_table_for_local(db: Database, class_name: str) -> pa.Table:
    cls = db._find_class(class_name)
    store = open_node_store(
        db.bundle,
        class_iri=cls.iri,
        local_name=cls.local_name or _local(cls.iri),
    )
    return store.to_table()


def _vector_id_column(table: pa.Table) -> str:
    return _INTERNAL_GID_COLUMN if _INTERNAL_GID_COLUMN in table.column_names else "nid"


def _validate_vector_filters_and_properties(
    table: pa.Table,
    filters: Mapping[str, Any],
    return_properties: tuple[str, ...],
) -> None:
    missing_filters = [name for name in filters if name not in table.column_names]
    if missing_filters:
        raise CaracalError(
            code="CDB-7091",
            message=f"vector search filter column missing: {missing_filters[0]!r}",
        )
    missing_props = [name for name in return_properties if name not in table.column_names]
    if missing_props:
        raise CaracalError(
            code="CDB-7091",
            message=f"vector search return property missing: {missing_props[0]!r}",
        )


def _row_matches_filters(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    return all(row.get(name) == value for name, value in filters.items())


def _normalize_vector_metric(metric: str) -> str:
    normalized = metric.lower()
    if normalized in {"cosine", "l2"}:
        return normalized
    if normalized in {"ip", "dot", "dot_product"}:
        return "dot_product"
    raise CaracalError(
        code="CDB-7090",
        message=f"unsupported vector metric: {metric!r}",
        hint="supported metrics are 'cosine', 'l2', and 'dot_product'",
    )


def _hnsw_metric(metric: str) -> str:
    return "ip" if metric in {"dot", "dot_product"} else metric


def _hnsw_config(meta: Mapping[str, Any], *, max_elements: int | None = None) -> HnswConfig:
    options = dict(meta.get("options", {}))
    count = max(1, int(meta.get("count", 1)))
    return HnswConfig(
        dim=int(meta["dimension"]),
        M=int(options.get("m", options.get("M", 16))),
        ef_construction=int(options.get("ef_construction", 200)),
        metric=_hnsw_metric(str(meta["metric"])),  # type: ignore[arg-type]
        max_elements=int(max_elements or options.get("max_elements", count)),
    )


def _ef_search(meta: Mapping[str, Any]) -> int | None:
    value = dict(meta.get("options", {})).get("ef_search")
    return None if value is None else int(value)


def _vector_index_file(db: Database, name: str) -> Path:
    return db.bundle.child("vec", f"{name}.hnsw")


def _load_vector_index_manifest(db: Database) -> dict[str, dict[str, Any]]:
    path = db.bundle.child("vec", _VECTOR_INDEX_MANIFEST)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CaracalError(
            code="CDB-7092",
            message=f"corrupt vector index metadata: {path}",
        ) from exc
    return {str(item["name"]): dict(item) for item in payload.get("indexes", [])}


def _write_vector_index_manifest(
    db: Database,
    metadata: Mapping[str, Mapping[str, Any]],
) -> None:
    root = db.bundle.child("vec")
    root.mkdir(parents=True, exist_ok=True)
    path = root / _VECTOR_INDEX_MANIFEST
    payload = {"indexes": [dict(metadata[name]) for name in sorted(metadata)]}
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _create_property_index(
    db: Database,
    *,
    name: str,
    node_type: str | None,
    property_name: str,
    edge_type: str | None,
) -> dict[str, Any]:
    _assert_index_name(name, "property index")
    if (node_type is None) == (edge_type is None):
        raise CaracalError(
            code="CDB-7093",
            message="property index requires exactly one of node_type or edge_type",
        )
    if node_type is not None:
        cls = db._find_class(node_type)
        owner = cls.local_name or _local(cls.iri)
        kind = "node"
        table = _node_table_for_local(db, owner)
    else:
        prop = db._find_property(edge_type or "")
        owner = prop.local_name or _local(prop.iri)
        kind = "edge"
        table = db.edge_table(owner)
    if property_name not in table.column_names:
        raise CaracalError(
            code="CDB-7093",
            message=f"property index source column missing: {property_name!r}",
        )
    metadata = _load_property_index_manifest(db)
    meta = {
        "name": name,
        "kind": kind,
        "node_type": owner if kind == "node" else None,
        "edge_type": owner if kind == "edge" else None,
        "property": property_name,
        "status": "ready",
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }
    if name in metadata:
        existing = metadata[name]
        comparable = {
            key: existing.get(key) for key in ("kind", "node_type", "edge_type", "property")
        }
        wanted = {key: meta.get(key) for key in ("kind", "node_type", "edge_type", "property")}
        if comparable != wanted:
            raise CaracalError(
                code="CDB-7093",
                message=f"property index already exists with a different definition: {name!r}",
            )
        return dict(existing)
    metadata[name] = meta
    _write_property_index_manifest(db, metadata)
    return dict(meta)


def _list_property_indexes(db: Database) -> list[dict[str, Any]]:
    metadata = _load_property_index_manifest(db)
    return [dict(metadata[name]) for name in sorted(metadata)]


def _load_property_index_manifest(db: Database) -> dict[str, dict[str, Any]]:
    path = db.bundle.child("indexes", _PROPERTY_INDEX_MANIFEST)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CaracalError(
            code="CDB-7093",
            message=f"corrupt property index metadata: {path}",
        ) from exc
    return {str(item["name"]): dict(item) for item in payload.get("indexes", [])}


def _write_property_index_manifest(
    db: Database,
    metadata: Mapping[str, Mapping[str, Any]],
) -> None:
    root = db.bundle.child("indexes")
    root.mkdir(parents=True, exist_ok=True)
    path = root / _PROPERTY_INDEX_MANIFEST
    payload = {"indexes": [dict(metadata[name]) for name in sorted(metadata)]}
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _assert_index_name(name: str, label: str) -> None:
    if not _INDEX_NAME_RE.match(name):
        raise CaracalError(
            code="CDB-7090",
            message=f"invalid {label} name: {name!r}",
            hint="index names must match [A-Za-z_][A-Za-z0-9_.-]*",
        )


def _neighbors(
    db: Database,
    *,
    seed_node_ids: tuple[Any, ...],
    edge_types: tuple[str, ...],
    direction: str,
    depth: int,
    limit: int | None,
    node_type_filters: tuple[str, ...],
    edge_filters: dict[str, Any],
    return_paths: bool,
    node_key_col: str,
    weight_property: str | None,
    top_edges_per_node: int | None,
    path_score: str | None,
    path_score_property: str,
    order_by_path_score: str | None,
) -> Result:
    if depth < 1:
        raise CaracalError(code="CDB-6031", message="neighbors depth must be >= 1")
    if direction not in {"out", "in", "both"}:
        raise CaracalError(
            code="CDB-6020",
            message=f"direction must be 'out', 'in', or 'both', got {direction!r}",
        )
    if limit is not None and limit < 0:
        raise CaracalError(code="CDB-6020", message="neighbors limit must be >= 0")
    if top_edges_per_node is not None and top_edges_per_node < 0:
        raise CaracalError(code="CDB-6020", message="top_edges_per_node must be >= 0")
    score_mode = _normalize_path_score_mode(path_score)
    score_order = _normalize_score_order(order_by_path_score)
    if not edge_types:
        raise CaracalError(code="CDB-6020", message="neighbors requires at least one edge type")
    seed_ids, _ = _resolve_graph_node_ids(db, seed_node_ids, node_key_col=node_key_col)
    node_lookup = _node_lookup(db, node_key_col=node_key_col)
    allowed_types = {_coerce_local_name(value, "node type filter") for value in node_type_filters}
    adjacency = _typed_adjacency(
        db,
        edge_types=edge_types,
        direction=direction,
        filters=edge_filters,
        order_by_property=weight_property,
        top_per_node=top_edges_per_node,
    )
    rows: list[dict[str, Any]] = []
    visited = set(seed_ids)
    queue: list[tuple[int, int, list[int], list[dict[str, Any]]]] = [
        (seed, 0, [seed], []) for seed in seed_ids
    ]
    while queue:
        current, current_depth, path_nodes, path_edges = queue.pop(0)
        if current_depth >= depth:
            continue
        for step in adjacency.get(current, []):
            target = int(step["next"])
            next_depth = current_depth + 1
            next_path_nodes = [*path_nodes, target]
            next_path_edges = [*path_edges, step]
            if target not in visited:
                visited.add(target)
                node_info = node_lookup.get(target, _fallback_node_info(target))
                if not allowed_types or node_info["node_type"] in allowed_types:
                    rows.append(
                        _neighbor_result_row(
                            node_info,
                            target,
                            next_depth,
                            step,
                            next_path_nodes,
                            next_path_edges,
                            node_lookup,
                            score_mode=score_mode,
                            score_property=path_score_property,
                            return_paths=return_paths,
                        )
                    )
                    if (
                        limit is not None
                        and len(rows) >= limit
                        and not (score_mode is not None and score_order is not None)
                    ):
                        table = _neighbors_result_table(rows, return_paths=return_paths)
                        return Result(_table_to_batches(table))
                queue.append((target, next_depth, next_path_nodes, next_path_edges))
    if score_mode is not None and score_order is not None:
        rows.sort(key=_path_score_sort_key(score_order))
    if limit is not None:
        rows = rows[:limit]
    return Result(_table_to_batches(_neighbors_result_table(rows, return_paths=return_paths)))


def _k_hop(
    db: Database,
    *,
    seeds: tuple[Any, ...],
    depth: int,
    edge_types: tuple[str, ...],
    direction: str,
    max_nodes: int,
    max_edges: int,
    node_key_col: str,
) -> dict[str, pa.Table]:
    if depth < 0:
        raise CaracalError(code="CDB-6031", message="k_hop depth must be >= 0")
    if max_nodes < 0 or max_edges < 0:
        raise CaracalError(code="CDB-6020", message="k_hop limits must be >= 0")
    if direction not in {"out", "in", "both"}:
        raise CaracalError(
            code="CDB-6020",
            message=f"direction must be 'out', 'in', or 'both', got {direction!r}",
        )
    seed_ids, _ = _resolve_graph_node_ids(db, seeds, node_key_col=node_key_col)
    node_lookup = _node_lookup(db, node_key_col=node_key_col)
    adjacency = _typed_adjacency(db, edge_types=edge_types, direction=direction, filters={})
    node_depth: dict[int, int] = {seed: 0 for seed in seed_ids}
    queue: list[tuple[int, int]] = [(seed, 0) for seed in seed_ids]
    edge_rows: list[dict[str, Any]] = []
    seen_edges: set[tuple[int, str]] = set()
    while queue and len(node_depth) < max_nodes:
        current, current_depth = queue.pop(0)
        if current_depth >= depth:
            continue
        for step in adjacency.get(current, []):
            if len(edge_rows) >= max_edges:
                break
            edge_key = (int(step["edge_id"]), str(step["edge_type"]))
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                edge_rows.append(_k_hop_edge_row(step, node_lookup, current_depth + 1))
            target = int(step["next"])
            if target not in node_depth and len(node_depth) < max_nodes:
                node_depth[target] = current_depth + 1
                queue.append((target, current_depth + 1))
    node_rows = [
        _k_hop_node_row(node_lookup.get(gid, _fallback_node_info(gid)), gid, depth_value)
        for gid, depth_value in sorted(node_depth.items(), key=lambda item: (item[1], item[0]))
    ]
    return {"nodes": _k_hop_nodes_table(node_rows), "edges": _k_hop_edges_table(edge_rows)}


def _paths(
    db: Database,
    *,
    source: Any,
    target: Any,
    edge_types: tuple[str, ...],
    max_depth: int,
    limit: int,
    direction: str,
    edge_filters: Mapping[str, Any],
    node_key_col: str,
    score: str | None,
    score_property: str,
    order: str,
) -> Result:
    if max_depth < 1:
        raise CaracalError(code="CDB-6031", message="paths max_depth must be >= 1")
    if limit < 0:
        raise CaracalError(code="CDB-6020", message="paths limit must be >= 0")
    if direction not in {"out", "in", "both"}:
        raise CaracalError(
            code="CDB-6020",
            message=f"direction must be 'out', 'in', or 'both', got {direction!r}",
        )
    score_mode = _normalize_path_score_mode(score)
    score_order = _normalize_score_order(order)
    source_ids, _ = _resolve_graph_node_ids(db, source, node_key_col=node_key_col)
    target_ids, _ = _resolve_graph_node_ids(db, target, node_key_col=node_key_col)
    rows = _enumerate_path_rows(
        db,
        source_id=int(source_ids[0]),
        target_id=int(target_ids[0]),
        edge_types=edge_types,
        max_depth=max_depth,
        direction=direction,
        edge_filters=edge_filters,
        node_key_col=node_key_col,
        score_mode=score_mode,
        score_property=score_property,
    )
    rows.sort(key=_path_result_sort_key(score_order if score_mode is not None else None))
    rows = rows[:limit]
    return Result(_table_to_batches(_paths_result_table(rows)))


def _shortest_path(
    db: Database,
    *,
    source: Any,
    target: Any,
    edge_types: tuple[str, ...],
    max_depth: int | None,
    direction: str,
    edge_filters: Mapping[str, Any],
    node_key_col: str,
) -> dict[str, Any] | None:
    if max_depth is not None and max_depth < 1:
        raise CaracalError(code="CDB-6031", message="shortest_path max_depth must be >= 1")
    bound = max_depth if max_depth is not None else max(1, _global_vertex_count(db))
    result = _paths(
        db,
        source=source,
        target=target,
        edge_types=edge_types,
        max_depth=bound,
        limit=1,
        direction=direction,
        edge_filters=edge_filters,
        node_key_col=node_key_col,
        score=None,
        score_property="weight",
        order="asc",
    )
    rows = result.rows()
    return rows[0] if rows else None


def _enumerate_path_rows(
    db: Database,
    *,
    source_id: int,
    target_id: int,
    edge_types: tuple[str, ...],
    max_depth: int,
    direction: str,
    edge_filters: Mapping[str, Any],
    node_key_col: str,
    score_mode: str | None,
    score_property: str,
) -> list[dict[str, Any]]:
    adjacency = _typed_adjacency(
        db,
        edge_types=edge_types,
        direction=direction,
        filters=edge_filters,
    )
    node_lookup = _node_lookup(db, node_key_col=node_key_col)
    rows: list[dict[str, Any]] = []
    queue: list[tuple[int, list[int], list[dict[str, Any]]]] = [(source_id, [source_id], [])]
    while queue:
        current, path_nodes, path_edges = queue.pop(0)
        if len(path_edges) >= max_depth:
            continue
        for step in adjacency.get(current, []):
            next_id = int(step["next"])
            if next_id in path_nodes:
                continue
            next_nodes = [*path_nodes, next_id]
            next_edges = [*path_edges, step]
            if next_id == target_id:
                rows.append(
                    _path_result_row(
                        next_nodes,
                        next_edges,
                        node_lookup,
                        score_mode=score_mode,
                        score_property=score_property,
                    )
                )
            if len(next_edges) < max_depth:
                queue.append((next_id, next_nodes, next_edges))
    return rows


def _path_result_row(
    path_nodes: list[int],
    path_edges: list[Mapping[str, Any]],
    node_lookup: Mapping[int, Mapping[str, Any]],
    *,
    score_mode: str | None,
    score_property: str,
) -> dict[str, Any]:
    node_ids = [
        str(node_lookup.get(node, _fallback_node_info(node))["node_id"]) for node in path_nodes
    ]
    return {
        "source": node_ids[0],
        "target": node_ids[-1],
        "depth": len(path_edges),
        "node_ids": node_ids,
        "internal_node_ids": path_nodes,
        "edge_ids": [int(edge["edge_id"]) for edge in path_edges],
        "relation_types": [str(edge["edge_type"]) for edge in path_edges],
        "directions": [str(edge["direction"]) for edge in path_edges],
        "edge_properties": [
            json.dumps(edge.get("properties", {}), sort_keys=True, default=str)
            for edge in path_edges
        ],
        "path_score": _path_score(path_edges, mode=score_mode, property_name=score_property),
    }


def _paths_result_table(rows: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("source", pa.string()),
            pa.field("target", pa.string()),
            pa.field("depth", pa.uint64()),
            pa.field("node_ids", pa.list_(pa.string())),
            pa.field("internal_node_ids", pa.list_(pa.uint64())),
            pa.field("edge_ids", pa.list_(pa.uint64())),
            pa.field("relation_types", pa.list_(pa.string())),
            pa.field("directions", pa.list_(pa.string())),
            pa.field("edge_properties", pa.list_(pa.string())),
            pa.field("path_score", pa.float64()),
        ]
    )
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return pa.table(
        [
            pa.array([row["source"] for row in rows], type=pa.string()),
            pa.array([row["target"] for row in rows], type=pa.string()),
            pa.array([row["depth"] for row in rows], type=pa.uint64()),
            pa.array([row["node_ids"] for row in rows], type=pa.list_(pa.string())),
            pa.array([row["internal_node_ids"] for row in rows], type=pa.list_(pa.uint64())),
            pa.array([row["edge_ids"] for row in rows], type=pa.list_(pa.uint64())),
            pa.array([row["relation_types"] for row in rows], type=pa.list_(pa.string())),
            pa.array([row["directions"] for row in rows], type=pa.list_(pa.string())),
            pa.array([row["edge_properties"] for row in rows], type=pa.list_(pa.string())),
            pa.array([row["path_score"] for row in rows], type=pa.float64()),
        ],
        schema=schema,
    )


def _normalize_path_score_mode(mode: str | None) -> str | None:
    if mode is None:
        return None
    normalized = mode.lower()
    if normalized not in {"sum", "average", "avg", "min", "max", "product"}:
        raise CaracalError(
            code="CDB-6020",
            message=f"unsupported path score aggregation: {mode!r}",
        )
    return "average" if normalized == "avg" else normalized


def _normalize_score_order(order: str | None) -> str | None:
    if order is None:
        return None
    normalized = order.lower()
    if normalized not in {"asc", "desc"}:
        raise CaracalError(code="CDB-6020", message=f"score order must be asc or desc: {order!r}")
    return normalized


def _path_score(
    path_edges: list[Mapping[str, Any]],
    *,
    mode: str | None,
    property_name: str,
) -> float | None:
    if mode is None:
        return None
    weights = [_edge_numeric_property(edge, property_name) for edge in path_edges]
    if not weights:
        return None
    if mode == "sum":
        return float(sum(weights))
    if mode == "average":
        return float(sum(weights) / len(weights))
    if mode == "min":
        return float(min(weights))
    if mode == "max":
        return float(max(weights))
    if mode == "product":
        product = 1.0
        for weight in weights:
            product *= weight
        return float(product)
    raise CaracalError(code="CDB-6020", message=f"unsupported path score aggregation: {mode!r}")


def _path_result_sort_key(order: str | None):
    def _key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        score = row.get("path_score")
        score_value = float(score) if score is not None else 0.0
        if order is None:
            return (int(row["depth"]), tuple(row["internal_node_ids"]), tuple(row["edge_ids"]))
        ordered_score = score_value if order == "asc" else -score_value
        return (
            ordered_score,
            int(row["depth"]),
            tuple(row["internal_node_ids"]),
            tuple(row["edge_ids"]),
        )

    return _key


def _path_score_sort_key(order: str):
    def _key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        score = row.get("path_score")
        score_value = float(score) if score is not None else 0.0
        ordered_score = score_value if order == "asc" else -score_value
        return (ordered_score, int(row["depth"]), int(row["internal_id"]))

    return _key


def _typed_adjacency(
    db: Database,
    *,
    edge_types: tuple[str, ...],
    direction: str,
    filters: Mapping[str, Any],
    order_by_property: str | None = None,
    top_per_node: int | None = None,
) -> dict[int, list[dict[str, Any]]]:
    adjacency: dict[int, list[dict[str, Any]]] = {}
    for relation in edge_types:
        prop = _find_property_by_local_name(db, relation)
        if prop is None:
            raise CaracalError(code="CDB-6023", message=f"edge type not found: {relation!r}")
        store = open_edge_store(
            db.bundle,
            property_iri=prop.iri,
            local_name=prop.local_name or _local(prop.iri),
        )
        for row in store.to_table().to_pylist():
            if not _edge_row_matches_filters(row, filters):
                continue
            src = int(row["src"])
            dst = int(row["dst"])
            base = {
                "edge_id": int(row["eid"]),
                "edge_type": relation,
                "src": src,
                "dst": dst,
                "properties": {
                    key: value
                    for key, value in row.items()
                    if key not in {"eid", "src", "dst", "_created_lsn", "_deleted_lsn"}
                },
            }
            if direction in {"out", "both"}:
                adjacency.setdefault(src, []).append({**base, "next": dst, "direction": "out"})
            if direction in {"in", "both"}:
                adjacency.setdefault(dst, []).append({**base, "next": src, "direction": "in"})
    for node_id, steps in list(adjacency.items()):
        if order_by_property is None:
            steps.sort(
                key=lambda item: (str(item["edge_type"]), int(item["edge_id"]), int(item["next"]))
            )
        else:
            steps.sort(
                key=lambda item: (
                    -_edge_numeric_property(item, order_by_property),
                    str(item["edge_type"]),
                    int(item["edge_id"]),
                    int(item["next"]),
                )
            )
        if top_per_node is not None:
            adjacency[node_id] = steps[:top_per_node]
    return adjacency


def _edge_row_matches_filters(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    for raw_key, expected in filters.items():
        key = str(raw_key)
        if key.endswith("_gte"):
            if not _numeric_compare(row.get(key[:-4]), expected, "ge"):
                return False
        elif key.endswith("_lte"):
            if not _numeric_compare(row.get(key[:-4]), expected, "le"):
                return False
        elif key.endswith("_gt"):
            if not _numeric_compare(row.get(key[:-3]), expected, "gt"):
                return False
        elif key.endswith("_lt"):
            if not _numeric_compare(row.get(key[:-3]), expected, "lt"):
                return False
        elif key.endswith("_eq"):
            if row.get(key[:-3]) != expected:
                return False
        elif row.get(key) != expected:
            return False
    return True


def _edge_numeric_property(edge: Mapping[str, Any], property_name: str) -> float:
    props = edge.get("properties", {})
    if not isinstance(props, Mapping):
        return 0.0
    value = props.get(property_name)
    if value is None:
        return 0.0
    return float(value)


def _numeric_compare(value: Any, expected: Any, op: str) -> bool:
    if value is None:
        return False
    left = float(value)
    right = float(expected)
    if op == "ge":
        return left >= right
    if op == "le":
        return left <= right
    if op == "gt":
        return left > right
    if op == "lt":
        return left < right
    return False


def _node_lookup(db: Database, *, node_key_col: str = "node_id") -> dict[int, dict[str, Any]]:
    lookup: dict[int, dict[str, Any]] = {}
    for class_name in list_node_stores(db.bundle):
        cls = db._find_class(class_name)
        store = open_node_store(
            db.bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
        )
        for row in store.to_table().to_pylist():
            gid = int(row.get(_INTERNAL_GID_COLUMN, row["nid"]))
            lookup[gid] = {
                "node_id": row.get(node_key_col, gid),
                "node_type": class_name,
                "row": row,
            }
    return lookup


def _fallback_node_info(gid: int) -> dict[str, Any]:
    return {"node_id": gid, "node_type": None, "row": {}}


def _neighbor_result_row(
    node_info: Mapping[str, Any],
    internal_id: int,
    depth: int,
    step: Mapping[str, Any],
    path_nodes: list[int],
    path_edges: list[Mapping[str, Any]],
    node_lookup: Mapping[int, Mapping[str, Any]],
    *,
    score_mode: str | None,
    score_property: str,
    return_paths: bool,
) -> dict[str, Any]:
    row = {
        "node_id": node_info["node_id"],
        "node_type": node_info["node_type"],
        "internal_id": internal_id,
        "depth": depth,
        "via_edge_id": int(step["edge_id"]),
        "via_edge_type": step["edge_type"],
        "path_score": _path_score(path_edges, mode=score_mode, property_name=score_property),
    }
    if return_paths:
        row["path_node_ids"] = [
            str(node_lookup.get(node, _fallback_node_info(node))["node_id"]) for node in path_nodes
        ]
        row["path_edge_ids"] = [int(edge["edge_id"]) for edge in path_edges]
        row["path_edge_types"] = [str(edge["edge_type"]) for edge in path_edges]
    return row


def _neighbors_result_table(rows: list[dict[str, Any]], *, return_paths: bool) -> pa.Table:
    fields = [
        pa.field("node_id", pa.string()),
        pa.field("node_type", pa.string()),
        pa.field("internal_id", pa.uint64()),
        pa.field("depth", pa.uint64()),
        pa.field("via_edge_id", pa.uint64()),
        pa.field("via_edge_type", pa.string()),
        pa.field("path_score", pa.float64()),
    ]
    if return_paths:
        fields.extend(
            [
                pa.field("path_node_ids", pa.list_(pa.string())),
                pa.field("path_edge_ids", pa.list_(pa.uint64())),
                pa.field("path_edge_types", pa.list_(pa.string())),
            ]
        )
    schema = pa.schema(fields)
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    arrays = [
        pa.array([str(row["node_id"]) for row in rows], type=pa.string()),
        pa.array([row["node_type"] for row in rows], type=pa.string()),
        pa.array([row["internal_id"] for row in rows], type=pa.uint64()),
        pa.array([row["depth"] for row in rows], type=pa.uint64()),
        pa.array([row["via_edge_id"] for row in rows], type=pa.uint64()),
        pa.array([row["via_edge_type"] for row in rows], type=pa.string()),
        pa.array([row["path_score"] for row in rows], type=pa.float64()),
    ]
    if return_paths:
        arrays.extend(
            [
                pa.array([row["path_node_ids"] for row in rows], type=pa.list_(pa.string())),
                pa.array([row["path_edge_ids"] for row in rows], type=pa.list_(pa.uint64())),
                pa.array([row["path_edge_types"] for row in rows], type=pa.list_(pa.string())),
            ]
        )
    return pa.table(arrays, schema=schema)


def _k_hop_node_row(node_info: Mapping[str, Any], internal_id: int, depth: int) -> dict[str, Any]:
    return {
        "node_id": node_info["node_id"],
        "node_type": node_info["node_type"],
        "internal_id": internal_id,
        "depth": depth,
    }


def _k_hop_edge_row(
    step: Mapping[str, Any],
    node_lookup: Mapping[int, Mapping[str, Any]],
    depth: int,
) -> dict[str, Any]:
    src = int(step["src"])
    dst = int(step["dst"])
    return {
        "edge_id": int(step["edge_id"]),
        "edge_type": step["edge_type"],
        "src": node_lookup.get(src, _fallback_node_info(src))["node_id"],
        "dst": node_lookup.get(dst, _fallback_node_info(dst))["node_id"],
        "src_internal_id": src,
        "dst_internal_id": dst,
        "direction": step["direction"],
        "depth": depth,
    }


def _k_hop_nodes_table(rows: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("node_id", pa.string()),
            pa.field("node_type", pa.string()),
            pa.field("internal_id", pa.uint64()),
            pa.field("depth", pa.uint64()),
        ]
    )
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return pa.table(
        [
            pa.array([str(row["node_id"]) for row in rows], type=pa.string()),
            pa.array([row["node_type"] for row in rows], type=pa.string()),
            pa.array([row["internal_id"] for row in rows], type=pa.uint64()),
            pa.array([row["depth"] for row in rows], type=pa.uint64()),
        ],
        schema=schema,
    )


def _k_hop_edges_table(rows: list[dict[str, Any]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("edge_id", pa.uint64()),
            pa.field("edge_type", pa.string()),
            pa.field("src", pa.string()),
            pa.field("dst", pa.string()),
            pa.field("src_internal_id", pa.uint64()),
            pa.field("dst_internal_id", pa.uint64()),
            pa.field("direction", pa.string()),
            pa.field("depth", pa.uint64()),
        ]
    )
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return pa.table(
        [
            pa.array([row["edge_id"] for row in rows], type=pa.uint64()),
            pa.array([row["edge_type"] for row in rows], type=pa.string()),
            pa.array([str(row["src"]) for row in rows], type=pa.string()),
            pa.array([str(row["dst"]) for row in rows], type=pa.string()),
            pa.array([row["src_internal_id"] for row in rows], type=pa.uint64()),
            pa.array([row["dst_internal_id"] for row in rows], type=pa.uint64()),
            pa.array([row["direction"] for row in rows], type=pa.string()),
            pa.array([row["depth"] for row in rows], type=pa.uint64()),
        ],
        schema=schema,
    )


def _table_to_batches(table: pa.Table) -> list[pa.RecordBatch]:
    if table.num_rows:
        return table.combine_chunks().to_batches()
    arrays = [pa.array([], type=field.type) for field in table.schema]
    return [pa.RecordBatch.from_arrays(arrays, schema=table.schema)]


def _compile_sql_operator(
    db: Database,
    text: str,
) -> tuple[Any, SnapshotId | None, int | None, str]:
    program = parse_tuft(text)
    try:
        bind_program(program, db.catalog)
    except CaracalError as exc:
        if exc.code not in {"TF-3001", "TF-3004"}:
            raise
    if len(program.statements) != 1 or not isinstance(program.statements[0], ta.QueryStmt):
        raise CaracalError(code="CDB-6020", message="profile/explain supports one query statement")
    query = program.statements[0].query
    assert query is not None
    if _is_multi_element_pattern(query):
        plan = _compile_pattern_query(query, db)
        return _build_pattern_pipeline(plan, db), plan.snapshot, plan.limit, "pattern_match"
    plan = _compile_query(query, db)
    return _build_pipeline(plan, db), plan.snapshot, plan.limit, "node_match"


def _profile_query(db: Database, text: str) -> dict[str, Any]:
    if text.strip().upper().startswith("CALL VECTOR.SEARCH"):
        return _profile_vector_search_call(db, text.strip())
    program = parse_tuft(text)
    query = _single_query_statement(program, "profile")
    if _has_variable_length_pattern(query):
        start = time.perf_counter()
        result = _execute_variable_length_pattern_query(db, query)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        count = result.arrow().num_rows
        return {
            "logical_plan": "variable_length_path",
            "physical_plan": "VariableLengthPath",
            "indexes_used": [],
            "vector_index_used": None,
            "node_rows_scanned": 0,
            "edge_rows_scanned": 0,
            "candidate_count": count,
            "result_count": count,
            "elapsed_ms": elapsed_ms,
            "operator_timings": [
                {
                    "name": "VariableLengthPath",
                    "rows": count,
                    "batches": len(list(result.record_batches())),
                    "elapsed_ms": elapsed_ms,
                    "peak_bytes": 0,
                }
            ],
            "fallback_flags": [],
        }
    start = time.perf_counter()
    op, snapshot, limit, logical = _compile_sql_operator(db, text)
    ctx = apply_as_of(ExecCtx(), snapshot)
    iterator, report = profile_pipeline(op, ctx)
    batches = list(iterator)
    if limit is not None:
        batches = _apply_limit(batches, limit)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    result_count = sum(batch.num_rows for batch in batches)
    return {
        "logical_plan": logical,
        "physical_plan": op.name,
        "indexes_used": [],
        "vector_index_used": None,
        "node_rows_scanned": _profile_rows(report, "NodeScan"),
        "edge_rows_scanned": _profile_rows(report, "Expand"),
        "candidate_count": result_count,
        "result_count": result_count,
        "elapsed_ms": elapsed_ms,
        "operator_timings": [
            {
                "name": item.name,
                "rows": item.rows,
                "batches": item.batches,
                "elapsed_ms": item.elapsed_ms,
                "peak_bytes": item.peak_bytes,
            }
            for item in report.operators
        ],
        "fallback_flags": [],
    }


def _explain_query(db: Database, text: str) -> dict[str, Any]:
    if text.strip().upper().startswith("CALL VECTOR.SEARCH"):
        return _explain_vector_search_call(db, text.strip())
    program = parse_tuft(text)
    query = _single_query_statement(program, "explain")
    if _has_variable_length_pattern(query):
        return {
            "logical_plan": "variable_length_path",
            "physical_plan": "VariableLengthPath",
            "indexes_used": [],
            "vector_index_used": None,
            "limit": (
                _eval_int_literal(query.modifiers.limit, "LIMIT")
                if query.modifiers.limit is not None
                else None
            ),
            "fallback_flags": [],
        }
    op, _snapshot, limit, logical = _compile_sql_operator(db, text)
    return {
        "logical_plan": logical,
        "physical_plan": op.name,
        "indexes_used": [],
        "vector_index_used": None,
        "limit": limit,
        "fallback_flags": [],
    }


def _profile_rows(report: Any, name: str) -> int:
    return sum(item.rows for item in report.operators if item.name == name)


def _single_query_statement(program: ta.Program, label: str) -> ta.Query:
    if len(program.statements) != 1 or not isinstance(program.statements[0], ta.QueryStmt):
        raise CaracalError(code="CDB-6020", message=f"{label} supports one query statement")
    query = program.statements[0].query
    assert query is not None
    return query


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
    if isinstance(expr, ta.ListExpr):
        return ("lit", [_literal_expr_value(item) for item in expr.items])
    if isinstance(expr, ta.FnCall):
        return _compile_scalar_fncall(expr, {alias})
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


def _literal_expr_value(expr: ta.Expr) -> Any:
    if isinstance(expr, ta.Literal):
        return expr.value
    if isinstance(expr, ta.ListExpr):
        return [_literal_expr_value(item) for item in expr.items]
    raise CaracalError(code="CDB-6020", message="list literals in expressions must be constant")


def _compile_scalar_fncall(expr: ta.FnCall, aliases: set[str]) -> object:
    fn_name = _fn_name(expr.name)
    if fn_name not in {"cosine_similarity", "cosine_distance", "dot_product", "l2_distance"}:
        raise CaracalError(code="CDB-6020", message=f"unsupported function call: {fn_name!r}")
    if len(expr.args) != 2:
        raise CaracalError(code="CDB-6020", message=f"{fn_name}() takes exactly two arguments")
    from caracaldb.lang.builtins import VECTOR_FUNCTIONS

    fn = VECTOR_FUNCTIONS[fn_name].dispatch
    return (
        "py_binary",
        lambda left, right, _fn=fn: _fn([left, right]),
        _walk_scalar_fn_arg(expr.args[0], aliases),
        _walk_scalar_fn_arg(expr.args[1], aliases),
    )


def _walk_scalar_fn_arg(expr: ta.Expr, aliases: set[str]) -> object:
    if isinstance(expr, ta.PathExpr):
        if expr.root is None or len(expr.steps) != 1:
            raise CaracalError(code="CDB-6020", message="function args require alias.field")
        if expr.root.name not in aliases:
            raise CaracalError(code="CDB-6020", message=f"unbound variable: {expr.root.name!r}")
        return ("col", expr.steps[0].name)
    if isinstance(expr, ta.Literal):
        return ("lit", expr.value)
    if isinstance(expr, ta.ListExpr):
        return ("lit", [_literal_expr_value(item) for item in expr.items])
    raise CaracalError(
        code="CDB-6020",
        message=f"unsupported function argument: {type(expr).__name__}",
    )


def _fn_name(name: ta.NameRef | ta.Ident | None) -> str:
    if isinstance(name, ta.Ident):
        return name.name
    if isinstance(name, ta.QName):
        return name.value.rsplit(":", 1)[-1]
    if isinstance(name, ta.Iri):
        return _local(name.value)
    raise CaracalError(code="CDB-6020", message="function call is missing a name")


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


def _has_variable_length_pattern(query: ta.Query) -> bool:
    match_clause = next((c for c in query.clauses if isinstance(c, ta.MatchClause)), None)
    if match_clause is None:
        return False
    for pattern in match_clause.patterns:
        for elem in pattern.elements:
            if isinstance(elem, ta.RelPattern) and (
                elem.hop_range.min_hops is not None or elem.hop_range.max_hops is not None
            ):
                return True
    return False


def _execute_variable_length_pattern_query(db: Database, query: ta.Query) -> Result:
    spec = _compile_variable_length_pattern(query, db)
    source_rows = _candidate_rows_for_node_pattern(db, spec["source_node"], query)
    target_rows = _candidate_rows_for_node_pattern(db, spec["target_node"], query)
    target_ids = {int(row["_internal_id"]) for row in target_rows}
    target_by_id = {int(row["_internal_id"]): row for row in target_rows}
    source_alias = spec["source_alias"]
    target_alias = spec["target_alias"]
    binding = spec["binding"]
    adjacency = _typed_adjacency(
        db,
        edge_types=spec["edge_types"],
        direction=spec["direction"],
        filters=spec["edge_filters"],
    )
    node_lookup = _node_lookup(db)
    rows: list[dict[str, Any]] = []
    for source_row in source_rows:
        source_id = int(source_row["_internal_id"])
        for path in _enumerate_paths_to_targets(
            source_id=source_id,
            target_ids=target_ids,
            adjacency=adjacency,
            min_depth=spec["min_depth"],
            max_depth=spec["max_depth"],
            node_lookup=node_lookup,
        ):
            target_row = target_by_id[int(path["internal_node_ids"][-1])]
            context = {
                source_alias: source_row,
                target_alias: target_row,
                binding: path,
            }
            if spec["where"] is not None and not _eval_var_path_expr(spec["where"], context):
                continue
            rows.append(_project_var_path_row(spec["projections"], context))
    table = _variable_path_result_table(spec["projections"], rows)
    batches = _apply_modifiers(_table_to_batches(table), query.modifiers)
    return Result(batches)


def _compile_variable_length_pattern(query: ta.Query, db: Database) -> dict[str, Any]:
    match_clause = next((c for c in query.clauses if isinstance(c, ta.MatchClause)), None)
    return_clause = next((c for c in query.clauses if isinstance(c, ta.ReturnClause)), None)
    where_clause = next((c for c in query.clauses if isinstance(c, ta.WhereClause)), None)
    if match_clause is None or return_clause is None:
        raise CaracalError(code="CDB-6020", message="variable-length MATCH requires RETURN")
    if len(match_clause.patterns) != 1:
        raise CaracalError(
            code="CDB-6020",
            message="variable-length MATCH currently supports exactly one path pattern",
        )
    pattern = match_clause.patterns[0]
    elements = list(pattern.elements)
    if len(elements) != 3 or not isinstance(elements[0], ta.NodePattern):
        raise CaracalError(
            code="CDB-6020",
            message="variable-length MATCH requires (a:Type)-[:REL*min..max]->(b:Type)",
        )
    rel = elements[1]
    target_node = elements[2]
    if not isinstance(rel, ta.RelPattern) or not isinstance(target_node, ta.NodePattern):
        raise CaracalError(
            code="CDB-6020",
            message="variable-length MATCH requires one relationship between two nodes",
        )
    if not rel.types:
        raise CaracalError(code="CDB-6020", message="variable-length rel requires a type")
    edge_types = tuple(_relation_local(rel_label, query) for rel_label in rel.types)
    min_depth = 1 if rel.hop_range.min_hops is None else int(rel.hop_range.min_hops)
    max_depth = rel.hop_range.max_hops
    if max_depth is None:
        max_depth = max(1, _global_vertex_count(db))
    max_depth = int(max_depth)
    if min_depth < 0 or max_depth < min_depth:
        raise CaracalError(
            code="CDB-6031",
            message=f"invalid variable path range: {min_depth}..{max_depth}",
        )
    source_alias = elements[0].var.name if elements[0].var is not None else "a"
    target_alias = target_node.var.name if target_node.var is not None else "b"
    binding = pattern.binding.name if pattern.binding is not None else "path"
    direction = rel.direction.value
    edge_filters = _prop_map_filters(rel.props)
    projections = tuple(
        _compile_var_path_projection(proj, binding, {source_alias, target_alias})
        for proj in return_clause.projections
    )
    return {
        "source_node": elements[0],
        "target_node": target_node,
        "source_alias": source_alias,
        "target_alias": target_alias,
        "binding": binding,
        "edge_types": edge_types,
        "direction": direction,
        "min_depth": min_depth,
        "max_depth": max_depth,
        "edge_filters": edge_filters,
        "where": where_clause.predicate if where_clause is not None else None,
        "projections": projections,
    }


def _candidate_rows_for_node_pattern(
    db: Database,
    node: ta.NodePattern,
    query: ta.Query,
) -> list[dict[str, Any]]:
    cls = _resolve_pattern_class(db, node, query)
    table = _node_table_for_local(db, cls.local_name or _local(cls.iri))
    filters = _prop_map_filters(node.props)
    out: list[dict[str, Any]] = []
    for row in table.to_pylist():
        if not _row_matches_filters(row, filters):
            continue
        clean = dict(row)
        clean["_internal_id"] = int(row.get(_INTERNAL_GID_COLUMN, row["nid"]))
        clean["_node_type"] = cls.local_name or _local(cls.iri)
        out.append(clean)
    out.sort(key=lambda item: int(item["_internal_id"]))
    return out


def _prop_map_filters(props: ta.PropMap | None) -> dict[str, Any]:
    if props is None:
        return {}
    filters: dict[str, Any] = {}
    for entry in props.entries:
        if not isinstance(entry.value, ta.Literal):
            raise CaracalError(
                code="CDB-6020",
                message="variable-length pattern property maps require literal values",
            )
        filters[entry.key.name] = entry.value.value
    return filters


def _relation_local(rel_label: ta.NameRef, query: ta.Query) -> str:
    iri = rel_label.value if isinstance(rel_label, ta.Iri) else _expand(rel_label, query)
    return _local(iri) if iri.startswith("http") else iri


def _enumerate_paths_to_targets(
    *,
    source_id: int,
    target_ids: set[int],
    adjacency: Mapping[int, list[dict[str, Any]]],
    min_depth: int,
    max_depth: int,
    node_lookup: Mapping[int, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    queue: list[tuple[int, list[int], list[dict[str, Any]]]] = [(source_id, [source_id], [])]
    while queue:
        current, path_nodes, path_edges = queue.pop(0)
        if len(path_edges) >= max_depth:
            continue
        for step in adjacency.get(current, []):
            next_id = int(step["next"])
            if next_id in path_nodes:
                continue
            next_nodes = [*path_nodes, next_id]
            next_edges = [*path_edges, step]
            depth = len(next_edges)
            if depth >= min_depth and next_id in target_ids:
                rows.append(
                    _path_result_row(
                        next_nodes,
                        next_edges,
                        node_lookup,
                        score_mode=None,
                        score_property="weight",
                    )
                )
            if depth < max_depth:
                queue.append((next_id, next_nodes, next_edges))
    rows.sort(
        key=lambda row: (
            int(row["depth"]),
            tuple(row["internal_node_ids"]),
            tuple(row["edge_ids"]),
        )
    )
    return rows


def _compile_var_path_projection(
    proj: ta.Projection,
    binding: str,
    node_aliases: set[str],
) -> dict[str, Any]:
    out_name = proj.alias.name if proj.alias is not None else _default_var_path_alias(proj.expr)
    expr = proj.expr
    if isinstance(expr, ta.Var) and expr.name is not None:
        if expr.name.name == binding:
            return {"kind": "path", "name": out_name, "binding": binding}
        if expr.name.name in node_aliases:
            return {"kind": "node_id", "name": out_name, "alias": expr.name.name}
    if (
        isinstance(expr, ta.PathExpr)
        and expr.root is not None
        and len(expr.steps) == 1
        and expr.root.name in node_aliases
    ):
        return {
            "kind": "property",
            "name": out_name,
            "alias": expr.root.name,
            "property": expr.steps[0].name,
        }
    if (
        isinstance(expr, ta.FnCall)
        and _fn_name(expr.name) == "length"
        and len(expr.args) == 1
        and isinstance(expr.args[0], ta.Var)
        and expr.args[0].name is not None
        and expr.args[0].name.name == binding
    ):
        return {"kind": "length", "name": out_name, "binding": binding}
    if isinstance(expr, ta.Literal):
        return {"kind": "literal", "name": out_name, "value": expr.value}
    raise CaracalError(
        code="CDB-6020",
        message="unsupported variable-length path RETURN expression",
    )


def _default_var_path_alias(expr: ta.Expr) -> str:
    if isinstance(expr, ta.Var) and expr.name is not None:
        return expr.name.name
    if isinstance(expr, ta.PathExpr) and expr.root is not None and len(expr.steps) == 1:
        return expr.steps[0].name
    if isinstance(expr, ta.FnCall) and _fn_name(expr.name) == "length":
        return "length"
    return "expr"


def _project_var_path_row(
    projections: tuple[dict[str, Any], ...],
    context: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for projection in projections:
        kind = projection["kind"]
        name = projection["name"]
        if kind == "path":
            row[name] = _path_object(context[projection["binding"]])
        elif kind == "length":
            row[name] = int(context[projection["binding"]]["depth"])
        elif kind == "property":
            row[name] = context[projection["alias"]].get(projection["property"])
        elif kind == "node_id":
            node = context[projection["alias"]]
            row[name] = node.get("node_id", node.get("_internal_id"))
        elif kind == "literal":
            row[name] = projection["value"]
    return row


def _path_object(path: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "depth": int(path["depth"]),
        "node_ids": list(path["node_ids"]),
        "internal_node_ids": list(path["internal_node_ids"]),
        "edge_ids": list(path["edge_ids"]),
        "relation_types": list(path["relation_types"]),
        "directions": list(path["directions"]),
        "edge_properties": list(path["edge_properties"]),
    }


def _variable_path_result_table(
    projections: tuple[dict[str, Any], ...],
    rows: list[dict[str, Any]],
) -> pa.Table:
    if rows:
        return pa.Table.from_pylist(rows)
    fields: list[pa.Field] = []
    for projection in projections:
        name = projection["name"]
        if projection["kind"] == "path":
            fields.append(pa.field(name, _path_struct_type()))
        elif projection["kind"] == "length":
            fields.append(pa.field(name, pa.uint64()))
        else:
            fields.append(pa.field(name, pa.null()))
    return pa.Table.from_batches([], schema=pa.schema(fields))


def _path_struct_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("depth", pa.uint64()),
            pa.field("node_ids", pa.list_(pa.string())),
            pa.field("internal_node_ids", pa.list_(pa.uint64())),
            pa.field("edge_ids", pa.list_(pa.uint64())),
            pa.field("relation_types", pa.list_(pa.string())),
            pa.field("directions", pa.list_(pa.string())),
            pa.field("edge_properties", pa.list_(pa.string())),
        ]
    )


def _eval_var_path_expr(expr: ta.Expr, context: Mapping[str, Mapping[str, Any]]) -> bool:
    value = _eval_var_path_value(expr, context)
    return bool(value)


def _eval_var_path_value(expr: ta.Expr, context: Mapping[str, Mapping[str, Any]]) -> Any:
    if isinstance(expr, ta.Literal):
        return expr.value
    if isinstance(expr, ta.PathExpr) and expr.root is not None and len(expr.steps) == 1:
        if expr.root.name not in context:
            raise CaracalError(code="CDB-6020", message=f"unbound variable: {expr.root.name!r}")
        return context[expr.root.name].get(expr.steps[0].name)
    if isinstance(expr, ta.Var) and expr.name is not None:
        return context.get(expr.name.name)
    if isinstance(expr, ta.FnCall) and _fn_name(expr.name) == "length":
        if len(expr.args) != 1 or not isinstance(expr.args[0], ta.Var) or expr.args[0].name is None:
            raise CaracalError(code="CDB-6020", message="length() requires a path variable")
        return int(context[expr.args[0].name.name]["depth"])
    if isinstance(expr, ta.BinOp):
        left = _eval_var_path_value(expr.left, context)
        right = _eval_var_path_value(expr.right, context)
        op = _BIN_OP_TO_TUPLE.get(expr.op)
        if op == "eq":
            return left == right
        if op == "ne":
            return left != right
        if op == "lt":
            return left < right
        if op == "le":
            return left <= right
        if op == "gt":
            return left > right
        if op == "ge":
            return left >= right
        if op == "and":
            return bool(left) and bool(right)
        if op == "or":
            return bool(left) or bool(right)
        raise CaracalError(code="CDB-6020", message=f"unsupported operator: {expr.op}")
    if isinstance(expr, ta.UnaryOp) and expr.op.lower() in {"not", "!"}:
        return not bool(_eval_var_path_value(expr.operand, context))
    raise CaracalError(
        code="CDB-6020",
        message=f"unsupported variable-length path predicate: {type(expr).__name__}",
    )


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


def _apply_modifiers(
    batches: list[pa.RecordBatch],
    modifiers: ta.Modifiers,
) -> list[pa.RecordBatch]:
    if not batches:
        return batches
    table = pa.Table.from_batches(batches)
    if modifiers.order_by:
        rows = table.to_pylist()
        for item in reversed(modifiers.order_by):
            column = _order_column_name(item.expr, table)
            rows.sort(key=lambda row, _column=column: row.get(_column), reverse=item.descending)
        table = pa.Table.from_pylist(rows, schema=table.schema) if rows else table.slice(0, 0)
    if modifiers.skip is not None:
        skip = _eval_int_literal(modifiers.skip, "SKIP")
        table = table.slice(skip)
    if modifiers.limit is not None:
        limit = _eval_int_literal(modifiers.limit, "LIMIT")
        table = table.slice(0, limit)
    return table.combine_chunks().to_batches() if table.num_rows else _table_to_batches(table)


def _order_column_name(expr: ta.Expr, table: pa.Table) -> str:
    if isinstance(expr, ta.Var) and expr.name is not None:
        name = expr.name.name
    elif isinstance(expr, ta.PathExpr) and expr.root is not None and len(expr.steps) == 1:
        dotted = f"{expr.root.name}.{expr.steps[0].name}"
        name = dotted if dotted in table.column_names else expr.steps[0].name
    else:
        name = _default_alias(expr, "n")
    if name not in table.column_names:
        raise CaracalError(
            code="CDB-6020",
            message=f"ORDER BY expression is not projected as a result column: {name!r}",
        )
    return name


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


__all__ = [
    "Connection",
    "Database",
    "NodeQuery",
    "ResourceRef",
    "Result",
    "connect",
    "cosine_distance",
    "cosine_similarity",
    "dot_product",
    "l2_distance",
]
