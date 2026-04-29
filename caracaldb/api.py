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
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pyarrow as pa

from caracaldb.exec.expr import compile_expr
from caracaldb.exec.operator import ExecCtx, PhysicalOperator, run_pipeline
from caracaldb.exec.operators import (
    ClosureScanOperator,
    FilterOperator,
    NodeScanOperator,
    ProjectOperator,
)
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import bind_program, parse_tuft
from caracaldb.onto.catalog import Catalog, ClassDef, load_catalog, save_catalog
from caracaldb.onto.closure import ClassClosureIndex
from caracaldb.storage import Bundle, create_bundle, open_bundle
from caracaldb.storage.edge_store import list_edge_stores, open_edge_store
from caracaldb.storage.node_store import NodeStore, list_node_stores, open_node_store
from caracaldb.storage.pack import is_packed, pack_bundle

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
    >>> ref = ResourceRef("employee/E12345", 42, "caracaldb://resource/employee/E12345")
    >>> ref.internal_id
    42
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
    >>> result = Result([])
    >>> result.arrow().num_rows
    0
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


class Connection:
    """Query connection bound to an open :class:`Database`.

    Examples
    --------
    >>> isinstance(Connection, type)
    True
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
        plan = _compile_query(query, self._db)
        op = _build_pipeline(plan, self._db)
        ctx = ExecCtx()
        batches = list(run_pipeline(op, ctx))
        if plan.limit is not None:
            batches = _apply_limit(batches, plan.limit)
        return Result(batches)


class Database:
    """Handle to open CaracalDB database.

    Use as a context manager to ensure packed files are re-packed on exit::

        with cdb.connect("data") as db:
            db.cursor().sql("MATCH ...")

    Examples
    --------
    >>> isinstance(Database, type)
    True
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
        rows: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    ) -> Any:
        payload = [dict(rows)] if isinstance(rows, Mapping) else [dict(row) for row in rows]
        if not payload:
            raise CaracalError(code="CDB-7011", message="cannot insert an empty node batch")

        cls = self._find_class(class_name)
        store = open_node_store(
            self._bundle,
            class_iri=cls.iri,
            local_name=cls.local_name or _local(cls.iri),
            create=True,
        )
        return store.append(pa.Table.from_pylist(payload))

    def insert_node_table(
        self,
        rows: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        key_col: str = "node_id",
        type_col: str = "type",
    ) -> dict[str, Any]:
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

    def insert_edge_table(
        self,
        rows: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        src_col: str = "src",
        dst_col: str = "dst",
        type_col: str = "type",
        node_key_col: str = "node_id",
    ) -> dict[str, Any]:
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
            refs[relation] = store.append(_edge_table(group))
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

    def _find_class(self, iri: str) -> ClassDef:
        cls = self._catalog.class_by_iri(iri)
        if cls is None:
            # Fallback: also accept local-name match for the M1 MVP.
            for candidate in self._catalog.classes:
                if (candidate.local_name or _local(candidate.iri)) == iri:
                    return candidate
            raise CaracalError(code="CDB-6021", message=f"class not found in catalog: {iri!r}")
        return cls

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

    Examples
    --------
    >>> import tempfile
    >>> root = tempfile.TemporaryDirectory()
    >>> db = connect(Path(root.name) / "demo", format="bundle")
    >>> db.close()
    >>> root.cleanup()
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

    return _CompiledQuery(
        class_iri=cls.iri,
        local_name=cls.local_name or _local(cls.iri),
        alias=alias,
        columns=columns,
        predicate=predicate,
        projections=tuple(projections),
        limit=limit,
        closure_base_iri=closure_base_iri,
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


def _resolve_external_node_id(id_map: Mapping[Any, int], value: Any, column: str) -> int:
    if value not in id_map:
        raise CaracalError(
            code="CDB-7021",
            message=f"edge {column!r} references unknown node_id: {value!r}",
            hint="insert the node table before inserting edges",
        )
    return int(id_map[value])


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


__all__ = ["Connection", "Database", "ResourceRef", "Result", "connect"]
