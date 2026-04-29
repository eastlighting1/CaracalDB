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

import pyarrow as pa

from caracaldb.exec.expr import compile_expr
from caracaldb.exec.operator import ExecCtx, run_pipeline
from caracaldb.exec.operators import FilterOperator, NodeScanOperator, ProjectOperator
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import bind_program, parse_tuft
from caracaldb.onto.catalog import Catalog, ClassDef, load_catalog, save_catalog
from caracaldb.storage import Bundle, create_bundle, open_bundle
from caracaldb.storage.node_store import NodeStore, list_node_stores, open_node_store
from caracaldb.storage.pack import is_packed, pack_bundle


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

    def define_class(self, name: str, *, iri: str | None = None) -> ClassDef:
        class_iri = iri or f"http://example.org/{name}"
        existing = self._catalog.class_by_iri(class_iri)
        if existing is not None:
            return existing
        for candidate in self._catalog.classes:
            if (candidate.local_name or _local(candidate.iri)) == name:
                return candidate
        cls = self._catalog.register_class(iri=class_iri, local_name=name)
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
    if where_clause is not None and where_clause.predicate is not None:
        predicate = _translate_expr(where_clause.predicate, alias)

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
    if plan.local_name not in list_node_stores(db.bundle):
        raise CaracalError(
            code="CDB-6023",
            message=f"node store missing for class {plan.class_iri!r} (local={plan.local_name!r})",
        )
    store = open_node_store(db.bundle, class_iri=plan.class_iri, local_name=plan.local_name)

    column_request = list(plan.columns)
    op: Any = NodeScanOperator(store, columns=column_request)
    if plan.predicate is not None:
        op = FilterOperator(op, compile_expr(plan.predicate))
    if plan.projections:
        op = ProjectOperator(op, [(compile_expr(e), name) for e, name in plan.projections])
    return op


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


__all__ = ["Connection", "Database", "Result", "connect"]
