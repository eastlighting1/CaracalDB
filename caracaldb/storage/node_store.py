"""Per-class node store backed by Arrow column segments.

Each class registered in the catalog maps to a directory under
``<bundle>/nodes/<local_name>/`` containing a JSON manifest plus one or more
``chunks/NNNNNNNN.col`` Arrow IPC segments. Nodes are appended in batches; the
store assigns a monotonically increasing ``nid`` (UInt64) per row inside each
class. Cross-class node identity is therefore (class_iri, nid) — global nid
materialisation lands when the catalog gains dense ``cid`` packing.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.column_store import (
    Codec,
    ColumnReader,
    ColumnSegmentFooter,
    ColumnWriter,
)

NODE_MANIFEST_NAME = "_manifest.json"
CHUNKS_DIRNAME = "chunks"
NID_COLUMN = "nid"
CREATED_LSN_COLUMN = "_created_lsn"
DELETED_LSN_COLUMN = "_deleted_lsn"
_LOCAL_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")
__all_local_name_re__ = _LOCAL_NAME_RE
_MVCC_COLUMNS = {CREATED_LSN_COLUMN, DELETED_LSN_COLUMN}


@dataclass(frozen=True, slots=True)
class NodeChunkRef:
    path: str
    row_count: int
    start_nid: int
    end_nid: int  # exclusive

    def to_json(self) -> dict[str, object]:
        return {
            "path": self.path,
            "row_count": self.row_count,
            "start_nid": self.start_nid,
            "end_nid": self.end_nid,
        }

    @classmethod
    def from_json(cls, value: dict[str, object]) -> NodeChunkRef:
        return cls(
            path=str(value["path"]),
            row_count=int(value["row_count"]),  # type: ignore[arg-type]
            start_nid=int(value["start_nid"]),  # type: ignore[arg-type]
            end_nid=int(value["end_nid"]),  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class NodeStoreManifest:
    class_iri: str
    local_name: str
    schema_json: str  # pa.Schema serialized (without nid)
    next_nid: int = 0
    chunks: list[NodeChunkRef] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.chunks is None:
            self.chunks = []

    def to_json(self) -> dict[str, object]:
        return {
            "class_iri": self.class_iri,
            "local_name": self.local_name,
            "schema": self.schema_json,
            "next_nid": self.next_nid,
            "chunks": [chunk.to_json() for chunk in self.chunks],
        }

    @classmethod
    def from_json(cls, value: dict[str, object]) -> NodeStoreManifest:
        return cls(
            class_iri=str(value["class_iri"]),
            local_name=str(value["local_name"]),
            schema_json=str(value["schema"]),
            next_nid=int(value.get("next_nid", 0)),  # type: ignore[arg-type]
            chunks=[NodeChunkRef.from_json(item) for item in value.get("chunks", [])],  # type: ignore[arg-type]
        )


def _assert_local_name(name: str) -> None:
    if not _LOCAL_NAME_RE.match(name):
        raise CaracalError(
            code="CDB-7010",
            message=f"invalid class local name: {name!r}",
            hint="local names must match [A-Za-z_][A-Za-z0-9_-]*",
        )


def _schema_to_json(schema: pa.Schema) -> str:
    fields = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema]
    return json.dumps(fields, sort_keys=True)


def _public_names(schema: pa.Schema) -> list[str]:
    return [name for name in schema.names if name not in _MVCC_COLUMNS]


def _select_public(batch: pa.RecordBatch) -> pa.RecordBatch:
    names = _public_names(batch.schema)
    if names == batch.schema.names:
        return batch
    return batch.select(names)


def _ensure_compatible(expected: pa.Schema, actual: pa.Schema) -> None:
    expected_names = _public_names(expected)
    actual_names = _public_names(actual)
    if expected_names != actual_names:
        raise CaracalError(
            code="CDB-7011",
            message=(
                f"node batch schema mismatch: expected columns {expected_names}, got {actual_names}"
            ),
        )
    for name in expected_names:
        if expected.field(name).type != actual.field(name).type:
            raise CaracalError(
                code="CDB-7011",
                message=(
                    f"node batch column {name!r} type mismatch: "
                    f"expected {expected.field(name).type}, got {actual.field(name).type}"
                ),
            )


class NodeStore:
    """Append-only column store for a single class."""

    def __init__(
        self,
        root: Path,
        manifest: NodeStoreManifest,
        *,
        codec: Codec = "none",
    ) -> None:
        self.root = root
        self.manifest = manifest
        self.codec: Codec = codec

    @property
    def schema(self) -> pa.Schema:
        # Reconstructed lazily; the manifest persists the human-readable form,
        # but for runtime checks we re-derive from the most recent chunk.
        if self.manifest.chunks:
            chunk_path = self.root / self.manifest.chunks[0].path
            return ColumnReader(chunk_path).table().schema
        # Empty store: fall back to nid-only schema.
        return pa.schema([pa.field(NID_COLUMN, pa.uint64(), nullable=False)])

    @property
    def num_rows(self) -> int:
        return sum(chunk.row_count for chunk in self.manifest.chunks)

    @property
    def next_nid(self) -> int:
        return self.manifest.next_nid

    def append(
        self,
        batch: pa.RecordBatch | pa.Table,
        *,
        created_lsn: int = 0,
    ) -> NodeChunkRef:
        """Append a batch of property rows, assigning fresh ``nid`` values."""
        if isinstance(batch, pa.Table):
            if batch.num_rows == 0:
                raise CaracalError(code="CDB-7011", message="cannot append empty node batch")
            record_batch = batch.combine_chunks().to_batches()[0]
        else:
            record_batch = batch
        if record_batch.num_rows == 0:
            raise CaracalError(code="CDB-7011", message="cannot append empty node batch")
        if NID_COLUMN in record_batch.schema.names:
            raise CaracalError(
                code="CDB-7011",
                message="node batches must not include a 'nid' column; it is assigned by the store",
            )
        for name in _MVCC_COLUMNS:
            if name in record_batch.schema.names:
                raise CaracalError(
                    code="CDB-7011",
                    message=f"node batches must not include reserved column {name!r}",
                )

        start_nid = self.manifest.next_nid
        end_nid = start_nid + record_batch.num_rows
        nid_array = pa.array(range(start_nid, end_nid), type=pa.uint64())
        created_array = pa.array([created_lsn] * record_batch.num_rows, type=pa.uint64())
        deleted_array = pa.nulls(record_batch.num_rows, type=pa.uint64())
        with_nid = pa.RecordBatch.from_arrays(
            [nid_array, *record_batch.columns, created_array, deleted_array],
            names=[
                NID_COLUMN,
                *record_batch.schema.names,
                CREATED_LSN_COLUMN,
                DELETED_LSN_COLUMN,
            ],
        )

        if self.manifest.chunks:
            existing_schema = self.schema
            _ensure_compatible(existing_schema, with_nid.schema)

        chunk_index = len(self.manifest.chunks)
        chunk_relpath = f"{CHUNKS_DIRNAME}/{chunk_index:08d}.col"
        chunk_path = self.root / chunk_relpath
        writer = ColumnWriter(chunk_path, codec=self.codec)
        writer.append(with_nid)
        footer: ColumnSegmentFooter = writer.close()

        ref = NodeChunkRef(
            path=chunk_relpath,
            row_count=footer.row_count,
            start_nid=start_nid,
            end_nid=end_nid,
        )
        self.manifest.chunks.append(ref)
        self.manifest.next_nid = end_nid
        if not self.manifest.schema_json:
            self.manifest.schema_json = _schema_to_json(with_nid.schema)
        self._persist_manifest()
        return ref

    def scan(
        self,
        *,
        columns: list[str] | None = None,
        snapshot_lsn: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        for chunk in self.manifest.chunks:
            reader = ColumnReader(self.root / chunk.path)
            for batch in reader.record_batches():
                if snapshot_lsn is not None:
                    batch = _filter_visible(batch, snapshot_lsn)
                if columns is not None:
                    batch = batch.select(columns)
                else:
                    batch = _select_public(batch)
                yield batch

    def to_table(
        self,
        *,
        columns: list[str] | None = None,
        snapshot_lsn: int | None = None,
    ) -> pa.Table:
        batches = list(self.scan(columns=columns, snapshot_lsn=snapshot_lsn))
        if not batches:
            schema = (
                pa.schema([field for field in self.schema if field.name not in _MVCC_COLUMNS])
                if columns is None
                else self.schema.select([self.schema.get_field_index(name) for name in columns])
            )
            return pa.Table.from_batches([], schema=schema)
        return pa.Table.from_batches(batches)

    def _persist_manifest(self) -> None:
        save_node_manifest(self.root, self.manifest)


def manifest_path(root: Path) -> Path:
    return root / NODE_MANIFEST_NAME


def save_node_manifest(root: Path, manifest: NodeStoreManifest) -> None:
    root.mkdir(parents=True, exist_ok=True)
    target = manifest_path(root)
    text = json.dumps(manifest.to_json(), indent=2, sort_keys=True) + "\n"
    tmp = target.with_name(f"{target.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def load_node_manifest(root: Path) -> NodeStoreManifest:
    target = manifest_path(root)
    if not target.is_file():
        raise CaracalError(code="CDB-7012", message=f"node store manifest missing: {target}")
    return NodeStoreManifest.from_json(json.loads(target.read_text(encoding="utf-8")))


def _filter_visible(batch: pa.RecordBatch, snapshot_lsn: int) -> pa.RecordBatch:
    names = batch.schema.names
    if CREATED_LSN_COLUMN not in names:
        return batch
    created = batch.column(CREATED_LSN_COLUMN)
    deleted = (
        batch.column(DELETED_LSN_COLUMN)
        if DELETED_LSN_COLUMN in names
        else pa.nulls(batch.num_rows, type=pa.uint64())
    )
    created_ok = pc.less_equal(created, pa.scalar(snapshot_lsn, type=pa.uint64()))
    deleted_null = pc.is_null(deleted)
    deleted_after = pc.fill_null(
        pc.greater(deleted, pa.scalar(snapshot_lsn, type=pa.uint64())),
        False,
    )
    visible = pc.and_(created_ok, pc.or_(deleted_null, deleted_after))
    return batch.filter(visible)


def open_node_store(
    bundle: Bundle,
    *,
    class_iri: str,
    local_name: str,
    create: bool = False,
    codec: Codec = "none",
) -> NodeStore:
    _assert_local_name(local_name)
    root = bundle.child("nodes", local_name)
    manifest_file = manifest_path(root)
    if manifest_file.is_file():
        manifest = load_node_manifest(root)
        if manifest.class_iri != class_iri:
            raise CaracalError(
                code="CDB-7013",
                message=(
                    f"node store {local_name!r} class mismatch: "
                    f"expected {class_iri}, found {manifest.class_iri}"
                ),
            )
        return NodeStore(root, manifest, codec=codec)
    if not create:
        raise CaracalError(
            code="CDB-7012",
            message=f"node store not found for class {class_iri!r}",
            hint="pass create=True to initialise a fresh node store",
        )
    (root / CHUNKS_DIRNAME).mkdir(parents=True, exist_ok=True)
    manifest = NodeStoreManifest(class_iri=class_iri, local_name=local_name, schema_json="")
    save_node_manifest(root, manifest)
    return NodeStore(root, manifest, codec=codec)


def list_node_stores(bundle: Bundle) -> list[str]:
    root = bundle.child("nodes")
    if not root.is_dir():
        return []
    return sorted(item.name for item in root.iterdir() if item.is_dir())


__all__ = [
    "NID_COLUMN",
    "CREATED_LSN_COLUMN",
    "DELETED_LSN_COLUMN",
    "NodeChunkRef",
    "NodeStore",
    "NodeStoreManifest",
    "list_node_stores",
    "load_node_manifest",
    "open_node_store",
    "save_node_manifest",
]
