"""Per-property edge store (row-oriented staging).

Edges live under ``<bundle>/edges/<PropertyLocalName>/``. The M1 layout keeps
incoming batches as Arrow IPC column segments rather than CSR; CSR/CSC builders
land in M2 (CDB-035/036). Each batch carries the columns ``(eid, src, dst)``
plus optional edge-property columns; ``eid`` is assigned monotonically by the
store while ``src`` / ``dst`` are caller-provided ``nid`` references.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.column_store import (
    Codec,
    ColumnReader,
    ColumnSegmentFooter,
    ColumnWriter,
)
from caracaldb.storage.node_store import _LOCAL_NAME_RE  # reuse local-name policy

EDGE_MANIFEST_NAME = "_manifest.json"
CHUNKS_DIRNAME = "chunks"
EID_COLUMN = "eid"
SRC_COLUMN = "src"
DST_COLUMN = "dst"
REQUIRED_EDGE_COLUMNS = (SRC_COLUMN, DST_COLUMN)


@dataclass(frozen=True, slots=True)
class EdgeChunkRef:
    path: str
    row_count: int
    start_eid: int
    end_eid: int

    def to_json(self) -> dict[str, object]:
        return {
            "path": self.path,
            "row_count": self.row_count,
            "start_eid": self.start_eid,
            "end_eid": self.end_eid,
        }

    @classmethod
    def from_json(cls, value: dict[str, object]) -> EdgeChunkRef:
        return cls(
            path=str(value["path"]),
            row_count=int(value["row_count"]),  # type: ignore[arg-type]
            start_eid=int(value["start_eid"]),  # type: ignore[arg-type]
            end_eid=int(value["end_eid"]),  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class EdgeStoreManifest:
    property_iri: str
    local_name: str
    src_class_iri: str | None = None
    dst_class_iri: str | None = None
    next_eid: int = 0
    chunks: list[EdgeChunkRef] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.chunks is None:
            self.chunks = []

    def to_json(self) -> dict[str, object]:
        return {
            "property_iri": self.property_iri,
            "local_name": self.local_name,
            "src_class_iri": self.src_class_iri,
            "dst_class_iri": self.dst_class_iri,
            "next_eid": self.next_eid,
            "chunks": [chunk.to_json() for chunk in self.chunks],
        }

    @classmethod
    def from_json(cls, value: dict[str, object]) -> EdgeStoreManifest:
        return cls(
            property_iri=str(value["property_iri"]),
            local_name=str(value["local_name"]),
            src_class_iri=value.get("src_class_iri") or None,  # type: ignore[arg-type]
            dst_class_iri=value.get("dst_class_iri") or None,  # type: ignore[arg-type]
            next_eid=int(value.get("next_eid", 0)),  # type: ignore[arg-type]
            chunks=[EdgeChunkRef.from_json(item) for item in value.get("chunks", [])],  # type: ignore[arg-type]
        )


def _assert_local_name(name: str) -> None:
    if not _LOCAL_NAME_RE.match(name):
        raise CaracalError(
            code="CDB-7020",
            message=f"invalid property local name: {name!r}",
            hint="local names must match [A-Za-z_][A-Za-z0-9_-]*",
        )


def _validate_batch(batch: pa.RecordBatch, *, established_schema: pa.Schema | None) -> None:
    if batch.num_rows == 0:
        raise CaracalError(code="CDB-7021", message="cannot append empty edge batch")
    names = batch.schema.names
    if EID_COLUMN in names:
        raise CaracalError(
            code="CDB-7021",
            message="edge batches must not include an 'eid' column; it is assigned by the store",
        )
    for required in REQUIRED_EDGE_COLUMNS:
        if required not in names:
            raise CaracalError(
                code="CDB-7021",
                message=f"edge batch is missing required column {required!r}",
            )
    src_type = batch.schema.field(SRC_COLUMN).type
    dst_type = batch.schema.field(DST_COLUMN).type
    if src_type != pa.uint64() or dst_type != pa.uint64():
        raise CaracalError(
            code="CDB-7021",
            message="edge 'src' and 'dst' must be UInt64 (nid) columns",
        )
    if established_schema is not None:
        if established_schema.names != names:
            raise CaracalError(
                code="CDB-7021",
                message=(
                    f"edge batch column drift: expected {established_schema.names}, got {names}"
                ),
            )
        for name in names:
            if established_schema.field(name).type != batch.schema.field(name).type:
                raise CaracalError(
                    code="CDB-7021",
                    message=(
                        f"edge batch column {name!r} type mismatch: "
                        f"{established_schema.field(name).type} vs {batch.schema.field(name).type}"
                    ),
                )


class EdgeStore:
    def __init__(
        self,
        root: Path,
        manifest: EdgeStoreManifest,
        *,
        codec: Codec = "none",
    ) -> None:
        self.root = root
        self.manifest = manifest
        self.codec: Codec = codec

    @property
    def num_rows(self) -> int:
        return sum(chunk.row_count for chunk in self.manifest.chunks)

    @property
    def next_eid(self) -> int:
        return self.manifest.next_eid

    @property
    def schema(self) -> pa.Schema:
        if self.manifest.chunks:
            chunk_path = self.root / self.manifest.chunks[0].path
            return ColumnReader(chunk_path).table().schema
        return pa.schema(
            [
                pa.field(EID_COLUMN, pa.uint64(), nullable=False),
                pa.field(SRC_COLUMN, pa.uint64(), nullable=False),
                pa.field(DST_COLUMN, pa.uint64(), nullable=False),
            ]
        )

    def append(self, batch: pa.RecordBatch | pa.Table) -> EdgeChunkRef:
        if isinstance(batch, pa.Table):
            if batch.num_rows == 0:
                raise CaracalError(code="CDB-7021", message="cannot append empty edge batch")
            record_batch = batch.combine_chunks().to_batches()[0]
        else:
            record_batch = batch

        if self.manifest.chunks:
            established = self.schema
            established_without_eid = pa.schema(
                [established.field(name) for name in established.names if name != EID_COLUMN]
            )
        else:
            established_without_eid = None
        _validate_batch(record_batch, established_schema=established_without_eid)

        start_eid = self.manifest.next_eid
        end_eid = start_eid + record_batch.num_rows
        eid_array = pa.array(range(start_eid, end_eid), type=pa.uint64())
        with_eid = pa.RecordBatch.from_arrays(
            [eid_array, *record_batch.columns],
            names=[EID_COLUMN, *record_batch.schema.names],
        )

        chunk_index = len(self.manifest.chunks)
        chunk_relpath = f"{CHUNKS_DIRNAME}/{chunk_index:08d}.col"
        chunk_path = self.root / chunk_relpath
        writer = ColumnWriter(chunk_path, codec=self.codec)
        writer.append(with_eid)
        footer: ColumnSegmentFooter = writer.close()

        ref = EdgeChunkRef(
            path=chunk_relpath,
            row_count=footer.row_count,
            start_eid=start_eid,
            end_eid=end_eid,
        )
        self.manifest.chunks.append(ref)
        self.manifest.next_eid = end_eid
        save_edge_manifest(self.root, self.manifest)
        return ref

    def scan(self, *, columns: list[str] | None = None) -> Iterator[pa.RecordBatch]:
        for chunk in self.manifest.chunks:
            reader = ColumnReader(self.root / chunk.path)
            for batch in reader.record_batches():
                if columns is not None:
                    batch = batch.select(columns)
                yield batch

    def to_table(self, *, columns: list[str] | None = None) -> pa.Table:
        batches = list(self.scan(columns=columns))
        if batches:
            return pa.Table.from_batches(batches)
        schema = self.schema
        if columns is not None:
            schema = pa.schema([schema.field(name) for name in columns])
        return pa.Table.from_batches([], schema=schema)


def manifest_path(root: Path) -> Path:
    return root / EDGE_MANIFEST_NAME


def save_edge_manifest(root: Path, manifest: EdgeStoreManifest) -> None:
    root.mkdir(parents=True, exist_ok=True)
    target = manifest_path(root)
    text = json.dumps(manifest.to_json(), indent=2, sort_keys=True) + "\n"
    tmp = target.with_name(f"{target.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def load_edge_manifest(root: Path) -> EdgeStoreManifest:
    target = manifest_path(root)
    if not target.is_file():
        raise CaracalError(code="CDB-7022", message=f"edge store manifest missing: {target}")
    return EdgeStoreManifest.from_json(json.loads(target.read_text(encoding="utf-8")))


def open_edge_store(
    bundle: Bundle,
    *,
    property_iri: str,
    local_name: str,
    src_class_iri: str | None = None,
    dst_class_iri: str | None = None,
    create: bool = False,
    codec: Codec = "none",
) -> EdgeStore:
    _assert_local_name(local_name)
    root = bundle.child("edges", local_name)
    manifest_file = manifest_path(root)
    if manifest_file.is_file():
        manifest = load_edge_manifest(root)
        if manifest.property_iri != property_iri:
            raise CaracalError(
                code="CDB-7023",
                message=(
                    f"edge store {local_name!r} property mismatch: "
                    f"expected {property_iri}, found {manifest.property_iri}"
                ),
            )
        return EdgeStore(root, manifest, codec=codec)
    if not create:
        raise CaracalError(
            code="CDB-7022",
            message=f"edge store not found for property {property_iri!r}",
            hint="pass create=True to initialise a fresh edge store",
        )
    (root / CHUNKS_DIRNAME).mkdir(parents=True, exist_ok=True)
    manifest = EdgeStoreManifest(
        property_iri=property_iri,
        local_name=local_name,
        src_class_iri=src_class_iri,
        dst_class_iri=dst_class_iri,
    )
    save_edge_manifest(root, manifest)
    return EdgeStore(root, manifest, codec=codec)


def list_edge_stores(bundle: Bundle) -> list[str]:
    root = bundle.child("edges")
    if not root.is_dir():
        return []
    return sorted(item.name for item in root.iterdir() if item.is_dir())


__all__ = [
    "DST_COLUMN",
    "EID_COLUMN",
    "EdgeChunkRef",
    "EdgeStore",
    "EdgeStoreManifest",
    "SRC_COLUMN",
    "list_edge_stores",
    "load_edge_manifest",
    "open_edge_store",
    "save_edge_manifest",
]
