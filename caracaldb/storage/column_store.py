"""Column segment read/write helpers.

The M0 implementation writes one Arrow IPC stream per `.col` file. It keeps the
format compatible with the documented CaracalDB segment shape:

    [ CRCL header ][ payload ][ footer json ][ u64 footer_offset ][ u32 footer_crc32 ]

The payload is an Arrow IPC stream, optionally compressed as a whole file frame.
Later milestones can replace the in-memory buffering with chunked streaming
without changing the footer contract.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.header import HEADER_SIZE, MAGIC, pack_header

Codec = Literal["none", "zstd", "lz4"]
FOOTER_TRAILER_FMT = "<QI"
FOOTER_TRAILER_SIZE = struct.calcsize(FOOTER_TRAILER_FMT)


@dataclass(frozen=True, slots=True)
class ColumnSegmentFooter:
    format_version: int
    codec: Codec
    row_count: int
    batch_count: int
    schema: str
    uncompressed_size: int
    payload_size: int

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> ColumnSegmentFooter:
        codec = str(value["codec"])
        if codec not in {"none", "zstd", "lz4"}:
            raise CaracalError(code="CDB-7002", message=f"unknown column codec: {codec}")
        return cls(
            format_version=int(value["format_version"]),
            codec=cast(Codec, codec),
            row_count=int(value["row_count"]),
            batch_count=int(value["batch_count"]),
            schema=str(value["schema"]),
            uncompressed_size=int(value["uncompressed_size"]),
            payload_size=int(value["payload_size"]),
        )

    def to_json(self) -> dict[str, object]:
        return {
            "format_version": self.format_version,
            "codec": self.codec,
            "row_count": self.row_count,
            "batch_count": self.batch_count,
            "schema": self.schema,
            "uncompressed_size": self.uncompressed_size,
            "payload_size": self.payload_size,
        }


class ColumnWriter:
    def __init__(self, path: str | Path, *, codec: Codec = "none") -> None:
        if codec not in {"none", "zstd", "lz4"}:
            raise ValueError(f"unsupported codec: {codec}")
        self.path = Path(path)
        self.codec = codec
        self._batches: list[pa.RecordBatch] = []
        self._closed = False

    def append(self, batch: pa.RecordBatch) -> None:
        if self._closed:
            raise CaracalError(code="CDB-7003", message="column writer is already closed")
        if self._batches and batch.schema != self._batches[0].schema:
            raise CaracalError(code="CDB-7001", message="record batch schema mismatch")
        self._batches.append(batch)

    def close(self) -> ColumnSegmentFooter:
        if self._closed:
            raise CaracalError(code="CDB-7003", message="column writer is already closed")
        if not self._batches:
            raise CaracalError(code="CDB-7001", message="cannot write an empty column segment")

        self.path.parent.mkdir(parents=True, exist_ok=True)
        ipc_payload = _batches_to_ipc(self._batches)
        payload = _compress(ipc_payload, self.codec)
        footer = ColumnSegmentFooter(
            format_version=1,
            codec=self.codec,
            row_count=sum(batch.num_rows for batch in self._batches),
            batch_count=len(self._batches),
            schema=self._batches[0].schema.to_string(),
            uncompressed_size=len(ipc_payload),
            payload_size=len(payload),
        )

        footer_bytes = json.dumps(footer.to_json(), sort_keys=True).encode("utf-8")
        footer_crc = zlib.crc32(footer_bytes) & 0xFFFFFFFF
        footer_offset = HEADER_SIZE + len(payload)

        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        with tmp_path.open("wb") as file:
            file.write(pack_header())
            file.write(payload)
            file.write(footer_bytes)
            file.write(struct.pack(FOOTER_TRAILER_FMT, footer_offset, footer_crc))
        tmp_path.replace(self.path)
        self._closed = True
        return footer


class ColumnReader:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.footer, self._payload = _read_segment(self.path)

    def record_batches(self) -> list[pa.RecordBatch]:
        stream_bytes = _decompress(self._payload, self.footer.codec)
        reader = pa.ipc.open_stream(pa.BufferReader(stream_bytes))
        return [batch for batch in reader]

    def table(self) -> pa.Table:
        return pa.Table.from_batches(self.record_batches())


def write_column_segment(
    path: str | Path,
    batches: list[pa.RecordBatch] | tuple[pa.RecordBatch, ...],
    *,
    codec: Codec = "none",
) -> ColumnSegmentFooter:
    writer = ColumnWriter(path, codec=codec)
    for batch in batches:
        writer.append(batch)
    return writer.close()


def read_column_segment(path: str | Path) -> pa.Table:
    return ColumnReader(path).table()


def _batches_to_ipc(batches: list[pa.RecordBatch]) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return cast(bytes, sink.getvalue().to_pybytes())


def _read_segment(path: Path) -> tuple[ColumnSegmentFooter, bytes]:
    data = path.read_bytes()
    if len(data) < HEADER_SIZE + FOOTER_TRAILER_SIZE:
        raise CaracalError(code="CDB-7001", message=f"column segment is too small: {path}")
    if data[: len(MAGIC)] != MAGIC:
        raise CaracalError(code="CDB-7001", message=f"invalid column segment magic: {path}")

    footer_offset, expected_crc = struct.unpack(FOOTER_TRAILER_FMT, data[-FOOTER_TRAILER_SIZE:])
    if footer_offset < HEADER_SIZE or footer_offset > len(data) - FOOTER_TRAILER_SIZE:
        raise CaracalError(code="CDB-7001", message=f"invalid column footer offset: {path}")

    footer_bytes = data[footer_offset:-FOOTER_TRAILER_SIZE]
    actual_crc = zlib.crc32(footer_bytes) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        raise CaracalError(code="CDB-7001", message=f"column footer checksum mismatch: {path}")

    footer = ColumnSegmentFooter.from_json(json.loads(footer_bytes.decode("utf-8")))
    payload = data[HEADER_SIZE:footer_offset]
    if len(payload) != footer.payload_size:
        raise CaracalError(code="CDB-7001", message=f"column payload size mismatch: {path}")
    return footer, payload


def _compress(payload: bytes, codec: Codec) -> bytes:
    if codec == "none":
        return payload
    if codec == "zstd":
        try:
            zstd = import_module("zstandard")
        except ImportError as exc:
            raise CaracalError(code="CDB-9006", message="zstandard is not installed") from exc
        return cast(bytes, zstd.ZstdCompressor(level=3).compress(payload))
    if codec == "lz4":
        try:
            lz4_frame = import_module("lz4.frame")
        except ImportError as exc:
            raise CaracalError(code="CDB-9006", message="lz4 is not installed") from exc
        return cast(bytes, lz4_frame.compress(payload))
    raise ValueError(f"unsupported codec: {codec}")


def _decompress(payload: bytes, codec: Codec) -> bytes:
    if codec == "none":
        return payload
    if codec == "zstd":
        try:
            zstd = import_module("zstandard")
        except ImportError as exc:
            raise CaracalError(code="CDB-9006", message="zstandard is not installed") from exc
        return cast(bytes, zstd.ZstdDecompressor().decompress(payload))
    if codec == "lz4":
        try:
            lz4_frame = import_module("lz4.frame")
        except ImportError as exc:
            raise CaracalError(code="CDB-9006", message="lz4 is not installed") from exc
        return cast(bytes, lz4_frame.decompress(payload))
    raise ValueError(f"unsupported codec: {codec}")


__all__ = [
    "Codec",
    "ColumnReader",
    "ColumnSegmentFooter",
    "ColumnWriter",
    "read_column_segment",
    "write_column_segment",
]
