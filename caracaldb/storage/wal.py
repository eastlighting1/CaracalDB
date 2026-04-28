"""Append-only Write-Ahead Log (M1).

The WAL stores ordered ``WalRecord`` entries inside rolling segments under
``<bundle>/wal/NNNNNN.wal``. Each segment is a CRCL header followed by a
sequence of records. A record is laid out as::

    [ u64 lsn ]
    [ u64 prev_lsn ]
    [ u32 kind_len ][ kind utf-8 ]
    [ u32 payload_len ][ payload bytes ]
    [ u32 crc32 ]   # over the rest of the record

Segment roll-over occurs when the next append would exceed ``roll_size``.
``flush()`` drives ``os.fsync`` according to the requested mode (``off``,
``group``, ``on``); the M1 implementation keeps things synchronous within a
single process — group commit batching lands in M3 (CDB-062).
"""

from __future__ import annotations

import struct
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Literal

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.header import HEADER_SIZE, MAGIC, pack_header

SyncMode = Literal["off", "group", "on"]
SEGMENT_GLOB = "*.wal"
DEFAULT_ROLL_SIZE = 64 * 1024 * 1024  # 64 MiB
RECORD_HEAD_FMT = "<QQ"  # lsn, prev_lsn
RECORD_HEAD_SIZE = struct.calcsize(RECORD_HEAD_FMT)


@dataclass(frozen=True, slots=True)
class WalRecord:
    lsn: int
    prev_lsn: int
    kind: str
    payload: bytes

    def encode(self) -> bytes:
        kind_bytes = self.kind.encode("utf-8")
        head = struct.pack(RECORD_HEAD_FMT, self.lsn, self.prev_lsn)
        body = (
            head
            + struct.pack("<I", len(kind_bytes))
            + kind_bytes
            + struct.pack("<I", len(self.payload))
            + self.payload
        )
        crc = zlib.crc32(body) & 0xFFFFFFFF
        return body + struct.pack("<I", crc)


@dataclass(slots=True)
class _Segment:
    path: Path
    file: IO[bytes]
    size: int

    def close(self) -> None:
        try:
            self.file.flush()
        finally:
            self.file.close()


@dataclass(slots=True)
class WalStats:
    last_lsn: int
    segments: int
    bytes_written: int
    flushes: int = 0
    appends: int = 0
    rolls: int = 0


class Wal:
    def __init__(
        self,
        directory: str | Path,
        *,
        roll_size: int = DEFAULT_ROLL_SIZE,
        sync: SyncMode = "group",
    ) -> None:
        if roll_size <= HEADER_SIZE + 64:
            raise CaracalError(code="CDB-7050", message="roll_size is too small")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.roll_size = roll_size
        self.sync_mode: SyncMode = sync
        self._segment: _Segment | None = None
        self._last_lsn = 0
        self._stats = WalStats(last_lsn=0, segments=0, bytes_written=0)
        # Initialise last_lsn from existing segments without keeping them open.
        for record in iter_all_records(self.directory):
            self._last_lsn = max(self._last_lsn, record.lsn)
        self._stats.last_lsn = self._last_lsn

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._segment is not None:
            self._segment.close()
            self._segment = None

    def __enter__(self) -> Wal:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Append / flush
    # ------------------------------------------------------------------
    def append(self, kind: str, payload: bytes = b"") -> int:
        prev = self._last_lsn
        next_lsn = prev + 1
        record = WalRecord(lsn=next_lsn, prev_lsn=prev, kind=kind, payload=payload)
        encoded = record.encode()

        segment = self._ensure_segment(extra_bytes=len(encoded))
        segment.file.write(encoded)
        segment.size += len(encoded)
        self._last_lsn = next_lsn
        self._stats.last_lsn = next_lsn
        self._stats.bytes_written += len(encoded)
        self._stats.appends += 1
        if self.sync_mode == "on":
            self._flush_segment(segment)
        return next_lsn

    def flush(self) -> None:
        if self._segment is None or self.sync_mode == "off":
            return
        self._flush_segment(self._segment)

    @property
    def last_lsn(self) -> int:
        return self._last_lsn

    def stats(self) -> WalStats:
        return WalStats(
            last_lsn=self._stats.last_lsn,
            segments=self._stats.segments,
            bytes_written=self._stats.bytes_written,
            flushes=self._stats.flushes,
            appends=self._stats.appends,
            rolls=self._stats.rolls,
        )

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------
    def iter_since(self, lsn: int) -> Iterator[WalRecord]:
        if self._segment is not None:
            self._segment.file.flush()
        for record in iter_all_records(self.directory):
            if record.lsn > lsn:
                yield record

    def truncate_before(self, lsn: int) -> int:
        """Delete WAL segments whose records are entirely ≤ ``lsn``.

        Returns the number of segments removed. The currently open segment is
        never deleted; rolling forward is the caller's responsibility.
        """
        removed = 0
        for path in sorted(self.directory.glob(SEGMENT_GLOB)):
            if self._segment is not None and path == self._segment.path:
                continue
            max_lsn = _segment_max_lsn(path)
            if max_lsn is not None and max_lsn <= lsn:
                path.unlink()
                removed += 1
        return removed

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _ensure_segment(self, *, extra_bytes: int) -> _Segment:
        if self._segment is None:
            self._resume_or_create()
        assert self._segment is not None
        if self._segment.size + extra_bytes > self.roll_size:
            self._segment.close()
            self._segment = None
            self._stats.rolls += 1
            self._create_new_segment()
        assert self._segment is not None
        return self._segment

    def _resume_or_create(self) -> None:
        existing = sorted(self.directory.glob(SEGMENT_GLOB))
        if existing:
            tail = existing[-1]
            if tail.stat().st_size < self.roll_size:
                f = tail.open("ab")
                self._segment = _Segment(path=tail, file=f, size=tail.stat().st_size)
                self._stats.segments = len(existing)
                return
        self._create_new_segment()

    def _create_new_segment(self) -> None:
        existing = sorted(self.directory.glob(SEGMENT_GLOB))
        next_index = (int(existing[-1].stem) + 1) if existing else 1
        new_path = self.directory / f"{next_index:06d}.wal"
        f = new_path.open("wb")
        f.write(pack_header())
        f.flush()
        self._segment = _Segment(path=new_path, file=f, size=HEADER_SIZE)
        self._stats.segments = len(existing) + 1

    def _flush_segment(self, segment: _Segment) -> None:
        try:
            segment.file.flush()
            import os as _os

            _os.fsync(segment.file.fileno())
            self._stats.flushes += 1
        except OSError as exc:
            raise CaracalError(code="CDB-7051", message=f"WAL fsync failed: {exc}") from exc


def _read_record(buffer: bytes, offset: int) -> tuple[WalRecord, int] | None:
    if offset + RECORD_HEAD_SIZE > len(buffer):
        return None
    lsn, prev_lsn = struct.unpack(RECORD_HEAD_FMT, buffer[offset : offset + RECORD_HEAD_SIZE])
    cursor = offset + RECORD_HEAD_SIZE
    if cursor + 4 > len(buffer):
        return None
    (kind_len,) = struct.unpack("<I", buffer[cursor : cursor + 4])
    cursor += 4
    if cursor + kind_len + 4 > len(buffer):
        return None
    kind = buffer[cursor : cursor + kind_len].decode("utf-8")
    cursor += kind_len
    (payload_len,) = struct.unpack("<I", buffer[cursor : cursor + 4])
    cursor += 4
    if cursor + payload_len + 4 > len(buffer):
        return None
    payload = buffer[cursor : cursor + payload_len]
    cursor += payload_len
    (expected_crc,) = struct.unpack("<I", buffer[cursor : cursor + 4])
    cursor += 4
    body = buffer[offset : cursor - 4]
    actual_crc = zlib.crc32(body) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        raise CaracalError(code="CDB-7052", message="WAL record CRC mismatch")
    return WalRecord(lsn=lsn, prev_lsn=prev_lsn, kind=kind, payload=payload), cursor


def iter_segment_records(path: Path) -> Iterator[WalRecord]:
    data = path.read_bytes()
    if len(data) < HEADER_SIZE or data[: len(MAGIC)] != MAGIC:
        raise CaracalError(code="CDB-7053", message=f"invalid WAL segment header: {path}")
    cursor = HEADER_SIZE
    while cursor < len(data):
        result = _read_record(data, cursor)
        if result is None:
            # Truncated tail — stop cleanly. Recovery treats this as the safe boundary.
            return
        record, cursor = result
        yield record


def iter_all_records(directory: str | Path) -> Iterator[WalRecord]:
    root = Path(directory)
    if not root.is_dir():
        return
    for path in sorted(root.glob(SEGMENT_GLOB)):
        yield from iter_segment_records(path)


def _segment_max_lsn(path: Path) -> int | None:
    last: int | None = None
    for record in iter_segment_records(path):
        last = record.lsn
    return last


__all__ = [
    "DEFAULT_ROLL_SIZE",
    "SyncMode",
    "Wal",
    "WalRecord",
    "WalStats",
    "iter_all_records",
    "iter_segment_records",
]
