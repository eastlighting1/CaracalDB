"""Global IRI dictionary (string ↔ id) backed by a sorted blob.

The format mirrors 04 §3.6: a CRCL-prefixed file containing a u32 entry count,
``offsets[num_entries+1]`` (u64), the concatenated UTF-8 blob, and a u32
footer CRC over the rest of the file. Strings are stored in sorted order so
``str → id`` is a binary search; ``id → str`` is a constant-time slice. The
loader keeps the offsets/blob memoryview-resident; persistence is rewritten
atomically on every merge to avoid partial-file states.
"""

from __future__ import annotations

import struct
import zlib
from bisect import bisect_left
from collections.abc import Iterable
from pathlib import Path

import numpy as np

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.header import HEADER_SIZE, MAGIC, pack_header

ENTRY_COUNT_FMT = "<I"
ENTRY_COUNT_SIZE = struct.calcsize(ENTRY_COUNT_FMT)
OFFSET_DTYPE = np.dtype("<u8")
FOOTER_FMT = "<I"
FOOTER_SIZE = struct.calcsize(FOOTER_FMT)


class DictStore:
    """In-memory dictionary with sorted-blob persistence."""

    def __init__(self, entries: list[str] | None = None) -> None:
        # Internal storage keeps the canonical (sorted, unique) string list.
        self._entries: list[str] = sorted(set(entries)) if entries else []

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._entries)

    def id_of(self, value: str) -> int | None:
        idx = bisect_left(self._entries, value)
        if idx < len(self._entries) and self._entries[idx] == value:
            return idx
        return None

    def str_of(self, dict_id: int) -> str:
        if dict_id < 0 or dict_id >= len(self._entries):
            raise CaracalError(
                code="CDB-7041",
                message=f"dictionary id out of range: {dict_id} (len={len(self._entries)})",
            )
        return self._entries[dict_id]

    def __contains__(self, value: object) -> bool:
        return isinstance(value, str) and self.id_of(value) is not None

    def __iter__(self) -> Iterable[str]:
        return iter(self._entries)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------
    def merge(self, values: Iterable[str]) -> dict[str, int]:
        """Add ``values`` to the dictionary and return ``{value: id}`` for the inputs.

        The id space is renumbered (sorted-blob format), so callers that cached
        ids before a merge must refresh them. M1 adapters resolve ids per query,
        so the simpler invariant is intentional.
        """
        inputs = list(values)
        before = set(self._entries)
        additions = [v for v in inputs if v not in before]
        if additions:
            self._entries = sorted(before.union(additions))
        result: dict[str, int] = {}
        for value in inputs:
            if value not in result:
                idx = self.id_of(value)
                if idx is None:
                    raise CaracalError(
                        code="CDB-7043",
                        message=f"dictionary merge failed to register value: {value!r}",
                    )
                result[value] = idx
        return result

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def to_bytes(self) -> bytes:
        encoded = [s.encode("utf-8") for s in self._entries]
        offsets = np.zeros(len(encoded) + 1, dtype=OFFSET_DTYPE)
        for i, payload in enumerate(encoded):
            offsets[i + 1] = offsets[i] + len(payload)
        blob = b"".join(encoded)

        body = struct.pack(ENTRY_COUNT_FMT, len(encoded)) + offsets.tobytes() + blob
        crc = zlib.crc32(body) & 0xFFFFFFFF
        return pack_header() + body + struct.pack(FOOTER_FMT, crc)

    def write_atomic(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f"{target.name}.tmp")
        tmp.write_bytes(self.to_bytes())
        tmp.replace(target)

    @classmethod
    def from_bytes(cls, data: bytes) -> DictStore:
        if len(data) < HEADER_SIZE + ENTRY_COUNT_SIZE + FOOTER_SIZE:
            raise CaracalError(code="CDB-7040", message="dictionary file is truncated")
        if data[: len(MAGIC)] != MAGIC:
            raise CaracalError(code="CDB-7040", message="invalid dictionary magic header")
        body = data[HEADER_SIZE:-FOOTER_SIZE]
        (expected_crc,) = struct.unpack(FOOTER_FMT, data[-FOOTER_SIZE:])
        actual_crc = zlib.crc32(body) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise CaracalError(code="CDB-7040", message="dictionary checksum mismatch")

        (count,) = struct.unpack(ENTRY_COUNT_FMT, body[:ENTRY_COUNT_SIZE])
        offsets_size = (count + 1) * OFFSET_DTYPE.itemsize
        if ENTRY_COUNT_SIZE + offsets_size > len(body):
            raise CaracalError(code="CDB-7040", message="dictionary offsets overflow")
        offsets = np.frombuffer(body, dtype=OFFSET_DTYPE, count=count + 1, offset=ENTRY_COUNT_SIZE)
        blob = body[ENTRY_COUNT_SIZE + offsets_size :]
        if int(offsets[-1]) != len(blob):
            raise CaracalError(code="CDB-7040", message="dictionary blob length mismatch")
        entries = [
            blob[int(offsets[i]) : int(offsets[i + 1])].decode("utf-8") for i in range(count)
        ]
        # Stored sorted by construction; trust the file.
        store = cls.__new__(cls)
        store._entries = entries
        return store

    @classmethod
    def read(cls, path: str | Path) -> DictStore:
        target = Path(path)
        if not target.is_file():
            raise CaracalError(code="CDB-7042", message=f"dictionary file not found: {target}")
        return cls.from_bytes(target.read_bytes())


__all__ = ["DictStore"]
