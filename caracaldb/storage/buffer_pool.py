"""Fixed-size page buffer pool.

This is an M0 in-memory implementation with the API shape needed by later
storage operators: fetch, allocate, pin, unpin, mark dirty, and flush. Disk I/O
is injected through callbacks so node/edge stores can later wire in real file
managers without changing the page lifecycle.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import NewType

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.header import DEFAULT_PAGE_SIZE

PageId = NewType("PageId", int)
ReadPageFn = Callable[[PageId, int], bytes]
WritePageFn = Callable[[PageId, bytes], None]


@dataclass(frozen=True, slots=True)
class BufferPoolStats:
    capacity_pages: int
    page_size: int
    resident_pages: int
    pinned_pages: int
    dirty_pages: int
    hits: int
    misses: int
    evictions: int


@dataclass(slots=True)
class PageFrame:
    page_id: PageId
    data: bytearray
    pin_count: int = 0
    dirty: bool = False
    access_history: list[int] = field(default_factory=list)

    @property
    def is_pinned(self) -> bool:
        return self.pin_count > 0


class PageGuard:
    """Context manager that automatically unpins a page."""

    def __init__(self, pool: BufferPool, frame: PageFrame) -> None:
        self._pool = pool
        self.frame = frame
        self._released = False

    @property
    def data(self) -> bytearray:
        return self.frame.data

    def mark_dirty(self) -> None:
        self._pool.mark_dirty(self.frame.page_id)

    def release(self, *, dirty: bool = False) -> None:
        if not self._released:
            self._pool.unpin_page(self.frame.page_id, dirty=dirty)
            self._released = True

    def __enter__(self) -> PageGuard:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()


class BufferPool:
    def __init__(
        self,
        *,
        capacity_pages: int,
        page_size: int = DEFAULT_PAGE_SIZE,
        read_page: ReadPageFn | None = None,
        write_page: WritePageFn | None = None,
        lru_k: int = 2,
    ) -> None:
        if capacity_pages <= 0:
            raise ValueError("capacity_pages must be positive")
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        if lru_k <= 0:
            raise ValueError("lru_k must be positive")

        self.capacity_pages = capacity_pages
        self.page_size = page_size
        self.lru_k = lru_k
        self._read_page = read_page or self._zero_read
        self._write_page = write_page
        self._frames: dict[PageId, PageFrame] = {}
        self._clock = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def fetch_page(self, page_id: PageId | int) -> PageFrame:
        normalized = _page_id(page_id)
        frame = self._frames.get(normalized)
        if frame is not None:
            self._hits += 1
            self._pin(frame)
            return frame

        self._misses += 1
        self._ensure_capacity()
        data = self._read_page(normalized, self.page_size)
        frame = PageFrame(page_id=normalized, data=_page_data(data, self.page_size), pin_count=1)
        self._record_access(frame)
        self._frames[normalized] = frame
        return frame

    def new_page(self, page_id: PageId | int) -> PageFrame:
        normalized = _page_id(page_id)
        existing = self._frames.get(normalized)
        if existing is not None:
            self._pin(existing)
            existing.data[:] = bytes(self.page_size)
            existing.dirty = True
            return existing

        self._ensure_capacity()
        frame = PageFrame(
            page_id=normalized,
            data=bytearray(self.page_size),
            pin_count=1,
            dirty=True,
        )
        self._record_access(frame)
        self._frames[normalized] = frame
        return frame

    def pin_page(self, page_id: PageId | int) -> PageFrame:
        return self.fetch_page(page_id)

    def guard_page(self, page_id: PageId | int) -> PageGuard:
        return PageGuard(self, self.fetch_page(page_id))

    def unpin_page(self, page_id: PageId | int, *, dirty: bool = False) -> None:
        normalized = _page_id(page_id)
        frame = self._frames.get(normalized)
        if frame is None:
            raise CaracalError(code="CDB-7005", message=f"page is not resident: {int(normalized)}")
        if frame.pin_count <= 0:
            raise CaracalError(code="CDB-7005", message=f"page is not pinned: {int(normalized)}")
        frame.pin_count -= 1
        if dirty:
            frame.dirty = True

    def mark_dirty(self, page_id: PageId | int) -> None:
        normalized = _page_id(page_id)
        frame = self._frames.get(normalized)
        if frame is None:
            raise CaracalError(code="CDB-7005", message=f"page is not resident: {int(normalized)}")
        frame.dirty = True

    def flush_page(self, page_id: PageId | int) -> None:
        normalized = _page_id(page_id)
        frame = self._frames.get(normalized)
        if frame is None:
            raise CaracalError(code="CDB-7005", message=f"page is not resident: {int(normalized)}")
        self._flush_frame(frame)

    def flush_all(self) -> None:
        for frame in list(self._frames.values()):
            self._flush_frame(frame)

    def stats(self) -> BufferPoolStats:
        return BufferPoolStats(
            capacity_pages=self.capacity_pages,
            page_size=self.page_size,
            resident_pages=len(self._frames),
            pinned_pages=sum(1 for frame in self._frames.values() if frame.is_pinned),
            dirty_pages=sum(1 for frame in self._frames.values() if frame.dirty),
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
        )

    def contains(self, page_id: PageId | int) -> bool:
        return _page_id(page_id) in self._frames

    def _pin(self, frame: PageFrame) -> None:
        frame.pin_count += 1
        self._record_access(frame)

    def _record_access(self, frame: PageFrame) -> None:
        self._clock += 1
        frame.access_history.append(self._clock)
        if len(frame.access_history) > self.lru_k:
            del frame.access_history[: -self.lru_k]

    def _ensure_capacity(self) -> None:
        if len(self._frames) < self.capacity_pages:
            return
        victim = self._choose_victim()
        self._flush_frame(victim)
        del self._frames[victim.page_id]
        self._evictions += 1

    def _choose_victim(self) -> PageFrame:
        candidates = [frame for frame in self._frames.values() if not frame.is_pinned]
        if not candidates:
            raise CaracalError(
                code="CDB-7005",
                message="buffer pool has no evictable pages; all frames are pinned",
            )

        def key(frame: PageFrame) -> tuple[int, int]:
            history = frame.access_history
            kth_access = history[0] if len(history) >= self.lru_k else -1
            latest_access = history[-1] if history else -1
            return (kth_access, latest_access)

        return min(candidates, key=key)

    def _flush_frame(self, frame: PageFrame) -> None:
        if not frame.dirty:
            return
        if self._write_page is not None:
            self._write_page(frame.page_id, bytes(frame.data))
        frame.dirty = False

    def _zero_read(self, _page_id: PageId, page_size: int) -> bytes:
        return bytes(page_size)


def _page_id(page_id: PageId | int) -> PageId:
    value = int(page_id)
    if value < 0:
        raise ValueError("page_id must be non-negative")
    return PageId(value)


def _page_data(data: bytes, page_size: int) -> bytearray:
    if len(data) > page_size:
        raise CaracalError(code="CDB-7005", message="read page returned too many bytes")
    page = bytearray(page_size)
    page[: len(data)] = data
    return page


__all__ = [
    "BufferPool",
    "BufferPoolStats",
    "PageFrame",
    "PageGuard",
    "PageId",
]
