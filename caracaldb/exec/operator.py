"""Physical operator base.

Every physical operator follows the pull-based Volcano-ish model:

    open()  → next_batch() → next_batch() → ... → None → close()

``next_batch()`` returns ``pa.RecordBatch | None``; ``None`` is the
end-of-stream sentinel. The base class wraps lifecycle bookkeeping (open
guard, close idempotency) so subclasses only override ``_open`` /
``_next_batch`` / ``_close``.

The Python prototype is single-threaded; ``ExecCtx`` carries the per-query
state (snapshot id, RNG seed, runtime budget) that operators need to read.
Spill-to-disk and worker pool integration land in M5.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError


@dataclass(slots=True)
class ExecCtx:
    snapshot_id: str | None = None
    seed: int = 0
    batch_size: int = 65_536
    metadata: dict[str, Any] = field(default_factory=dict)


class PhysicalOperator(ABC):
    """Pull-based Arrow operator base.

    Subclasses override ``_open(ctx)`` (initialisation that needs the runtime
    context), ``_next_batch()`` (return next batch or ``None``), and
    ``_close()`` (release resources). They MUST treat ``next_batch()`` as
    idempotent post-EOS — once it returns ``None`` it must keep returning
    ``None``.
    """

    name: str = "Operator"

    def __init__(self) -> None:
        self._opened = False
        self._closed = False

    def open(self, ctx: ExecCtx) -> None:
        if self._opened:
            raise CaracalError(code="CDB-6001", message=f"{self.name}: already open")
        self._open(ctx)
        self._opened = True

    def next_batch(self) -> pa.RecordBatch | None:
        if not self._opened:
            raise CaracalError(code="CDB-6002", message=f"{self.name}: next_batch before open")
        if self._closed:
            return None
        return self._next_batch()

    def close(self) -> None:
        if self._closed:
            return
        if self._opened:
            self._close()
        self._closed = True

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------
    @abstractmethod
    def _next_batch(self) -> pa.RecordBatch | None: ...

    def _open(self, ctx: ExecCtx) -> None:  # pragma: no cover - default no-op
        return None

    def _close(self) -> None:  # pragma: no cover - default no-op
        return None


def run_pipeline(root: PhysicalOperator, ctx: ExecCtx | None = None) -> Iterator[pa.RecordBatch]:
    """Execute a pipeline rooted at ``root`` and yield record batches.

    The function takes ownership of opening/closing the operator tree and is
    safe to use in a ``for`` loop or ``list()`` consumer.
    """
    ctx = ctx or ExecCtx()
    root.open(ctx)
    try:
        while True:
            batch = root.next_batch()
            if batch is None:
                return
            yield batch
    finally:
        root.close()


__all__ = ["ExecCtx", "PhysicalOperator", "run_pipeline"]
