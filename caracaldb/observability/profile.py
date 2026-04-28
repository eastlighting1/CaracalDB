"""``PROFILE`` execution-statistics collector.

Wraps a physical operator tree and instruments each ``next_batch`` call to
record per-operator wall-time, row counts, and (best-effort) peak memory in
bytes. ``profile_pipeline`` mirrors ``run_pipeline`` so callers can swap one
in for the other without changing the surrounding code.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator


@dataclass(slots=True)
class OperatorProfile:
    name: str
    rows: int = 0
    batches: int = 0
    elapsed_ms: float = 0.0
    peak_bytes: int = 0


@dataclass(slots=True)
class ProfileReport:
    operators: list[OperatorProfile] = field(default_factory=list)
    wall_ms: float = 0.0

    def to_text(self) -> str:
        if not self.operators:
            return f"Wall: {self.wall_ms:.2f}ms (no operators measured)"
        header = f"Wall: {self.wall_ms:.2f}ms"
        rows = ["Operator              Rows      Time      Peak"]
        for prof in self.operators:
            rows.append(
                f"{prof.name:<22}{prof.rows:>8}  {prof.elapsed_ms:>6.2f}ms  {prof.peak_bytes:>8}B"
            )
        return header + "\n" + "\n".join(rows)


def _instrument(op: PhysicalOperator, report: ProfileReport) -> PhysicalOperator:
    """Recursively wrap ``op`` and any child operators referenced via ``_child``
    or ``_build`` / ``_probe`` attributes.
    """
    for attr in ("_child", "_build", "_probe"):
        inner = getattr(op, attr, None)
        if isinstance(inner, PhysicalOperator):
            setattr(op, attr, _instrument(inner, report))
    profile = OperatorProfile(name=op.name)
    report.operators.append(profile)

    original_next = op.next_batch

    def instrumented_next() -> pa.RecordBatch | None:
        start = time.perf_counter()
        batch = original_next()
        elapsed = (time.perf_counter() - start) * 1000.0
        profile.elapsed_ms += elapsed
        if batch is not None:
            profile.rows += batch.num_rows
            profile.batches += 1
            profile.peak_bytes = max(
                profile.peak_bytes,
                sum(buf.size for col in batch.columns for buf in col.buffers() if buf is not None),
            )
        return batch

    op.next_batch = instrumented_next  # type: ignore[method-assign]
    return op


def profile_pipeline(
    root: PhysicalOperator, ctx: ExecCtx | None = None
) -> tuple[Iterator[pa.RecordBatch], ProfileReport]:
    """Run a pipeline and return (iterator, report). The report is filled
    progressively as the iterator is consumed.
    """
    ctx = ctx or ExecCtx()
    report = ProfileReport()
    instrumented = _instrument(root, report)

    def _drive() -> Iterator[pa.RecordBatch]:
        wall_start = time.perf_counter()
        instrumented.open(ctx)
        try:
            while True:
                batch = instrumented.next_batch()
                if batch is None:
                    return
                yield batch
        finally:
            instrumented.close()
            report.wall_ms = (time.perf_counter() - wall_start) * 1000.0

    return _drive(), report


__all__ = ["OperatorProfile", "ProfileReport", "profile_pipeline"]
