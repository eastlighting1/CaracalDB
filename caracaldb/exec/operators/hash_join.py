"""HashJoin operator.

The build side is materialised in full during ``open()``: rows are grouped by
their join key and stored as ``key → list[row_index]`` over a single Arrow
``Table``. The probe side is then streamed; each probe batch produces an
output batch by extracting matching build rows via ``pyarrow.compute.take``.

Spill-to-disk lands in M5 (CDB-041 carries a future hook); for M2 the build
side is bounded by the M1 single-class node count and fits comfortably in
memory.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

import pyarrow as pa

from caracaldb.exec.operator import ExecCtx, PhysicalOperator
from caracaldb.lang.diagnostics import CaracalError

JoinKind = Literal["inner", "left"]


def _drain(op: PhysicalOperator) -> list[pa.RecordBatch]:
    out: list[pa.RecordBatch] = []
    while True:
        batch = op.next_batch()
        if batch is None:
            return out
        out.append(batch)


def _rename_fields(table: pa.Table, mapping: dict[str, str]) -> pa.Table:
    new_names = [mapping.get(name, name) for name in table.column_names]
    return table.rename_columns(new_names)


class HashJoinOperator(PhysicalOperator):
    name = "HashJoin"

    def __init__(
        self,
        build: PhysicalOperator,
        probe: PhysicalOperator,
        *,
        build_key: str,
        probe_key: str,
        kind: JoinKind = "inner",
        build_prefix: str | None = None,
        probe_prefix: str | None = None,
    ) -> None:
        super().__init__()
        if kind not in ("inner", "left"):
            raise CaracalError(code="CDB-6040", message=f"unsupported join kind: {kind}")
        self._build = build
        self._probe = probe
        self._build_key = build_key
        self._probe_key = probe_key
        self._kind = kind
        self._build_prefix = build_prefix
        self._probe_prefix = probe_prefix
        self._build_table: pa.Table | None = None
        self._index: dict[object, list[int]] = {}

    def _open(self, ctx: ExecCtx) -> None:
        self._build.open(ctx)
        try:
            batches = _drain(self._build)
        finally:
            self._build.close()
        if batches:
            schema = batches[0].schema
            table = pa.Table.from_batches(batches, schema=schema)
        else:
            # Empty build side: outer-style left-join still has rows from probe.
            table = pa.table({self._build_key: pa.array([], type=pa.uint64())})
        if self._build_key not in table.column_names:
            raise CaracalError(
                code="CDB-6040",
                message=f"build side missing key column {self._build_key!r}",
            )
        self._build_table = table
        for i, k in enumerate(table.column(self._build_key).to_pylist()):
            self._index.setdefault(k, []).append(i)
        self._probe.open(ctx)

    def _close(self) -> None:
        self._probe.close()

    def _next_batch(self) -> pa.RecordBatch | None:
        assert self._build_table is not None
        while True:
            probe = self._probe.next_batch()
            if probe is None:
                return None
            joined = self._join_one(probe)
            if joined is not None and joined.num_rows > 0:
                return joined

    # ------------------------------------------------------------------
    def _join_one(self, probe: pa.RecordBatch) -> pa.RecordBatch | None:
        if self._probe_key not in probe.schema.names:
            raise CaracalError(
                code="CDB-6040",
                message=f"probe side missing key column {self._probe_key!r}",
            )
        keys = probe.column(self._probe_key).to_pylist()
        build_indices: list[int] = []
        probe_indices: list[int] = []
        for pi, key in enumerate(keys):
            matches = self._index.get(key, ())
            if matches:
                for bi in matches:
                    build_indices.append(bi)
                    probe_indices.append(pi)
            elif self._kind == "left":
                build_indices.append(-1)
                probe_indices.append(pi)
        if not probe_indices:
            return None

        # Build side projection (use take with a -1-aware mapping for left joins).
        assert self._build_table is not None
        if self._kind == "left" and any(bi == -1 for bi in build_indices):
            # Replace -1 with 0 and null out via a mask afterwards.
            placeholder = [0 if bi == -1 else bi for bi in build_indices]
            build_take = self._build_table.take(pa.array(placeholder, type=pa.int64()))
            null_mask = pa.array([bi == -1 for bi in build_indices])
            arrays: list[pa.Array] = []
            for col in build_take.columns:
                arrays.append(pa.compute.if_else(null_mask, pa.scalar(None, type=col.type), col))
            build_take = pa.Table.from_arrays(arrays, names=build_take.column_names)
        else:
            build_take = self._build_table.take(pa.array(build_indices, type=pa.int64()))

        # Probe side projection.
        probe_table = pa.Table.from_batches([probe])
        probe_take = probe_table.take(pa.array(probe_indices, type=pa.int64()))

        # Apply optional prefixes to disambiguate overlapping column names.
        if self._build_prefix:
            build_take = _rename_fields(
                build_take,
                {name: f"{self._build_prefix}.{name}" for name in build_take.column_names},
            )
        if self._probe_prefix:
            probe_take = _rename_fields(
                probe_take,
                {name: f"{self._probe_prefix}.{name}" for name in probe_take.column_names},
            )

        # Concatenate columns side-by-side (build || probe).
        out_arrays: list[pa.Array] = []
        out_names: list[str] = []
        for name, col in zip(build_take.column_names, build_take.columns, strict=False):
            out_arrays.append(col.combine_chunks())
            out_names.append(name)
        for name, col in zip(probe_take.column_names, probe_take.columns, strict=False):
            out_arrays.append(col.combine_chunks())
            out_names.append(name)
        return pa.RecordBatch.from_arrays(
            [arr if isinstance(arr, pa.Array) else arr.chunk(0) for arr in out_arrays],
            names=out_names,
        )


__all__ = ["HashJoinOperator", "JoinKind"]


def _build_iter(items: Iterable[pa.RecordBatch]) -> list[pa.RecordBatch]:
    return list(items)
