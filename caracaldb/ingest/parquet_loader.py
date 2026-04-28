"""Parquet → node/edge bulk loader (M1).

The loader streams Parquet input through PyArrow's record-batch reader, applies
optional column renames, validates each chunk against the destination schema,
and routes failed rows to an isolation list rather than aborting the whole
import. The loader does not perform IRI→nid lookups for edges; callers are
expected to supply ``src``/``dst`` columns that are already UInt64 nids
(typically by joining against an upstream node-staging table). Edge property
columns flow through unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.column_store import Codec
from caracaldb.storage.edge_store import (
    DST_COLUMN,
    EID_COLUMN,
    SRC_COLUMN,
    EdgeStore,
    open_edge_store,
)
from caracaldb.storage.node_store import (
    NID_COLUMN,
    NodeStore,
    open_node_store,
)


@dataclass(slots=True)
class ParquetLoadReport:
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    quarantined: list[dict[str, object]] = field(default_factory=list)
    chunks: int = 0


def _rename(table: pa.Table, mapping: Mapping[str, str] | None) -> pa.Table:
    if not mapping:
        return table
    new_names = [mapping.get(name, name) for name in table.column_names]
    return table.rename_columns(new_names)


def _drop_columns(table: pa.Table, drop: set[str]) -> pa.Table:
    keep = [name for name in table.column_names if name not in drop]
    return table.select(keep)


def _resolve_chunksize(chunksize: int) -> int:
    if chunksize <= 0:
        raise CaracalError(code="CDB-7030", message="chunksize must be positive")
    return chunksize


def ingest_nodes_from_parquet(
    bundle: Bundle,
    *,
    parquet_path: str | Path,
    class_iri: str,
    local_name: str,
    column_map: Mapping[str, str] | None = None,
    create: bool = True,
    chunksize: int = 65536,
    codec: Codec = "none",
    quarantine_limit: int = 100,
) -> tuple[NodeStore, ParquetLoadReport]:
    """Stream a Parquet file into the node store for ``class_iri``.

    Rows whose conversion to the established schema fails are isolated in
    ``ParquetLoadReport.quarantined`` instead of aborting the import.
    """
    chunksize = _resolve_chunksize(chunksize)
    store = open_node_store(
        bundle,
        class_iri=class_iri,
        local_name=local_name,
        create=create,
        codec=codec,
    )
    report = ParquetLoadReport()

    pf = pq.ParquetFile(str(parquet_path))
    for raw in pf.iter_batches(batch_size=chunksize):
        report.rows_read += raw.num_rows
        table = pa.Table.from_batches([raw])
        table = _rename(table, column_map)
        # Always strip an inbound 'nid' column — the store assigns it.
        if NID_COLUMN in table.column_names:
            table = _drop_columns(table, {NID_COLUMN})
        if table.num_rows == 0:
            continue
        try:
            store.append(table)
            report.rows_written += table.num_rows
            report.chunks += 1
        except CaracalError as exc:
            _quarantine(report, table, exc, quarantine_limit)
    return store, report


def ingest_edges_from_parquet(
    bundle: Bundle,
    *,
    parquet_path: str | Path,
    property_iri: str,
    local_name: str,
    src_class_iri: str | None = None,
    dst_class_iri: str | None = None,
    column_map: Mapping[str, str] | None = None,
    create: bool = True,
    chunksize: int = 65536,
    codec: Codec = "none",
    quarantine_limit: int = 100,
) -> tuple[EdgeStore, ParquetLoadReport]:
    chunksize = _resolve_chunksize(chunksize)
    store = open_edge_store(
        bundle,
        property_iri=property_iri,
        local_name=local_name,
        src_class_iri=src_class_iri,
        dst_class_iri=dst_class_iri,
        create=create,
        codec=codec,
    )
    report = ParquetLoadReport()

    pf = pq.ParquetFile(str(parquet_path))
    for raw in pf.iter_batches(batch_size=chunksize):
        report.rows_read += raw.num_rows
        table = pa.Table.from_batches([raw])
        table = _rename(table, column_map)
        if EID_COLUMN in table.column_names:
            table = _drop_columns(table, {EID_COLUMN})
        for required in (SRC_COLUMN, DST_COLUMN):
            if required not in table.column_names:
                raise CaracalError(
                    code="CDB-7031",
                    message=(
                        f"Parquet edge source missing required column {required!r}; "
                        "use column_map= to rename"
                    ),
                )
        # Coerce src/dst to UInt64 if expressible; otherwise quarantine the chunk.
        try:
            table = _coerce_uint64(table, [SRC_COLUMN, DST_COLUMN])
        except CaracalError as exc:
            _quarantine(report, table, exc, quarantine_limit)
            continue
        if table.num_rows == 0:
            continue
        try:
            store.append(table)
            report.rows_written += table.num_rows
            report.chunks += 1
        except CaracalError as exc:
            _quarantine(report, table, exc, quarantine_limit)
    return store, report


def _coerce_uint64(table: pa.Table, columns: list[str]) -> pa.Table:
    arrays: list[pa.ChunkedArray] = []
    names = table.column_names
    for name in names:
        column = table[name]
        if name in columns and column.type != pa.uint64():
            try:
                column = column.cast(pa.uint64(), safe=True)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise CaracalError(
                    code="CDB-7031",
                    message=f"cannot cast column {name!r} to UInt64: {exc}",
                ) from exc
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def _quarantine(
    report: ParquetLoadReport,
    table: pa.Table,
    exc: CaracalError,
    limit: int,
) -> None:
    report.rows_quarantined += table.num_rows
    if len(report.quarantined) < limit:
        report.quarantined.append(
            {
                "rows": table.num_rows,
                "code": exc.code,
                "message": exc.message,
            }
        )


__all__ = [
    "ParquetLoadReport",
    "ingest_edges_from_parquet",
    "ingest_nodes_from_parquet",
]
