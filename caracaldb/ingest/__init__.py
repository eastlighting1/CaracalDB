"""Bulk ingestion utilities for CaracalDB."""

from caracaldb.ingest.parquet_loader import (
    ParquetLoadReport,
    ingest_edges_from_parquet,
    ingest_nodes_from_parquet,
)

__all__ = [
    "ParquetLoadReport",
    "ingest_edges_from_parquet",
    "ingest_nodes_from_parquet",
]
