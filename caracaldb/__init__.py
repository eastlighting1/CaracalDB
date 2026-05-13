"""CaracalDB Python package scaffold."""

from caracaldb._version import __version__
from caracaldb.api import (
    Connection,
    Database,
    GraphRAGResult,
    NodeQuery,
    ResourceRef,
    Result,
    connect,
    cosine_distance,
    cosine_similarity,
    dot_product,
    l2_distance,
)
from caracaldb.engine import EngineSelection, resolve_engine, rust_available

__all__ = [
    "Connection",
    "Database",
    "GraphRAGResult",
    "NodeQuery",
    "ResourceRef",
    "Result",
    "__version__",
    "connect",
    "cosine_distance",
    "cosine_similarity",
    "dot_product",
    "EngineSelection",
    "l2_distance",
    "resolve_engine",
    "rust_available",
]
