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
    "l2_distance",
]
