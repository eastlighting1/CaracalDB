"""CaracalDB Python package scaffold."""

from caracaldb._version import __version__
from caracaldb.api import Connection, Database, NodeQuery, ResourceRef, Result, connect

__all__ = ["Connection", "Database", "NodeQuery", "ResourceRef", "Result", "__version__", "connect"]
