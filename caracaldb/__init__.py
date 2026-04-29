"""CaracalDB Python package scaffold."""

from caracaldb._version import __version__
from caracaldb.api import Connection, Database, ResourceRef, Result, connect

__all__ = ["Connection", "Database", "ResourceRef", "Result", "__version__", "connect"]
