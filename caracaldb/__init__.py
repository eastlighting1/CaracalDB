"""CaracalDB Python package scaffold."""

from caracaldb._version import __version__
from caracaldb.api import Connection, Database, Result, connect

__all__ = ["Connection", "Database", "Result", "__version__", "connect"]
