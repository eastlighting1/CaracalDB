"""Transaction manager (single writer + many readers, snapshot isolation)."""

from caracaldb.tx.manager import (
    BEGIN_KIND,
    COMMIT_KIND,
    ROLLBACK_KIND,
    Transaction,
    TransactionManager,
    TxConflictError,
)

__all__ = [
    "BEGIN_KIND",
    "COMMIT_KIND",
    "ROLLBACK_KIND",
    "Transaction",
    "TransactionManager",
    "TxConflictError",
]
