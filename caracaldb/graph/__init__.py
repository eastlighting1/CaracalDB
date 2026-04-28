"""Graph adjacency builders and readers (CSR / CSC)."""

from caracaldb.graph.csc_builder import build_csc
from caracaldb.graph.csr_builder import CSRBuildResult, build_csr
from caracaldb.graph.csr_format import (
    CSR_FLAG_HAS_EIDS,
    CSR_FOOTER_FMT,
    CSR_FOOTER_SIZE,
    CSR_HEAD_FMT,
    CSR_HEAD_SIZE,
    read_csr,
    write_csr,
)
from caracaldb.graph.csr_reader import CsrReader

__all__ = [
    "CSR_FLAG_HAS_EIDS",
    "CSR_FOOTER_FMT",
    "CSR_FOOTER_SIZE",
    "CSR_HEAD_FMT",
    "CSR_HEAD_SIZE",
    "CSRBuildResult",
    "CsrReader",
    "build_csc",
    "build_csr",
    "read_csr",
    "write_csr",
]
