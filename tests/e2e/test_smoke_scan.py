"""End-to-end smoke: Parquet → ingest → conn.sql → row count.

The WBS asks for 100k nodes; CI environments without much spare disk benefit
from a smaller default, so the row count is parameterised through an env var.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import caracaldb as cdb
from caracaldb.ingest import ingest_nodes_from_parquet
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle


def _row_count() -> int:
    return int(os.environ.get("CARACAL_E2E_ROWS", "100000"))


def test_smoke_parquet_to_node_scan(tmp_path: Path) -> None:
    n = _row_count()
    parquet_path = tmp_path / "genes.parquet"
    pq.write_table(
        pa.table(
            {
                "symbol": [f"G{i:06d}" for i in range(n)],
                "chromosome": [str(((i * 7) % 23) + 1) for i in range(n)],
            }
        ),
        str(parquet_path),
    )

    bundle_path = tmp_path / "bio"
    bundle = create_bundle(bundle_path)
    catalog = Catalog.empty()
    catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
    save_catalog(bundle, catalog)

    _, report = ingest_nodes_from_parquet(
        bundle,
        parquet_path=parquet_path,
        class_iri="http://example.org/Gene",
        local_name="Gene",
        chunksize=16_384,
    )
    assert report.rows_written == n
    assert report.rows_quarantined == 0

    db = cdb.connect(bundle_path, format="bundle")
    conn = db.cursor()
    table = conn.sql("MATCH (g:Gene) RETURN g.symbol").arrow()
    assert table.num_rows == n
    # Sanity: filtered count matches the synthetic distribution exactly.
    chr5 = conn.sql("MATCH (g:Gene) WHERE g.chromosome = '5' RETURN g.symbol").arrow()
    expected = sum(1 for i in range(n) if str(((i * 7) % 23) + 1) == "5")
    assert chr5.num_rows == expected
