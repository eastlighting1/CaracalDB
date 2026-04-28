"""``EXPORT SUBGRAPH AS ARROW`` operator.

Materialises a ``Subgraph`` into a single Arrow IPC file. The container is a
two-column ``RecordBatch`` whose rows index per-class node tables and
per-property edge tables: ``(key, kind, payload)``. ``payload`` is itself a
serialised Arrow IPC stream of the per-class / per-property table, so the
file is fully self-describing without a side-car manifest.

Layout (logical):

    container.arrow:
        RecordBatch[ (key="nodes/<class>", kind="node",  payload=<bytes>),
                     (key="edges/<prop>",  kind="edge",  payload=<bytes>),
                     (key="meta",          kind="meta",  payload=<bytes>) ]
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml.subgraph import Subgraph

NODES_PREFIX = "nodes/"
EDGES_PREFIX = "edges/"
META_KEY = "meta"

_CONTAINER_SCHEMA = pa.schema(
    [
        pa.field("key", pa.string()),
        pa.field("kind", pa.string()),
        pa.field("payload", pa.binary()),
    ]
)


def _serialize_table(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _deserialize_table(payload: bytes) -> pa.Table:
    reader = ipc.open_stream(pa.BufferReader(payload))
    return pa.Table.from_batches(list(reader))


def export_subgraph_to_arrow(subgraph: Subgraph, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if not subgraph.nodes and not subgraph.edges:
        raise CaracalError(code="CDB-6160", message="cannot export an empty subgraph")

    keys: list[str] = []
    kinds: list[str] = []
    payloads: list[bytes] = []
    for cls, tbl in subgraph.nodes.items():
        keys.append(f"{NODES_PREFIX}{cls}")
        kinds.append("node")
        payloads.append(_serialize_table(tbl))
    for prop, tbl in subgraph.edges.items():
        keys.append(f"{EDGES_PREFIX}{prop}")
        kinds.append("edge")
        payloads.append(_serialize_table(tbl))
    if subgraph.meta:
        meta_table = pa.table(
            {
                "key": pa.array(list(subgraph.meta.keys())),
                "value": pa.array(list(subgraph.meta.values())),
            }
        )
        keys.append(META_KEY)
        kinds.append("meta")
        payloads.append(_serialize_table(meta_table))

    container = pa.RecordBatch.from_arrays(
        [pa.array(keys), pa.array(kinds), pa.array(payloads, type=pa.binary())],
        schema=_CONTAINER_SCHEMA,
    )

    tmp = target.with_name(f"{target.name}.tmp")
    with tmp.open("wb") as f, ipc.new_file(f, _CONTAINER_SCHEMA) as writer:
        writer.write_batch(container)
    tmp.replace(target)
    return target


def import_subgraph_from_arrow(path: str | Path) -> Subgraph:
    target = Path(path)
    sg = Subgraph()
    with target.open("rb") as f, ipc.open_file(f) as reader:
        for i in range(reader.num_record_batches):
            batch = reader.get_batch(i)
            keys = batch.column("key").to_pylist()
            kinds = batch.column("kind").to_pylist()
            payloads = batch.column("payload").to_pylist()
            for key, kind, payload in zip(keys, kinds, payloads, strict=True):
                if kind == "node":
                    sg.add_nodes(key[len(NODES_PREFIX) :], _deserialize_table(payload))
                elif kind == "edge":
                    sg.add_edges(key[len(EDGES_PREFIX) :], _deserialize_table(payload))
                elif kind == "meta":
                    meta_tbl = _deserialize_table(payload)
                    for k, v in zip(
                        meta_tbl["key"].to_pylist(),
                        meta_tbl["value"].to_pylist(),
                        strict=True,
                    ):
                        sg.meta[k] = v
    return sg


__all__ = [
    "EDGES_PREFIX",
    "META_KEY",
    "NODES_PREFIX",
    "export_subgraph_to_arrow",
    "import_subgraph_from_arrow",
]
