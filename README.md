<h1 align="center">CaracalDB</h1>

<p align="center">
  <strong>An Embedded, Ontology-Leaning, Arrow-Native Analytical GraphDB for KG and GNN Workflows.</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/caracaldb/"><img src="https://img.shields.io/pypi/v/caracaldb" alt="PyPI version"></a>
  <img src="https://img.shields.io/pypi/pyversions/caracaldb" alt="Python versions">
  <img src="https://img.shields.io/badge/status-pre--alpha-D97706" alt="pre-alpha">
  <img src="https://img.shields.io/badge/engine-Python_reference-2563EB" alt="python-reference-engine">
  <img src="https://img.shields.io/badge/Rust_core-planned-CE412B" alt="rust-core-planned">
  <img src="https://img.shields.io/badge/license-Apache--2.0-4B5563" alt="Apache-2.0">
</p>

<p align="center">
  <a href="#why-caracaldb">Why CaracalDB</a> |
  <a href="#quickstart">Quickstart</a> |
  <a href="#api-overview">API Overview</a> |
  <a href="#architecture">Architecture</a>
</p>

`CaracalDB` is an embedded graph database for knowledge graphs, ontology-aware query planning, GNN sampling, and ML feature workflows. The current implementation is a Python reference engine that validates the `.crcl` storage format, Tuft query language, planner surface, and user-facing API. A Rust core is planned, but it is not part of the current package.

## Quickstart

### Install

```bash
pip install caracaldb
```

or

```bash
uv add caracaldb
```

For development from a repository checkout:

```bash
uv sync --extra dev
uv run pytest
```

## 30-Second Quickstart

```python
import caracaldb as cdb
from pathlib import Path

path = Path(cdb.__file__).resolve().parents[1] / "examples/data/example_simple.crcl"
with cdb.connect(path, mode="ro") as db:
    rows = db.sql("MATCH (p:Person) RETURN p.name, p.city LIMIT 2").rows()
    print(rows)
```

Expected output:

```text
[{'name': 'Alice', 'city': 'New York'}, {'name': 'Bob', 'city': 'London'}]
```

The current Python reference query path supports a single `MATCH (alias:Class)` node pattern with `WHERE`, `RETURN`, and `LIMIT`. Broader graph patterns, richer binding, and multi-hop query execution are tracked in the milestone docs.

## Start Here

- Language spec: `docs/01_language_spec.md`
- Engine spec: `docs/02_engine_spec.md`
- Modeling case study: `docs/03_user_modeling_case_study.md`
- Implementation plan: `docs/04_caracaldb_implementation.md`
- Work breakdown: `docs/05_wbs.md`
- Error index: `docs/errors/TF-INDEX.md`
- Examples: `examples/`
- Benchmark CI: `.github/workflows/bench.yml`

## Why CaracalDB

CaracalDB is built around explicit storage, ontology, and execution boundaries:

```mermaid
flowchart LR
    A["Tuft query"] --> B["Parser and diagnostics"]
    B --> C["Binder and ontology catalog"]
    C --> D["Logical plan"]
    D --> E["Physical operators"]
    E --> F["Arrow RecordBatch"]
    G[".crcl bundle or packed file"] --> H["Catalog, WAL, snapshots, stores"]
    H --> E
    H --> I["CSR / CSC graph indexes"]
    I --> J["Traversal, sampling, and ML adapters"]
```

- Embedded-first operation: no required server process.
- Tuft combines Cypher-like graph patterns with SPARQL-like ontology semantics.
- Arrow is the execution boundary for scan results and downstream analytics.
- CSR and CSC graph layouts support traversal, neighbor sampling, and GNN workflows.
- Snapshot, WAL, and packed `.crcl` storage paths are tested as first-class engine pieces.
- The Python API is intentionally small; Rust core work is planned after the reference behavior is stable.

## Benchmarks

Benchmark automation is scaffolded in the repository:

- CI automation: `.github/workflows/bench.yml`
- Benchmark harness tests: `tests/test_bench_pkg/`

The CLI exposes a benchmark command for registered scenarios:

```bash
caracal bench NAME
```

## CLI

The CLI is available as `caracal`:

```bash
# Initialise an empty .crcl bundle
caracal init demo

# Run a Tuft query from a file
caracal run demo.crcl --file query.tuft

# Print an explain tree
caracal explain demo.crcl Gene

# Pack and unpack .crcl storage
caracal pack demo.crcl -o demo-packed.crcl
caracal unpack demo-packed.crcl -o restored.crcl
```

## API Overview

### Top-level functions and types

| API | Description |
|---|---|
| `cdb.connect(path, mode="rw", format="auto")` | Open or create a `.crcl` database |
| `Database.cursor()` | Create a query connection |
| `Database.catalog` | Access the ontology catalog |
| `Database.bundle` | Access the underlying storage bundle |
| `Database.open_node_store(class_iri)` | Open a node store for a class |
| `Connection.sql(text, params=None)` | Execute supported Tuft query text |
| `Result.arrow()` | Return a `pyarrow.Table` |
| `Result.record_batches()` | Iterate `pyarrow.RecordBatch` results |

### CLI commands

| Command | Description |
|---|---|
| `caracal init PATH` | Initialise an empty `.crcl` bundle |
| `caracal run BUNDLE --file QUERY` | Execute a Tuft query and emit JSON |
| `caracal explain BUNDLE QUERY` | Print a logical explain tree |
| `caracal bench NAME` | Run a registered microbenchmark |
| `caracal pack BUNDLE -o FILE` | Package a directory bundle into a packed `.crcl` file |
| `caracal unpack FILE -o DIR` | Restore a packed `.crcl` file into a bundle |

## Architecture

CaracalDB is organized as a Python package with focused modules for language, planning, execution, storage, graph layout, ontology, and ML interop:

```text
caracaldb/
  api.py                 Public connect / Database / Connection / Result API
  cli/                   Typer command-line interface
  lang/tuft/             Tuft parser, AST, binder, transformer, typing
  plan/                  Logical plan nodes, rules, cost model, pattern compiler
  exec/                  Physical operators and execution context
  storage/               .crcl bundle, WAL, snapshots, pack/unpack, stores
  graph/                 CSR / CSC builders, readers, HNSW support
  onto/                  Catalog, hierarchy, closure, reasoner
  ingest/                Parquet ingestion helpers
  ml/                    Subgraph, neighbor loader, framework adapters
  observability/         Explain, profile, and tracing helpers
  udf/                   Python and Tuft UDF registry
```

### Execution Pipeline

```text
Tuft text
    |
    v
Parser -> Binder -> Logical plan -> Physical pipeline
                                      |
                                      v
NodeScan / Filter / Project / Expand / Join / Aggregate operators
                                      |
                                      v
Arrow RecordBatch -> pyarrow.Table
```

### Storage Pipeline

```text
.crcl path
    |
    +-- packed single file
    |       |
    |       v
    |   temporary working bundle -> repacked on close
    |
    +-- directory bundle
            |
            v
    manifest / catalog / WAL / snapshots / node stores / edge stores / indexes
```

## Repository Layout

```text
caracaldb/   Python package source
tests/       Unit, golden, property, and end-to-end tests
schema/      FlatBuffers and storage/catalog schemas
docs/        Design documents and user documentation
examples/    Runnable examples and case-study notebooks
```

## Project Status

CaracalDB is pre-release and not yet suitable for production use. M0 through M5 are accepted in `docs/milestones/`, and the engine is currently in the v0.2.x docs and benchmark sweep. Multi-hop pattern matching, rel-type unions, and the `degree()` graph built-in are wired through `Connection.sql`; variable-length paths, multi-label nodes, and the remaining graph-topology built-ins (`neighbors`, `shortest_path`, `k_hop`) are tracked carry-overs.

The closest peers — embedded analytical graph engines — are [kuzu](https://github.com/kuzudb/kuzu), [DuckPGQ](https://duckpgq.org), and Memgraph's embedded library mode. Comparisons against server-tier graph databases (Neo4j Enterprise, Neptune, TigerGraph) are not the right reference frame for an embedded `.crcl` file.

## Non-goals

CaracalDB is deliberately scoped against a small set of features that belong in a different product:

- **No server process, no network protocol.** No Bolt, no gRPC, no HTTP endpoint. The analogue is DuckDB or SQLite, not Neo4j Enterprise.
- **No multi-writer concurrency.** A `.crcl` bundle is opened by one writer; readers can hold older snapshots. Coordinating multiple writers belongs to a layer above the engine.
- **No authentication, authorization, or row-level ACLs.** Filesystem permissions are the only access boundary. Embedded governance belongs to the host application or a server tier.
- **No SPARQL endpoint, no full OWL-DL.** CaracalDB supports OWL-RL-style class/property hierarchies and IRI identity; RDF/Turtle is an import concern, not an engine surface (see [docs/adr/0005-rdf-as-import-only.md](docs/adr/0005-rdf-as-import-only.md)).
- **No bundled LLM / GraphRAG framework.** CaracalDB is a substrate for GNN and KG workflows; LLM glue is the host application's job. The Arrow `record_batches()` / `arrow()` outputs are the integration contract.

The one governance-adjacent feature that *does* fit the embedded model is **deterministic, named snapshots with content-addressable manifests**, plus a `caracal diff` command for auditing graph versions. That lets an outer governance layer pin and diff a database without the engine taking on multi-tenant concerns.

## Contributing

Start with `docs/04_caracaldb_implementation.md` and `docs/05_wbs.md`. The core project constraints are:

1. Keep the engine embedded-first.
2. Preserve Arrow-native execution boundaries.
3. Treat Tuft diagnostics and golden parser tests as public contract.
4. Keep `.crcl` storage reproducible through WAL, snapshots, and pack/unpack tests.
5. Measure performance changes before claiming speedups.

## License

Apache License 2.0. See `LICENSE`.
