# Changelog

All notable changes to this project will be documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to [Semantic Versioning](https://semver.org/).

## [0.2.6] - 2026-05-06

Patch release for graph query and adjacency index APIs.

### Added

- Added `db.nodes("Class").where(...).select(...)` via `NodeQuery` for
  Arrow-backed equality filtering, counting, first-row lookup, and row/table
  materialization.
- Added public adjacency helpers: `db.out(...)`, `db.in_(...)`, and
  `db.degree(...)`, backed by the existing lazy CSR/CSC graph indexes.
- Added `db.common_neighbors(...)` and `db.overlap(...)` for overlap-style
  recommendation queries over indexed neighbor sets.
- Added tests covering external `node_id` resolution, outgoing/incoming
  traversal, degree lookup, common-neighbor overlap, and stale adjacency index
  rebuilds.

### Changed

- Reuse cached CSR/CSC readers per relation and direction, while detecting
  stale on-disk index files by vertex and edge counts before traversal.
- Invalidate derived graph index files automatically after node or edge appends
  through `Database`.
- Documented the new database-level traversal API in the graph API reference.

## [0.2.5] - 2026-05-06

Patch release for Arrow-native graph table workflows and documentation polish.

### Added

- Added direct `pyarrow.Table` support for `insert_nodes`, `insert_node_table`,
  and `insert_edge_table`.
- Added `insert_node_table_arrow` and `insert_edge_table_arrow` helpers for
  Arrow-first node and edge ingestion without row-dict conversion.
- Added `node_table` and `edge_table` APIs for reading stored node and edge
  data back as Arrow tables.
- Added example `.crcl` bundles for simple, weighted, and complex graph
  inspection workflows.

### Changed

- Reorganized the API documentation into focused storage, graph, ML, IO,
  ontology, extension, and query-engine pages.
- Refreshed Quickstart, tour, guide, and concept examples with executable code
  snippets and package-relative example paths.
- Improved documentation diagrams, theme palette behavior, and quickstart code
  checking for absolute paths and output readability.

## [0.2.4] - 2026-05-03

Patch release for snapshot reads and storage scan cleanup.

### Added

- Added named snapshot management through `create_snapshot`, `list_snapshots`,
  and `release_snapshot`.
- Added Tuft `AS_OF SNAPSHOT 'name'` reads with node and edge visibility
  filtering against snapshot LSNs.
- Added coverage for successful snapshot reads, missing snapshot diagnostics,
  and post-snapshot node/edge filtering.

### Changed

- Updated the snapshot guide to describe the supported `AS_OF SNAPSHOT`
  workflow and its current schema-level caveat.
- Streamlined node and edge batch selection during storage scans.

## [0.2.3] - 2026-05-02

Patch release for the local `.crcl` web viewer workflow.

### Added

- Added a folder-rooted viewer mode so `caracal view` defaults to `data/` and
  the web UI can discover and switch between `.crcl` files.
- Added in-view direct path opening for `.crcl` files and bundles.
- Added a persisted light/dark theme toggle for the viewer.

### Changed

- Updated the viewer default inspection query to show all node data with
  `MATCH (n) RETURN n LIMIT 100`.
- Refined the viewer layout, table sizing, dark-mode contrast, and query/result
  proportions for browser use.

## [0.2.2] - 2026-05-02

Patch release for the v0.2.x documentation and release metadata line.

### Changed

- Updated release metadata and smoke tests for the v0.2.2 package version.
- Refreshed the ML integration docs to name the current v0.2.2 resource ingest
  behavior.
- Kept the Quickstart-adjacent documentation checks and generated docs gates
  aligned with the v0.2.x docs surface.

## [0.2.1] - 2026-04-30

Patch release for documentation polish and generated diagnostics alignment.

### Changed

- Updated Quickstart-adjacent public docs, ADRs, tutorials, and concept pages
  so v0.2.x support levels are clearer to outside readers.
- Registered documented CDB diagnostics in the runtime error table so the
  generated error index stays aligned with public per-code pages.
- Refreshed generated error metadata for the v0.2.x documentation line.

## [0.2.0] - 2026-04-30

Minor release for flexible graph/resource ingest.

### Added

- Added `Database.insert_triples(...)` for RDF-like subject/predicate/object
  input that maps `rdf:type` to classes, literal objects to properties, and
  resource objects to edges.
- Added `Database.import_resource(...)` and `Database.import_resources(...)`
  with shape detection for Neo4j-style JSON objects, IRI resources, triples,
  typed node rows, and typed edge rows.
- Added `Database.resource(...)` and `ResourceRef` so user-facing resource ids
  can resolve to CaracalDB internal ids and `caracaldb://resource/...` display
  IRIs.
- Added `Database.export_resource_turtle(...)` for explicit Turtle-style
  resource display without treating ontology IRIs as web pages.

### Changed

- Treat IRI as optional metadata in public ingest flows. Dataset ids such as
  `employee/E12345` remain stable user ids, while CaracalDB assigns compact
  internal ids for storage.
- Updated Quickstart, Tour, and ML/interop docs to describe typed graph tables
  and flexible resource ingest.

## [0.1.4] - 2026-04-29

Patch release for idempotent ontology hierarchy updates.

### Fixed

- `Database.define_class(..., superclass_iris=...)` now merges superclass
  metadata into an existing class definition instead of returning the old
  class unchanged.
- Existing packed databases can be reopened and upgraded with superclass
  metadata without deleting the `.crcl` file first.

### Added

- Added a regression test for reopening a database, adding superclass metadata
  to an existing class, and querying it with `SUBCLASSOF*`.

## [0.1.3] - 2026-04-29

Patch release for focused ontology class-closure queries.

### Added

- Added `Database.define_class(..., superclass_iris=...)` so public examples
  can register class hierarchy metadata without lower-level catalog wiring.
- Wired the focused `alias.class SUBCLASSOF* <IRI>` Tuft predicate into the
  public `db.sql()` MVP path through the existing closure scan operator.
- Added regression tests for `SUBCLASSOF*` queries with and without additional
  `AND` filters.

### Changed

- Updated ontology docs and the 30-Minute Tour to show the now-executable
  class-closure path while keeping broader reasoning surfaces marked
  experimental.

## [0.1.2] - 2026-04-29

Patch release for the beginner-facing API and release gates.

### Added

- Added `Database.define_class`, `Database.insert_nodes`, `Database.sql`,
  `Database.exec`, and `Result.rows` as small public convenience APIs for
  Quickstart-scale examples.
- Reworked the Quickstart minimal query so it no longer exposes PyArrow or
  internal node-store wiring.
- Clarified that v0.1.x ships a Python reference engine while the Rust core
  remains planned.

### Changed

- Relaxed benchmark regression tolerance in release and manual benchmark
  workflows to reduce runner-noise failures.
- Kept packed `.crcl` as the default `connect()` path in public docs.

## [0.1.1] - 2026-04-29

Patch release for the documentation cleanup and release pipeline.

### Changed

- Added GitHub Pages deployment for the MkDocs site.
- Fixed malformed Markdown code fences across public docs.
- Strengthened documentation code-block validation so malformed closing
  fences fail CI.
- Decoupled public milestone documentation from untracked internal
  milestone gate reports.
- Added generated error-index hints from the diagnostics table.
- Expanded implementation-status notes for experimental guides.

## [0.1.0] — 2026-04-27

The first public release of **CaracalDB** — an embedded, ontology-leaning,
Arrow-native analytical GraphDB. Reaches the M5 milestone (CDB-080…088 +
CDB-092…094) with 270+ tests, 4 micro-benchmarks, and three executable
tutorial notebooks.

### Added

- **Storage**: `.crcl` directory bundle, MANIFEST + WAL + checkpoint +
  recovery, per-class node store and per-property edge store with chunked
  Arrow IPC segments, sorted-blob IRI dictionary, page buffer pool.
- **Ontology**: catalog (FlatBuffers JSON envelope), class hierarchy DAG,
  Roaring class-closure bitmap, forward-chaining `INFER CLOSURE`
  (SYMMETRIC / TRANSITIVE).
- **Tuft language**: Lark grammar with arrow / label-union / hop-range
  pattern syntax, span-based diagnostics, binder, type checker, scalar /
  aggregate / graph / vector built-in registry.
- **Planner**: logical plan (NodeScan / Selection / Project / Aggregate
  / OrderBy / Limit), pattern compiler (NodeScan + Expand + Join chain),
  predicate pushdown / projection pruning, simple cost model, `WITH`
  pipeline splitter.
- **Executor**: pull-based PhysicalOperator base, NodeScan / Filter /
  Project, Expand (forward / reverse / both, optional eid alias),
  variable-length VarPath, HashJoin (inner / left + prefix), HashAggregate
  (count\* / sum / avg / min / max / collect), TopK, ClosureScan,
  TripleScan, KnnOperator, NeighborSample (layered fan-out + reservoir
  sampling), RandomWalk / Node2Vec, EXPORT SUBGRAPH AS ARROW.
- **Adjacency**: CSR / CSC builders (`np.argsort` + `bincount` +
  `cumsum`), mmap reader with vectorised `batch_neighbors`.
- **Vector index**: HNSW wrapper around `hnswlib` with atomic save / load.
- **MVCC + transactions**: SnapshotId, named snapshot registry, single
  writer + many readers with write-write conflict detection
  (`CDB-8002`), AS_OF SNAPSHOT plumbing through ExecCtx.
- **ML / GNN**: Subgraph IR, PyG / DGL / jraph adapters
  (importorskip-or-actionable-error), Lynxes GraphFrame bridge,
  `conn.neighbor_loader(...)`.
- **UDF / Procedure**: Pure Tuft UDFs, `@cdb.udf` Python decorator with
  type contract, IF / FOR / WHILE procedure runtime with iteration cap.
- **Feature store**: `OnlineFeatureView` with point-in-time lookup
  (p99 < 5 ms target on small tables).
- **Observability**: EXPLAIN tree renderer, PROFILE per-operator metrics,
  in-memory tracer (OTLP-compatible duck typing).
- **CLI**: `caracal init / run / explain / bench` (Typer).
- **Bench**: 1-hop / 2-hop / k-NN / NeighborSample harness with baseline
  comparison and a `bench.yml` GitHub Action.
- **Quality**: parser fuzz, recovery fault-injection matrix, MVCC
  reader-writer stress.
- **Examples**: `examples/{biomed,fraud,recsys}.ipynb` smoke-tested in CI.

### Documented

- `docs/01_language_spec.md` — `04_caracaldb_implementation.md` design
  documents (carried forward from M0).
- `docs/format/csr.md` — CSR / CSC on-disk format.
- `docs/milestones/M0…M5-gate.md` — milestone-by-milestone gate reports.

### Known limitations

- The M5 release intentionally **does not** include the user-facing Tuft
  language reference (`docs/user/tuft-ref.md`), error catalogue
  (`docs/errors/*.md`), or API reference (`docs/api/*.md`) — those are
  carried over to v0.2.0 (originally CDB-089/090/091).
- ``conn.sql`` still wraps the M1 single-class shortcut for unprefixed
  names; the pattern compiler's logical output is not yet wired through
  the public API. Multi-hop pattern queries should compose physical
  operators directly (see `tests/golden/case_a`).
- `AS_OF DATETIME` is reserved (`CDB-6021`) until per-row commit
  timestamps land.
- `repl` is not part of the v0.1.0 CLI; it lands alongside the docs in
  v0.2.0.

### Next milestones

The Rust-core port (`caracal-core` crate) begins after v0.1.0, keeping the
Python public API intact via `maturin`.
