---
applies_to: v0.2.x
status: experimental
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Query Engine

The Query Engine covers the two layers between a parsed Tuft query and materialized Arrow output:
the **logical plan** (what to compute) and the **physical operators** (how to compute it).

Most application code never interacts with these APIs directly — `Connection.sql` handles
planning and execution end-to-end. These APIs are intended for planner contributors,
optimizer work, and pipeline-level integration tests.

---

## Logical Plan

The logical plan is a frozen, immutable tree that the planner builds from a parsed Tuft AST
before lowering to physical operators.

```text
LNodeScan               ← leaf: read a node class
  └─ LSelection         ← filter on a predicate
       └─ LProject      ← select output columns
            └─ LLimit   ← top-N result
```

```python
from caracaldb.plan import LNodeScan, LSelection, LProject, LLimit, walk

plan = LLimit(
    child=LProject(
        child=LSelection(
            child=LNodeScan(class_iri="caracaldb://class/Gene"),
            predicate=("eq", ("col", "chromosome"), ("lit", "17")),
        ),
        projections=(("col", "symbol"),),
    ),
    n=5,
)

for node in walk(plan):
    print(type(node).__name__)
# LLimit → LProject → LSelection → LNodeScan
```

### Key objects

| Name | Description |
|---|---|
| `LogicalOp` | Abstract base for all logical plan nodes. |
| `LNodeScan` | Scan all nodes of a given class. |
| `LSelection` | Filter rows by a predicate expression. |
| `LProject` | Select and rename output columns. |
| `LAggregate` | Group-by and aggregation (`COUNT`, `SUM`, `AVG`, etc.). |
| `LOrderBy` | Sort output by one or more columns. |
| `LLimit` | Restrict the number of output rows. |
| `walk` | Yield all plan nodes in pre-order. |

### Reference

::: caracaldb.plan
    options:
      show_root_heading: false
      show_source: true

---

## Physical Operators

Physical operators are pull-based: each operator implements `__iter__` yielding
`pyarrow.RecordBatch` values. `run_pipeline` drains a root operator to completion.
Each operator receives an `ExecCtx` carrying runtime state (snapshot LSN, tracer, budget).

```python
from caracaldb.exec import ExecCtx, run_pipeline
from caracaldb.exec.operators import NodeScanOperator, FilterOperator, ProjectOperator

ctx = ExecCtx()
pipeline = ProjectOperator(
    child=FilterOperator(
        child=NodeScanOperator(bundle=bundle, class_iri="caracaldb://class/Gene",
                               columns=("symbol", "chromosome")),
        predicate=("eq", ("col", "chromosome"), ("lit", "17")),
    ),
    projections=(("col", "symbol"),),
)

batches = list(run_pipeline(pipeline, ctx))
```

### Core API

| Name | Description |
|---|---|
| `ExecCtx` | Runtime context: snapshot LSN, tracer, memory budget. |
| `PhysicalOperator` | Abstract base for all pull-based physical operators. |
| `run_pipeline` | Drain a root operator into a list of `RecordBatch` values. |

### Operator catalogue

| Operator | Description |
|---|---|
| `NodeScanOperator` | Scan node column segments for a given class. |
| `ClosureScanOperator` | Scan with transitive class closure (`SUBCLASSOF*`). |
| `ExpandOperator` | Traverse outgoing edges from seed node ids (CSR-based). |
| `VarPathOperator` | Variable-length path expansion via repeated expansion. |
| `HashJoinOperator` | Probe-side hash join for multi-hop pattern assembly. |
| `FilterOperator` | Apply a predicate expression to each batch. |
| `ProjectOperator` | Select and rename output columns. |
| `RenameOperator` | Prefix all columns with an alias (used before joins). |
| `DropColumnsOperator` | Remove named columns after join deduplication. |
| `HashAggregateOperator` | Group-by and aggregation. |
| `TopKOperator` | Sort and emit the top-N rows. |
| `UnionAllOperator` | Concatenate batches from multiple child operators. |
| `KnnOperator` | k-nearest-neighbour lookup using the vector index. |
| `NeighborSampleOperator` | Sample fixed fan-out neighbors for GNN mini-batching. |
| `TripleScanOperator` | Scan triple-encoded edges. |
| `TriplePatternStep` | Single step of a SPARQL-style triple pattern traversal. |

### Reference

::: caracaldb.exec
    options:
      show_root_heading: false
      show_source: true

---

## See Also

- [Observability](extensions.md) — `explain_logical` renders a plan tree; `profile_pipeline` measures operators
- [Graph](graph.md) — CSR indexes that `ExpandOperator` and `NeighborSampleOperator` read
- [Pattern Queries Guide](../guides/pattern-queries.md) — writing multi-hop queries
