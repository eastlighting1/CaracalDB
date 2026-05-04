---
applies_to: v0.2.x
status: experimental
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Extensions

This page covers three power-user APIs: **Observability** for understanding query plans and
measuring execution, **UDFs** for extending Tuft with custom functions, and the **Viewer**
for visually inspecting a database bundle.

---

## Observability

Three complementary tools for understanding query behavior:

| Tool | Use case |
|---|---|
| **Explain** | Understand *what* the planner decided — the logical plan tree |
| **Profiling** | Measure *how long* each operator took |
| **Tracing** | Attach structured spans to any runtime work |

### Explain a query

```python
import caracaldb as cdb
from caracaldb.observability import explain_logical, render_explain
from caracaldb.lang.tuft import parse_tuft

with cdb.connect("biomedical") as db:
    program = parse_tuft("MATCH (g:Gene) WHERE g.chromosome = '17' RETURN g.symbol LIMIT 5")
    plan = explain_logical(program, db)
    print(render_explain(plan))
```

Output:

```text
LLimit (5)
  └─ LProject [symbol]
       └─ LSelection [chromosome = '17']
            └─ LNodeScan Gene
```

### Profile a pipeline

```python
from caracaldb.exec import ExecCtx, run_pipeline
from caracaldb.observability import profile_pipeline

ctx = ExecCtx()
with profile_pipeline(root_op, ctx) as report:
    batches = list(run_pipeline(root_op, ctx))

for entry in report.operators:
    print(f"{entry.name}: {entry.elapsed_ms:.2f} ms, {entry.output_rows} rows")
```

### Key objects

| Name | Description |
|---|---|
| `explain_logical` | Compile a parsed program into a logical plan tree. |
| `render_explain` | Render a logical plan tree as a human-readable string. |
| `profile_pipeline` | Context manager that wraps a pipeline to measure per-operator timing. |
| `get_tracer` / `set_tracer` | Get or replace the process-global span tracer. |
| `ExplainNode` | One node in the explain tree (name, children, annotations). |
| `ProfileReport` / `OperatorProfile` | Aggregated timing report and per-operator entries. |
| `Tracer` / `Span` / `SpanRecord` | Span recording interface and data types. |

### Reference

::: caracaldb.observability
    options:
      show_root_heading: false
      show_source: true

---

## User-Defined Functions

Extend Tuft with custom scalar functions. Python UDFs receive and return Arrow arrays;
Tuft UDFs stay inside the expression pipeline without crossing the Python boundary.

### Python UDFs

```python
import pyarrow as pa
import pyarrow.compute as pc
from caracaldb.udf import udf, UdfRegistry

registry = UdfRegistry()

@udf(registry=registry, name="normalize_symbol")
def normalize_symbol(symbols: pa.Array) -> pa.Array:
    return pc.utf8_upper(symbols)

# Use in a Tuft query via a Connection wired to this registry:
# MATCH (g:Gene) RETURN normalize_symbol(g.symbol)
```

### Tuft UDFs

```python
from caracaldb.udf import define_tuft_udf

zscore = define_tuft_udf(
    name="zscore",
    params=["x", "mean", "std"],
    body=("div", ("sub", ("col", "x"), ("col", "mean")), ("col", "std")),
)
```

### Key objects

| Name | Description |
|---|---|
| `udf` | Decorator that registers a Python function as a UDF. |
| `UdfRegistry` | Container for named Python UDFs passed to a `Connection`. |
| `define_tuft_udf` | Create a pure Tuft UDF from a parameter list and tuple-IR body. |
| `PyUdf` / `TuftUdf` | Internal records for registered UDFs. |

### Reference

::: caracaldb.udf
    options:
      show_root_heading: false
      show_source: true

---

## Viewer

The Viewer starts a local web server for visually inspecting a `.crcl` bundle without writing
any query code — showing classes, properties, node counts, edge counts, snapshots, and manifest fields.

!!! tip "Quick start from the CLI"
    ```
    caracal viewer mydb.crcl
    ```

```python
from caracaldb.viewer import serve

serve("mydb.crcl", port=8421, open_browser=True)
```

### Reference

::: caracaldb.viewer
    options:
      show_root_heading: false
      show_source: true

---

## See Also

- [Observability Guide](../guides/observability-explain-profile.md) — walkthrough with real queries
- [UDFs Guide](../guides/udfs-tuft-and-python.md) — UDF registration and usage
- [Packaging and CLI Guide](../guides/packaging-and-cli.md) — `caracal viewer` command reference
- [Query Engine](query-engine.md) — the logical plan and physical operators these tools inspect
