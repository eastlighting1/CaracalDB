---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# UDFs In Tuft And Python

Use this guide when built-ins are not enough and you need a custom expression.

!!! warning "Experimental surface"
    Python UDF registration is available as a reference API, while Tuft-level UDF execution is still being shaped. Treat Tuft UDF examples as design guidance unless your installed version documents execution support.

## Problem

CaracalDB distinguishes pure Tuft UDFs from Python UDFs. Pure Tuft UDFs stay inside the expression pipeline; Python UDFs call Python code over Arrow batches.

## Steps

1. Define a Python UDF.

```python
import pyarrow as pa
from caracaldb.udf import udf

@udf(returns=pa.int64(), arg_types=(pa.int64(),))
def add_one(x):
    return pa.compute.add(x, 1)
```
2. Register it in a `UdfRegistry`.

```python
from caracaldb.udf import UdfRegistry

registry = UdfRegistry()
registry.register(add_one)
```
3. Use pure Tuft UDFs for expression-level reuse when the body can stay in tuple IR.

## Verification

Validate argument count, argument types, return type, and output length against a small Arrow batch.

## Common Pitfalls

- Python UDFs run once per batch, not once per scalar.
- Return a `pyarrow.Array` or a value convertible to one.
- Use built-ins when possible; they are easier to optimize.

## Related ADR

UDF security, determinism, and packaging rules should be captured before broad plugin-style UDF loading.
