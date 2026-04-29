---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# UDF API

UDF APIs cover Python batch UDFs and pure Tuft UDF expression wrappers. Python UDFs operate over Arrow arrays; Tuft UDFs stay inside the tuple-IR expression pipeline.

## Common Entry Points

| Name | Use |
|---|---|
| `udf` | Decorate a Python function as a UDF. |
| `UdfRegistry` | Register and call Python UDFs. |
| `define_tuft_udf` | Define a pure Tuft UDF from parameters and a tuple-IR body. |

## Reference

::: caracaldb.udf
    options:
      show_root_heading: false
      show_source: true
