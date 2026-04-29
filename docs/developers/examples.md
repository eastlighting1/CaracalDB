---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Examples

Examples connect tutorial narratives to runnable notebooks and sample bundles in the repository root.

## Notebook Catalog

| Notebook | Topic |
|---|---|
| `examples/biomed.ipynb` | Biomedical knowledge graph workflow |
| `examples/fraud.ipynb` | Fintech fraud graph workflow |
| `examples/recsys.ipynb` | Recommendation graph workflow |

## Data

Sample `.crcl` bundles live under `examples/data/`. Treat them as small fixtures for learning and smoke tests, not performance datasets.

## Run

```bash
uv run jupyter lab examples
```
