---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Notebooks

The repository keeps runnable notebooks under the top-level `examples/` directory. The documentation pages in this section are narrative versions of those notebooks.

## Notebook Catalog

| Notebook | Tutorial | Purpose |
|---|---|---|
| `examples/biomed.ipynb` | [Case A: Biomedical Graph](case-a-biomedical.md) | Gene, tissue, interaction, traversal, and aggregation patterns |
| `examples/fraud.ipynb` | [Case B: Fintech Graph](case-b-fintech.md) | Accounts, transfers, features, kNN, transactions, and snapshots |
| `examples/recsys.ipynb` | [Case C: Recommendation Graph](case-c-recsys.md) | User-item sampling, embeddings, subgraphs, and ML export |

## Generate Sample Bundles

```bash
cd examples
uv run python generate_dbs.py
```
This creates small `.crcl` bundles under `examples/data/` for local experimentation.

## Run A Notebook

```bash
uv sync --extra dev --extra docs
uv run jupyter lab examples
```
If your environment does not include Jupyter, install it in your development environment rather than adding it to the runtime package dependencies.

## Verification

For CI-grade behavior, prefer the golden tests under `tests/golden/case_a`, `tests/golden/case_b`, and `tests/golden/case_c`.
