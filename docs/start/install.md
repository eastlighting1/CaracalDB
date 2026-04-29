---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Install

Use this page to install CaracalDB and verify that the CLI and Python package are available.

## Package Install

```bash
pip install caracaldb
```
or:

```bash
uv add caracaldb
```
## Repository Checkout

```bash
uv sync --extra dev --extra docs
uv run python -c "import caracaldb; print(caracaldb.__version__)"
uv run caracal --help
```
## Verification

A working install can import `caracaldb`, print the package version, and display the `caracal` command help.
