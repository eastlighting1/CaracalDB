---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Contributing

This page mirrors the public contributor entry point for the documentation site.

## Before A PR

Run the focused checks for the area you changed, then run the standard quality gate:

```bash
uv run ruff check bench caracaldb tests tools
uv run black --check bench caracaldb tests tools
uv run pytest -x -q
uv run mkdocs build --strict -f mkdocs.yml
```
## Documentation Rule

Public behavior needs public documentation. If a change adds an error code, API, guide workflow, or release-facing behavior, update the matching page in the same PR.

## More

See [Developer Contributing](developers/contributing.md) for the contributor workflow and [Testing Strategy](developers/testing-strategy.md) for test selection.
