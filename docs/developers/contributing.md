---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Contributing

Contributions should keep CaracalDB easy to reason about: small changes, explicit tests, public docs for public behavior, and no accidental exposure of private design notes.

## Local Checks

```bash
uv run ruff check bench caracaldb tests tools
uv run black --check bench caracaldb tests tools
uv run pytest -x -q
uv run mkdocs build --strict -f mkdocs.yml
```
## Pull Request Rule

If a change adds a user-visible behavior, update the relevant guide, reference page, or error page in the same PR.

## Documentation Rule

Public docs live in the allowlisted `docs/` paths. Do not move internal notes into the public site until they have been rewritten for users or contributors.
