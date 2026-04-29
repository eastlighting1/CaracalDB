---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Error Policy

Error codes are part of the public debugging interface. A good CaracalDB error should identify the failing layer, explain what happened, and point users toward a fix without exposing internal implementation noise.

## Code Families

| Prefix | Scope |
|---|---|
| `TF-1xxx` | Lexical Tuft errors |
| `TF-2xxx` | Tuft parser errors |
| `TF-3xxx` | Name binding and catalog lookup |
| `TF-4xxx` | Type checking |
| `TF-5xxx` | Query semantics |
| `TF-6xxx` | Graph budget and execution limits |
| `TF-7xxx` | Index or graph structure integrity |
| `TF-8xxx` | Transaction-facing aliases |
| `TF-95xx` | Ontology constraints |
| `CDB-*` | Engine, storage, transaction, and I/O errors |

## Adding A Code

1. Add the code to `ERROR_TABLE` in `caracaldb.lang.diagnostics`.
2. Use a short title that can fit in a table.
3. Add a hint when the user can take a clear next action.
4. Regenerate the public error index.

```bash
uv run python tools/gen_errors.py
uv run python tools/gen_errors.py --check
```
## Message Style

Error messages should describe the failed condition. Hints should describe the repair. Keep them separate so tooling can render them differently.

```text
CDB-8002: transaction conflict
help: another transaction committed a conflicting write; retry on a fresh snapshot
```
## Common Pitfall

Do not create a new code for every call site. Reuse a code when the user action is the same, and specialize the message with the local detail.
