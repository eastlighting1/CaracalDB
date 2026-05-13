"""SQL-to-operator compilation orchestration.

The Python reference compiler still owns most helper implementations in
``caracaldb.api``. This module is the structural boundary used by the Rust
transition so API-facing code no longer needs to host the top-level compile
decision tree.
"""

from __future__ import annotations

from typing import Any

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta


def compile_sql_operator(
    db: Any,
    text: str,
) -> tuple[Any, Any, int | None, str, tuple[str, ...]]:
    from caracaldb import api as api_mod

    program = api_mod.parse_tuft(text)
    try:
        api_mod.bind_program(program, db.catalog)
    except CaracalError as exc:
        if exc.code not in {"TF-3001", "TF-3004"}:
            raise
    if len(program.statements) != 1 or not isinstance(program.statements[0], ta.QueryStmt):
        raise CaracalError(code="CDB-6020", message="profile/explain supports one query statement")
    query = program.statements[0].query
    assert query is not None
    if api_mod._is_multi_element_pattern(query):
        plan = api_mod._compile_pattern_query(query, db)
        return (
            api_mod._build_pattern_pipeline(plan, db),
            plan.snapshot,
            plan.limit,
            "pattern_match",
            (),
        )
    plan = api_mod._compile_query(query, db)
    return (
        api_mod._build_pipeline(plan, db),
        plan.snapshot,
        plan.limit,
        "node_match",
        plan.indexes_used,
    )
