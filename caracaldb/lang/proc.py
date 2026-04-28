"""DEFINE PROCEDURE: minimal IF / FOR / WHILE control flow.

The procedure executor is intentionally narrow: it consumes a list of
``Step`` records and runs them against a mutable ``ProcedureContext``
holding variables and a transaction handle. Procedures are deterministic in
their state-mutation order (single-thread execution); branching primitives
(``IfStep``, ``ForStep``, ``WhileStep``) are evaluated via the same tuple-IR
``compile_expr`` engine so condition expressions reuse the existing
binder / typer pipeline.

Statements are not yet wired through the parser surface; M4 ships the
runtime so call sites (``conn.run_procedure(name, args)``) and the case-B
goldens can compose procedures programmatically. Parser glue lands when the
DEFINE PROCEDURE grammar is finalised.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from caracaldb.lang.diagnostics import CaracalError

ProcedureCallback = Callable[["ProcedureContext"], None]


@dataclass(slots=True)
class ProcedureContext:
    variables: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Step:
    """Marker base; subclasses are instantiated by the procedure builder."""


@dataclass(frozen=True, slots=True)
class CallStep(Step):
    fn: ProcedureCallback


@dataclass(frozen=True, slots=True)
class AssignStep(Step):
    var: str
    expr: Callable[[ProcedureContext], Any]


@dataclass(frozen=True, slots=True)
class IfStep(Step):
    predicate: Callable[[ProcedureContext], bool]
    then_steps: tuple[Step, ...]
    else_steps: tuple[Step, ...] = ()


@dataclass(frozen=True, slots=True)
class ForStep(Step):
    var: str
    iterable: Callable[[ProcedureContext], Sequence[Any]]
    body: tuple[Step, ...]


@dataclass(frozen=True, slots=True)
class WhileStep(Step):
    predicate: Callable[[ProcedureContext], bool]
    body: tuple[Step, ...]
    max_iterations: int = 10_000


@dataclass(slots=True)
class Procedure:
    name: str
    params: tuple[str, ...]
    steps: tuple[Step, ...]

    def run(self, args: Mapping[str, Any] | None = None) -> ProcedureContext:
        ctx = ProcedureContext()
        if args:
            for k in args:
                if k not in self.params:
                    raise CaracalError(code="CDB-6140", message=f"unknown procedure arg: {k!r}")
            ctx.variables.update(args)
        for missing in [p for p in self.params if p not in ctx.variables]:
            raise CaracalError(code="CDB-6140", message=f"missing procedure arg: {missing!r}")
        _execute(self.steps, ctx)
        return ctx


def _execute(steps: Sequence[Step], ctx: ProcedureContext) -> None:
    for step in steps:
        if isinstance(step, CallStep):
            step.fn(ctx)
        elif isinstance(step, AssignStep):
            ctx.variables[step.var] = step.expr(ctx)
        elif isinstance(step, IfStep):
            if step.predicate(ctx):
                _execute(step.then_steps, ctx)
            else:
                _execute(step.else_steps, ctx)
        elif isinstance(step, ForStep):
            for value in step.iterable(ctx):
                ctx.variables[step.var] = value
                _execute(step.body, ctx)
        elif isinstance(step, WhileStep):
            iterations = 0
            while step.predicate(ctx):
                if iterations >= step.max_iterations:
                    raise CaracalError(
                        code="CDB-6141",
                        message=f"WHILE exceeded {step.max_iterations} iterations",
                    )
                _execute(step.body, ctx)
                iterations += 1
        else:  # pragma: no cover
            raise CaracalError(code="CDB-6140", message=f"unsupported step: {type(step).__name__}")


__all__ = [
    "AssignStep",
    "CallStep",
    "ForStep",
    "IfStep",
    "Procedure",
    "ProcedureContext",
    "Step",
    "WhileStep",
]
