"""Source diagnostics and error rendering for Tuft and language phases."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, TextIO

from rich.console import Console, ConsoleRenderable
from rich.panel import Panel
from rich.text import Text

if TYPE_CHECKING:
    from caracaldb.lang.tuft.ast import Span


class DiagnosticSeverity(StrEnum):
    ERROR = "error"
    WARNING = "warning"
    NOTE = "note"


@dataclass(frozen=True, slots=True)
class ErrorInfo:
    code: str
    title: str
    hint: str | None = None


ERROR_TABLE: dict[str, ErrorInfo] = {
    "TF-1001": ErrorInfo(
        "TF-1001",
        "invalid character",
        hint="remove the unsupported character or quote it inside a string literal",
    ),
    "TF-1002": ErrorInfo(
        "TF-1002",
        "unterminated string",
        hint="add the closing quote or escape an embedded quote with a backslash",
    ),
    "TF-2001": ErrorInfo(
        "TF-2001",
        "unexpected token",
        hint="check the token near the highlighted span against the Tuft grammar",
    ),
    "TF-2015": ErrorInfo(
        "TF-2015",
        "missing pattern after MATCH",
        hint="add a node or relationship pattern immediately after MATCH",
    ),
    "TF-3001": ErrorInfo(
        "TF-3001",
        "undefined prefix",
        hint="declare the namespace prefix before using it in an IRI or qualified name",
    ),
    "TF-3004": ErrorInfo(
        "TF-3004",
        "unknown class",
        hint="register the class in the catalog or use an existing class local name",
    ),
    "TF-3005": ErrorInfo(
        "TF-3005",
        "unknown property",
        hint="check the property name against the catalog for the matched class",
    ),
    "TF-4001": ErrorInfo(
        "TF-4001",
        "type mismatch",
        hint="compare operands with compatible types or cast explicitly where supported",
    ),
    "TF-4010": ErrorInfo(
        "TF-4010",
        "implicit cast forbidden",
        hint="rewrite the expression so both sides have the same expected type",
    ),
    "TF-5003": ErrorInfo(
        "TF-5003",
        "aggregate not allowed in WHERE",
        hint="move aggregate predicates to a grouped or post-aggregation query stage",
    ),
    "TF-6012": ErrorInfo(
        "TF-6012",
        "graph function limit exceeded",
        hint="lower the traversal fanout, depth, or row budget before retrying",
    ),
    "TF-7004": ErrorInfo(
        "TF-7004",
        "index corruption detected",
        hint="rebuild the affected index from trusted source data",
    ),
    "TF-8002": ErrorInfo(
        "TF-8002",
        "transaction conflict",
        hint="retry the transaction from a fresh snapshot",
    ),
    "CDB-8002": ErrorInfo(
        "CDB-8002",
        "transaction conflict",
        hint="another transaction committed a conflicting write; retry on a fresh snapshot",
    ),
    "TF-9501": ErrorInfo(
        "TF-9501",
        "ontology constraint violated",
        hint="fix the catalog or data so it satisfies the declared ontology constraint",
    ),
}


def docs_url(code: str) -> str:
    return f"https://caracaldb.dev/errors/{code}"


def lookup_error(code: str) -> ErrorInfo:
    return ERROR_TABLE.get(code, ErrorInfo(code, "unknown error"))


@dataclass(frozen=True, slots=True)
class SourceLocation:
    line: int
    column: int


@dataclass(frozen=True, slots=True)
class SourceExcerpt:
    line_number: int
    line: str
    column: int
    width: int


def offset_to_location(source: str, offset: int) -> SourceLocation:
    offset = max(0, min(offset, len(source)))
    line = source.count("\n", 0, offset) + 1
    line_start = source.rfind("\n", 0, offset) + 1
    column = offset - line_start + 1
    return SourceLocation(line=line, column=column)


def excerpt_for_span(source: str, span: Span | None) -> SourceExcerpt | None:
    if span is None:
        return None
    start = max(0, min(span.start, len(source)))
    end = max(start + 1, min(span.end, len(source)))
    loc = offset_to_location(source, start)
    line_start = source.rfind("\n", 0, start) + 1
    line_end = source.find("\n", start)
    if line_end < 0:
        line_end = len(source)
    width = max(1, min(end, line_end) - start)
    return SourceExcerpt(
        line_number=loc.line,
        line=source[line_start:line_end],
        column=loc.column,
        width=width,
    )


@dataclass(slots=True)
class CaracalError(Exception):
    code: str
    message: str
    span: Span | None = None
    hint: str | None = None
    source_name: str | None = None
    source_text: str | None = None
    severity: DiagnosticSeverity = DiagnosticSeverity.ERROR

    def __post_init__(self) -> None:
        Exception.__init__(self, f"{self.code}: {self.message}")

    @property
    def docs_url(self) -> str:
        return docs_url(self.code)

    def diagnostic(self) -> Diagnostic:
        info = lookup_error(self.code)
        hint = self.hint if self.hint is not None else info.hint
        return Diagnostic(
            code=self.code,
            message=self.message,
            span=self.span,
            hint=hint,
            source_name=self.source_name,
            source_text=self.source_text,
            severity=self.severity,
        )

    def render(self, *, color: bool = True) -> str:
        return self.diagnostic().render(color=color)


@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: str
    message: str
    span: Span | None = None
    hint: str | None = None
    source_name: str | None = None
    source_text: str | None = None
    severity: DiagnosticSeverity = DiagnosticSeverity.ERROR

    @property
    def docs_url(self) -> str:
        return docs_url(self.code)

    def to_rich(self) -> ConsoleRenderable:
        title = Text()
        title.append(f"{self.severity.value}[{self.code}]", style="bold red")
        title.append(f": {self.message}", style="bold")

        body = Text()
        excerpt = excerpt_for_span(self.source_text or "", self.span)
        if excerpt is not None:
            span_file = self.span.file_id if self.span is not None else None
            source_name = self.source_name or span_file or "<query>"
            body.append(f" --> {source_name}:{excerpt.line_number}:{excerpt.column}\n")
            gutter = f"{excerpt.line_number:>4} | "
            body.append(gutter, style="dim")
            body.append(excerpt.line)
            body.append("\n")
            body.append(" " * len(gutter), style="dim")
            body.append(" " * max(0, excerpt.column - 1))
            body.append("^" * excerpt.width, style="bold red")
            body.append("\n")
        if self.hint:
            body.append(" = help: ", style="cyan")
            body.append(self.hint)
            body.append("\n")
        body.append(" = docs: ", style="cyan")
        body.append(self.docs_url)

        return Panel(body, title=title, border_style="red", expand=False)

    def render(self, *, color: bool = True, file: TextIO | None = None) -> str:
        console = Console(
            color_system="auto" if color else None,
            force_terminal=color,
            file=file,
            width=100,
            record=True,
        )
        console.print(self.to_rich())
        return console.export_text(styles=color)


__all__ = [
    "CaracalError",
    "Diagnostic",
    "DiagnosticSeverity",
    "ERROR_TABLE",
    "ErrorInfo",
    "SourceExcerpt",
    "SourceLocation",
    "docs_url",
    "excerpt_for_span",
    "lookup_error",
    "offset_to_location",
]
