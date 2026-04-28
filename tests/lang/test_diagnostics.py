from caracaldb.lang.diagnostics import CaracalError, docs_url, excerpt_for_span
from caracaldb.lang.tuft.ast import Span


def test_excerpt_for_span_maps_line_and_caret_width() -> None:
    source = "MATCH (g)\nRETURN g.symbol\n"
    span = Span(start=17, end=25, file_id="query.tuft")

    excerpt = excerpt_for_span(source, span)

    assert excerpt is not None
    assert excerpt.line_number == 2
    assert excerpt.column == 8
    assert excerpt.width == 8


def test_caracal_error_renders_code_hint_and_docs_url() -> None:
    source = "MATCH (g) RETURN g.symbol"
    err = CaracalError(
        code="TF-2001",
        message="unexpected token",
        span=Span(start=10, end=16),
        hint="expected a pattern clause",
        source_name="query.tuft",
        source_text=source,
    )

    rendered = err.render(color=False)

    assert "TF-2001" in rendered
    assert "unexpected token" in rendered
    assert "expected a pattern clause" in rendered
    assert docs_url("TF-2001") in rendered
