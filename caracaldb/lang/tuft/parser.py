"""Parser entry points for Tuft."""

from __future__ import annotations

from functools import lru_cache

from lark import Lark
from lark.exceptions import LarkError, UnexpectedCharacters, UnexpectedInput

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft.ast import Program
from caracaldb.lang.tuft.transformer import TuftTransformer, _load_grammar


@lru_cache(maxsize=1)
def get_parser() -> Lark:
    return Lark(_load_grammar(), parser="lalr", start="start", propagate_positions=True)


def parse_tuft(source: str) -> Program:
    try:
        tree = get_parser().parse(source)
    except UnexpectedCharacters as exc:
        raise CaracalError(
            code="TF-1001",
            message=f"invalid character at position {exc.pos_in_stream}",
            hint="check for stray punctuation, unbalanced quotes, or unexpected unicode",
        ) from exc
    except UnexpectedInput as exc:
        raise CaracalError(
            code="TF-2001",
            message=f"unexpected token at position {getattr(exc, 'pos_in_stream', 0)}",
        ) from exc
    except LarkError as exc:
        raise CaracalError(code="TF-2001", message=f"parse error: {exc}") from exc
    result = TuftTransformer().transform(tree)
    if not isinstance(result, Program):
        raise CaracalError(code="TF-2001", message=f"expected Program, got {type(result).__name__}")
    return result


__all__ = ["get_parser", "parse_tuft"]
