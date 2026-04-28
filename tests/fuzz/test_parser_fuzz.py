"""Parser / planner fuzzer.

Hypothesis is the recommended driver, but it is an optional dev dependency.
The test falls back to a deterministic random fuzzer (fixed-seed
``random.Random``) when ``hypothesis`` is unavailable so the suite stays
green in lean CI images.

Invariant: parsing any string in the fuzzer's output must EITHER produce
an AST OR raise ``CaracalError``. Anything else (e.g. unguarded
``IndexError``, ``KeyError``, ``UnicodeDecodeError``) counts as a bug.
"""

from __future__ import annotations

import random
import string

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import parse_tuft

try:  # pragma: no cover - depends on dev image
    import hypothesis
    import hypothesis.strategies as st

    HAS_HYPOTHESIS = True
except ImportError:  # pragma: no cover
    HAS_HYPOTHESIS = False


_KEYWORDS = ["MATCH", "RETURN", "WHERE", "WITH", "LIMIT", "ORDER", "BY"]
_LITERALS = ["'TP53'", "17", "true", "null"]


def _emit(rng: random.Random) -> str:
    parts: list[str] = []
    while len(parts) < 12:
        kind = rng.choice(("kw", "lit", "ident", "punct"))
        if kind == "kw":
            parts.append(rng.choice(_KEYWORDS))
        elif kind == "lit":
            parts.append(rng.choice(_LITERALS))
        elif kind == "ident":
            parts.append("".join(rng.choices(string.ascii_letters, k=rng.randint(1, 6))))
        else:
            parts.append(rng.choice(("(", ")", ":", ",", "->", "=", ".", "*")))
    return " ".join(parts)


def _check(text: str) -> None:
    try:
        parse_tuft(text)
    except CaracalError:
        return
    except Exception:  # pragma: no cover - this is the bug surface
        # Any non-CaracalError exception is a fuzzer find.
        raise
    # Successful parse is fine — the fuzzer occasionally produces valid forms.


def test_parser_fuzz_random_inputs_do_not_crash() -> None:
    import contextlib

    rng = random.Random(0xC0FFEE)
    for _ in range(200):
        text = _emit(rng)
        with contextlib.suppress(CaracalError):
            _check(text)


@pytest.mark.skipif(not HAS_HYPOTHESIS, reason="hypothesis is not installed")
def test_parser_fuzz_with_hypothesis() -> None:  # pragma: no cover - exercised when dep present
    import contextlib

    @hypothesis.given(text=st.text(min_size=0, max_size=64))
    @hypothesis.settings(max_examples=50, deadline=None)
    def _runner(text: str) -> None:
        with contextlib.suppress(CaracalError):
            _check(text)

    _runner()


def test_parser_round_trip_known_query_remains_stable() -> None:
    program = parse_tuft("MATCH (g:Gene) WHERE g.symbol = 'TP53' RETURN g.symbol LIMIT 5")
    assert len(program.statements) == 1
