import json
from pathlib import Path

import pytest

from caracaldb.lang.tuft import parse_tuft
from caracaldb.lang.tuft.format import to_jsonable

GOLDEN_DIR = Path(__file__).parents[1] / "golden" / "parser"


@pytest.mark.parametrize("source_path", sorted(GOLDEN_DIR.glob("*.tuft")), ids=lambda p: p.stem)
def test_parser_golden(source_path: Path) -> None:
    expected_path = source_path.with_suffix(".expected.json")

    program = parse_tuft(source_path.read_text(encoding="utf-8"))
    actual = to_jsonable(program)
    expected = json.loads(expected_path.read_text(encoding="utf-8"))

    assert actual == expected
