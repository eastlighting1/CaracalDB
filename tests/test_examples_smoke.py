"""Smoke-check the tutorial notebooks: extract code cells and run them.

The notebooks are JSON; we read their ``code`` cells and exec them in a
fresh namespace. Any exception fails the test. ``nbformat`` / ``jupyter``
is intentionally not required.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

NOTEBOOKS = ["biomed.ipynb", "fraud.ipynb", "recsys.ipynb"]


@pytest.mark.parametrize("name", NOTEBOOKS)
def test_notebook_runs_end_to_end(name: str) -> None:
    repo = Path(__file__).resolve().parents[1]
    nb_path = repo / "examples" / name
    payload = json.loads(nb_path.read_text(encoding="utf-8"))
    code_cells = [
        "".join(cell["source"]) for cell in payload["cells"] if cell["cell_type"] == "code"
    ]
    namespace: dict[str, object] = {}
    for source in code_cells:
        exec(source, namespace)  # noqa: S102 - tutorial smoke
