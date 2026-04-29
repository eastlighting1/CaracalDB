"""Validate executable documentation code blocks."""

from __future__ import annotations

import runpy
from dataclasses import dataclass
from pathlib import Path

from check_docs_metadata import is_public_page

COMPILED_LANGUAGES = {"python"}
KNOWN_LANGUAGES = {
    "bash",
    "cypher",
    "ebnf",
    "json",
    "lark",
    "mermaid",
    "powershell",
    "python",
    "sparql",
    "sql",
    "text",
    "tuft",
}


@dataclass(frozen=True, slots=True)
class Fence:
    index: int
    line_number: int
    language: str
    code: str


def fence_language(info: str) -> str:
    return (info.strip().split() or [""])[0]


def iter_fences(path: Path, text: str) -> tuple[list[Fence], list[str]]:
    fences: list[Fence] = []
    failures: list[str] = []
    in_fence = False
    opening_line = 0
    opening_language = ""
    body: list[str] = []

    for line_number, line in enumerate(text.splitlines(keepends=True), start=1):
        stripped = line.rstrip("\r\n")
        if not stripped.startswith("```"):
            if in_fence:
                body.append(line)
            continue

        info = stripped[3:].strip()
        if not in_fence:
            in_fence = True
            opening_line = line_number
            opening_language = fence_language(info)
            body = []
            continue

        if info:
            failures.append(
                f"{path}:{line_number}: closing code fence must be plain ``` " f"(found ```{info})"
            )
        fences.append(
            Fence(
                index=len(fences) + 1,
                line_number=opening_line,
                language=opening_language,
                code="".join(body),
            )
        )
        in_fence = False
        opening_line = 0
        opening_language = ""
        body = []

    if in_fence:
        failures.append(f"{path}:{opening_line}: unclosed code fence")
    return fences, failures


def main() -> int:
    failures: list[str] = []
    for path in sorted(Path("docs").rglob("*.md")):
        if not is_public_page(path):
            continue
        text = path.read_text(encoding="utf-8")
        fences, fence_failures = iter_fences(path, text)
        failures.extend(fence_failures)
        for fence in fences:
            lang = fence.language
            if not lang:
                failures.append(f"{path}:{fence.line_number}: code fence has no language")
                continue
            if lang not in KNOWN_LANGUAGES:
                failures.append(f"{path}:{fence.line_number}: unknown code fence language {lang!r}")
                continue
            if lang in COMPILED_LANGUAGES:
                try:
                    compile(fence.code, f"{path}:{fence.index}", "exec")
                except SyntaxError as exc:
                    failures.append(
                        f"{path}:{fence.line_number}: Python block does not compile: {exc}"
                    )

    if failures:
        raise SystemExit("\n".join(failures))

    runpy.run_path("tools/check_quickstart_code.py", run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
