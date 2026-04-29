"""Generate the public milestone index from milestone gate files."""

from __future__ import annotations

import argparse
from pathlib import Path

MILESTONE_DIR = Path("docs/milestones")


def gate_title(path: Path) -> str:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return path.stem


def render_index() -> str:
    gates = sorted(MILESTONE_DIR.glob("M*-gate.md"))
    lines = [
        "---",
        "applies_to: v0.1.x",
        "status: generated",
        "last_updated: 2026-04-28",
        "engine_status: python-reference; rust-engine-planned",
        "---",
        "",
        "# Milestones",
        "",
        (
            "Milestone pages summarize release gates and implementation readiness. "
            "The public index is generated from gate files, while detailed gate "
            "reports stay outside the public navigation until reviewed."
        ),
        "",
        "## Gate Index",
        "",
        "| Gate | Public focus |",
        "|---|---|",
    ]
    focus = {
        "M0": "Repository, package, CI, parser, and empty bundle foundation",
        "M1": "Catalog, storage, WAL, basic planning, and first query path",
        "M2": "Graph indexes, pattern planning, and broader execution operators",
        "M3": "Ontology reasoning, snapshots, and transactional behavior",
        "M4": "ML, feature, observability, and interop surfaces",
        "M5": "Documentation, packaging, release, and quality gates",
    }
    for gate in gates:
        title = gate_title(gate)
        key = title.split()[0]
        lines.append(f"| `{key}` | {focus.get(key, title)} |")
    lines.extend(
        [
            "",
            "## Rule",
            "",
            (
                "Public milestone documentation should describe status and "
                "user-visible implications, not private work notes."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("docs/developers/milestones.md"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    content = render_index()
    if args.check:
        existing = args.out.read_text(encoding="utf-8") if args.out.exists() else None
        if existing != content:
            raise SystemExit(f"{args.out} is out of date")
        return 0

    args.out.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
