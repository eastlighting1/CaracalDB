"""Typer-based CLI: ``caracal init / repl / run / bench``.

The commands are written as plain functions that return integers (exit
codes) so they can be tested directly with no Typer / shell involvement.
``main()`` is the Typer entry point referenced from ``pyproject.toml``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import typer

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.observability import explain_logical, render_explain
from caracaldb.plan.cost import CatalogStats
from caracaldb.plan.logical import LNodeScan
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.pack import pack_bundle, unpack_bundle

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="caracal — embedded GraphDB CLI",
)


@app.command()
def init(path: Path) -> None:
    """Initialise an empty `.crcl` bundle at ``path``."""
    rc = cmd_init(path)
    raise typer.Exit(rc)


@app.command()
def run(
    bundle_path: Path = typer.Argument(..., help="Path to a `.crcl` bundle"),  # noqa: B008
    query_file: Path = typer.Option(None, "-f", "--file", help="Query file"),  # noqa: B008
    output: Path = typer.Option(  # noqa: B008
        None, "-o", "--output", help="Optional JSON output path"
    ),
) -> None:
    """Run a Tuft query against an existing bundle."""
    rc = cmd_run(bundle_path, query_file, output)
    raise typer.Exit(rc)


@app.command()
def explain(
    bundle_path: Path = typer.Argument(..., help="Path to a `.crcl` bundle"),  # noqa: B008
    query: str = typer.Argument(..., help="Tuft query text"),  # noqa: B008
) -> None:
    """Print the EXPLAIN tree for a query."""
    rc = cmd_explain(bundle_path, query)
    raise typer.Exit(rc)


@app.command()
def bench(
    name: str = typer.Argument(..., help="Bench scenario name"),
) -> None:
    """Run a registered micro-benchmark."""
    rc = cmd_bench(name)
    raise typer.Exit(rc)


@app.command()
def pack(
    bundle_path: Path = typer.Argument(  # noqa: B008
        ..., help="Path to a `.crcl` directory bundle"
    ),
    output: Path = typer.Option(None, "-o", "--output", help="Output file path"),  # noqa: B008
    codec: str = typer.Option(  # noqa: B008
        "deflate", "--codec", help="Compression codec: deflate | stored"
    ),
) -> None:
    """Package a `.crcl` directory bundle into a single file."""
    rc = cmd_pack(bundle_path, output, codec)
    raise typer.Exit(rc)


@app.command()
def unpack(
    file_path: Path = typer.Argument(..., help="Path to a packed `.crcl` file"),  # noqa: B008
    output: Path = typer.Option(None, "-o", "--output", help="Output directory path"),  # noqa: B008
) -> None:
    """Restore a packed `.crcl` file back to a directory bundle."""
    rc = cmd_unpack(file_path, output)
    raise typer.Exit(rc)


# ---------------------------------------------------------------------------
# Programmable command surface (used by tests and CronCreate workflows).
# ---------------------------------------------------------------------------


def cmd_init(path: Path) -> int:
    target = Path(path)
    create_bundle(target, exist_ok=True)
    typer.echo(f"caracal: initialised bundle at {target.with_suffix('.crcl')}")
    return 0


def cmd_run(
    bundle_path: Path,
    query_file: Path | None,
    output: Path | None,
) -> int:
    db = cdb.connect(bundle_path)
    if query_file is None:
        typer.echo("caracal: no query file given (use -f QUERY.tuft)", err=True)
        return 2
    text = Path(query_file).read_text(encoding="utf-8")
    try:
        result = db.cursor().sql(text)
    except CaracalError as exc:
        typer.echo(f"caracal: {exc.code}: {exc.message}", err=True)
        return 1
    table = result.arrow()
    payload: list[dict[str, Any]] = table.to_pylist()
    if output is not None:
        Path(output).write_text(json.dumps(payload, default=str, indent=2), encoding="utf-8")
        typer.echo(f"caracal: wrote {table.num_rows} rows to {output}")
    else:
        typer.echo(json.dumps(payload, default=str, indent=2))
    return 0


def cmd_explain(bundle_path: Path, query_text: str) -> int:
    bundle = open_bundle(Path(bundle_path).with_suffix(".crcl"))
    # The CLI does not depend on the M3 binder bypass — it lowers a single
    # NodeScan stand-in so users can validate connection wiring quickly.
    plan = LNodeScan(class_iri=query_text, local_name=query_text, alias="n")
    typer.echo(render_explain(explain_logical(plan, CatalogStats())))
    typer.echo(f"# bundle={bundle.path}")
    return 0


def cmd_bench(name: str) -> int:
    from bench.harness import RUNNERS

    runner = RUNNERS.get(name)
    if runner is None:
        typer.echo(
            f"caracal bench: unknown scenario {name!r}; choices: {sorted(RUNNERS)}", err=True
        )
        return 2
    result = runner()
    typer.echo(json.dumps(result, indent=2, default=str))
    return 0


def cmd_pack(bundle_path: Path, output: Path | None, codec: str) -> int:
    try:
        dest = pack_bundle(bundle_path, output, codec=codec)
    except CaracalError as exc:
        typer.echo(f"caracal: {exc.code}: {exc.message}", err=True)
        return 1
    typer.echo(f"caracal: packed bundle to {dest}")
    return 0


def cmd_unpack(file_path: Path, output: Path | None) -> int:
    try:
        dest = unpack_bundle(file_path, output)
    except CaracalError as exc:
        typer.echo(f"caracal: {exc.code}: {exc.message}", err=True)
        return 1
    typer.echo(f"caracal: unpacked bundle to {dest}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Programmable entry point. ``typer`` raises ``SystemExit``; we trap it
    so ``main()`` can be tested without spawning a subprocess.
    """
    try:
        app(args=argv if argv is not None else sys.argv[1:], standalone_mode=False)
    except typer.Exit as exc:
        return int(exc.exit_code)
    except SystemExit as exc:  # pragma: no cover
        return int(exc.code or 0)
    return 0


__all__ = [
    "app",
    "cmd_bench",
    "cmd_explain",
    "cmd_init",
    "cmd_pack",
    "cmd_run",
    "cmd_unpack",
    "main",
]
