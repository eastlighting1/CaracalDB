from __future__ import annotations

from caracaldb.cli.app import cmd_migrate
from caracaldb.storage import create_bundle


def test_cmd_migrate_check_is_explicit_noop(tmp_path, capsys) -> None:
    bundle = create_bundle(tmp_path / "demo")
    rc = cmd_migrate(bundle.path, target_format=1, check=True)
    assert rc == 0
    assert '"status": "ok"' in capsys.readouterr().out


def test_cmd_migrate_blocks_unknown_target_format(tmp_path, capsys) -> None:
    bundle = create_bundle(tmp_path / "demo")
    rc = cmd_migrate(bundle.path, target_format=2, check=False)
    assert rc == 2
    assert '"status": "blocked"' in capsys.readouterr().out
