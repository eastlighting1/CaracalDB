"""Execute Python code fences from the public quickstart page."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

import caracaldb as cdb

FENCE_RE = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


def main() -> int:
    path = Path("docs/start/quickstart.md")
    text = path.read_text(encoding="utf-8")
    blocks = FENCE_RE.findall(text)
    if not blocks:
        raise SystemExit(f"no python code fences found in {path}")

    with tempfile.TemporaryDirectory(prefix="caracal_docs_") as tmp:
        repo_root = Path(cdb.__file__).resolve().parents[1].as_posix()
        for index, block in enumerate(blocks, start=1):
            # 문서에는 상대 경로로 두되, 테스트 코드 실행 시에는 절대 경로로 치환
            block = block.replace('"examples/data/', f'"{repo_root}/examples/data/')
            script = Path(tmp) / f"quickstart_{index}.py"
            script.write_text(block, encoding="utf-8")
            subprocess.run([sys.executable, str(script)], cwd=tmp, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
