"""FastAPI composition helpers keep optional pillars out of plain REST imports."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[3] / "src"


def test_ai_wiring_module_import_does_not_load_optional_ai_pillar() -> None:
    script = (
        "import loom.rest.fastapi._auto_ai\n"
        "import sys\n"
        "print(any(name == 'loom.ai' or name.startswith('loom.ai.') "
        "for name in sys.modules))\n"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert (result.returncode, result.stdout.strip()) == (0, "False"), result.stderr
