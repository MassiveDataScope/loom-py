"""``loom.core`` knows no transport: importing all of it loads no REST stack.

Runs in a clean interpreter, since the pytest process has already imported
FastAPI for other suites.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src"

_SCRIPT = """
import importlib
import json
import pkgutil
import sys

import loom.core

# An Alembic env script: it reads alembic.context.config, which exists only
# while Alembic runs it.
ALEMBIC_ENV = "loom.core.repository.sqlalchemy.migrations.env_template"

for module in pkgutil.walk_packages(loom.core.__path__, "loom.core."):
    if module.name != ALEMBIC_ENV:
        importlib.import_module(module.name)

transports = ("loom.rest", "fastapi", "starlette")
print(json.dumps(sorted(
    name for name in sys.modules
    if any(name == root or name.startswith(root + ".") for root in transports)
)))
"""


def test_importing_every_core_module_loads_no_rest_stack() -> None:
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == []
