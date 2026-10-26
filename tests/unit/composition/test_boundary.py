"""The composition loads no transport: Lambda and the worker import it without HTTP."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[3] / "src"

# Not even the Celery job service: it is loaded only when ``celery`` is configured.
_FORBIDDEN = ("fastapi", "starlette", "celery", "loom.rest", "loom.celery")

_SCRIPT = f"""
import json
import sys

import loom.composition

print(json.dumps(sorted(
    name for name in sys.modules
    if any(name == root or name.startswith(root + ".") for root in {_FORBIDDEN!r})
)))
"""


def test_importing_the_composition_loads_no_transport() -> None:
    result = subprocess.run(
        # Not -I: it ignores PYTHONPATH, and the check must run against this tree.
        [sys.executable, "-s", "-c", _SCRIPT],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == []
