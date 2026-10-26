"""The composition loads no transport: Lambda and the worker import it without HTTP."""

from __future__ import annotations

import subprocess
import sys

_FORBIDDEN = ("fastapi", "starlette", "celery", "loom.rest")


def test_importing_the_composition_loads_no_transport() -> None:
    probe = (
        "import sys, loom.composition\n"
        f"loaded = [m for m in {_FORBIDDEN!r} if any(n == m or n.startswith(m + '.') "
        "for n in sys.modules)]\n"
        "print(','.join(loaded))\n"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", probe], capture_output=True, text=True, check=True
    )

    assert result.stdout.strip() == ""
