"""Import guard: the Bytewax backend names the extra and the Python versions it needs.

Lives outside ``tests/unit/streaming/bytewax/`` on purpose: that tree is skipped
where bytewax is absent, which is exactly where this message matters. The import
runs in a clean subprocess with bytewax blocked, so the check is the same on every
Python version and never evicts modules from the pytest interpreter.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[3] / "src"

_IMPORT_WITHOUT_BYTEWAX = """
import sys
sys.modules["bytewax"] = None
try:
    import loom.streaming.bytewax  # noqa: F401
except ImportError as exc:
    print(exc)
    raise SystemExit(0)
raise SystemExit("loom.streaming.bytewax imported without bytewax")
"""


class TestBytewaxImportGuard:
    def test_importing_the_backend_without_bytewax_names_the_extra_and_the_versions(self) -> None:
        env = {**os.environ, "PYTHONPATH": str(_SRC)}
        result = subprocess.run(
            [sys.executable, "-c", _IMPORT_WITHOUT_BYTEWAX],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        message = result.stdout
        assert "'streaming' extra" in message
        assert "Python < 3.13" in message
        assert f"Python {sys.version_info.major}.{sys.version_info.minor}." in message
