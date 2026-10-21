"""Import guard: the Bytewax runtime must load without SQLAlchemy installed.

The import check runs in a clean subprocess so that modules imported by
unrelated suites inside the pytest interpreter cannot mask a dependency leak.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.async_bridge import AsyncBridge  # noqa: E402
from loom.core.persistence.database import DatabaseConfig  # noqa: E402
from loom.streaming.bytewax._resource_manager import ResourceManager  # noqa: E402

_SRC = Path(__file__).resolve().parents[4] / "src"

_IMPORT_RUNNER_SCRIPT = """
import sys
sys.modules["sqlalchemy"] = None
import loom.streaming.bytewax.runner  # noqa: F401
"""


def _run_in_clean_interpreter(script: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a fresh interpreter that can see the repository ``src``."""
    env = {**os.environ, "PYTHONPATH": str(_SRC)}
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_bytewax_runner_imports_without_sqlalchemy() -> None:
    """``import loom.streaming.bytewax.runner`` succeeds with ``sqlalchemy`` hidden."""
    result = _run_in_clean_interpreter(_IMPORT_RUNNER_SCRIPT)

    assert result.returncode == 0, f"import loom.streaming.bytewax.runner failed:\n{result.stderr}"


def test_session_manager_for_names_sqlalchemy_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Building a SQLAlchemy session manager without the extra raises a hinting ImportError."""
    monkeypatch.setitem(sys.modules, "loom.core.repository.sqlalchemy.session_manager", None)
    manager = ResourceManager(cast(AsyncBridge, object()))

    with pytest.raises(ImportError, match=r"loom-kernel\[sqlalchemy\]"):
        manager.session_manager_for(DatabaseConfig(url="sqlite+aiosqlite:///:memory:"))
