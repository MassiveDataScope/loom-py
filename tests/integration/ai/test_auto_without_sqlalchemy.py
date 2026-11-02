"""AC1: ``create_app`` boots an agents-only application without SQLAlchemy.

The check runs in a subprocess because the block must be in place before any
loom module is imported, and this test session already has the SQLAlchemy
backend loaded.  The child (``_agents_only_app.py``) installs the finder as its
first statement, builds the app with ``stores.default.backend: none`` and proves
the block is effective by importing ``loom.core.backend``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_HELPER = Path(__file__).with_name("_agents_only_app.py")
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SENTINEL = "AGENTS_ONLY_APP_OK"


def test_create_app_boots_agents_only_app_with_sqlalchemy_blocked() -> None:
    """``import loom.rest.fastapi.auto`` and ``create_app`` survive a blocked ``sqlalchemy``."""
    result = subprocess.run(
        [sys.executable, str(_HELPER)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _SENTINEL in result.stdout, result.stderr
