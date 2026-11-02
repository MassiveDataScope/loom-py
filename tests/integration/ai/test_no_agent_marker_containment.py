"""FR-050 pinned by execution: no ``ai:`` section means no ``loom.ai`` import.

``resolve_ai`` and ``verify_agent_markers`` (in ``loom.composition._ai``)
both carry a docstring or an inline comment arguing that an application with
no ``ai:`` section must never pull the AI pillar in — but until this test,
nothing ran ``create_app`` on such an application and inspected
``sys.modules`` to check. The check runs in a subprocess
(``_no_marker_app.py``) because this test session, and this very module's
own package (``tests.integration.ai``), already import ``loom.ai.abc`` for
every other suite in this directory — a same-process assertion could never
observe the absence this property claims.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_HELPER = Path(__file__).with_name("_no_marker_app.py")
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SENTINEL = "NO_MARKER_APP_OK"


def test_create_app_does_not_import_loom_ai_when_there_is_no_ai_section() -> None:
    """``create_app`` on a plain, agent-less application never imports ``loom.ai``."""
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
