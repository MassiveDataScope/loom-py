"""Import guard: ``loom.ai`` must import without any optional extra installed.

The check runs in a clean subprocess so that modules imported by unrelated
suites inside the pytest interpreter cannot mask a real dependency leak.

Note: there is deliberately no "``import loom.core`` pulls no third-party
package" test here. ``loom.core`` is an implicit namespace package, so
importing it executes no module body and the assertion would be vacuous;
asserting the property over the whole ``loom.core`` subtree is impossible
today because ``loom.core.discovery`` legitimately imports ``loom.rest``
(and therefore pydantic/fastapi), which would require an allowlist. The
containment invariant that actually matters is asserted directly on the leaf
module in ``tests/unit/core/plugins/test_import_containment.py``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[3] / "src"

_IMPORT_AI_SCRIPT = """
import loom.ai  # noqa: F401
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


class TestImportWithoutExtras:
    def test_import_loom_ai_works_with_no_extras_installed(self) -> None:
        """``import loom.ai`` must succeed with no optional engine extra installed."""
        result = _run_in_clean_interpreter(_IMPORT_AI_SCRIPT)

        assert result.returncode == 0, f"import loom.ai failed:\n{result.stderr}"
