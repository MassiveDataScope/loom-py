"""IntoTable's optional SQLAlchemy import hides only a missing ``sqlalchemy``.

Each check runs in a clean interpreter so that modules already imported by
other suites cannot satisfy or mask the import under test.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src"


def _import_table_module_with(blocked: str) -> subprocess.CompletedProcess[str]:
    script = (
        "import sys\n"
        f"sys.modules[{blocked!r}] = None\n"
        "import loom.streaming.nodes._table.common as common\n"
        "print(common._SA_AVAILABLE)\n"
    )
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )


def test_a_missing_sqlalchemy_disables_the_sqlalchemy_backend() -> None:
    result = _import_table_module_with("sqlalchemy")

    assert (result.returncode, result.stdout.strip()) == (0, "False"), result.stderr


def test_an_import_error_naming_another_module_is_raised() -> None:
    result = _import_table_module_with("loom.core.repository.sqlalchemy.values")

    assert result.returncode == 1
    assert (
        "ModuleNotFoundError: import of loom.core.repository.sqlalchemy.values halted"
        in result.stderr
    )
