"""IntoTable loads SQLAlchemy only when its backend is selected.

Each check runs in a clean interpreter so that modules already imported by
other suites cannot satisfy or mask the import under test.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src"


def _select_sqlalchemy_with(blocked: str) -> subprocess.CompletedProcess[str]:
    script = (
        "import sys\n"
        f"sys.modules[{blocked!r}] = None\n"
        "from loom.core.plugins.optional import MissingExtraError\n"
        "from loom.streaming.nodes._table.common import IntoTable, SqlAlchemySinkConfig\n"
        "class Row:\n"
        "    id: int\n"
        "config = SqlAlchemySinkConfig.from_config("
        "{'table': 'events', 'database': 'db'}, default_table='events')\n"
        "try:\n"
        "    IntoTable(payload=Row).build_partition("
        "config, 0, 1, bridge=object(), session_manager=object())\n"
        "except MissingExtraError as exc:\n"
        "    print(exc.missing)\n"
        "    print(type(exc.__cause__).__name__)\n"
    )
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )


def test_a_missing_sqlalchemy_is_reported_when_that_backend_is_selected() -> None:
    result = _select_sqlalchemy_with("sqlalchemy")

    assert (result.returncode, result.stdout.strip().splitlines()) == (
        0,
        ["sqlalchemy", "ModuleNotFoundError"],
    ), result.stderr


def test_an_internal_sqlalchemy_island_import_error_keeps_its_cause() -> None:
    result = _select_sqlalchemy_with("loom.core.repository.sqlalchemy.values")

    assert (result.returncode, result.stdout.strip().splitlines()) == (
        0,
        ["loom.core.repository.sqlalchemy.values", "ModuleNotFoundError"],
    ), result.stderr


def test_importing_common_does_not_probe_optional_storage_sdks() -> None:
    script = (
        "import builtins\n"
        "original_import = builtins.__import__\n"
        "blocked = ('sqlalchemy', 'deltalake', 'pyarrow', 'polars', 'clickhouse_connect')\n"
        "def guarded_import(name, *args, **kwargs):\n"
        "    if name.startswith(blocked):\n"
        "        raise RuntimeError(f'unexpected SDK import: {name}')\n"
        "    return original_import(name, *args, **kwargs)\n"
        "builtins.__import__ = guarded_import\n"
        "from loom.streaming.nodes._table.common import IntoTable, Backend\n"
        "print(IntoTable.__name__, Backend.DELTA.value)\n"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert (result.returncode, result.stdout.strip()) == (0, "IntoTable delta"), result.stderr
