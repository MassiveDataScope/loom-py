"""Import guard: ``loom`` must import without the ``sqlalchemy`` extra installed.

``loom-kernel[streaming]`` and ``loom-kernel[testing]`` are installed without
SQLAlchemy, so every module outside the SQLAlchemy-specific packages must keep
its import lazy. The check runs in a clean subprocess with ``sqlalchemy``
blocked on ``sys.meta_path`` so that modules imported by unrelated suites
inside the pytest interpreter cannot mask a real dependency leak. Modules are
enumerated from the file tree rather than ``pkgutil.walk_packages`` because
``loom.core`` is an implicit namespace package, invisible to ``pkgutil``.

Only two packages are skipped because SQLAlchemy is their subject, not a leak:
``loom.core.repository.sqlalchemy`` (the SQLAlchemy repository backend) and
``loom.core.backend`` (the SQLAlchemy model compiler, re-exported eagerly from
its ``__init__``). Every other module is walked, including the Mongo, DynamoDB,
Prefect, Spark and ETL I/O adapters: an ``ImportError`` there that does not
mention sqlalchemy is another missing extra and is ignored.

``loom.testing.repository_harness`` and the selected streaming SQLAlchemy
island are allowed SQLAlchemy-hard modules: both owning packages import safely.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src"

_SQLALCHEMY_HARD_MODULES = frozenset(
    {"loom.testing.repository_harness", "loom.streaming.nodes._table._sqlalchemy"}
)

_WALK_WITHOUT_SQLALCHEMY_SCRIPT = """
import importlib
import sys
from pathlib import Path

import loom

SKIPPED_PREFIXES = ("loom.core.repository.sqlalchemy", "loom.core.backend")


class _BlockSqlAlchemy:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlalchemy" or fullname.startswith("sqlalchemy."):
            raise ImportError("blocked: sqlalchemy")
        return None


sys.meta_path.insert(0, _BlockSqlAlchemy())

def _importing_module(exc, fallback):
    origin = fallback
    tb = exc.__traceback__
    while tb is not None:
        module = tb.tb_frame.f_globals.get("__name__", "")
        if module.startswith("loom.") and not module.startswith(SKIPPED_PREFIXES):
            origin = module
        tb = tb.tb_next
    return origin


def _module_names():
    root = Path(loom.__path__[0])
    for file in sorted(root.rglob("*.py")):
        if "__pycache__" in file.parts:
            continue
        parts = list(file.relative_to(root.parent).with_suffix("").parts)
        if parts[-1] == "__init__":
            parts.pop()
        yield ".".join(parts)


broken = set()
for name in _module_names():
    if name.startswith(SKIPPED_PREFIXES):
        continue
    try:
        importlib.import_module(name)
    except ImportError as exc:
        if "sqlalchemy" in str(exc).lower():
            broken.add(_importing_module(exc, name))
    except Exception:
        pass

for name in sorted(broken):
    print(name)
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


def test_every_module_imports_with_sqlalchemy_blocked() -> None:
    """Only the allow-listed module may fail to import with ``sqlalchemy`` blocked."""
    result = _run_in_clean_interpreter(_WALK_WITHOUT_SQLALCHEMY_SCRIPT)

    assert result.returncode == 0, f"module walk failed:\n{result.stderr}"
    broken = set(result.stdout.split())
    unexpected = sorted(broken ^ _SQLALCHEMY_HARD_MODULES)
    assert not unexpected, f"modules importing sqlalchemy eagerly: {unexpected}"
