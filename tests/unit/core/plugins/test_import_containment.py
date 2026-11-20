"""Import-containment regression tests for the shared entry-point machinery.

Two invariants are guarded here:

* ``loom.core.plugins.entrypoints`` is a stdlib-only leaf: importing it must not
  pull any other ``loom`` module nor any third-party package into the process.
* The ETL runner stays a leaf consumer of that machinery: importing it may not
  drag the REST pillar (or its ``pydantic``/``fastapi`` dependencies) in.

Both checks run in a clean subprocess because the pytest interpreter has
already imported those modules for other suites.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src"

_FORBIDDEN: tuple[str, ...] = ("loom.rest", "pydantic", "fastapi")

_SCRIPT = """
import json
import sys

import loom.etl.runner._engines  # noqa: F401

forbidden = {forbidden!r}
leaked = [
    name
    for name in sys.modules
    if any(name == root or name.startswith(root + ".") for root in forbidden)
]
print(json.dumps(sorted(leaked)))
"""

_ALLOWED_LOOM_MODULES: frozenset[str] = frozenset(
    {
        "loom",
        "loom.core",
        "loom.core.plugins",
        "loom.core.plugins.entrypoints",
    }
)

_LEAF_SCRIPT = """
import json
import sys

baseline = frozenset(sys.modules)

import loom.core.plugins.entrypoints  # noqa: F401

delta = frozenset(sys.modules) - baseline
roots = {name.split(".")[0] for name in delta}
print(
    json.dumps(
        {
            "loom": sorted(name for name in delta if name.split(".")[0] == "loom"),
            "third_party": sorted(roots - sys.stdlib_module_names - {"loom"}),
        }
    )
)
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


class TestPluginImportContainment:
    def test_the_entry_point_loader_imports_nothing_outside_the_stdlib(self) -> None:
        """Importing the entry-point leaf must add no ``loom`` nor third-party module."""
        result = _run_in_clean_interpreter(_LEAF_SCRIPT)
        assert result.returncode == 0, result.stderr

        imported = json.loads(result.stdout.strip().splitlines()[-1])

        unexpected_loom = sorted(set(imported["loom"]) - _ALLOWED_LOOM_MODULES)
        assert unexpected_loom == [], f"unexpected loom modules imported: {unexpected_loom}"
        assert imported["third_party"] == [], (
            f"non-stdlib packages imported: {imported['third_party']}"
        )

    def test_importing_the_provider_loader_does_not_pull_in_rest_or_pydantic(self) -> None:
        """Importing the ETL provider loader must not pull in REST-only modules."""
        result = _run_in_clean_interpreter(_SCRIPT.format(forbidden=_FORBIDDEN))
        assert result.returncode == 0, result.stderr

        leaked = json.loads(result.stdout.strip().splitlines()[-1])

        assert leaked == [], f"forbidden modules imported: {leaked}\n{result.stderr}"
