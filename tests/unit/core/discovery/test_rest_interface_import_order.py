"""A package scan finds a REST interface without importing its module first.

The check runs in a fresh interpreter: the pytest process has already imported
``loom.rest.model`` for other suites, which would hide an import-order bug.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src"

_INTERFACE = (
    "from loom.rest.model import RestInterface\n"
    "class AcmeInterface(RestInterface[object]):\n"
    "    prefix = '/acme'\n"
)

_SCRIPT = """
import json
import sys

assert "loom.rest.model" not in sys.modules
from loom.core.discovery.scanner import PackageScanner

assert "loom.rest.model" not in sys.modules
found = PackageScanner(["acme_rest_scan"]).discover()
print(json.dumps([item.__name__ for item in found.interfaces]))
"""


class TestRestInterfaceImportOrder:
    def test_a_fresh_interpreter_finds_an_interface_without_importing_it_first(
        self, tmp_path: Path
    ) -> None:
        package = tmp_path / "acme_rest_scan"
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "interface.py").write_text(_INTERFACE)
        env = {**os.environ, "PYTHONPATH": os.pathsep.join((str(tmp_path), str(_SRC)))}

        result = subprocess.run(
            [sys.executable, "-c", _SCRIPT],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == ["AcmeInterface"]
