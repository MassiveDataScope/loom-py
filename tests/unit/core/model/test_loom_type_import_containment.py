"""Import containment of ``loom.core.model.loom_type``.

``loom_type`` reaches for ``pydantic`` only through ``sys.modules`` and imports
it locally, so a deployment installed without pydantic keeps compiling and
decoding ``msgspec.Struct`` types. The guard runs in a clean subprocess because
the pytest interpreter has already imported pydantic for other suites.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src"

_SCRIPT = """
import json
import sys

import msgspec

import loom.core.model
from loom.core.model import loom_type


class Doc(msgspec.Struct, forbid_unknown_fields=True):
    name: str


compiled = loom_type(Doc)
value = compiled.decode_json('{"name": "a"}')
print(
    json.dumps(
        {
            "name": value.name,
            "library": compiled.library,
            "pydantic": sorted(
                name
                for name in sys.modules
                if name == "pydantic" or name.startswith("pydantic.")
            ),
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


class TestLoomTypeImportContainment:
    def test_compiling_a_struct_type_does_not_import_pydantic(self) -> None:
        """The msgspec path must never touch the optional pydantic dependency."""
        result = _run_in_clean_interpreter(_SCRIPT)
        assert result.returncode == 0, result.stderr

        observed = json.loads(result.stdout.strip().splitlines()[-1])

        assert observed["name"] == "a"
        assert observed["library"] == "msgspec"
        assert observed["pydantic"] == [], f"pydantic modules imported: {observed['pydantic']}"
