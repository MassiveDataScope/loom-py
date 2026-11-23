"""``loom.core.log_names`` is a leaf: naming what is logged costs no other import."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[4] / "src"

_SCRIPT = """
import json
import sys

sys.path.insert(0, {src!r})
baseline = frozenset(sys.modules)

import loom.core.log_names  # noqa: F401

delta = frozenset(sys.modules) - baseline
roots = {{name.split(".")[0] for name in delta}}
print(
    json.dumps(
        {{
            "loom": sorted(name for name in delta if name.split(".")[0] == "loom"),
            "third_party": sorted(roots - sys.stdlib_module_names - {{"loom"}}),
        }}
    )
)
"""


class TestLogNamesIsALeaf:
    def test_importing_it_loads_no_other_loom_module_and_nothing_third_party(self) -> None:
        result = subprocess.run(
            [sys.executable, "-I", "-c", _SCRIPT.format(src=str(_SRC))],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr

        imported = json.loads(result.stdout.strip().splitlines()[-1])

        assert set(imported["loom"]) <= {"loom", "loom.core", "loom.core.log_names"}
        assert imported["third_party"] == []
