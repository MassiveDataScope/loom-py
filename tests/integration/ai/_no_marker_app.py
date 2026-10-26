"""Boot a plain application, no ``ai:`` section, no ``Agent()`` marker (FR-050).

Run by ``test_no_agent_marker_containment.py`` in a fresh interpreter: the AI
pillar is optional, and several docstrings on the ``create_app`` path
(``resolve_ai``, ``verify_agent_markers``) argue at length that neither
guard imports ``loom.ai`` when no ``ai:`` section is present. Nothing here
proves that on its own — this script is the proof, isolated from every other
suite's already-imported modules, and from ``conftest`` modules (this one
included) that import ``loom.ai.abc`` in their own header and would poison a
same-process assertion.

Prints ``NO_MARKER_APP_OK`` and exits 0 on success; prints the traceback and
exits 1 otherwise.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Any

import yaml

from loom.rest.fastapi.auto import create_app

SENTINEL = "NO_MARKER_APP_OK"
_MANIFEST_MODULE = "nomarker.manifest"

_USE_CASE_SOURCE = '''
from loom.core.use_case import UseCase


class PingUseCase(UseCase[object, str]):
    """A use case with no parameters and no ``Agent()`` marker."""

    async def execute(self) -> str:
        return "pong"
'''

_MANIFEST_SOURCE = """
from nomarker.use_cases import PingUseCase

USE_CASES = [PingUseCase]
"""


def _write_project(root: Path) -> str:
    """Write the ``nomarker`` package and its YAML config; no ``ai:`` section at all."""
    package = root / "nomarker"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "use_cases.py").write_text(_USE_CASE_SOURCE, encoding="utf-8")
    (package / "manifest.py").write_text(_MANIFEST_SOURCE, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "nomarker-demo",
            "code_path": str(root),
            "discovery": {
                "mode": "manifest",
                "manifest": {"module": _MANIFEST_MODULE},
            },
        },
        "persistence": {"backend": "none"},
        # Deliberately no 'ai:' section: this is exactly the absent-section
        # case FR-050 covers.
    }
    config_path = root / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _assert_ai_pillar_never_imported() -> None:
    leaked = sorted(
        name for name in sys.modules if name == "loom.ai" or name.startswith("loom.ai.")
    )
    if leaked:
        raise RuntimeError(f"loom.ai was imported although no 'ai:' section was declared: {leaked}")


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="loom-nomarker-"))
    try:
        config_path = _write_project(root)
        app = create_app(config_path)
        if app is None:
            raise RuntimeError("create_app returned no application")
        _assert_ai_pillar_never_imported()
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print(SENTINEL)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
