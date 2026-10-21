"""``create_app`` without a ``celery`` section never imports the Celery support."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from tests.integration.rest.conftest import write_project

_REPO_ROOT = Path(__file__).resolve().parents[3]

_CHILD = """
import sys
import warnings

from loom.rest.fastapi.auto import create_app

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    create_app(sys.argv[1])

leaked = sorted(name for name in sys.modules if name == "celery" or name.startswith("loom.celery"))
assert not leaked, leaked
assert any("Celery config section is missing" in str(w.message) for w in caught), caught
"""


def test_create_app_without_a_celery_section_leaves_celery_unimported(tmp_path: Path) -> None:
    config_path, _module = write_project(tmp_path)
    env = {
        **os.environ,
        "LOOM_ENV": "development",
        "PYTHONPATH": os.pathsep.join([str(tmp_path), str(_REPO_ROOT)]),
    }

    result = subprocess.run(
        [sys.executable, "-c", _CHILD, config_path],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
