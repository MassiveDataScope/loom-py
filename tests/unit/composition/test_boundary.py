"""The composition loads no transport: Lambda and the worker import it without HTTP."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[3] / "src"

# Not even the Celery job service: it is loaded only when ``celery`` is configured.
_FORBIDDEN = ("fastapi", "starlette", "celery", "loom.rest", "loom.celery")

_SCRIPT = f"""
import json
import sys

import loom.composition

print(json.dumps(sorted(
    name for name in sys.modules
    if any(name == root or name.startswith(root + ".") for root in {_FORBIDDEN!r})
)))
"""


def test_importing_the_composition_loads_no_transport() -> None:
    result = subprocess.run(
        # Not -I: it ignores PYTHONPATH, and the check must run against this tree.
        [sys.executable, "-s", "-c", _SCRIPT],
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_SRC)},
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == []


# The Celery job service loads ``celery`` itself, and creating a Celery app
# imports ``celery.signals``: neither is the worker. Its adapter, its
# bootstrap and the prefork pool are.
_WORKER = (
    "loom.celery.adapter",
    "loom.celery.bootstrap",
    "celery.concurrency.prefork",
    "fastapi",
    "starlette",
)

_OPEN_SCRIPT = """
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])

from loom.composition import Recipe, build_application
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence.profile import WORKER_PROFILE


class Bare:
    def recipe(self, source):
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=(), interfaces=()),
            backend=source.backend(default="none"),
        )

    def bind(self, application):
        return application


async def main():
    application = build_application(sys.argv[2], adapter=Bare(), environment="test")
    async with application.running():
        pass


asyncio.run(main())
# The Celery job service did load: the absences below are not a fallback to inline.
assert "loom.celery.service" in sys.modules, "the celery job service did not load"
print(json.dumps(sorted(name for name in json.loads(sys.argv[3]) if name in sys.modules)))
"""

_CONFIG = """
app:
  name: boundary
  code_path: .
persistence:
  backend: none
celery:
  broker_url: "memory://"
  result_backend: "cache+memory://"
"""


def test_composing_and_opening_with_celery_loads_no_worker(tmp_path: Path) -> None:
    config = tmp_path / "app.yaml"
    config.write_text(_CONFIG, encoding="utf-8")

    result = subprocess.run(
        [sys.executable, "-I", "-c", _OPEN_SCRIPT, str(_SRC), str(config), json.dumps(_WORKER)],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == []
