"""Import containment of the cached-call codec.

``build_call_codec`` reaches for ``pydantic`` only when a resolved return
annotation is outside the msgspec grammar, and only when pydantic is already
in ``sys.modules``. The package must therefore stay usable — and cheap — on a
deployment installed without the ``rest`` extra, where pydantic is absent.

Two things are guarded, both in a clean subprocess because the pytest
interpreter has already imported pydantic for other suites (which is also why
``test_no_extras_import.py`` is not the guard here):

* the msgspec path never touches pydantic;
* the ``sys.modules`` gate holds, so an annotation outside the msgspec grammar
  answers ``None`` in a process that has not imported pydantic, instead of
  importing it to build an adapter.
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

import loom.core.cache  # noqa: F401
from loom.core.cache.result_codec import build_call_codec


class Doc(msgspec.Struct):
    name: str


async def fetch(query: str) -> list[Doc]: ...


codec = build_call_codec(fetch)
print(
    json.dumps(
        {
            "codec": codec is not None,
            "pydantic": sorted(
                name
                for name in sys.modules
                if name == "pydantic" or name.startswith("pydantic.")
            ),
        }
    )
)
"""


_UNGATED_SCRIPT = """
import json
import sys

from loom.core.cache.result_codec import build_call_codec


class Doc:
    name: str


async def fetch(query: str) -> Doc: ...


codec = build_call_codec(fetch)
print(
    json.dumps(
        {
            "codec": codec is not None,
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


class TestCacheImportContainment:
    def test_building_a_msgspec_call_codec_does_not_import_pydantic(self) -> None:
        """The msgspec path must never touch the optional pydantic dependency."""
        result = _run_in_clean_interpreter(_SCRIPT)
        assert result.returncode == 0, result.stderr

        observed = json.loads(result.stdout.strip().splitlines()[-1])

        assert observed["codec"] is True, "the msgspec annotation should have produced a codec"
        assert observed["pydantic"] == [], f"pydantic modules imported: {observed['pydantic']}"

    def test_an_annotation_outside_the_msgspec_grammar_does_not_import_pydantic(self) -> None:
        """The ``sys.modules`` gate, not the annotation, decides the pydantic branch.

        Without the gate the codec would import pydantic to build an adapter for
        ``Doc``, which is exactly what a deployment without the ``rest`` extra
        cannot afford. Uncached is the right answer there.
        """
        result = _run_in_clean_interpreter(_UNGATED_SCRIPT)
        assert result.returncode == 0, result.stderr

        observed = json.loads(result.stdout.strip().splitlines()[-1])

        assert observed["codec"] is False, "no codec is possible without pydantic in the process"
        assert observed["pydantic"] == [], f"pydantic modules imported: {observed['pydantic']}"
