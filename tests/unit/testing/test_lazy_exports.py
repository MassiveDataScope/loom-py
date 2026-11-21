"""Lazy exports of ``loom.testing`` that depend on optional extras.

The import check runs in a clean subprocess so that modules imported by
unrelated suites inside the pytest interpreter cannot mask a dependency leak.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import loom.testing
from loom.testing import repository_harness

_SRC = Path(__file__).resolve().parents[3] / "src"

_IMPORT_TESTING_SCRIPT = """
import sys
sys.modules["sqlalchemy"] = None
sys.modules["mangum"] = None
sys.modules["httpx"] = None
from loom.testing import (  # noqa: F401
    GoldenHarness,
    InMemoryRepository,
    RecordingJobTransport,
    UseCaseTest,
    LambdaEvent,
)
"""

_OPTIONAL_EXPORTS = [
    ("HttpTestHarness", "loom.testing.http_harness", "fastapi", "'rest' extra"),
    ("LambdaTestClient", "loom.testing.lambda_http", "httpx", "loom-kernel[lambda,testing]"),
    ("make", "loom.testing.data", "polyfactory", "loom-kernel[testing]"),
    ("SampleFactory", "loom.testing.data", "polyfactory", "loom-kernel[testing]"),
    (
        "RepositoryIntegrationHarness",
        "loom.testing.repository_harness",
        "sqlalchemy",
        "loom-kernel[sqlalchemy]",
    ),
]

_SQLALCHEMY_EXPORTS = ("RepositoryIntegrationHarness", "ScenarioDict", "build_repository_harness")


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


class TestLazyExports:
    def test_loom_testing_imports_without_optional_extras(self) -> None:
        """The eager exports import with ``sqlalchemy``, ``mangum`` and ``httpx`` hidden."""
        result = _run_in_clean_interpreter(_IMPORT_TESTING_SCRIPT)

        assert result.returncode == 0, f"import loom.testing failed:\n{result.stderr}"

    @pytest.mark.parametrize("name", _SQLALCHEMY_EXPORTS)
    def test_repository_harness_exports_resolve_lazily(self, name: str) -> None:
        "Each SQLAlchemy-backed export resolves to the object defined in ``repository_harness``."
        assert getattr(loom.testing, name) is getattr(repository_harness, name)

    @pytest.mark.parametrize(("name", "module", "dependency", "extra"), _OPTIONAL_EXPORTS)
    def test_export_names_its_extra_when_its_dependency_is_missing(
        self,
        name: str,
        module: str,
        dependency: str,
        extra: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        reloaded = (dependency, "loom.stores.sqlalchemy")
        for loaded in [m for m in sys.modules if m.startswith(reloaded)]:
            monkeypatch.delitem(sys.modules, loaded)
        monkeypatch.delitem(sys.modules, module, raising=False)
        monkeypatch.setitem(sys.modules, dependency, None)

        with pytest.raises(ImportError, match=re.escape(extra)):
            getattr(loom.testing, name)

    @pytest.mark.parametrize(("name", "module", "dependency", "extra"), _OPTIONAL_EXPORTS)
    def test_export_lets_any_other_missing_module_surface(
        self,
        name: str,
        module: str,
        dependency: str,
        extra: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setitem(sys.modules, module, None)

        with pytest.raises(ImportError) as raised:
            getattr(loom.testing, name)

        assert raised.value.name == module

    def test_unknown_attribute_raises_attribute_error(self) -> None:
        """Names outside the lazy export set keep the standard ``AttributeError``."""
        with pytest.raises(AttributeError, match="does_not_exist"):
            _ = loom.testing.does_not_exist
