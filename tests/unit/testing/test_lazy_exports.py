"""Lazy exports of ``loom.testing`` that depend on optional extras.

The import check runs in a clean subprocess so that modules imported by
unrelated suites inside the pytest interpreter cannot mask a dependency leak.
"""

from __future__ import annotations

import os
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

    @pytest.mark.parametrize("name", _SQLALCHEMY_EXPORTS)
    def test_repository_harness_exports_name_sqlalchemy_extra_when_missing(
        self, name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Resolving a SQLAlchemy-backed export without the extra raises a hinting ImportError."""
        monkeypatch.setitem(sys.modules, "loom.testing.repository_harness", None)

        with pytest.raises(ImportError, match=r"loom-kernel\[sqlalchemy\]"):
            getattr(loom.testing, name)

    def test_lambda_test_client_names_its_extras_when_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "loom.testing.lambda_http", None)

        with pytest.raises(ImportError, match=r"loom-kernel\[lambda,testing\]"):
            _ = loom.testing.LambdaTestClient

    @pytest.mark.parametrize("name", ["make", "LoomFactory"])
    def test_data_exports_name_the_testing_extra_when_polyfactory_is_missing(
        self, name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for module in [m for m in sys.modules if m.partition(".")[0] == "polyfactory"]:
            monkeypatch.delitem(sys.modules, module)
        monkeypatch.delitem(sys.modules, "loom.testing.data", raising=False)
        monkeypatch.setitem(sys.modules, "polyfactory", None)

        with pytest.raises(ImportError, match=r"requires polyfactory.*loom-kernel\[testing\]"):
            getattr(loom.testing, name)

    def test_data_exports_let_any_other_missing_module_surface(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "loom.testing.job_factories", None)

        with pytest.raises(ImportError) as raised:
            _ = loom.testing.make

        assert raised.value.name == "loom.testing.job_factories"

    def test_unknown_attribute_raises_attribute_error(self) -> None:
        """Names outside the lazy export set keep the standard ``AttributeError``."""
        with pytest.raises(AttributeError, match="does_not_exist"):
            _ = loom.testing.does_not_exist
