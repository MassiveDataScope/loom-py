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
from loom.testing import (  # noqa: F401
    GoldenHarness,
    InMemoryRepository,
    RecordingJobService,
    UseCaseTest,
    sqs_record,
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


def test_loom_testing_imports_without_sqlalchemy_or_mangum() -> None:
    """The eager exports import with ``sqlalchemy`` and ``mangum`` hidden."""
    result = _run_in_clean_interpreter(_IMPORT_TESTING_SCRIPT)

    assert result.returncode == 0, f"import loom.testing failed:\n{result.stderr}"


@pytest.mark.parametrize("name", _SQLALCHEMY_EXPORTS)
def test_repository_harness_exports_resolve_lazily(name: str) -> None:
    """Each SQLAlchemy-backed export resolves to the object defined in ``repository_harness``."""
    assert getattr(loom.testing, name) is getattr(repository_harness, name)


@pytest.mark.parametrize("name", _SQLALCHEMY_EXPORTS)
def test_repository_harness_exports_name_sqlalchemy_extra_when_missing(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolving a SQLAlchemy-backed export without the extra raises a hinting ImportError."""
    monkeypatch.setitem(sys.modules, "loom.testing.repository_harness", None)

    with pytest.raises(ImportError, match=r"loom-kernel\[sqlalchemy\]"):
        getattr(loom.testing, name)


@pytest.mark.parametrize("name", ["LambdaResponse", "LambdaTestClient"])
def test_lambda_http_exports_name_the_lambda_extra_when_missing(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "loom.testing.lambda_http", None)

    with pytest.raises(ImportError, match=r"loom-kernel\[lambda\]"):
        getattr(loom.testing, name)


def test_unknown_attribute_raises_attribute_error() -> None:
    """Names outside the lazy export set keep the standard ``AttributeError``."""
    with pytest.raises(AttributeError, match="does_not_exist"):
        _ = loom.testing.does_not_exist
