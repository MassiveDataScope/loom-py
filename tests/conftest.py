"""Root test configuration and golden testing fixtures."""

from __future__ import annotations

import json
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.engine.plan import ExecutionPlan
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.logger import configure_logging
from loom.testing.golden import GoldenHarness, _serialize_result, serialize_plan

_GOLDEN_DIR = Path(__file__).parent / "golden"
_PLANS_DIR = _GOLDEN_DIR / "plans"
_OUTPUTS_DIR = _GOLDEN_DIR / "outputs"
_BUILTIN_RESOLVER_NAMES = ("secrets", "ssm")


def _clear_builtin_resolvers() -> None:
    for name in _BUILTIN_RESOLVER_NAMES:
        if OmegaConf.has_resolver(name):
            OmegaConf.clear_resolver(name)


@pytest.fixture
def clear_builtin_resolvers() -> Iterator[None]:
    """Unregister ``secrets`` and ``ssm`` before and after the test."""
    _clear_builtin_resolvers()
    yield
    _clear_builtin_resolvers()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Regenerate golden snapshot files instead of comparing them.",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Run reload-based contract tests last to avoid module identity drift.

    Some ETL contract tests intentionally call ``importlib.reload`` on modules.
    Running those tests early can invalidate class/enum identity assumptions
    in already-imported tests from other folders.

    Keep collection deterministic by pushing any test file containing reload
    helpers to the end of the global test run.
    """

    items.sort(key=_is_reload_contract_item)


def _is_reload_contract_item(item: pytest.Item) -> bool:
    path_obj = getattr(item, "path", None)
    if isinstance(path_obj, Path):
        return _file_has_reload_calls(path_obj)
    fspath = getattr(item, "fspath", None)
    if fspath is None:
        return False
    return _file_has_reload_calls(Path(str(fspath)))


@lru_cache(maxsize=512)
def _file_has_reload_calls(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return "importlib.reload(" in text or "_reload_module(" in text or "_reload_modules(" in text


@pytest.fixture
def update_golden(request: pytest.FixtureRequest) -> bool:
    """Return ``True`` when the ``--update-golden`` flag is active."""
    return bool(request.config.getoption("--update-golden"))


@pytest.fixture
def assert_plan_snapshot(update_golden: bool) -> Any:
    """Fixture that compares an ExecutionPlan against a stored JSON snapshot.

    On first run (no snapshot file) the snapshot is written automatically.
    Pass ``--update-golden`` to force-regenerate existing snapshots.

    Example::

        def test_my_plan(assert_plan_snapshot):
            plan = compiler.get_plan(MyUseCase)
            assert_plan_snapshot(plan, "my_use_case")
    """

    def _assert(plan: ExecutionPlan, name: str) -> None:
        snapshot_path = _PLANS_DIR / f"{name}.json"
        current = json.dumps(serialize_plan(plan), indent=2, sort_keys=True)

        if update_golden or not snapshot_path.exists():
            _PLANS_DIR.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(current)
            return

        stored = snapshot_path.read_text()
        assert current == stored, (
            f"ExecutionPlan snapshot mismatch for '{name}'.\n"
            f"Run with --update-golden to regenerate.\n"
            f"Expected:\n{stored}\n\nActual:\n{current}"
        )

    return _assert


@pytest.fixture
def golden_harness() -> GoldenHarness:
    """Return a fresh :class:`~loom.testing.golden.GoldenHarness` instance."""
    return GoldenHarness()


@pytest.fixture
def assert_output_snapshot(update_golden: bool) -> Any:
    """Fixture that compares a use-case output against a stored JSON snapshot.

    Example::

        async def test_output(assert_output_snapshot, golden_harness):
            harness.inject_repo(IRepo, FakeRepo())
            result = await harness.run(MyUseCase, payload={"name": "x"})
            assert_output_snapshot(result, "my_use_case_create")
    """

    def _assert(result: Any, name: str) -> None:
        snapshot_path = _OUTPUTS_DIR / f"{name}.json"
        current = json.dumps(_serialize_result(result), indent=2, sort_keys=True)

        if update_golden or not snapshot_path.exists():
            _OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(current)
            return

        stored = snapshot_path.read_text()
        assert current == stored, (
            f"Output snapshot mismatch for '{name}'.\n"
            f"Run with --update-golden to regenerate.\n"
            f"Expected:\n{stored}\n\nActual:\n{current}"
        )

    return _assert


@pytest.fixture(scope="session", autouse=True)
def _test_environment() -> Iterator[None]:
    """Run the whole suite in the ``test`` environment, as ``LOOM_ENV=test`` would.

    Without it every application the suite builds would start as production,
    loom's default, and run the deploy checks. A test about the production
    path names the environment itself, or removes the variable.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv(ENVIRONMENT_VARIABLE, Environment.TEST.value)
        yield


@pytest.fixture(scope="session", autouse=True)
def _configure_loom_logging(_test_environment: None) -> None:
    """Configure logging once per session, as an application does at startup.

    Without this the suite ran against structlog's *unconfigured* default,
    which is a pipeline no application ever uses and which does not survive
    pytest's default fd-level capture: ``structlog/_output.py`` binds
    ``from sys import stdout`` at import time, so ``PrintLogger`` freezes
    whatever ``sys.stdout`` was when structlog was first imported. Under
    ``--capture=fd`` that is the temporary capture file of whichever test
    happened to import it first; once that test finished and pytest closed the
    descriptor, every later log call raised ``OSError: Bad file descriptor``.

    The symptom was eight unrelated integration tests failing in a full run
    while passing in isolation. Configuring logging installs
    ``structlog.stdlib.LoggerFactory``, which routes through stdlib logging and
    never touches that frozen stream — and makes the suite exercise the same
    logging path production does.
    """
    configure_logging()
