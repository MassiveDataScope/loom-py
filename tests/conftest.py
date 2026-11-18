"""Root test configuration and golden testing fixtures."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
from moto import mock_aws
from omegaconf import OmegaConf

from loom.core.config import default_resolvers
from loom.core.engine.plan import ExecutionPlan
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.logger import configure_logging
from loom.core.plugins.entrypoints import list_entry_points
from loom.testing.golden import GoldenHarness, _serialize_result, serialize_plan
from tests.helpers.aws import ACCOUNT, FAKE_KEY, REGION, is_moto_answering, scrub_aws_environment
from tests.helpers.aws_island import FakeAwsIsland
from tests.helpers.extras import BOTO3_ISLAND, without_extra

pytest_plugins = ["tests.helpers.delegation"]

_GOLDEN_DIR = Path(__file__).parent / "golden"
_PLANS_DIR = _GOLDEN_DIR / "plans"
_OUTPUTS_DIR = _GOLDEN_DIR / "outputs"
_BUILTIN_RESOLVER_NAMES = tuple(resolver.name for resolver in default_resolvers())


def _clear_builtin_resolvers() -> None:
    for name in _BUILTIN_RESOLVER_NAMES:
        if OmegaConf.has_resolver(name):
            OmegaConf.clear_resolver(name)


@pytest.fixture
def clear_builtin_resolvers() -> Iterator[None]:
    """Unregister the built-in resolvers before and after the test."""
    _clear_builtin_resolvers()
    yield
    _clear_builtin_resolvers()


@pytest.fixture(autouse=True)
def _bare_environment(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove the AWS setup and the proxy variables, and let every host bypass a proxy.

    A ``live`` test keeps the environment it was given.
    """
    if request.node.get_closest_marker("live"):
        return
    scrub_aws_environment(monkeypatch)
    for name in tuple(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")


@pytest.fixture
def mocked_aws(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Serve every AWS service from moto, in ``REGION`` and the account of ``ACCOUNT``.

    Raises:
        AssertionError: When moto still answers after the test, which means a mock leaked.
    """
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("MOTO_ACCOUNT_ID", ACCOUNT)
    with mock_aws():
        yield
    assert not is_moto_answering(), "moto still answers after the test: a mock was left running"


@pytest.fixture
def island(monkeypatch: pytest.MonkeyPatch) -> FakeAwsIsland:
    """The fake AWS island, handing out a mock client per build."""
    return FakeAwsIsland().install(monkeypatch)


@pytest.fixture
def no_aws_clients(island: FakeAwsIsland) -> None:
    """Hand every AWS client loom builds out as a mock: nothing reaches AWS or moto."""


@pytest.fixture
def absent_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``aws`` extra is absent."""
    without_extra(monkeypatch, ["boto3"], [BOTO3_ISLAND])


@pytest.fixture
def dev_profile(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> str:
    """A config file with the profile ``dev``, fake keys included; returns the profile name."""
    config = tmp_path / "config"
    config.write_text(
        f"[profile dev]\nregion = {REGION}\naws_access_key_id = {FAKE_KEY}\n"
        f"aws_secret_access_key = {FAKE_KEY}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    return "dev"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Regenerate golden snapshot files instead of comparing them.",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Run reload-based contract tests last, and let ``live`` tests use the network.

    Some ETL contract tests intentionally call ``importlib.reload`` on modules.
    Running those tests early can invalidate class/enum identity assumptions
    in already-imported tests from other folders.

    Keep collection deterministic by pushing any test file containing reload
    helpers to the end of the global test run.
    """

    items.sort(key=_is_reload_contract_item)
    for item in items:
        if item.get_closest_marker("live"):
            item.add_marker(pytest.mark.enable_socket)


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


@pytest.fixture(autouse=True)
def _entry_points_read_afresh() -> Iterator[None]:
    """Read the installed entry points afresh in each test.

    ``list_entry_points`` keeps each group for the life of the process; a test
    that replaces ``entry_points`` or installs a distribution would otherwise
    resolve plugins from what an earlier test read.
    """
    list_entry_points.cache_clear()
    yield
    list_entry_points.cache_clear()


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
