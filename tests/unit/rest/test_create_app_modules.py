"""Discovered installers register DI bindings before verification.

A use case whose constructor needs a port no repository provides (a clock, an
identity verifier, a bridge between bounded contexts) starts only when a
module binds that port; without the module, startup fails.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.core.bootstrap import BootstrapError
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.installer import Installer
from loom.core.di.scope import Scope
from loom.core.discovery.scanner import PackageScanner
from loom.rest.fastapi.auto import create_app

_FIXED_NOW = "2026-01-01T00:00:00Z"
_NON_IDENTIFIER = re.compile(r"\W", re.ASCII)


class Clock(Protocol):
    """Port the fixture use case injects; bound only by a caller module."""

    def now(self) -> str: ...


class FixedClock:
    """Implementation bound to :class:`Clock` by the modules under test."""

    def now(self) -> str:
        return _FIXED_NOW


_APP_SOURCE = '''\
"""Fixture app whose use case injects a port no repository provides."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute
from tests.unit.rest.test_create_app_modules import Clock


class ClockRecord(BaseModel):
    __tablename__ = "clock_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)


class NowUseCase(UseCase[ClockRecord, str]):
    def __init__(self, clock: Clock) -> None:
        super().__init__()
        self._clock = clock

    async def execute(self, **kwargs: Any) -> str:
        return self._clock.now()


class NowInterface(RestInterface[str]):
    prefix = "/now"
    routes = (RestRoute(use_case=NowUseCase, method="GET", path="/"),)
'''


@pytest.fixture
def config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Write the fixture project; undo its sys.path entry and imported module."""
    module = f"loom_modules_fixture_app_{_NON_IDENTIFIER.sub('_', tmp_path.name)}"
    package = tmp_path / module
    package.mkdir()
    (package / "__init__.py").write_text(_APP_SOURCE, encoding="utf-8")
    config = {
        "app": {
            "name": "modules-demo",
            "code_path": ".",
            "packages": [module],
        },
        "stores": {
            "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"},
            "settings": {"cursors": {"signing": "ephemeral"}},
        },
    }
    config_file = tmp_path / "app.yaml"
    config_file.write_text(yaml.safe_dump(config), encoding="utf-8")
    # Snapshots sys.path, so create_app's own code_path insert is undone too.
    monkeypatch.syspath_prepend(str(tmp_path.resolve()))
    yield str(config_file)
    sys.modules.pop(module, None)


def _bind_clock_instance(container: LoomContainer) -> None:
    container.register(Clock, lambda: FixedClock(), scope=Scope.APPLICATION)


def _bind_clock_class(container: LoomContainer) -> None:
    container.register(Clock, FixedClock)


class TestCreateAppInstallers:
    @pytest.mark.parametrize("binding", [_bind_clock_instance, _bind_clock_class])
    def test_an_installer_binding_reaches_the_use_case(
        self, config_path: str, binding, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class ClockInstaller(Installer):
            def install(self, container: LoomContainer) -> None:
                binding(container)

        original = PackageScanner.discover
        monkeypatch.setattr(
            PackageScanner,
            "discover",
            lambda self, **kwargs: original(self, **kwargs).with_changes(
                installers=(ClockInstaller,)
            ),
        )
        app = create_app(config_path)

        with TestClient(app) as client:
            response = client.get("/now/")

        assert response.status_code == 200, response.text
        assert response.json() == _FIXED_NOW

    def test_installers_run_in_the_given_order(
        self, config_path: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        class First(Installer):
            def install(self, container: LoomContainer) -> None:
                calls.append("first")
                _bind_clock_instance(container)

        class Second(Installer):
            def install(self, container: LoomContainer) -> None:
                calls.append("second")

        original = PackageScanner.discover
        monkeypatch.setattr(
            PackageScanner,
            "discover",
            lambda self, **kwargs: original(self, **kwargs).with_changes(
                installers=(First, Second)
            ),
        )
        create_app(config_path)

        assert calls == ["first", "second"]

    def test_startup_fails_without_an_installer(self, config_path: str) -> None:
        with pytest.raises(BootstrapError, match=r"NowUseCase -> clock: .*Clock") as caught:
            create_app(config_path)

        assert isinstance(caught.value.__cause__, ResolutionError)
