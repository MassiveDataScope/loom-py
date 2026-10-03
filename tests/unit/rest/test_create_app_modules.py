"""``create_app(modules=...)`` registers caller DI bindings before verification.

A use case whose constructor needs a port no repository provides (a clock, an
identity verifier, a bridge between bounded contexts) starts only when a
module binds that port; without the module, startup fails.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope
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


def _write_project(tmp_path: Path) -> str:
    module = f"loom_modules_fixture_app_{_NON_IDENTIFIER.sub('_', tmp_path.name)}"
    (tmp_path / f"{module}.py").write_text(_APP_SOURCE, encoding="utf-8")
    config = {
        "app": {
            "name": "modules-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [module], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _bind_clock_instance(container: LoomContainer) -> None:
    container.register(Clock, lambda: FixedClock(), scope=Scope.APPLICATION)


def _bind_clock_class(container: LoomContainer) -> None:
    container.register(Clock, FixedClock)


@pytest.mark.parametrize("module", [_bind_clock_instance, _bind_clock_class])
def test_a_module_binding_reaches_the_use_case(
    tmp_path: Path, module: Callable[[LoomContainer], None]
) -> None:
    app = create_app(_write_project(tmp_path), modules=[module])

    with TestClient(app) as client:
        response = client.get("/now/")

    assert response.status_code == 200, response.text
    assert response.json() == _FIXED_NOW


def test_modules_run_in_the_given_order(tmp_path: Path) -> None:
    calls: list[str] = []

    def first(container: LoomContainer) -> None:
        calls.append("first")
        _bind_clock_instance(container)

    def second(_: LoomContainer) -> None:
        calls.append("second")

    create_app(_write_project(tmp_path), modules=(first, second))

    assert calls == ["first", "second"]


def test_startup_fails_without_the_module(tmp_path: Path) -> None:
    with pytest.raises(ResolutionError, match=r"NowUseCase injects clock: .*Clock"):
        create_app(_write_project(tmp_path))
