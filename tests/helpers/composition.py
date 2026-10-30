"""Fixtures for tests that compose an application: a project on disk, and its openings."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, Application, CompositionSource
from loom.core.config import ConfigContext, ConfigKey
from loom.core.environment import Environment
from loom.core.observability.runtime import ObservabilityRuntime
from tests.unit.rest._fixture_app import write_project

ProjectFactory = Callable[..., str]


def composition_source(
    ctx: ConfigContext,
    *,
    environment: Environment = Environment.TEST,
    app: AppConfig | None = None,
    code_path: Path | None = None,
    config_root: Path | None = None,
) -> CompositionSource:
    """What ``build_application`` would have resolved from files holding *ctx*.

    Without *app*, the ``app`` section of *ctx* is decoded; the paths default
    to the working directory.
    """
    return CompositionSource(
        config=ctx,
        environment=environment,
        code_path=code_path if code_path is not None else Path.cwd(),
        config_root=config_root if config_root is not None else Path.cwd(),
        app=app if app is not None else ctx.section(ConfigKey.APP, AppConfig),
        observability=ObservabilityRuntime.noop(),
    )


@pytest.fixture
def fixture_project(tmp_path: Path) -> ProjectFactory:
    """Write a discoverable fixture project in its own directory and return its config path.

    Each name gets its own directory and module, so one test can compose
    several applications side by side. Keyword arguments go to
    :func:`tests.unit.rest._fixture_app.write_project`.
    """

    def write(name: str = "app", **options: Any) -> str:
        directory = tmp_path / name
        directory.mkdir()
        return write_project(directory, **options)

    return write


@pytest.fixture
def application_openings(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record ``"open"`` and ``"close"`` each time an :class:`Application` runs."""
    events: list[str] = []
    running = Application.running

    @asynccontextmanager
    async def recorded(self: Application, **kwargs: Any) -> AsyncIterator[Application]:
        events.append("open")
        async with running(self, **kwargs) as application:
            yield application
        events.append("close")

    monkeypatch.setattr(Application, "running", recorded)
    return events
