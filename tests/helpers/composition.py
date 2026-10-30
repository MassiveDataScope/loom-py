"""Fixtures for tests that compose an application: a project on disk, and its openings."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from loom.composition import AppConfig, Application, CompositionSource, Resource, ResourceKind
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


def application_of(*resources: Resource) -> Application:
    """An application owning *resources*, whose composition facts play no part in its lifecycle."""
    unused = cast(Any, SimpleNamespace())
    return Application(
        kernel=unused,
        config=unused,
        environment=unused,
        code_path=unused,
        discovered=unused,
        observability=unused,
        sql=unused,
        ai=unused,
        persistence=unused,
        resources=resources,
    )


class TaskAffine:
    """Refuses, as ``AgentRuntime`` and an MCP client do, an exit from a task it was not entered in.

    Attributes:
        entered_in: The task that entered it.
        exited_in: The task that exited it, once it has.
    """

    def __init__(self) -> None:
        self.entered_in: asyncio.Task[Any] | None = None
        self.exited_in: asyncio.Task[Any] | None = None

    def resource(self) -> Resource:
        """The agent runtime slot of an application, opening and closing this."""
        return Resource(kind=ResourceKind.AGENTS, name="agents", manager=lambda: self)

    async def __aenter__(self) -> None:
        self.entered_in = asyncio.current_task()

    async def __aexit__(self, *exc_info: object) -> None:
        current = asyncio.current_task()
        if current is not self.entered_in:
            raise RuntimeError("must be exited by the task that entered it")
        self.exited_in = current
