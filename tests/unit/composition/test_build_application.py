"""Composition without a transport, and an adapter written outside loom."""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from pathlib import Path
from typing import Any, assert_type

from loom.composition import (
    Application,
    CompositionSource,
    Recipe,
    ResourceKind,
    build_application,
    create_app,
)
from loom.core.command import Command
from loom.core.config import ConfigContext
from loom.core.model import BaseModel
from loom.core.persistence import PersistenceWiring
from loom.core.persistence.profile import (
    SERVER_PROFILE,
    BackendRuntime,
    PoolDefaults,
    RuntimeProfile,
)
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.use_case.use_case import UseCase
from tests.helpers.composition import ProjectFactory


class _Bare:
    """No transport at all: what a script or a test composes."""

    def recipe(self, source: CompositionSource) -> Recipe:
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=source.discover(),
            backend=source.backend(default="sqlalchemy"),
        )

    def bind(self, application: Application) -> Application:
        return application


class _CreateRecord(Command, frozen=True):
    name: str


async def test_composes_and_serves_without_any_transport(
    fixture_project: ProjectFactory,
) -> None:
    application = build_application(fixture_project(), adapter=_Bare(), environment="test")
    (record,) = application.discovered.models
    (ping,) = application.discovered.use_cases

    assert not application.is_open
    assert [r.kind for r in application.resources] == [ResourceKind.STORE]
    async with application.running():
        repository = application.kernel.container.resolve_repo(record)
        created = await repository.create(_CreateRecord(name="first"))
        found = await repository.get_by_id(created.id)
        answer = await application.kernel.app.invoke(ping)

    assert found is not None
    assert found.name == "first"
    assert answer == "pong"


_TINY = RuntimeProfile(
    name="tiny",
    pool=PoolDefaults(size=1, max_overflow=0, timeout_s=5, recycle_s=300, pre_ping=True),
)


class _Answer(UseCase[Any, int]):
    async def execute(self, **_: Any) -> int:
        return 42


class _Recorded(SQLAlchemyBackend):
    """The SQLAlchemy backend, recording the runtime the composition hands it."""

    def __init__(self, runtimes: list[BackendRuntime]) -> None:
        self.runtimes = runtimes

    def build(
        self, ctx: ConfigContext, models: Sequence[type[BaseModel]], runtime: BackendRuntime
    ) -> PersistenceWiring:
        self.runtimes.append(runtime)
        return super().build(ctx, models, runtime)


class _Handler:
    def __init__(self, application: Application) -> None:
        self.application = application

    async def __call__(self) -> int:
        return int(await self.application.kernel.app.invoke(_Answer))


# The adapter under test: a profile of its own, one extra use case, its own result type.
class _TinyAdapter:
    def __init__(self, runtimes: list[BackendRuntime]) -> None:
        self.runtimes = runtimes

    def recipe(self, source: CompositionSource) -> Recipe:
        discovered = source.discover()
        return Recipe(
            profile=_TINY,
            discovered=dataclasses.replace(discovered, use_cases=(*discovered.use_cases, _Answer)),
            backend=_Recorded(self.runtimes),
        )

    def bind(self, application: Application) -> _Handler:
        return _Handler(application)


async def test_an_adapter_of_its_own_gets_its_profile_its_use_case_and_its_type(
    fixture_project: ProjectFactory,
) -> None:
    runtimes: list[BackendRuntime] = []
    config = fixture_project()

    handler = create_app(config, adapter=_TinyAdapter(runtimes), environment="test")

    assert_type(handler, _Handler)
    assert [runtime.profile for runtime in runtimes] == [_TINY]
    async with handler.application.running():
        assert await handler() == 42


def test_the_adapter_under_test_stays_under_thirty_lines() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    start = source.index("class _TinyAdapter:")
    end = source.index("async def test_an_adapter_of_its_own")
    lines = [line for line in source[start:end].splitlines() if line.strip()]

    assert len(lines) < 30
