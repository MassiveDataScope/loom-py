"""Composition without a transport, and an adapter written outside loom."""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from pathlib import Path
from typing import Any, assert_type

import pytest

from loom.composition import (
    Application,
    CompositionSource,
    Recipe,
    build_application,
    create_app,
    load_source,
)
from loom.composition.resources import ResourceKind
from loom.core.command import Command
from loom.core.config import ConfigContext, ConfigError
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
            default_backend="sqlalchemy",
        )

    def bind(self, application: Application) -> Application:
        return application


def test_load_source_requires_app_name_with_or_without_app_section(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    options = {
        "config_dir": None,
        "environment": "test",
        "code_path": None,
        "resolvers": (),
        "caller": "create_app",
    }

    config.write_text("celery: {}\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="app.name is required"):
        load_source(str(config), **options)

    config.write_text("app:\n  code_path: .\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="app.name is required"):
        load_source(str(config), **options)


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


@pytest.fixture
def runtimes(monkeypatch: pytest.MonkeyPatch) -> list[BackendRuntime]:
    """Record the runtime the composition hands the SQLAlchemy backend."""
    recorded: list[BackendRuntime] = []
    build = SQLAlchemyBackend.build

    def recording(
        self: SQLAlchemyBackend,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        recorded.append(runtime)
        return build(self, ctx, models, runtime)

    monkeypatch.setattr(SQLAlchemyBackend, "build", recording)
    return recorded


class _Handler:
    def __init__(self, application: Application) -> None:
        self.application = application

    async def __call__(self) -> int:
        return int(await self.application.kernel.app.invoke(_Answer))


# The adapter under test: a profile of its own, one extra use case, its own result type.
class _TinyAdapter:
    def recipe(self, source: CompositionSource) -> Recipe:
        discovered = source.discover()
        return Recipe(
            profile=_TINY,
            discovered=dataclasses.replace(discovered, use_cases=(*discovered.use_cases, _Answer)),
        )

    def bind(self, application: Application) -> _Handler:
        return _Handler(application)


async def test_an_adapter_of_its_own_gets_its_profile_its_use_case_and_its_type(
    fixture_project: ProjectFactory, runtimes: list[BackendRuntime]
) -> None:
    config = fixture_project()

    handler = create_app(config, adapter=_TinyAdapter(), environment="test")

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


def test_the_composition_registers_no_elevation_sink_without_a_store_that_elevates(
    fixture_project: ProjectFactory,
) -> None:
    application = build_application(fixture_project(), adapter=_Bare(), environment="test")

    assert application.kernel.executor._elevation_sink is None


def test_the_rollback_deadline_comes_from_the_persistence_section(
    fixture_project: ProjectFactory,
) -> None:
    project = fixture_project(persistence={"rollback_timeout": 5})

    application = build_application(project, adapter=_Bare(), environment="test")

    assert application.kernel.executor._coordinator._rollback_timeout == 5.0


@pytest.mark.parametrize("timeout", [0, -1])
def test_a_rollback_deadline_not_above_zero_is_refused(
    fixture_project: ProjectFactory, timeout: int
) -> None:
    project = fixture_project(persistence={"rollback_timeout": timeout})

    with pytest.raises(ConfigError, match="persistence.rollback_timeout must be above zero"):
        build_application(project, adapter=_Bare(), environment="test")
