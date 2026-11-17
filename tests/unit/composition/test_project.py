"""Composition without a transport, and an adapter written outside loom."""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any, assert_type

import pytest

from loom.composition import Application, Project, Recipe, create_app
from loom.composition.resources import ResourceKind
from loom.core.authz.elevation import ElevationSink
from loom.core.command import Command
from loom.core.config import ConfigContext, ConfigError
from loom.core.model import BaseModel
from loom.core.stores import StoreWiring
from loom.core.stores.profile import (
    SERVER_PROFILE,
    PoolDefaults,
    RuntimeProfile,
    StoreRuntime,
)
from loom.core.use_case.use_case import UseCase
from loom.stores.sqlalchemy.backend import SQLAlchemyBackend
from tests.helpers.composition import ProjectFactory, coordinator_of


class AcmeTransportFreeAdapter:
    """No transport at all: what a script or a test composes."""

    def recipe(self, project: Project) -> Recipe:
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=project.discovered,
            default_store_backend="sqlalchemy",
        )

    def bind(self, application: Application) -> Application:
        return application


def _bootstrap_worker(*paths: str, config_dir: str) -> object:
    celery_bootstrap = pytest.importorskip("loom.celery.bootstrap")
    return celery_bootstrap.bootstrap_worker(*paths, config_dir=config_dir)


class _CreateRecord(Command, frozen=True):
    name: str


_TINY = RuntimeProfile(
    name="tiny",
    pool=PoolDefaults(size=1, max_overflow=0, timeout_s=5, recycle_s=300, pre_ping=True),
)


class _Answer(UseCase[Any, int]):
    async def execute(self, **_: Any) -> int:
        return 42


class _Handler:
    def __init__(self, application: Application) -> None:
        self.application = application

    async def __call__(self) -> int:
        return int(await self.application.kernel.app.invoke(_Answer))


# The adapter under test: a profile of its own, one extra use case, its own result type.
class _TinyAdapter:
    def recipe(self, project: Project) -> Recipe:
        discovered = project.discovered
        return Recipe(
            profile=_TINY,
            discovered=dataclasses.replace(discovered, use_cases=(*discovered.use_cases, _Answer)),
        )

    def bind(self, application: Application) -> _Handler:
        return _Handler(application)


class TestTransportFreeComposition:
    async def test_an_adapter_of_its_own_gets_its_profile_its_use_case_and_its_type(
        self, fixture_project: ProjectFactory, runtimes: list[StoreRuntime]
    ) -> None:
        config = fixture_project()

        handler = create_app(config, adapter=_TinyAdapter(), environment="test")

        assert_type(handler, _Handler)
        assert [runtime.profile for runtime in runtimes] == [_TINY]
        async with handler.application.running():
            assert await handler() == 42

    async def test_composes_and_serves_without_any_transport(
        self,
        fixture_project: ProjectFactory,
    ) -> None:
        application = Project.load(fixture_project(), environment="test").compose(
            AcmeTransportFreeAdapter()
        )
        (record,) = application.discovered.models
        (ping,) = application.discovered.use_cases

        assert not application.is_open
        assert [r.kind for r in application.resources] == [
            ResourceKind.AWS,
            ResourceKind.STORE,
            ResourceKind.TIMER,
        ]
        async with application.running():
            repository = application.kernel.container.resolve_repo(record)
            created = await repository.create(_CreateRecord(name="first"))
            found = await repository.get_by_id(created.id)
            answer = await application.kernel.app.invoke(ping)

        assert found is not None
        assert found.name == "first"
        assert answer == "pong"

    def test_serve_mounts_the_adapter_transport_on_the_composed_application(
        self, fixture_project: ProjectFactory
    ) -> None:
        project = Project.load(fixture_project(), environment="test")

        handler = project.serve(_TinyAdapter())

        assert_type(handler, _Handler)
        assert isinstance(handler.application, Application)
        assert not handler.application.is_open

    def test_the_adapter_under_test_stays_under_thirty_lines(self) -> None:
        source = Path(__file__).read_text(encoding="utf-8")
        start = source.index("class _TinyAdapter:")
        end = source.index("async def test_an_adapter_of_its_own")
        lines = [line for line in source[start:end].splitlines() if line.strip()]

        assert len(lines) < 30

    def test_the_composition_registers_no_elevation_sink_without_a_store_that_elevates(
        self,
        fixture_project: ProjectFactory,
    ) -> None:
        application = Project.load(fixture_project(), environment="test").compose(
            AcmeTransportFreeAdapter()
        )

        assert application.kernel.container.resolve_or_none(ElevationSink) is None

    def test_the_rollback_deadline_comes_from_the_shared_store_settings(
        self,
        fixture_project: ProjectFactory,
    ) -> None:
        project = fixture_project(settings={"rollback_timeout": 5})

        application = Project.load(project, environment="test").compose(AcmeTransportFreeAdapter())

        assert coordinator_of(application.kernel.executor)._rollback_timeout == 5.0  # noqa: SLF001

    @pytest.mark.parametrize(
        "timeout", [0, -1, math.inf, math.nan], ids=["zero", "negative", "infinite", "nan"]
    )
    def test_a_rollback_deadline_not_a_finite_number_above_zero_is_refused(
        self, fixture_project: ProjectFactory, timeout: float
    ) -> None:
        project = fixture_project(settings={"rollback_timeout": timeout})
        adapter = AcmeTransportFreeAdapter()

        with pytest.raises(
            ConfigError, match="stores.settings.rollback_timeout must be a finite number above zero"
        ):
            Project.load(project, environment="test").compose(adapter)

    @pytest.fixture
    def runtimes(self, monkeypatch: pytest.MonkeyPatch) -> list[StoreRuntime]:
        """Record the runtime the composition hands the SQLAlchemy backend."""
        recorded: list[StoreRuntime] = []
        build = SQLAlchemyBackend.build

        def recording(
            self: SQLAlchemyBackend,
            ctx: ConfigContext,
            models: Sequence[type[BaseModel]],
            runtime: StoreRuntime,
        ) -> StoreWiring:
            recorded.append(runtime)
            return build(self, ctx, models, runtime)

        monkeypatch.setattr(SQLAlchemyBackend, "build", recording)
        return recorded


class TestLoadSource:
    def test_project_load_requires_app_name_with_or_without_app_section(
        self, tmp_path: Path
    ) -> None:
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
            Project.load(str(config), **options)

        config.write_text("app:\n  code_path: .\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="app.name is required"):
            Project.load(str(config), **options)

    def test_project_load_needs_only_the_config_paths(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("app:\n  name: acme\n", encoding="utf-8")

        project = Project.load(str(config))

        assert project.app.name == "acme"

    @pytest.mark.parametrize(
        ("entry", "name"),
        [
            (
                lambda *p, config_dir: create_app(
                    *p, adapter=AcmeTransportFreeAdapter(), config_dir=config_dir
                ),
                "create_app",
            ),
            (
                lambda *p, config_dir: Project.load(*p, config_dir=config_dir),
                "Project.load",
            ),
            (_bootstrap_worker, "bootstrap_worker"),
        ],
        ids=["create_app", "project_load", "bootstrap_worker"],
    )
    def test_each_entry_point_names_itself_when_paths_conflict(
        self, tmp_path: Path, entry: Any, name: str
    ) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("app:\n  name: acme\n", encoding="utf-8")

        with pytest.raises(ConfigError, match=rf"^{name} takes config files"):
            entry(str(config), config_dir=str(tmp_path))
