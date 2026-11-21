"""``CeleryRuntime`` composes the worker on the shared, transport-neutral recipe (US4, T026)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from celery import Celery

from loom.ai.abc import AgentHandle, McpHandle
from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX, TASK_JOB_PREFIX
from loom.celery.runtime import CeleryRuntime
from loom.composition import (
    AppConfig,
    Application,
    CompositionErrorCode,
    CompositionStateError,
    Project,
    Recipe,
)
from loom.core.cache.gateway import CacheGateway
from loom.core.checks import CheckScope
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.callback import JobCallback
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.stores import WORKER_PROFILE
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.sql import CallerBoundSql, SqlQueryService
from loom.core.use_case import Agent, Mcp
from loom.core.use_case.use_case import UseCase
from tests.helpers import acme_callbacks
from tests.helpers.acme_callbacks import AcmePingCallback
from tests.helpers.aws import OTHER_REGION
from tests.helpers.aws_island import FakeAwsIsland
from tests.helpers.composition import composition_project
from tests.helpers.loggers import CapturedLog
from tests.unit.core.cache._doubles import SERIALIZED_BACKEND

_CELERY = {"broker_url": "memory://", "result_backend": "cache+memory://"}
_SQLITE = "sqlite+aiosqlite:///:memory:"


class _PingJob(Job[int]):
    __queue__ = "default"

    def execute(self) -> int:
        return 1


class _SecondJob(Job[int]):
    __queue__ = "default"

    def execute(self) -> int:
        return 2


class _OtherCallback(JobCallback):
    pass


class _TriageUseCase(UseCase[object, object]):
    async def execute(self, triage: AgentHandle[dict] = Agent("triage")) -> object:
        return triage


class _KnowledgeUseCase(UseCase[object, object]):
    async def execute(self, gateway: McpHandle = Mcp("knowledge", include=["search_*"])) -> object:
        return gateway


def _project(tmp_path: Path, config: dict[str, Any], *modules: str) -> Project:
    return composition_project(
        ConfigContext.from_dict({"celery": _CELERY, **config}),
        app=AppConfig(name="worker", packages=list(modules)),
        code_path=tmp_path,
        config_root=tmp_path,
    )


def _application(
    tmp_path: Path,
    config: dict[str, Any],
    adapter: CeleryRuntime,
    *modules: str,
) -> Application:
    return _project(tmp_path, config, *modules).compose(adapter)


def _default_backend(project: Project, recipe: Recipe) -> str:
    """The backend the composition builds the default store of *recipe* with."""
    return project.resolve_store_plugin(DEFAULT_STORE, default=recipe.default_store_plugin).name


class TestBackendRule:
    """The rest of the rule (no default store, sink stores only, a retired key) is pinned by
    ``test_bootstrap_database``."""

    @pytest.mark.parametrize("backend", ["none", "mongo", "dynamodb"])
    def test_an_explicit_backend_wins_over_the_database_section(
        self, tmp_path: Path, backend: str
    ) -> None:
        config = {"stores": {"default": {"backend": backend, "url": _SQLITE}}}

        project = _project(tmp_path, config)
        recipe = CeleryRuntime(jobs=[_PingJob]).recipe(project)

        assert _default_backend(project, recipe) == backend

    def test_a_database_url_without_backend_serves_sqlalchemy(self, tmp_path: Path) -> None:
        stores = {"default": {"backend": "sqlalchemy", "url": _SQLITE}}
        project = _project(tmp_path, {"stores": stores})
        recipe = CeleryRuntime(jobs=[_PingJob]).recipe(project)

        assert _default_backend(project, recipe) == "sqlalchemy"

    def test_credentials_without_url_are_the_workers_own_section_decoded_strictly(
        self, tmp_path: Path
    ) -> None:
        project = _project(
            tmp_path, {"stores": {"default": {"backend": "sqlalchemy", "username": "app"}}}
        )

        recipe = CeleryRuntime(jobs=[_PingJob]).recipe(project)

        assert _default_backend(project, recipe) == "sqlalchemy"
        adapter = CeleryRuntime(jobs=[_PingJob])
        with pytest.raises(ConfigError, match="url"):
            project.compose(adapter)


class TestRecipe:
    def test_the_worker_declares_its_profile_checks_and_no_agents(self, tmp_path: Path) -> None:
        recipe = CeleryRuntime(jobs=[_PingJob]).recipe(_project(tmp_path, {}))

        assert recipe.profile is WORKER_PROFILE
        assert recipe.check_scope == CheckScope.without_rest()
        assert recipe.agents is False

    def test_jobs_and_use_cases_are_compiled(self, tmp_path: Path) -> None:
        class _Lookup(UseCase[object, object]):
            async def execute(self) -> object:
                return None

        recipe = CeleryRuntime(jobs=[_PingJob], use_cases=[_Lookup]).recipe(_project(tmp_path, {}))

        assert set(recipe.discovered.use_cases) == {_PingJob, _Lookup}

    def test_the_discovered_callbacks_reach_the_recipe_as_graph_roots(self, tmp_path: Path) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])

        recipe = adapter.recipe(_project(tmp_path, {}, acme_callbacks.MODULE))

        assert AcmePingCallback in recipe.discovered.callbacks
        assert AcmePingCallback not in recipe.discovered.use_cases

    def test_the_metrics_reach_the_recipe(self, tmp_path: Path) -> None:
        observer: Any = object()

        recipe = CeleryRuntime(jobs=[_PingJob], execution_observer=observer).recipe(
            _project(tmp_path, {})
        )

        assert recipe.execution_observer is observer

    def test_a_second_recipe_on_the_same_runtime_is_refused(self, tmp_path: Path) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])
        adapter.recipe(_project(tmp_path, {}))

        project = _project(tmp_path, {})
        with pytest.raises(
            CompositionStateError, match="one runtime instance composes one application"
        ) as raised:
            adapter.recipe(project)
        assert raised.value.issue.code is CompositionErrorCode.RUNTIME_ALREADY_COMPOSED

    def test_mount_before_recipe_is_refused(self) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])
        application: Any = object()

        with pytest.raises(CompositionStateError, match="CeleryRuntime.mount .* call recipe first"):
            adapter.mount(application)

    def test_a_failed_recipe_can_be_retried(self, tmp_path: Path) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])
        project = _project(tmp_path, {"celery": {"broker_url": 5}})
        with pytest.raises(ConfigError):
            adapter.recipe(project)

        assert adapter.recipe(_project(tmp_path, {})).profile is WORKER_PROFILE


class TestBind:
    def test_bind_returns_a_celery_app_with_jobs_and_discovered_callbacks(
        self, tmp_path: Path
    ) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])

        app = adapter.mount(_application(tmp_path, {}, adapter, acme_callbacks.MODULE))

        assert isinstance(app, Celery)
        assert f"{TASK_JOB_PREFIX}.{_PingJob.__qualname__}" in app.tasks
        assert f"{TASK_CALLBACK_PREFIX}.{AcmePingCallback.__qualname__}" in app.tasks
        assert f"{TASK_CALLBACK_ERROR_PREFIX}.{AcmePingCallback.__qualname__}" in app.tasks

    def test_bind_logs_one_policy_line_for_all_its_jobs_as_celery(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob, _SecondJob])

        adapter.mount(_application(tmp_path, {}, adapter))

        (line,) = CapturedLog(caplog).named("Job retry policies")
        assert (line["transport"], line["jobs"]) == ("celery", "list")

    def test_mount_without_recipe_is_refused(self, tmp_path: Path) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])
        application = _application(tmp_path, {}, adapter)

        unbound = CeleryRuntime(jobs=[_PingJob])
        with pytest.raises(RuntimeError, match="recipe"):
            unbound.mount(application)

    def test_an_ai_section_is_ignored(self, tmp_path: Path) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])

        application = _application(tmp_path, {"ai": {"agents": {}}}, adapter)

        assert application.ai.agent_host is None


class TestServices:
    def test_the_job_service_dispatches_the_discovered_callbacks_and_refuses_others(
        self, tmp_path: Path
    ) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])
        container = _application(tmp_path, {}, adapter, acme_callbacks.MODULE).kernel.container
        service = container.resolve(JobService)

        service.dispatch_after_commit(_PingJob, on_success=AcmePingCallback)
        with pytest.raises(ValueError, match="_OtherCallback was not discovered"):
            service.dispatch_after_commit(_PingJob, on_success=_OtherCallback)

    def test_job_and_sql_services_are_resolvable(self, tmp_path: Path) -> None:
        adapter = CeleryRuntime(jobs=[_PingJob])

        container = _application(tmp_path, {}, adapter).kernel.container

        assert container.resolve(JobService) is not None
        assert isinstance(container.resolve(SqlQueryService), SqlQueryService)
        assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)

    async def test_the_cache_gateways_close_when_the_worker_closes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        closed: list[CacheGateway] = []

        async def _close(self: CacheGateway) -> None:
            closed.append(self)

        monkeypatch.setattr(CacheGateway, "close", _close)
        config = {"cache": {"aiocache_config": {"default": dict(SERIALIZED_BACKEND)}}}
        application = _application(tmp_path, config, CeleryRuntime(jobs=[_PingJob]))

        async with application.running():
            assert closed == []

        assert closed


class TestAwsClientsLifecycle:
    @staticmethod
    def _dynamodb_application(tmp_path: Path) -> Application:
        stores = {
            "stores": {"default": {"backend": "dynamodb", "region": OTHER_REGION, "table": "t"}}
        }
        return _application(tmp_path, stores, CeleryRuntime(jobs=[_PingJob]))

    async def test_the_parent_check_builds_no_further_aws_client_and_calls_none(
        self, tmp_path: Path, island: FakeAwsIsland
    ) -> None:
        application = self._dynamodb_application(tmp_path)
        composed = len(island.built)

        await application.check()

        assert len(island.built) == composed
        assert all(client.method_calls == [] for client in island.created)

    async def test_a_worker_without_aws_builds_no_client_across_check_and_close(
        self, tmp_path: Path, island: FakeAwsIsland
    ) -> None:
        application = _application(tmp_path, {}, CeleryRuntime(jobs=[_PingJob]))

        await application.check()
        async with application.running(check=False):
            pass

        assert island.built == []

    async def test_running_the_application_releases_the_aws_clients_on_exit(
        self, tmp_path: Path, island: FakeAwsIsland
    ) -> None:
        application = self._dynamodb_application(tmp_path)

        async with application.running():
            assert [client.close.call_count for client in island.created] == [0]

        assert [client.close.call_count for client in island.created] == [1]
