"""``CeleryAdapter`` composes the worker on the shared, transport-neutral recipe (US4, T026)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from celery import Celery, current_app

from loom.ai.abc import AgentHandle, McpHandle
from loom.celery.adapter import CeleryAdapter
from loom.celery.constants import TASK_CALLBACK_PREFIX, TASK_JOB_PREFIX
from loom.composition import AppConfig, Application, CompositionSource
from loom.composition import compose_application as compose
from loom.core.cache.gateway import CacheGateway
from loom.core.checks import REST_CHECKS
from loom.core.config import ConfigContext, ConfigError
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.persistence import WORKER_PROFILE
from loom.core.sql import CallerBoundSql, SqlQueryService
from loom.core.use_case import Agent, Mcp
from loom.core.use_case.use_case import UseCase
from tests.helpers.composition import composition_source
from tests.unit.core.cache._doubles import SERIALIZED_BACKEND

_CELERY = {"broker_url": "memory://", "result_backend": "cache+memory://"}
_SQLITE = "sqlite+aiosqlite:///:memory:"


class _PingJob(Job[int]):
    __queue__ = "default"
    __retries__ = 0

    def execute(self) -> int:
        return 1


class _PingCallback:
    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        del job_id, result, context

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        del job_id, exc_type, exc_msg, context


class _TriageUseCase(UseCase[object, object]):
    async def execute(self, triage: AgentHandle[dict] = Agent("triage")) -> object:
        return triage


class _KnowledgeUseCase(UseCase[object, object]):
    async def execute(self, gateway: McpHandle = Mcp("knowledge", include=["search_*"])) -> object:
        return gateway


def _source(tmp_path: Path, config: dict[str, Any]) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict({"celery": _CELERY, **config}),
        app=AppConfig(name="worker"),
        code_path=tmp_path,
        config_root=tmp_path,
    )


def _application(tmp_path: Path, config: dict[str, Any], adapter: CeleryAdapter) -> Application:
    return compose(_source(tmp_path, config), adapter)


class TestBackendRule:
    """The rest of the rule (no database, named databases, a stray key) is pinned by
    ``test_bootstrap_database``."""

    @pytest.mark.parametrize("backend", ["none", "mongo", "dynamodb"])
    def test_an_explicit_backend_wins_over_the_database_section(
        self, tmp_path: Path, backend: str
    ) -> None:
        config = {"persistence": {"backend": backend}, "database": {"url": _SQLITE}}

        recipe = CeleryAdapter(jobs=[_PingJob]).recipe(_source(tmp_path, config))

        assert recipe.backend.name == backend

    def test_a_database_url_without_backend_serves_sqlalchemy(self, tmp_path: Path) -> None:
        recipe = CeleryAdapter(jobs=[_PingJob]).recipe(
            _source(tmp_path, {"database": {"url": _SQLITE}})
        )

        assert recipe.backend.name == "sqlalchemy"

    def test_credentials_without_url_are_the_workers_own_section_decoded_strictly(
        self, tmp_path: Path
    ) -> None:
        source = _source(tmp_path, {"database": {"username": "app"}})

        recipe = CeleryAdapter(jobs=[_PingJob]).recipe(source)

        assert recipe.backend.name == "sqlalchemy"
        with pytest.raises(ConfigError, match="url"):
            compose(source, CeleryAdapter(jobs=[_PingJob]))


class TestRecipe:
    def test_the_worker_declares_its_profile_checks_and_no_agents(self, tmp_path: Path) -> None:
        recipe = CeleryAdapter(jobs=[_PingJob]).recipe(_source(tmp_path, {}))

        assert recipe.profile is WORKER_PROFILE
        assert recipe.skip_checks == frozenset({REST_CHECKS})
        assert recipe.agents is False

    def test_jobs_and_use_cases_are_compiled(self, tmp_path: Path) -> None:
        class _Lookup(UseCase[object, object]):
            async def execute(self) -> object:
                return None

        recipe = CeleryAdapter(jobs=[_PingJob], use_cases=[_Lookup]).recipe(_source(tmp_path, {}))

        assert set(recipe.discovered.use_cases) == {_PingJob, _Lookup}

    def test_the_metrics_reach_the_recipe(self, tmp_path: Path) -> None:
        metrics: Any = object()

        recipe = CeleryAdapter(jobs=[_PingJob], metrics=metrics).recipe(_source(tmp_path, {}))

        assert recipe.metrics is metrics

    def test_a_second_recipe_on_the_same_adapter_is_refused(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob])
        adapter.recipe(_source(tmp_path, {}))

        with pytest.raises(RuntimeError, match="one adapter instance composes one application"):
            adapter.recipe(_source(tmp_path, {}))

    def test_a_failed_recipe_can_be_retried(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob])
        with pytest.raises(ConfigError):
            adapter.recipe(_source(tmp_path, {"database": {"URL": "x"}}))

        assert adapter.recipe(_source(tmp_path, {})).profile is WORKER_PROFILE


class TestBind:
    def test_bind_sets_the_worker_app_as_current(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob])

        app = adapter.bind(_application(tmp_path, {}, adapter))

        assert current_app._get_current_object() is app

    def test_bind_returns_a_celery_app_with_jobs_and_callbacks(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob], callbacks=[_PingCallback])

        app = adapter.bind(_application(tmp_path, {}, adapter))

        assert isinstance(app, Celery)
        assert f"{TASK_JOB_PREFIX}.{_PingJob.__qualname__}" in app.tasks
        assert f"{TASK_CALLBACK_PREFIX}.{_PingCallback.__qualname__}" in app.tasks

    def test_bind_without_recipe_is_refused(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob])
        application = _application(tmp_path, {}, adapter)

        with pytest.raises(RuntimeError, match="recipe"):
            CeleryAdapter(jobs=[_PingJob]).bind(application)

    def test_an_ai_section_is_ignored(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob])

        application = _application(tmp_path, {"ai": {"agents": {}}}, adapter)

        assert application.ai.runtime is None


class TestServices:
    def test_job_and_sql_services_are_resolvable(self, tmp_path: Path) -> None:
        adapter = CeleryAdapter(jobs=[_PingJob])

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
        application = _application(tmp_path, config, CeleryAdapter(jobs=[_PingJob]))

        async with application.running():
            assert closed == []

        assert closed
