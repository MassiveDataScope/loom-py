"""A framework service the caller's DI module registered survives the composition.

The composition registers its SQL collaborators and its observability runtime
after the caller's modules, only where no module already did, in every entry
point: the API and the worker alike. ``test_job_service`` covers ``JobService``.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, Application, build_application, compose_application
from loom.core.config import ConfigContext
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope
from loom.core.job.job import Job
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import CallerBoundSql, NullSqlQueryService, SqlConfig, SqlQueryService
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory, composition_source


class _OverrideJob(Job[int]):
    def execute(self) -> int:
        return 1


_OWN_SQL = NullSqlQueryService()
_OWN_OBSERVABILITY = ObservabilityRuntime.noop()
_OWN_CALLER_BOUND = CallerBoundSql(_OWN_SQL, SqlConfig(connections={}), _OWN_OBSERVABILITY)

_OVERRIDES: list[tuple[BindingKey, object]] = [
    (SqlQueryService, _OWN_SQL),
    (CallerBoundSql, _OWN_CALLER_BOUND),
    (ObservabilityRuntime, _OWN_OBSERVABILITY),
]
_IDS = [key.__name__ for key, _ in _OVERRIDES if isinstance(key, type)]


def _module(key: BindingKey, own: object) -> Callable[[LoomContainer], None]:
    def register(container: LoomContainer) -> None:
        container.register(key, lambda: own, scope=Scope.APPLICATION)

    return register


def _worker(tmp_path: Path, module: Callable[[LoomContainer], None]) -> Application:
    source = composition_source(
        ConfigContext.from_dict(
            {"celery": {"broker_url": "memory://", "result_backend": "cache+memory://"}}
        ),
        app=AppConfig(name="worker"),
        code_path=tmp_path,
        config_root=tmp_path,
    )
    return compose_application(source, CeleryAdapter(jobs=[_OverrideJob]), modules=[module])


@pytest.mark.parametrize(("key", "own"), _OVERRIDES, ids=_IDS)
def test_the_api_keeps_the_service_a_module_registered(
    fixture_project: ProjectFactory, key: BindingKey, own: object
) -> None:
    application = build_application(
        fixture_project(),
        adapter=FastAPIAdapter(),
        environment="test",
        modules=[_module(key, own)],
    )

    assert application.kernel.container.resolve(key) is own


@pytest.mark.parametrize(("key", "own"), _OVERRIDES, ids=_IDS)
def test_the_worker_keeps_the_service_a_module_registered(
    tmp_path: Path, key: BindingKey, own: object
) -> None:
    application = _worker(tmp_path, _module(key, own))

    assert application.kernel.container.resolve(key) is own
