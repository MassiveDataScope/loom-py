"""A caller's DI module cannot bind a key the framework owns.

The composition runs the caller's modules first and registers its own
services after them. The framework-owned keys are reserved: a module binding
one fails the composition at start-up with :class:`ReservedBindingError`, in
every entry point: the API and the worker alike. ``JobService`` stays the
caller's to override (``test_job_service``).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, Application, build_application, compose_application
from loom.composition.errors import CompositionErrorCode, ReservedBindingError
from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap import BootstrapError
from loom.core.config import ConfigContext
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.job import Job
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository import DefaultRepositoryBuilder
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.sql import CallerBoundSql, SqlQueryService
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory, composition_source


class _OverrideJob(Job[int]):
    def execute(self) -> int:
        return 1


_RESERVED: list[BindingKey] = [
    SessionManager,
    SqlQueryService,
    CallerBoundSql,
    ObservabilityRuntime,
    ElevationSink,
    RuntimeExecutor,
    UseCaseRegistry,
    ApplicationInvoker,
]
_IDS = [key.__name__ for key in _RESERVED if isinstance(key, type)]


def _module(key: BindingKey) -> Callable[[LoomContainer], None]:
    def register(container: LoomContainer) -> None:
        container.register(key, object, scope=Scope.APPLICATION)

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


def _assert_reserved(error: ReservedBindingError, key: BindingKey) -> None:
    assert error.key is key
    assert error.issue.code is CompositionErrorCode.RESERVED_BINDING
    assert error.issue.field == getattr(key, "__qualname__", None)
    assert str(error) == error.issue.message


@pytest.mark.parametrize("key", _RESERVED, ids=_IDS)
def test_the_api_refuses_a_module_binding_a_reserved_key(
    fixture_project: ProjectFactory, key: BindingKey
) -> None:
    with pytest.raises(ReservedBindingError) as caught:
        build_application(
            fixture_project(),
            adapter=FastAPIAdapter(),
            environment="test",
            modules=[_module(key)],
        )

    _assert_reserved(caught.value, key)


@pytest.mark.parametrize("key", _RESERVED, ids=_IDS)
def test_the_worker_refuses_a_module_binding_a_reserved_key(
    tmp_path: Path, key: BindingKey
) -> None:
    with pytest.raises(ReservedBindingError) as caught:
        _worker(tmp_path, _module(key))

    _assert_reserved(caught.value, key)


def test_the_error_is_a_bootstrap_error_naming_the_key(tmp_path: Path) -> None:
    with pytest.raises(BootstrapError, match="SqlQueryService"):
        _worker(tmp_path, _module(SqlQueryService))


def test_the_composition_registers_one_session_manager_for_repositories(
    fixture_project: ProjectFactory,
) -> None:
    application = build_application(fixture_project(), adapter=FastAPIAdapter(), environment="test")
    container = application.kernel.container

    manager = container.resolve(SessionManager)
    uow_factory: Any = application.persistence.uow_factory
    assert uow_factory._session_manager is manager
    assert container.resolve(DefaultRepositoryBuilder).session_manager is manager
