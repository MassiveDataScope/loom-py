"""A ``JobService`` the caller's DI module registers survives the composition.

The composition registers the Celery or inline service only when no module
already did, in every entry point: the API and the worker alike.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from celery import current_app

from loom.celery.adapter import CeleryAdapter
from loom.celery.service import CeleryJobService
from loom.composition import AppConfig, CompositionSource, build_application, compose_application
from loom.composition.resources import ResourceKind
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService, JobService
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory, composition_source


class _PingJob(Job[int]):
    def execute(self) -> int:
        return 1


class _OwnJobService(InlineJobService):
    """The caller's own service, recognisable by type."""

    def __init__(self) -> None:
        pass


def _own_job_service(container: LoomContainer) -> None:
    container.register(JobService, _OwnJobService, scope=Scope.APPLICATION)


def _celery_source(tmp_path: Path) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict(
            {"celery": {"broker_url": "memory://", "result_backend": "cache+memory://"}}
        ),
        app=AppConfig(name="worker"),
        code_path=tmp_path,
        config_root=tmp_path,
    )


def test_composing_celery_service_preserves_current_app(tmp_path: Path) -> None:
    before = current_app._get_current_object()

    compose_application(_celery_source(tmp_path), CeleryAdapter(jobs=[_PingJob]))

    assert current_app._get_current_object() is before


async def test_application_closes_its_celery_service_once(tmp_path: Path) -> None:
    application = compose_application(_celery_source(tmp_path), CeleryAdapter(jobs=[_PingJob]))
    service = application.kernel.container.resolve(JobService)
    assert isinstance(service, CeleryJobService)
    assert [resource.kind for resource in application.resources].count(ResourceKind.CELERY) == 1

    with patch.object(service.celery_app, "close", wraps=service.celery_app.close) as close:
        async with application.running(check=False):
            close.assert_not_called()
        close.assert_called_once_with()


def test_caller_service_has_no_celery_resource(tmp_path: Path) -> None:
    application = compose_application(
        _celery_source(tmp_path), CeleryAdapter(jobs=[_PingJob]), modules=[_own_job_service]
    )

    assert all(resource.kind != ResourceKind.CELERY for resource in application.resources)


def test_inline_service_has_no_celery_resource(fixture_project: ProjectFactory) -> None:
    application = build_application(fixture_project(), adapter=FastAPIAdapter(), environment="test")

    assert all(resource.kind != ResourceKind.CELERY for resource in application.resources)


def test_the_api_keeps_the_job_service_a_module_registered(
    fixture_project: ProjectFactory,
) -> None:
    application = build_application(
        fixture_project(),
        adapter=FastAPIAdapter(),
        environment="test",
        modules=[_own_job_service],
    )

    assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)


def test_the_worker_keeps_the_job_service_a_module_registered(tmp_path: Path) -> None:
    source = composition_source(
        # A celery section would otherwise register the Celery service.
        ConfigContext.from_dict(
            {"celery": {"broker_url": "memory://", "result_backend": "cache+memory://"}}
        ),
        app=AppConfig(name="worker"),
        code_path=tmp_path,
        config_root=tmp_path,
    )

    application = compose_application(
        source, CeleryAdapter(jobs=[_PingJob]), modules=[_own_job_service]
    )

    assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)
