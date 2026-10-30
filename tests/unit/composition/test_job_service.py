"""A ``JobService`` the caller's DI module registers survives the composition.

The composition registers the Celery or inline service only when no module
already did, in every entry point: the API and the worker alike.
"""

from __future__ import annotations

from pathlib import Path

from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, CompositionSource, build_application, compose_application
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.environment import Environment
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService, JobService
from loom.core.observability.runtime import ObservabilityRuntime
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory


class _PingJob(Job[int]):
    def execute(self) -> int:
        return 1


class _OwnJobService(InlineJobService):
    """The caller's own service, recognisable by type."""

    def __init__(self) -> None:
        pass


def _own_job_service(container: LoomContainer) -> None:
    container.register(JobService, _OwnJobService, scope=Scope.APPLICATION)


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
    source = CompositionSource(
        # A celery section would otherwise register the Celery service.
        config=ConfigContext.from_dict(
            {"celery": {"broker_url": "memory://", "result_backend": "cache+memory://"}}
        ),
        environment=Environment.TEST,
        code_path=tmp_path,
        config_root=tmp_path,
        app=AppConfig(name="worker"),
        observability=ObservabilityRuntime.noop(),
    )

    application = compose_application(
        source, CeleryAdapter(jobs=[_PingJob]), modules=[_own_job_service]
    )

    assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)
