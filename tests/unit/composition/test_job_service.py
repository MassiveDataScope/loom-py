"""A ``JobService`` the caller's DI module registers survives the composition.

The composition registers the Celery or inline service only when no module
already did, in every entry point: the API and the worker alike. Without
``app.job_service`` the ``celery`` section still picks it, deprecated.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from celery import current_app

from loom.celery.adapter import CeleryAdapter
from loom.celery.service import CeleryJobService
from loom.composition import build_application, compose_application
from loom.composition.resources import ResourceKind
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService, JobService
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory
from tests.helpers.jobs import BROKER, JobsAdapter, job_source

_CELERY = {"celery": BROKER}


class _PingJob(Job[int]):
    def execute(self) -> int:
        return 1


class _OwnJobService(InlineJobService):
    """The caller's own service, recognisable by type."""

    def __init__(self) -> None:
        pass


def _own_job_service(container: LoomContainer) -> None:
    container.register(JobService, _OwnJobService, scope=Scope.APPLICATION)


def test_composing_celery_service_preserves_current_app(tmp_path: Path) -> None:
    before = current_app._get_current_object()

    compose_application(job_source(tmp_path, _CELERY), CeleryAdapter(jobs=[_PingJob]))

    assert current_app._get_current_object() is before


@pytest.mark.parametrize(
    ("config", "job_service"),
    [(_CELERY, None), ({"celery": {**BROKER, "queues": ["default"]}}, "inline")],
    ids=["celery", "routed to celery"],
)
async def test_application_closes_its_celery_service_once(
    tmp_path: Path, config: dict[str, Any], job_service: str | None
) -> None:
    source = job_source(tmp_path, config, job_service)
    application = compose_application(source, CeleryAdapter(jobs=[_PingJob]))
    assert [resource.kind for resource in application.resources].count(ResourceKind.CELERY) == 1

    with patch.object(CeleryJobService, "close", autospec=True) as close:
        async with application.running(check=False):
            close.assert_not_called()
        close.assert_called_once()


def test_caller_service_has_no_celery_resource(tmp_path: Path) -> None:
    application = compose_application(
        job_source(tmp_path, _CELERY), CeleryAdapter(jobs=[_PingJob]), modules=[_own_job_service]
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
    # A celery section would otherwise register the Celery service.
    source = job_source(tmp_path, _CELERY)

    application = compose_application(
        source, CeleryAdapter(jobs=[_PingJob]), modules=[_own_job_service]
    )

    assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)


def _implicit_service(tmp_path: Path, config: dict[str, Any]) -> JobService:
    application = compose_application(job_source(tmp_path, config), JobsAdapter())
    return application.kernel.container.resolve(JobService)


def test_without_job_service_a_celery_section_picks_celery(tmp_path: Path) -> None:
    with pytest.deprecated_call(match="app.job_service is not set"):
        service = _implicit_service(tmp_path, _CELERY)

    assert isinstance(service, CeleryJobService)


def test_without_job_service_or_celery_section_jobs_run_inline(tmp_path: Path) -> None:
    with (
        pytest.deprecated_call(match="app.job_service is not set"),
        pytest.warns(UserWarning, match="Celery config section is missing"),
    ):
        service = _implicit_service(tmp_path, {})

    assert type(service) is InlineJobService


def test_without_job_service_a_missing_extra_falls_back_to_inline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "loom.celery.service", None)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        service = _implicit_service(tmp_path, _CELERY)

    assert type(service) is InlineJobService
