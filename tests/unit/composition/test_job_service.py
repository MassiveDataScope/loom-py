"""A ``JobService`` the caller's DI module registers survives the composition.

The composition registers the Celery or inline service only when no module
already did, in every entry point: the API and the worker alike. Without
``app.job_service`` the ``celery`` section still picks it, deprecated.
"""

from __future__ import annotations

import sys
import warnings
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from celery import current_app

from loom.celery.adapter import CeleryAdapter
from loom.celery.service import CeleryJobService
from loom.composition import Application, Project, Recipe
from loom.composition.resources import ResourceKind
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.service import InlineJobService, JobService
from loom.core.job.timer import AsyncioTimer, JobDelayRefused, JobTimer
from loom.core.stores.profile import LAMBDA_PROFILE
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.testing import ManualTimer, Override
from tests.helpers.composition import ProjectFactory, WithOverrides
from tests.helpers.jobs import BROKER, JobsAdapter, job_project
from tests.helpers.loggers import RecordingLogger
from tests.unit.composition.doubles import AcmePingJob

_CELERY = {"celery": BROKER}


class _OwnJobService(InlineJobService):
    """The caller's own service, recognisable by type."""

    def __init__(self) -> None:
        pass


def _own_job_service(container: LoomContainer) -> None:
    container.register(JobService, _OwnJobService, scope=Scope.APPLICATION)


def _implicit_service(tmp_path: Path, config: dict[str, Any]) -> JobService:
    application = job_project(tmp_path, config).compose(JobsAdapter())
    return application.kernel.container.resolve(JobService)


class TestRegisteredJobService:
    def test_composing_celery_service_preserves_current_app(self, tmp_path: Path) -> None:
        before = current_app._get_current_object()

        job_project(tmp_path, _CELERY).compose(CeleryAdapter(jobs=[AcmePingJob]))

        assert current_app._get_current_object() is before

    @pytest.mark.parametrize(
        ("config", "job_service"),
        [(_CELERY, None), ({"celery": {**BROKER, "queues": ["default", "emails"]}}, "inline")],
        ids=["celery", "routed to celery"],
    )
    async def test_application_closes_its_celery_service_once(
        self, tmp_path: Path, config: dict[str, Any], job_service: str | None
    ) -> None:
        project = job_project(tmp_path, config, job_service)
        application = project.compose(CeleryAdapter(jobs=[AcmePingJob]))
        assert [resource.kind for resource in application.resources].count(ResourceKind.CELERY) == 1

        with patch.object(CeleryJobService, "close", autospec=True) as close:
            async with application.running(check=False):
                close.assert_not_called()
            close.assert_called_once()

    def test_caller_service_has_no_celery_resource(self, tmp_path: Path) -> None:
        application = job_project(tmp_path, _CELERY).compose(
            WithOverrides(CeleryAdapter(jobs=[AcmePingJob]), _own_job_service)
        )

        assert all(resource.kind != ResourceKind.CELERY for resource in application.resources)

    def test_inline_service_has_no_celery_resource(self, fixture_project: ProjectFactory) -> None:
        application = Project.load(fixture_project(), environment="test").compose(FastAPIAdapter())

        assert all(resource.kind != ResourceKind.CELERY for resource in application.resources)

    def test_the_api_keeps_the_job_service_a_module_registered(
        self,
        fixture_project: ProjectFactory,
    ) -> None:
        application = Project.load(fixture_project(), environment="test").compose(
            WithOverrides(FastAPIAdapter(), _own_job_service)
        )

        assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)

    def test_the_worker_keeps_the_job_service_a_module_registered(self, tmp_path: Path) -> None:
        # A celery section would otherwise register the Celery service.
        project = job_project(tmp_path, _CELERY)

        application = project.compose(
            WithOverrides(CeleryAdapter(jobs=[AcmePingJob]), _own_job_service)
        )

        assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)


class TestImplicitJobService:
    def test_without_job_service_a_celery_section_picks_celery(self, tmp_path: Path) -> None:
        with pytest.deprecated_call(match="app.job_service is not set"):
            service = _implicit_service(tmp_path, _CELERY)

        assert isinstance(service, CeleryJobService)

    def test_without_job_service_or_celery_section_jobs_run_inline(self, tmp_path: Path) -> None:
        with (
            pytest.deprecated_call(match="app.job_service is not set"),
            pytest.warns(UserWarning, match="Celery config section is missing"),
        ):
            service = _implicit_service(tmp_path, {})

        assert type(service) is InlineJobService

    def test_without_job_service_a_missing_extra_falls_back_to_inline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "loom.celery.config", None)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            service = _implicit_service(tmp_path, _CELERY)

        assert type(service) is InlineJobService


class _LambdaJobsAdapter(JobsAdapter):
    """Compose the given jobs under the Lambda profile."""

    def recipe(self, project: Project) -> Recipe:
        return replace(super().recipe(project), profile=LAMBDA_PROFILE)


@pytest.fixture
def warnings_logged(monkeypatch: pytest.MonkeyPatch) -> RecordingLogger:
    log = RecordingLogger()
    monkeypatch.setattr("loom.composition._jobs._log", log)
    return log


class TestInlineTimer:
    def _compose(self, tmp_path: Path, adapter: Any) -> Application:
        return job_project(tmp_path, {}, "inline").compose(adapter)

    def test_the_inline_service_gets_a_timer_closed_with_the_application(
        self, tmp_path: Path, warnings_logged: RecordingLogger
    ) -> None:
        application = self._compose(tmp_path, JobsAdapter(AcmePingJob))

        assert [resource.kind for resource in application.resources].count(ResourceKind.TIMER) == 1
        assert len(warnings_logged.calls) == 1
        assert "wait in process memory" in warnings_logged.calls[0][0]
        assert "Celery or SQS" in warnings_logged.calls[0][0]

    async def test_a_countdown_waits_on_the_timer_of_the_application(self, tmp_path: Path) -> None:
        application = self._compose(tmp_path, JobsAdapter(AcmePingJob))
        service = application.kernel.container.resolve(JobService)

        service.dispatch_after_commit(AcmePingJob, countdown=60)
        await flush_pending_dispatches()

        timer = service._timer  # type: ignore[attr-defined]
        assert isinstance(timer, AsyncioTimer)
        assert len(timer._tasks) == 1
        await timer.close()

    async def test_a_timer_bound_in_the_container_replaces_it(
        self, tmp_path: Path, warnings_logged: RecordingLogger
    ) -> None:
        timer = ManualTimer()
        application = self._compose(
            tmp_path,
            WithOverrides(JobsAdapter(AcmePingJob), Override.instance(JobTimer, timer).install),
        )
        service = application.kernel.container.resolve(JobService)

        service.dispatch_after_commit(AcmePingJob, countdown=60)
        await flush_pending_dispatches()

        assert timer.pending == 1
        assert all(resource.kind != ResourceKind.TIMER for resource in application.resources)
        assert warnings_logged.calls == []

    async def test_lambda_composes_no_timer_and_refuses_a_delay(
        self, tmp_path: Path, warnings_logged: RecordingLogger
    ) -> None:
        application = self._compose(tmp_path, _LambdaJobsAdapter(AcmePingJob))
        service = application.kernel.container.resolve(JobService)

        with pytest.raises(JobDelayRefused):
            service.dispatch_after_commit(AcmePingJob, countdown=60)
        assert all(resource.kind != ResourceKind.TIMER for resource in application.resources)
        assert warnings_logged.calls == []

    def test_a_celery_application_has_no_timer(self, tmp_path: Path) -> None:
        application = job_project(tmp_path, _CELERY, "celery").compose(JobsAdapter(AcmePingJob))

        assert all(resource.kind != ResourceKind.TIMER for resource in application.resources)
