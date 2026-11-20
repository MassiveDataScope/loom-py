"""A ``JobService`` the caller's DI module registers survives the composition.

The composition registers the Celery or inline service only when no module
already did, in every entry point: the API and the worker alike. Without
``app.job_service`` the ``celery`` section still picks it, deprecated.
"""

from __future__ import annotations

import logging
import sys
import warnings
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from celery import current_app

from loom.celery.adapter import CeleryAdapter
from loom.celery.service import CeleryJobTransport
from loom.composition import Application, Project, Recipe
from loom.composition.resources import ResourceKind
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.errors import JobTimeoutError
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.job.timer import JobDelayRefused, JobTimer
from loom.core.stores.profile import LAMBDA_PROFILE
from loom.jobs.inline import InlineJobTransport
from loom.rest.fastapi.adapter import FastAPIAdapter
from loom.testing import ManualTimer, Override
from tests.helpers.composition import ProjectFactory, WithOverrides
from tests.helpers.jobs import BROKER, InlineServiceFixture, JobsAdapter, job_project
from tests.helpers.loggers import CapturedLog
from tests.unit.composition.doubles import AcmePingJob

_CELERY = {"celery": BROKER}


class _OwnJobService(InlineServiceFixture):
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

        with patch.object(CeleryJobTransport, "close", autospec=True) as close:
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

        assert type(service.default_transport) is CeleryJobTransport

    def test_without_job_service_or_celery_section_jobs_run_inline(self, tmp_path: Path) -> None:
        with (
            pytest.deprecated_call(match="app.job_service is not set"),
            pytest.warns(UserWarning, match="Celery config section is missing"),
        ):
            service = _implicit_service(tmp_path, {})

        assert type(service.default_transport) is InlineJobTransport

    def test_without_job_service_a_missing_extra_falls_back_to_inline(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "loom.celery.config", None)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            service = _implicit_service(tmp_path, _CELERY)

        assert type(service.default_transport) is InlineJobTransport


class _LambdaJobsAdapter(JobsAdapter):
    """Compose the given jobs under the Lambda profile."""

    def recipe(self, project: Project) -> Recipe:
        return replace(super().recipe(project), profile=LAMBDA_PROFILE)


@pytest.fixture
def warnings_logged(caplog: pytest.LogCaptureFixture) -> CapturedLog:
    caplog.set_level(logging.INFO)
    return CapturedLog(caplog, "loom.composition._jobs")


class TestInlineTimer:
    def _compose(self, tmp_path: Path, adapter: Any) -> Application:
        return job_project(tmp_path, {}, "inline").compose(adapter)

    def test_the_inline_service_gets_a_timer_closed_with_the_application(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        application = self._compose(tmp_path, JobsAdapter(AcmePingJob))

        assert [resource.kind for resource in application.resources].count(ResourceKind.TIMER) == 1
        warning, policies = warnings_logged.calls
        assert "wait in process memory" in warning[0]
        assert "Celery or SQS" in warning[0]
        assert policies[1]["transport"] == "inline"
        assert "max_attempts=3" in policies[1]["jobs"][0]["retry"]

    async def test_a_countdown_runs_when_the_timer_of_the_application_reaches_it(
        self, tmp_path: Path
    ) -> None:
        timer = ManualTimer()
        application = self._compose(
            tmp_path,
            WithOverrides(JobsAdapter(AcmePingJob), Override.instance(JobTimer, timer).install),
        )
        service = application.kernel.container.resolve(JobService)

        handle = service.dispatch_after_commit(AcmePingJob, countdown=60)
        await flush_pending_dispatches()
        with pytest.raises(JobTimeoutError):
            handle.wait()
        await timer.advance(59)
        with pytest.raises(JobTimeoutError):
            handle.wait()
        await timer.advance(1)

        assert handle.wait() == await service.run(AcmePingJob)

    async def test_a_timer_bound_in_the_container_replaces_it(
        self, tmp_path: Path, warnings_logged: CapturedLog
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
        assert [event for event, _ in warnings_logged.calls] == ["Job retry policies"]

    async def test_lambda_composes_no_timer_and_refuses_a_delay(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        application = self._compose(tmp_path, _LambdaJobsAdapter(AcmePingJob))
        service = application.kernel.container.resolve(JobService)

        with pytest.raises(JobDelayRefused):
            service.dispatch_after_commit(AcmePingJob, countdown=60)
        assert all(resource.kind != ResourceKind.TIMER for resource in application.resources)
        ((event, fields),) = warnings_logged.calls
        assert (event, fields["transport"]) == ("Job retry policies", "inline")
        assert "max_attempts=1 (no redelivery)" in fields["jobs"][0]["retry"]

    def test_a_celery_application_has_no_timer(self, tmp_path: Path) -> None:
        application = job_project(tmp_path, _CELERY, "celery").compose(JobsAdapter(AcmePingJob))

        assert all(resource.kind != ResourceKind.TIMER for resource in application.resources)


class AcmeEmailJob(Job[int]):
    __queue__ = "emails"

    def execute(self) -> int:
        return 1


class TestRetryPolicyLog:
    """The line is logged where a runner applies the policy; a producer applies none."""

    def test_a_celery_producer_logs_no_policy_line(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        job_project(tmp_path, _CELERY, "celery").compose(JobsAdapter(AcmePingJob))

        assert warnings_logged.named("Job retry policies") == []

    def test_an_inline_line_lists_only_the_jobs_the_inline_runner_serves(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        config = {"celery": {**BROKER, "queues": ["default", "emails"]}}

        job_project(tmp_path, config, "inline").compose(JobsAdapter(AcmePingJob, AcmeEmailJob))

        (line,) = warnings_logged.named("Job retry policies")
        assert line["transport"] == "inline"
        assert [entry["job"] for entry in line["jobs"]] == ["AcmePingJob"]

    def test_an_app_whose_jobs_all_go_to_celery_logs_no_inline_line(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        config = {"celery": {**BROKER, "queues": ["default", "emails"]}}

        job_project(tmp_path, config, "inline").compose(JobsAdapter(AcmeEmailJob))

        assert warnings_logged.named("Job retry policies") == []
