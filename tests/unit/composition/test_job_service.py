"""A ``JobService`` the caller's DI module registers survives the composition.

The composition registers the Celery or inline service only when no module
already did, in every entry point: the API and the worker alike. Without
``app.job_service`` the ``celery`` section still picks it, deprecated.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import warnings
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from celery import current_app

from loom.celery.runtime import CeleryRuntime
from loom.celery.service import CeleryJobTransport
from loom.composition import Application, Project, Recipe
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.errors import JobTimeoutError
from loom.core.job.config import JobShutdown, ShutdownMode
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import JobPostponement
from loom.core.job.service import JobService
from loom.core.job.timer import JobDelayRefusedError, JobTimer
from loom.core.resources import ResourceKind
from loom.core.stores.profile import LAMBDA_PROFILE
from loom.core.use_case import Delivery
from loom.jobs.inline import InlineJobTransport
from loom.rest.fastapi.runtime import FastAPIRuntime
from loom.testing import ManualTimer, Override
from tests.helpers.composition import ProjectFactory, WithOverrides
from tests.helpers.jobs import BROKER, InlineServiceFixture, JobActivity, JobsAdapter, job_project
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

        job_project(tmp_path, _CELERY).compose(CeleryRuntime(jobs=[AcmePingJob]))

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
        application = project.compose(CeleryRuntime(jobs=[AcmePingJob]))
        assert [resource.kind for resource in application.resources].count(ResourceKind.CELERY) == 1

        with patch.object(CeleryJobTransport, "close", autospec=True) as close:
            async with application.running(check=False):
                close.assert_not_called()
            close.assert_called_once()

    def test_caller_service_has_no_celery_resource(self, tmp_path: Path) -> None:
        application = job_project(tmp_path, _CELERY).compose(
            WithOverrides(CeleryRuntime(jobs=[AcmePingJob]), _own_job_service)
        )

        assert all(resource.kind != ResourceKind.CELERY for resource in application.resources)

    def test_inline_service_has_no_celery_resource(self, fixture_project: ProjectFactory) -> None:
        application = Project.load(fixture_project(), environment="test").compose(FastAPIRuntime())

        assert all(resource.kind != ResourceKind.CELERY for resource in application.resources)

    def test_the_api_keeps_the_job_service_a_module_registered(
        self,
        fixture_project: ProjectFactory,
    ) -> None:
        application = Project.load(fixture_project(), environment="test").compose(
            WithOverrides(FastAPIRuntime(), _own_job_service)
        )

        assert isinstance(application.kernel.container.resolve(JobService), _OwnJobService)

    def test_the_worker_keeps_the_job_service_a_module_registered(self, tmp_path: Path) -> None:
        # A celery section would otherwise register the Celery service.
        project = job_project(tmp_path, _CELERY)

        application = project.compose(
            WithOverrides(CeleryRuntime(jobs=[AcmePingJob]), _own_job_service)
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

    def test_waiting_at_shutdown_does_not_warn_that_delayed_jobs_are_lost(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        job_project(tmp_path, {"jobs": {"shutdown": {"mode": "wait"}}}, "inline").compose(
            JobsAdapter(AcmePingJob)
        )

        assert not any("wait in process memory" in str(call[0]) for call in warnings_logged.calls)

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
        assert [resource.kind for resource in application.resources].count(ResourceKind.TIMER) == 1
        assert [event for event, _ in warnings_logged.calls] == ["Job retry policies"]

    async def test_lambda_runs_jobs_on_a_timer_that_refuses_a_delay(
        self, tmp_path: Path, warnings_logged: CapturedLog
    ) -> None:
        application = self._compose(tmp_path, _LambdaJobsAdapter(AcmePingJob))
        service = application.kernel.container.resolve(JobService)

        with pytest.raises(JobDelayRefusedError):
            service.dispatch_after_commit(AcmePingJob, countdown=60)
        assert [resource.kind for resource in application.resources].count(ResourceKind.TIMER) == 1
        ((event, fields),) = warnings_logged.calls
        assert (event, fields["transport"]) == ("Job retry policies", "inline")
        assert "max_attempts=1 (no redelivery)" in fields["jobs"][0]["retry"]

    def test_a_celery_application_has_no_timer(self, tmp_path: Path) -> None:
        application = job_project(tmp_path, _CELERY, "celery").compose(JobsAdapter(AcmePingJob))

        assert all(resource.kind != ResourceKind.TIMER for resource in application.resources)


class PostponingOnceJob(Job[str]):
    """Asks to run again at once the first time, and records each of its runs in the activity."""

    def __init__(self, activity: JobActivity) -> None:
        self._activity = activity

    def execute(self, delivery: JobDelivery | None = Delivery()) -> str:
        assert delivery is not None
        self._activity.record(f"run {delivery.postponements}")
        if delivery.postponements == 0:
            raise JobPostponement("not yet", 0)
        return "done"


class TestInlineShutdown:
    """``Application.running()`` exit is the only trigger of ``jobs.shutdown``.

    A job sent now waits on the timer, so stopping right after the dispatch finds it pending.
    """

    @pytest.fixture
    def compose_over(self, tmp_path: Path, activity: JobActivity) -> Callable[..., Application]:
        def compose(*overrides: Override) -> Application:
            return job_project(tmp_path, {}, "inline").compose(
                WithOverrides(
                    JobsAdapter(PostponingOnceJob),
                    Override.instance(JobActivity, activity).install,
                    *(override.install for override in overrides),
                )
            )

        return compose

    @pytest.mark.parametrize(
        ("shutdown", "runs_before_stopping"),
        [
            pytest.param(
                {"mode": "wait"}, ["run 0", "run 1"], id="wait-runs-the-job-and-its-postponement"
            ),
            pytest.param({"mode": "cancel"}, [], id="cancel-loses-the-job-not-yet-run"),
        ],
    )
    async def test_the_shutdown_mode_decides_what_runs_before_the_application_stops(
        self,
        tmp_path: Path,
        activity: JobActivity,
        shutdown: dict[str, Any],
        runs_before_stopping: list[str],
    ) -> None:
        application = job_project(tmp_path, {"jobs": {"shutdown": shutdown}}, "inline").compose(
            WithOverrides(
                JobsAdapter(PostponingOnceJob), Override.instance(JobActivity, activity).install
            )
        )

        async with application.running():
            await application.jobs.dispatch_now(PostponingOnceJob)

        assert activity.entries == runs_before_stopping

    async def test_a_job_sent_now_waits_on_the_timer_while_the_application_runs(
        self, compose_over: Callable[..., Application], activity: JobActivity
    ) -> None:
        timer = ManualTimer()
        application = compose_over(Override.instance(JobTimer, timer))

        async with application.running():
            await application.jobs.dispatch_now(PostponingOnceJob)

            assert (activity.entries, timer.pending) == ([], 1)

    async def test_a_bound_manual_timer_is_closed_on_exit_by_its_own_policy(
        self, compose_over: Callable[..., Application], activity: JobActivity
    ) -> None:
        timer = ManualTimer(JobShutdown(mode=ShutdownMode.WAIT))
        application = compose_over(Override.instance(JobTimer, timer))
        async with application.running():
            await application.jobs.dispatch_now(PostponingOnceJob)

        assert (activity.entries, timer.pending, timer.lost) == (["run 0", "run 1"], 0, [])

    async def test_a_bound_manual_timer_in_cancel_mode_loses_the_job_on_exit(
        self, compose_over: Callable[..., Application], activity: JobActivity
    ) -> None:
        timer = ManualTimer()
        application = compose_over(Override.instance(JobTimer, timer))
        async with application.running():
            await application.jobs.dispatch_now(PostponingOnceJob)

        assert (activity.entries, len(timer.lost)) == ([], 1)


class OpenWatch:
    """Records whether the application it watches is open each time a job asks it to."""

    def __init__(self) -> None:
        self.application: Application | None = None
        self.seen_open: list[bool] = []

    def record(self) -> None:
        assert self.application is not None
        self.seen_open.append(self.application.is_open)


class OpenProbeJob(Job[None]):
    """Records whether its application is open, and the first link sends a second."""

    def __init__(self, jobs: JobService, watch: OpenWatch) -> None:
        self._jobs = jobs
        self._watch = watch

    async def execute(self, link: int) -> None:
        self._watch.record()
        if link == 0:
            await self._jobs.dispatch_now(OpenProbeJob, params={"link": 1})


class TestWaitUntilIdle:
    @pytest.fixture
    def open_watch(self) -> OpenWatch:
        return OpenWatch()

    @pytest.fixture
    def probed_application(self, tmp_path: Path, open_watch: OpenWatch) -> Application:
        application = job_project(tmp_path, {}, "inline").compose(
            WithOverrides(
                JobsAdapter(OpenProbeJob), Override.instance(OpenWatch, open_watch).install
            )
        )
        open_watch.application = application
        return application

    async def test_inside_running_it_returns_once_a_chain_ran_with_the_application_open(
        self, probed_application: Application, open_watch: OpenWatch
    ) -> None:
        async with probed_application.running():
            await probed_application.jobs.dispatch_now(OpenProbeJob, params={"link": 0})

            await probed_application.jobs.wait_until_idle()

        assert open_watch.seen_open == [True, True]

    async def test_a_celery_application_returns_at_once(self, tmp_path: Path) -> None:
        application = job_project(tmp_path, _CELERY, "celery").compose(JobsAdapter(AcmePingJob))

        idle = await asyncio.wait_for(application.jobs.wait_until_idle(), 1)

        assert idle is None


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
