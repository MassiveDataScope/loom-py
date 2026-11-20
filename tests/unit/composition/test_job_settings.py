"""Each application dispatches a job with its own ``jobs:`` section; none leaks into another.

T090: the composition used to write each ``jobs.<Job>`` section onto the
job's ClassVars, so with two applications in one process the last one won and
one without the section kept the other's values.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom.composition import Application
from loom.core.config import ConfigError
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from tests.helpers.celery import CeleryBroker
from tests.helpers.jobs import BROKER, JobsAdapter, job_project

_CONFIGURED = {"queue": "reports", "countdown": 5, "priority": 3, "timeout": 60}
_DECLARED = {"queue": "default", "countdown": 0, "priority": 0, "soft_time_limit": None}


class _ReportJob(Job[None]):
    def execute(self) -> None:
        return None


def _compose(
    tmp_path: Path,
    name: str,
    jobs: dict[str, Any] | None,
    *,
    celery: dict[str, Any] = BROKER,
    job_service: str = "celery",
) -> Application:
    config: dict[str, Any] = {"celery": celery}
    if jobs is not None:
        config["jobs"] = jobs
    directory = tmp_path / name
    directory.mkdir()
    project = job_project(directory, config, job_service)
    return project.compose(JobsAdapter(_ReportJob))


async def _sent(application: Application) -> dict[str, Any] | None:
    """The routing the broker got when *application* dispatched the job; ``None`` if not sent."""
    broker = CeleryBroker(["default", "reports"])
    service = application.kernel.container.resolve(JobService)
    service.dispatch_after_commit(_ReportJob)
    await flush_pending_dispatches()
    sent = broker.sent()
    if not sent:
        return None
    (message,) = sent
    return {key: getattr(message, key) for key in _DECLARED}


def _in_order(configured_first: bool) -> list[tuple[str, dict[str, Any] | None]]:
    order: list[tuple[str, dict[str, Any] | None]] = [
        ("configured", {"_ReportJob": _CONFIGURED}),
        ("plain", None),
    ]
    return order if configured_first else order[::-1]


_ORDERS = pytest.mark.parametrize(
    "configured_first", [True, False], ids=["configured first", "configured last"]
)


class TestJobSettings:
    @_ORDERS
    async def test_each_application_dispatches_with_its_own_job_section(
        self, tmp_path: Path, configured_first: bool
    ) -> None:
        order = _in_order(configured_first)
        applications = {name: _compose(tmp_path, name, jobs) for name, jobs in order}

        sent = {name: await _sent(application) for name, application in applications.items()}

        assert sent == {
            "configured": {
                "queue": "reports",
                "countdown": 5,
                "priority": 3,
                "soft_time_limit": 60,
            },
            "plain": _DECLARED,
        }
        assert (_ReportJob.__queue__, _ReportJob.__countdown__, _ReportJob.__timeout__) == (
            "default",
            0,
            None,
        )

    @_ORDERS
    async def test_each_application_routes_by_the_queue_of_its_own_job_section(
        self, tmp_path: Path, configured_first: bool
    ) -> None:
        """``celery.queues`` sends ``reports`` to Celery; only the configured job is on it."""
        celery = {**BROKER, "queues": ["default", "reports"]}
        applications = {
            name: _compose(tmp_path, name, jobs, celery=celery, job_service="inline")
            for name, jobs in _in_order(configured_first)
        }

        sent = {name: await _sent(application) for name, application in applications.items()}

        assert sent["plain"] is None
        assert sent["configured"] is not None
        assert sent["configured"]["queue"] == "reports"

    def test_a_malformed_jobs_section_fails_the_composition(self, tmp_path: Path) -> None:
        project = job_project(tmp_path, {"jobs": {"_ReportJob": {"timeout": "60s"}}}, "inline")
        adapter = JobsAdapter(_ReportJob)

        with pytest.raises(ConfigError, match="_ReportJob"):
            project.compose(adapter)

    def test_start_up_logs_one_line_with_each_jobs_effective_retry_policy(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        project = job_project(
            tmp_path, {"jobs": {"_ReportJob": {"retry": {"max_attempts": 5}}}}, "inline"
        )

        with caplog.at_level("INFO"):
            project.compose(JobsAdapter(_ReportJob))

        lines = [
            record.getMessage() for record in caplog.records if "retry policies" in str(record.msg)
        ]
        assert len(lines) == 1
        assert "_ReportJob" in lines[0]
        assert "jobs.<Job>.retry" in lines[0]
        assert "max_attempts=5 (jobs.<Job>.retry)" in lines[0]

    def test_a_retry_that_breaks_the_backoff_fails_the_composition(self, tmp_path: Path) -> None:
        project = job_project(
            tmp_path, {"jobs": {"_ReportJob": {"retry": {"max_s": 1000}}}}, "inline"
        )

        with pytest.raises(ConfigError, match=r"_ReportJob\.retry"):
            project.compose(JobsAdapter(_ReportJob))


class TestCountdownBoundAtCompose:
    def test_a_declared_countdown_outside_the_bound_fails_compose(self, tmp_path: Path) -> None:
        class LateJob(Job[None]):
            __countdown__ = 901

            def execute(self) -> None:
                return None

        project = job_project(tmp_path, {}, "inline")

        with pytest.raises(ConfigError, match="LateJob"):
            project.compose(JobsAdapter(LateJob))

    def test_a_configured_countdown_outside_the_bound_fails_compose(self, tmp_path: Path) -> None:
        project = job_project(tmp_path, {"jobs": {"_ReportJob": {"countdown": 901}}}, "inline")

        with pytest.raises(ConfigError, match="_ReportJob"):
            project.compose(JobsAdapter(_ReportJob))
