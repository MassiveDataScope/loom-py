"""Each application dispatches a job with its own ``jobs:`` section; none leaks into another.

T090: the composition used to write each ``jobs.<Job>`` section onto the
job's ClassVars, so with two applications in one process the last one won and
one without the section kept the other's values.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from celery import Celery

from loom.composition import Application, compose_application
from loom.core.config import ConfigError
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from tests.helpers.jobs import BROKER, JobsAdapter, job_source

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
    source = job_source(directory, config, job_service)
    return compose_application(source, JobsAdapter(_ReportJob))


async def _sent(application: Application) -> dict[str, Any] | None:
    """The routing ``send_task`` got when *application* dispatched the job; ``None`` if not sent."""
    service = application.kernel.container.resolve(JobService)
    with patch.object(Celery, "send_task", autospec=True) as send_task:
        service.dispatch(_ReportJob)
        await flush_pending_dispatches()
    if send_task.call_args is None:
        return None
    kwargs = send_task.call_args.kwargs
    return {key: kwargs[key] for key in _DECLARED}


def _in_order(configured_first: bool) -> list[tuple[str, dict[str, Any] | None]]:
    order: list[tuple[str, dict[str, Any] | None]] = [
        ("configured", {"_ReportJob": _CONFIGURED}),
        ("plain", None),
    ]
    return order if configured_first else order[::-1]


_ORDERS = pytest.mark.parametrize(
    "configured_first", [True, False], ids=["configured first", "configured last"]
)


@_ORDERS
async def test_each_application_dispatches_with_its_own_job_section(
    tmp_path: Path, configured_first: bool
) -> None:
    order = _in_order(configured_first)
    applications = {name: _compose(tmp_path, name, jobs) for name, jobs in order}

    sent = {name: await _sent(application) for name, application in applications.items()}

    assert sent == {
        "configured": {"queue": "reports", "countdown": 5, "priority": 3, "soft_time_limit": 60},
        "plain": _DECLARED,
    }
    assert (_ReportJob.__queue__, _ReportJob.__countdown__, _ReportJob.__timeout__) == (
        "default",
        0,
        None,
    )


@_ORDERS
async def test_each_application_routes_by_the_queue_of_its_own_job_section(
    tmp_path: Path, configured_first: bool
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


def test_a_malformed_jobs_section_fails_the_composition(tmp_path: Path) -> None:
    source = job_source(tmp_path, {"jobs": {"_ReportJob": {"timeout": "60s"}}}, "inline")
    adapter = JobsAdapter(_ReportJob)

    with pytest.raises(ConfigError, match="_ReportJob"):
        compose_application(source, adapter)
