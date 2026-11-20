"""``app.job_service`` picks the default job transport; ``celery.queues`` routes to Celery.

Only the transports in use are built. One transport serves every queue; a Celery
queue is sent by Celery and the rest by the default.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.celery.service import CeleryJobTransport
from loom.composition import JobTransportContext, JobTransportError, Project
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.service import JobService
from loom.jobs.inline import InlineJobTransport
from loom.rest.fastapi.runtime import FastAPIRuntime
from loom.testing import RecordingJobTransport
from tests.helpers.composition import ProjectFactory
from tests.helpers.jobs import BROKER, EmailJob, JobsAdapter, job_project

_SRC = Path(__file__).resolve().parents[3] / "src"
_ROUTED = {"celery": {**BROKER, "queues": ["default", "emails"]}}
_RECORDING = "loom.testing:RecordingJobTransport"


class _ContextTransport(RecordingJobTransport):
    """A ``module:Class`` transport that keeps the context it was built with."""

    def __init__(self, context: JobTransportContext) -> None:
        super().__init__(context)
        self.context = context


def _service(tmp_path: Path, config: dict[str, Any], job_service: str) -> JobService:
    application = job_project(tmp_path, config, job_service).compose(JobsAdapter())
    return application.kernel.container.resolve(JobService)


# ---------------------------------------------------------------------------
# The API process
# ---------------------------------------------------------------------------

_REPORT_JOB = """

from loom.core.job.job import Job


class ReportJob(Job[None]):
    def execute(self) -> None:
        return None
"""


_NO_CELERY_SCRIPT = """
import json
import sys

from loom.composition import Project, Recipe
from loom.core.discovery.base import DiscoveryResult
from loom.core.stores.profile import WORKER_PROFILE


class Bare:
    def recipe(self, project):
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=()),
        )

    def mount(self, application):
        return application


Project.load(sys.argv[1], environment="test").compose(Bare())
print(json.dumps(sorted(
    name for name in sys.modules
    if name == "celery" or name.startswith(("celery.", "loom.celery"))
)))
"""


class TestJobTransports:
    @pytest.mark.parametrize(
        ("queue", "recorded_by_default"),
        [("emails", False), ("reports", True), ("default", True)],
        ids=["celery queue", "other queue", "celery default queue"],
    )
    async def test_celery_queues_are_sent_by_celery_and_the_rest_by_the_default(
        self, tmp_path: Path, queue: str, recorded_by_default: bool
    ) -> None:
        service = _service(tmp_path, _ROUTED, _RECORDING)
        default = service.default_transport
        assert isinstance(default, RecordingJobTransport)

        service.dispatch_after_commit(EmailJob, queue=queue)
        await flush_pending_dispatches()

        assert [record.queue for record in default.dispatched] == (
            [queue] if recorded_by_default else []
        )

    @pytest.mark.parametrize(
        ("config", "job_service", "expected"),
        [
            ({"celery": BROKER}, "inline", InlineJobTransport),
            ({}, "inline", InlineJobTransport),
            (_ROUTED, "celery", CeleryJobTransport),
            ({"celery": BROKER}, "celery", CeleryJobTransport),
            ({"celery": {**BROKER, "queues": ["default"]}}, "inline", InlineJobTransport),
        ],
        ids=[
            "celery without queues",
            "no celery",
            "celery declares queues",
            "celery default",
            "celery declares only its default queue",
        ],
    )
    def test_one_transport_serves_every_queue_when_nothing_is_routed(
        self, tmp_path: Path, config: dict[str, Any], job_service: str, expected: type[object]
    ) -> None:
        service = _service(tmp_path, config, job_service)

        assert type(service.default_transport) is expected
        assert service.transport_for("emails") is service.default_transport

    def test_a_module_class_transport_receives_the_transport_context(self, tmp_path: Path) -> None:
        project = job_project(tmp_path, {}, f"{__name__}:_ContextTransport")

        application = project.compose(JobsAdapter())

        transport = application.kernel.container.resolve(JobService).default_transport
        assert isinstance(transport, _ContextTransport)
        assert transport.context.config is project.config
        assert transport.context.factory is application.kernel.factory
        assert transport.context.executor is application.kernel.executor
        assert transport.context.observability is project.observability

    def test_a_name_of_no_transport_stops_startup(self, tmp_path: Path) -> None:
        project = job_project(tmp_path, {}, "rabbit")
        adapter = JobsAdapter()

        with pytest.raises(JobTransportError, match="'rabbit' names no job transport"):
            project.compose(adapter)

    @pytest.mark.parametrize(
        ("config", "job_service"),
        [({"celery": BROKER}, "celery"), (_ROUTED, "inline")],
        ids=["celery default", "celery queues"],
    )
    def test_celery_without_its_extra_stops_startup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        config: dict[str, Any],
        job_service: str,
    ) -> None:
        monkeypatch.setitem(sys.modules, "loom.celery.config", None)
        project = job_project(tmp_path, config, job_service)
        adapter = JobsAdapter()

        with pytest.raises(JobTransportError, match=r"loom\[celery\] extra is not installed"):
            project.compose(adapter)

    def test_celery_queues_without_its_default_queue_stops_startup(self, tmp_path: Path) -> None:
        project = job_project(tmp_path, {"celery": {**BROKER, "queues": ["emails"]}}, "inline")
        adapter = JobsAdapter()

        with pytest.raises(JobTransportError, match="the celery section is invalid"):
            project.compose(adapter)


class TestApiJobSettings:
    def test_the_api_applies_the_job_config(self, fixture_project: ProjectFactory) -> None:
        # Its own module name: an earlier test may have imported the default one.
        config_path = Path(fixture_project("job_config_api"))
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        package = config["app"]["packages"][0]
        with (config_path.parent / package / "__init__.py").open("a", encoding="utf-8") as source:
            source.write(_REPORT_JOB)
        config["app"]["job_service"] = "inline"
        config["jobs"] = {"ReportJob": {"queue": "reports", "countdown": 5, "priority": 3}}
        config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

        application = Project.load(str(config_path), environment="test").compose(FastAPIRuntime())

        (job,) = application.discovered.jobs
        options = application.job_settings.options_for(job)
        assert (job.__name__, options.queue, options.countdown, options.priority) == (
            "ReportJob",
            "reports",
            5,
            3,
        )
        assert (job.__queue__, job.__countdown__, job.__priority__) == ("default", 0, 0)


class TestInlineJobsLoadNoCelery:
    def test_an_inline_default_without_celery_queues_loads_no_celery(self, tmp_path: Path) -> None:
        config = tmp_path / "app.yaml"
        document = {
            "app": {"name": "inline", "code_path": ".", "job_service": "inline"},
            "stores": {"default": {"backend": "none"}},
            "celery": BROKER,
        }
        config.write_text(yaml.safe_dump(document), encoding="utf-8")

        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                f"import sys; sys.path.insert(0, {str(_SRC)!r})\n" + _NO_CELERY_SCRIPT,
                str(config),
            ],
            capture_output=True,
            text=True,
            check=False,
            cwd=tmp_path,
        )

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout.splitlines()[-1]) == []
