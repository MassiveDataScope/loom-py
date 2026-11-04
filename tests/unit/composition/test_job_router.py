"""``app.job_service`` picks the default job backend; ``celery.queues`` routes to Celery.

Only the backends in use are built. One backend is registered as itself;
several sit behind a ``JobRouter``.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml

from loom.celery.service import CeleryJobService
from loom.composition import (
    JobBackendContext,
    JobBackendError,
    build_application,
    compose_application,
)
from loom.core.job.service import InlineJobService, JobService
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory
from tests.helpers.jobs import BROKER, EmailJob, JobsAdapter, job_source

_SRC = Path(__file__).resolve().parents[3] / "src"
_ROUTED = {"celery": {**BROKER, "queues": ["default", "emails"]}}


class _ContextBackend(InlineJobService):
    """A ``module:Class`` backend that keeps the context it was built with."""

    def __init__(self, context: JobBackendContext) -> None:
        super().__init__(context.factory, context.executor)
        self.context = context


def _service(tmp_path: Path, config: dict[str, Any], job_service: str) -> JobService:
    application = compose_application(job_source(tmp_path, config, job_service), JobsAdapter())
    return application.kernel.container.resolve(JobService)


@pytest.mark.parametrize(
    ("queue", "expected"),
    [
        (None, CeleryJobService),
        ("emails", CeleryJobService),
        ("reports", InlineJobService),
        ("default", InlineJobService),
    ],
    ids=["job queue", "per-call celery queue", "per-call other queue", "celery default queue"],
)
def test_celery_queues_go_to_celery_and_the_rest_to_the_default(
    tmp_path: Path, queue: str | None, expected: type[JobService]
) -> None:
    service = _service(tmp_path, _ROUTED, "inline")

    with (
        patch.object(CeleryJobService, "dispatch", autospec=True) as celery,
        patch.object(InlineJobService, "dispatch", autospec=True) as inline,
    ):
        service.dispatch(EmailJob, queue=queue)

    reached = {CeleryJobService: celery, InlineJobService: inline}
    assert [backend for backend, mock in reached.items() if mock.called] == [expected]


@pytest.mark.parametrize(
    ("config", "job_service", "expected"),
    [
        ({"celery": BROKER}, "inline", InlineJobService),
        ({}, "inline", InlineJobService),
        (_ROUTED, "celery", CeleryJobService),
        ({"celery": BROKER}, "celery", CeleryJobService),
        ({"celery": {**BROKER, "queues": ["default"]}}, "inline", InlineJobService),
    ],
    ids=[
        "celery without queues",
        "no celery",
        "celery declares queues",
        "celery default",
        "celery declares only its default queue",
    ],
)
def test_one_backend_is_registered_as_itself(
    tmp_path: Path, config: dict[str, Any], job_service: str, expected: type[JobService]
) -> None:
    assert type(_service(tmp_path, config, job_service)) is expected


def test_a_module_class_backend_receives_the_backend_context(tmp_path: Path) -> None:
    source = job_source(tmp_path, {}, f"{__name__}:_ContextBackend")

    application = compose_application(source, JobsAdapter())

    service = application.kernel.container.resolve(JobService)
    assert isinstance(service, _ContextBackend)
    assert service.context.config is source.config
    assert service.context.factory is application.kernel.factory
    assert service.context.executor is application.kernel.executor
    assert service.context.observability is source.observability


def test_a_name_of_no_backend_stops_startup(tmp_path: Path) -> None:
    source = job_source(tmp_path, {}, "rabbit")
    adapter = JobsAdapter()

    with pytest.raises(JobBackendError, match="'rabbit' names no job backend"):
        compose_application(source, adapter)


@pytest.mark.parametrize(
    ("config", "job_service"),
    [({"celery": BROKER}, "celery"), (_ROUTED, "inline")],
    ids=["celery default", "celery queues"],
)
def test_celery_without_its_extra_stops_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config: dict[str, Any], job_service: str
) -> None:
    monkeypatch.setitem(sys.modules, "loom.celery.config", None)
    source = job_source(tmp_path, config, job_service)
    adapter = JobsAdapter()

    with pytest.raises(JobBackendError, match=r"loom\[celery\] extra is not installed"):
        compose_application(source, adapter)


def test_celery_queues_without_its_default_queue_stops_startup(tmp_path: Path) -> None:
    source = job_source(tmp_path, {"celery": {**BROKER, "queues": ["emails"]}}, "inline")
    adapter = JobsAdapter()

    with pytest.raises(JobBackendError, match="the celery section is invalid"):
        compose_application(source, adapter)


# ---------------------------------------------------------------------------
# The API process
# ---------------------------------------------------------------------------

_REPORT_JOB = """

from loom.core.job.job import Job


class ReportJob(Job[None]):
    def execute(self) -> None:
        return None
"""


def test_the_api_applies_the_job_config(fixture_project: ProjectFactory) -> None:
    # Its own module name: an earlier test may have imported the default one.
    config_path = Path(fixture_project("job_config_api"))
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    module = config["app"]["discovery"]["interfaces"]["modules"][0]
    with (config_path.parent / f"{module}.py").open("a", encoding="utf-8") as source:
        source.write(_REPORT_JOB)
    config["app"]["discovery"] = {"mode": "modules", "modules": {"include": [module]}}
    config["app"]["job_service"] = "inline"
    config["jobs"] = {"ReportJob": {"queue": "reports", "countdown": 5, "priority": 3}}
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    application = build_application(str(config_path), adapter=FastAPIAdapter(), environment="test")

    (job,) = application.discovered.jobs
    assert (job.__name__, job.__queue__, job.__countdown__, job.__priority__) == (
        "ReportJob",
        "reports",
        5,
        3,
    )


_NO_CELERY_SCRIPT = """
import json
import sys

from loom.composition import Recipe, build_application
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence.profile import WORKER_PROFILE


class Bare:
    def recipe(self, source):
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=(), interfaces=()),
            backend=source.backend(default="none"),
        )

    def bind(self, application):
        return application


build_application(sys.argv[1], adapter=Bare(), environment="test")
print(json.dumps(sorted(
    name for name in sys.modules
    if name == "celery" or name.startswith(("celery.", "loom.celery"))
)))
"""


def test_an_inline_default_without_celery_queues_loads_no_celery(tmp_path: Path) -> None:
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
