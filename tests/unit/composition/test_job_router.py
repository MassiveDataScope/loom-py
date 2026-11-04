"""``app.job_service`` picks the default job backend; a section's ``queues`` routes to its own.

Only the default and the backends whose section declares queues are built.
One backend is registered as itself; several sit behind a ``JobRouter``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
import yaml

from loom.celery.service import CeleryJobService
from loom.composition import (
    AppConfig,
    Application,
    CompositionSource,
    JobBackendContext,
    JobBackendError,
    Recipe,
    _jobs,
    build_application,
    compose_application,
)
from loom.composition.resources import ResourceKind
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.job import Job
from loom.core.job.router import BUILTIN_JOB_BACKENDS, DuplicateJobQueue, JobRouter
from loom.core.job.service import InlineJobService, JobService
from loom.core.persistence.profile import WORKER_PROFILE
from loom.core.persistence.stores import NONE_BACKEND
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import ProjectFactory, composition_source

_SRC = Path(__file__).resolve().parents[3] / "src"
_BROKER = {"broker_url": "memory://", "result_backend": "cache+memory://"}
_ROUTED = {"celery": {**_BROKER, "queues": ["default", "emails"]}}


class _EmailJob(Job[str]):
    __queue__ = "emails"

    def execute(self) -> str:
        return "sent"


class _ContextBackend(InlineJobService):
    """A ``module:Class`` backend that keeps the context it was built with."""

    def __init__(self, context: JobBackendContext) -> None:
        super().__init__(context.factory, context.executor)
        self.context = context


class _Jobs:
    """Compose the given jobs, as a worker would, and mount nothing."""

    def __init__(self, *jobs: type[Job[Any]]) -> None:
        self._jobs = jobs

    def recipe(self, source: CompositionSource) -> Recipe:
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(
                models=(), use_cases=cast(Any, self._jobs), interfaces=(), jobs=self._jobs
            ),
            backend=source.backend(default=NONE_BACKEND),
            connection_only=True,
        )

    def bind(self, application: Application) -> Application:
        return application


def _source(tmp_path: Path, config: dict[str, Any], job_service: str | None) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict(config),
        app=AppConfig(name="jobs", job_service=job_service),
        code_path=tmp_path,
        config_root=tmp_path,
    )


def _service(
    tmp_path: Path, config: dict[str, Any], job_service: str | None, *jobs: type[Job[Any]]
) -> JobService:
    application = compose_application(_source(tmp_path, config, job_service), _Jobs(*jobs))
    return application.kernel.container.resolve(JobService)


def test_the_overrides_allow_exactly_the_built_in_backends() -> None:
    assert tuple(_jobs._BUILTIN_JOB_BACKENDS) == BUILTIN_JOB_BACKENDS


def test_a_section_declaring_queues_routes_them_to_its_backend(tmp_path: Path) -> None:
    service = _service(tmp_path, _ROUTED, "inline", _EmailJob)

    assert isinstance(service, JobRouter)
    assert isinstance(service.default, InlineJobService)
    assert {queue: type(backend) for queue, backend in service.routes.items()} == {
        "default": CeleryJobService,
        "emails": CeleryJobService,
    }


@pytest.mark.parametrize(
    ("config", "job_service", "expected"),
    [
        ({"celery": _BROKER}, "inline", InlineJobService),
        ({}, "inline", InlineJobService),
        (_ROUTED, "celery", CeleryJobService),
        ({"celery": _BROKER}, "celery", CeleryJobService),
    ],
    ids=["celery without queues", "no celery", "celery declares queues", "celery default"],
)
def test_one_backend_is_registered_as_itself(
    tmp_path: Path, config: dict[str, Any], job_service: str, expected: type[JobService]
) -> None:
    assert type(_service(tmp_path, config, job_service)) is expected


def test_a_queue_two_backends_declare_stops_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(
        cast(dict[str, Any], _jobs._BUILTIN_JOB_BACKENDS),
        "rival",
        lambda context: (InlineJobService(context.factory, context.executor), None),
    )
    source = _source(tmp_path, {**_ROUTED, "rival": {"queues": ["emails"]}}, "inline")
    adapter = _Jobs(_EmailJob)

    with pytest.raises(DuplicateJobQueue, match="'emails'"):
        compose_application(source, adapter)


def test_a_module_class_backend_receives_the_backend_context(tmp_path: Path) -> None:
    source = _source(tmp_path, {}, f"{__name__}:_ContextBackend")

    application = compose_application(source, _Jobs())

    service = application.kernel.container.resolve(JobService)
    assert isinstance(service, _ContextBackend)
    assert service.context.config is source.config
    assert service.context.factory is application.kernel.factory
    assert service.context.executor is application.kernel.executor
    assert service.context.observability is source.observability


@pytest.mark.parametrize(
    ("job_service", "message"),
    [
        ("sqs", "sqs job backend arrives with T076"),
        ("rabbit", "'rabbit' names no job backend"),
    ],
)
def test_a_backend_that_cannot_be_built_stops_startup(
    tmp_path: Path, job_service: str, message: str
) -> None:
    source = _source(tmp_path, {}, job_service)
    adapter = _Jobs()

    with pytest.raises(JobBackendError, match=message):
        compose_application(source, adapter)


async def test_run_executes_in_process_a_job_routed_to_celery(tmp_path: Path) -> None:
    service = _service(tmp_path, _ROUTED, "inline", _EmailJob)
    assert isinstance(service, JobRouter)
    celery = service.routes["emails"]

    with patch.object(celery, "run", side_effect=AssertionError("run reached Celery")):
        assert await service.run(_EmailJob) == "sent"


async def test_each_backend_resource_closes_once(tmp_path: Path) -> None:
    application = compose_application(_source(tmp_path, _ROUTED, "inline"), _Jobs())
    service = application.kernel.container.resolve(JobService)
    assert isinstance(service, JobRouter)
    celery = cast(CeleryJobService, service.routes["emails"])
    job_resources = [r for r in application.resources if r.kind is ResourceKind.CELERY]
    assert len(job_resources) == 1

    with patch.object(celery, "close", wraps=celery.close) as close:
        async with application.running(check=False):
            close.assert_not_called()

    close.assert_called_once_with()


def test_celery_named_without_its_extra_stops_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "loom.celery.service", None)
    source = _source(tmp_path, {"celery": _BROKER}, "celery")
    adapter = _Jobs()

    with pytest.raises(JobBackendError, match=r"loom\[celery\] extra is not installed"):
        compose_application(source, adapter)


# ---------------------------------------------------------------------------
# Without app.job_service: today's rule, deprecated
# ---------------------------------------------------------------------------


def test_without_job_service_a_celery_section_picks_celery(tmp_path: Path) -> None:
    with pytest.deprecated_call(match="app.job_service is not set"):
        service = _service(tmp_path, {"celery": _BROKER}, None)

    assert isinstance(service, CeleryJobService)


def test_without_job_service_or_celery_section_jobs_run_inline(tmp_path: Path) -> None:
    with (
        pytest.deprecated_call(match="app.job_service is not set"),
        pytest.warns(UserWarning, match="Celery config section is missing"),
    ):
        service = _service(tmp_path, {}, None)

    assert type(service) is InlineJobService


def test_without_job_service_a_missing_extra_falls_back_to_inline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "loom.celery.service", None)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        service = _service(tmp_path, {"celery": _BROKER}, None)

    assert type(service) is InlineJobService


def test_without_job_service_no_routing_check_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(
        cast(dict[str, Any], _jobs._BUILTIN_JOB_BACKENDS),
        "rival",
        lambda context: (InlineJobService(context.factory, context.executor), None),
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        service = _service(tmp_path, {**_ROUTED, "rival": {"queues": ["emails"]}}, None)

    assert isinstance(service, CeleryJobService)


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
        "celery": _BROKER,
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
