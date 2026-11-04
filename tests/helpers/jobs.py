"""Jobs and sources the job service and job routing tests share."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from loom.composition import AppConfig, Application, CompositionSource, Recipe
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.job.job import Job
from loom.core.persistence.profile import WORKER_PROFILE
from loom.core.persistence.stores import NONE_BACKEND
from tests.helpers.composition import composition_source

BROKER = {"broker_url": "memory://", "result_backend": "cache+memory://"}


class EmailJob(Job[str]):
    __queue__ = "emails"

    def execute(self) -> str:
        return "email"


class JobsAdapter:
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


def job_source(
    tmp_path: Path, config: dict[str, Any], job_service: str | None = None
) -> CompositionSource:
    return composition_source(
        ConfigContext.from_dict(config),
        app=AppConfig(name="jobs", job_service=job_service),
        code_path=tmp_path,
        config_root=tmp_path,
    )
