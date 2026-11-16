"""Every adapter that derives a recipe keeps the callbacks the project's scan found."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.celery.adapter import CeleryAdapter
from loom.composition import AppConfig, Project, Recipe
from loom.core.bootstrap import BootstrapError
from loom.core.config import ConfigContext
from loom.core.job.callback import JobCallback
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.rest.compiler import RouteSources
from loom.serverless.aws.lambda_ import LambdaAdapter
from loom.testing.http_harness import _HarnessAdapter
from tests.helpers import acme_callbacks, acme_unbound_callbacks
from tests.helpers.composition import composition_project
from tests.unit.serverless.aws.lambda_._records import WORKER, with_worker

_CELERY = {"broker_url": "memory://", "result_backend": "cache+memory://"}


class _AcmeJob(Job[int]):
    __retries__ = 0

    def execute(self) -> int:
        return 1


class _AcmeOtherCallback(JobCallback):
    pass


def _project(tmp_path: Path, *modules: str, job_service: str = "celery") -> Project:
    document = with_worker({"celery": _CELERY})
    return composition_project(
        ConfigContext.from_dict(document),
        app=AppConfig(name="acme", entry=WORKER, job_service=job_service, packages=list(modules)),
        code_path=tmp_path,
        config_root=tmp_path,
    )


DERIVING_ADAPTERS = pytest.mark.parametrize(
    "adapter",
    [
        pytest.param(lambda: CeleryAdapter(jobs=[_AcmeJob]), id="celery"),
        pytest.param(lambda: LambdaAdapter(jobs=[_AcmeJob]), id="lambda-jobs"),
        pytest.param(lambda: _HarnessAdapter(RouteSources(), overrides=()), id="http-harness"),
    ],
)


class TestRecipeKeepsTheScannedCallbacks:
    @DERIVING_ADAPTERS
    def test_the_recipe_holds_what_the_project_scan_found(
        self, tmp_path: Path, adapter: Callable[[], Any]
    ) -> None:
        project = _project(tmp_path, acme_callbacks.MODULE)
        derived: Recipe = _recipe_of(adapter(), project)

        assert derived.discovered.callbacks == project.discovered.callbacks
        assert acme_callbacks.AcmePingCallback in derived.discovered.callbacks


def _recipe_of(adapter: Any, project: Project) -> Recipe:
    if isinstance(adapter, LambdaAdapter):
        from loom.serverless.aws.lambda_._jobs import jobs_recipe

        return jobs_recipe(project, [_AcmeJob], [], None)[0]
    recipe: Recipe = adapter.recipe(project)
    return recipe


class TestLambdaJobCallbacks:
    def test_a_lambda_jobs_function_dispatching_to_celery_accepts_a_discovered_callback(
        self, tmp_path: Path
    ) -> None:
        project = _project(tmp_path, acme_callbacks.MODULE)
        adapter = LambdaAdapter(jobs=[_AcmeJob])
        application = project.compose(adapter)
        service = application.kernel.container.resolve(JobService)

        service.dispatch(_AcmeJob, on_success=acme_callbacks.AcmePingCallback)
        with pytest.raises(ValueError, match="_AcmeOtherCallback was not discovered"):
            service.dispatch(_AcmeJob, on_success=_AcmeOtherCallback)
        adapter.close()

    def test_a_lambda_callback_with_an_unbound_dependency_is_refused_at_compose(
        self, tmp_path: Path
    ) -> None:
        project = _project(tmp_path, acme_unbound_callbacks.MODULE)

        with pytest.raises(BootstrapError, match="AcmeNeedsUnboundCallback"):
            project.compose(LambdaAdapter(jobs=[_AcmeJob]))
