"""Every adapter that derives a recipe keeps the callbacks the project's scan found."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from loom.celery.runtime import CeleryAdapter
from loom.composition import AppConfig, Project, Recipe
from loom.core.bootstrap import BootstrapError
from loom.core.config import ConfigContext
from loom.core.job.callback import JobCallback
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.rest.compiler import RouteSources
from loom.serverless.aws.lambda_ import LambdaAdapter
from loom.serverless.aws.lambda_._jobs import jobs_recipe
from loom.testing.http_harness import _HarnessAdapter
from tests.helpers import acme_callbacks, acme_installers, acme_unbound_callbacks
from tests.helpers.composition import composition_project
from tests.unit.serverless.aws.lambda_._records import WORKER, with_worker

_CELERY = {"broker_url": "memory://", "result_backend": "cache+memory://"}


class _AcmeJob(Job[int]):
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


RECIPES = pytest.mark.parametrize(
    "recipe_of",
    [
        pytest.param(lambda project: CeleryAdapter(jobs=[_AcmeJob]).recipe(project), id="celery"),
        pytest.param(
            lambda project: jobs_recipe(project, [_AcmeJob], [], None)[0], id="lambda-jobs"
        ),
        pytest.param(
            lambda project: _HarnessAdapter(RouteSources(), overrides=()).recipe(project),
            id="http-harness",
        ),
    ],
)


class TestRecipeKeepsTheScan:
    @RECIPES
    def test_the_recipe_holds_the_callbacks_the_project_scan_found(
        self, tmp_path: Path, recipe_of: Callable[[Project], Recipe]
    ) -> None:
        project = _project(tmp_path, acme_callbacks.MODULE)

        derived = recipe_of(project)

        assert derived.discovered.callbacks == project.discovered.callbacks
        assert acme_callbacks.AcmePingCallback in derived.discovered.callbacks

    @RECIPES
    def test_the_recipe_holds_the_installers_the_project_scan_found(
        self, tmp_path: Path, recipe_of: Callable[[Project], Recipe]
    ) -> None:
        project = _project(tmp_path, acme_installers.MODULE)

        derived = recipe_of(project)

        assert derived.discovered.installers == project.discovered.installers
        assert acme_installers.AcmeInstaller in derived.discovered.installers


class TestJobServiceFromContext:
    def test_a_recording_transport_selected_by_app_job_service_refuses_an_undiscovered_callback(
        self, tmp_path: Path
    ) -> None:
        project = _project(
            tmp_path, acme_callbacks.MODULE, job_service="loom.testing.jobs:RecordingJobTransport"
        )
        adapter = CeleryAdapter(jobs=[_AcmeJob])
        service = project.compose(adapter).kernel.container.resolve(JobService)

        service.dispatch_after_commit(_AcmeJob, on_success=acme_callbacks.AcmePingCallback)
        with pytest.raises(ValueError, match="_AcmeOtherCallback was not discovered"):
            service.dispatch_after_commit(_AcmeJob, on_success=_AcmeOtherCallback)


class TestLambdaJobCallbacks:
    def test_a_lambda_jobs_function_dispatching_to_celery_accepts_a_discovered_callback(
        self, tmp_path: Path
    ) -> None:
        project = _project(tmp_path, acme_callbacks.MODULE)
        adapter = LambdaAdapter(jobs=[_AcmeJob])
        application = project.compose(adapter)
        service = application.kernel.container.resolve(JobService)

        service.dispatch_after_commit(_AcmeJob, on_success=acme_callbacks.AcmePingCallback)
        with pytest.raises(ValueError, match="_AcmeOtherCallback was not discovered"):
            service.dispatch_after_commit(_AcmeJob, on_success=_AcmeOtherCallback)
        adapter.close()

    def test_a_lambda_callback_with_an_unbound_dependency_is_refused_at_compose(
        self, tmp_path: Path
    ) -> None:
        project = _project(tmp_path, acme_unbound_callbacks.MODULE)

        with pytest.raises(BootstrapError, match="AcmeNeedsUnboundCallback"):
            project.compose(LambdaAdapter(jobs=[_AcmeJob]))
