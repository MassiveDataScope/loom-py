"""Each start-up path, composing the same components.

A path takes a :class:`Composition` and returns the container it built. The
composed paths discover their components from one manifest module placed in
``sys.modules``; the HTTP harness serves the composition's interfaces over a
fake repository; the standalone ones take the use cases and modules directly.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, cast

import pytest

from loom.composition import AppConfig, Application, Project
from loom.composition.recipe import Adapter
from loom.core.bootstrap import bootstrap_app, create_kernel
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.discovery.base import DiscoveryResult
from loom.core.environment import Environment
from loom.testing.in_memory import InMemoryRepository
from tests.helpers.composition import composition_project
from tests.helpers.jobs import BROKER
from tests.helpers.lambda_jobs import jobs_queue, jobs_trigger
from tests.unit.composition.phases.components import (
    AcmeInterface,
    AcmeJob,
    AcmePing,
    AcmeRecord,
)

SQLITE = {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}
_WORKER = "worker"
_EVENTS = "events"
_SYSTEM = {"subject": "svc:acme"}

Module = Callable[[LoomContainer], None]


@dataclass(frozen=True)
class Composition:
    """What a path composes: the manifest's components, caller modules and configuration.

    Attributes:
        models: ``MODELS`` of the manifest.
        use_cases: ``USE_CASES`` of the manifest; the standalone paths compile them.
        jobs: ``JOBS`` of the manifest.
        interfaces: ``INTERFACES`` of the manifest; the ones the harness serves.
        modules: Caller DI modules.
        config: Merged over the path's own document, section by section.
        environment: Environment the application composes in.
    """

    models: tuple[type[Any], ...] = (AcmeRecord,)
    use_cases: tuple[type[Any], ...] = (AcmePing,)
    jobs: tuple[type[Any], ...] = (AcmeJob,)
    interfaces: tuple[type[Any], ...] = (AcmeInterface,)
    overrides: tuple[Module, ...] = ()
    config: Mapping[str, Any] = field(default_factory=dict)
    environment: Environment = Environment.TEST


StartupPath = Callable[[Composition, Path, pytest.MonkeyPatch], LoomContainer]


def fastapi(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    pytest.importorskip("fastapi")
    from loom.rest.fastapi.adapter import FastAPIAdapter

    return _composed(composition, {}, None, FastAPIAdapter(), tmp_path, monkeypatch)


def lambda_http(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    pytest.importorskip("mangum")
    from loom.serverless.aws.lambda_ import LambdaAdapter

    return _composed(composition, {}, None, LambdaAdapter(), tmp_path, monkeypatch)


def lambda_jobs(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    from loom.serverless.aws.lambda_ import LambdaAdapter

    document = {**jobs_queue(), "lambda": {"functions": {_WORKER: {"trigger": jobs_trigger()}}}}
    return _composed(composition, document, _WORKER, LambdaAdapter(), tmp_path, monkeypatch)


def lambda_events(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    from loom.serverless.aws.lambda_ import LambdaAdapter

    trigger = {"source": "eventbridge", "job": AcmeJob.__qualname__, "identity": _SYSTEM}
    document = {"lambda": {"functions": {_EVENTS: {"trigger": trigger}}}}
    return _composed(composition, document, _EVENTS, LambdaAdapter(), tmp_path, monkeypatch)


def celery(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    pytest.importorskip("celery")
    from loom.celery.adapter import CeleryAdapter

    document = {"app": {"job_service": "celery"}, "celery": BROKER}
    return _composed(composition, document, None, CeleryAdapter(), tmp_path, monkeypatch)


def harness(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    """``HttpTestHarness.build_app`` serving *composition*'s interfaces over a fake repository.

    The harness keeps its composed application to itself: the path captures it.
    """
    del tmp_path
    pytest.importorskip("fastapi")
    from loom.testing import http_harness

    applications: list[Application] = []

    compose = Project.compose

    def captured(self: Project, adapter: Adapter[object], **kwargs: Any) -> Application:
        application = compose(self, adapter, **kwargs)
        applications.append(application)
        return application

    monkeypatch.setattr(Project, "compose", captured)
    built = http_harness.HttpTestHarness()
    built.inject_repo(AcmeRecord, InMemoryRepository(AcmeRecord))
    built.build_app(interfaces=list(composition.interfaces))
    (application,) = applications
    return application.kernel.container


def standalone_kernel(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    del tmp_path, monkeypatch
    kernel = create_kernel(
        config=AppConfig(name="acme"),
        use_cases=composition.use_cases,
        modules=composition.overrides,
        environment=composition.environment,
    )
    return kernel.container


def standalone_bootstrap(
    composition: Composition, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> LoomContainer:
    del tmp_path, monkeypatch
    result = bootstrap_app(
        config=AppConfig(name="acme"),
        use_cases=composition.use_cases,
        modules=composition.overrides,
        environment=composition.environment,
    )
    return result.container


def _composed(
    composition: Composition,
    document: dict[str, Any],
    entry: str | None,
    adapter: object,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> LoomContainer:
    discovered = DiscoveryResult(
        models=composition.models,
        use_cases=composition.use_cases,
        jobs=composition.jobs,
        interfaces=composition.interfaces,
    )
    monkeypatch.setattr(
        "loom.composition.project.PackageScanner.discover",
        lambda self, agents=None: discovered,
    )
    app = {
        "name": "acme",
        "entry": entry,
        "job_service": "inline",
        "packages": ["acme"],
    }
    base = {
        "app": app,
        "stores": {"default": SQLITE, "settings": {"cursors": {"signing": "ephemeral"}}},
    }
    written = _merged(_merged(base, document), composition.config)
    project = composition_project(
        ConfigContext.from_dict(written),
        environment=composition.environment,
        code_path=tmp_path,
        config_root=tmp_path,
    )
    application = project.compose(
        _OverridesAdapter(cast(Adapter[object], adapter), composition.overrides)
    )
    return application.kernel.container


class _OverridesAdapter:
    def __init__(self, adapter: Adapter[object], overrides: tuple[Module, ...]) -> None:
        self._adapter = adapter
        self._overrides = overrides

    def recipe(self, project: Project) -> Any:
        recipe = self._adapter.recipe(project)
        return replace(
            recipe,
            overrides=(*recipe.overrides, *self._overrides),
        )

    def bind(self, application: Application) -> object:
        return self._adapter.bind(application)


def _merged(base: Mapping[str, Any], over: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in over.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _merged(cast(Mapping[str, Any], current), cast(Mapping[str, Any], value))
        else:
            merged[key] = value
    return merged


COMPOSED: dict[str, StartupPath] = {
    "fastapi": fastapi,
    "lambda_http": lambda_http,
    "lambda_jobs": lambda_jobs,
    "lambda_events": lambda_events,
    "celery": celery,
}
"""The paths through ``Project.compose``, by name."""

STANDALONE: dict[str, StartupPath] = {
    "create_kernel": standalone_kernel,
    "bootstrap_app": standalone_bootstrap,
}
"""The paths calling the kernel directly, by name."""

UNCOMPOSED: dict[str, StartupPath] = {"harness": harness, **STANDALONE}
"""The paths that do not go through ``Project.compose``, by name."""

HTTP = ("fastapi", "lambda_http")
"""The paths serving HTTP, where some refusals read differently: see each variant."""


def refusal(
    paths: Mapping[str, StartupPath],
    path: str,
    composition: Composition,
    tmp_path: Path,
    prepare: Callable[[pytest.MonkeyPatch], None] | None = None,
) -> BaseException:
    """What *path* raises composing *composition*, in ``tmp_path / path`` and its own patches."""
    directory = tmp_path / path
    directory.mkdir()
    with pytest.MonkeyPatch.context() as monkeypatch:
        if prepare is not None:
            prepare(monkeypatch)
        try:
            paths[path](composition, directory, monkeypatch)
        except Exception as error:  # noqa: BLE001 - the refusal is what the caller records
            return error
    pytest.fail(f"{path} composed without refusing")
