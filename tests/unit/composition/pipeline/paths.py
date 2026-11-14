"""Each start-up path, composing the same components.

A path takes a :class:`Composition` and returns the container it built. The
composed paths discover their components from one manifest module placed in
``sys.modules``; the HTTP harness serves the composition's interfaces over a
fake repository; the standalone ones take the use cases and modules directly.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pytest

from loom.composition import AppConfig, Application, compose_application
from loom.composition.recipe import Adapter
from loom.core.bootstrap import bootstrap_app, create_kernel
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.environment import Environment
from loom.testing.in_memory import InMemoryRepository
from tests.helpers.composition import composition_source
from tests.helpers.jobs import BROKER
from tests.unit.composition.pipeline.components import (
    AcmeInterface,
    AcmeJob,
    AcmePing,
    AcmeRecord,
)

MANIFEST = "acme_pipeline_manifest"
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
    modules: tuple[Module, ...] = ()
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

    document = {"lambda": {"functions": {_WORKER: {"trigger": {"source": "jobs"}}}}}
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

    Survey of the harness-based suites before the pipeline (T01a, input to
    T01e): one graph would trip a structural refusal. In
    ``tests/integration/core/rest/test_rest_error_mapping.py``,
    ``ProductRestInterface`` routes ``FindProductByNameUseCase``, whose
    constructor injects ``ProductRepo``, which the harness never binds (it
    binds only ``("repo", Product)``): a missing binding (refusal 1) once every
    route's use case is a root. It passed only because no test called that
    route. Fixed in T01e (D18): the harness binds a fake under its repository
    protocol bases too, and that file injects an ``InMemoryProductRepository``.
    No harness test or use case depends on the ``_NullConfig`` the harness
    bound before T01e.
    """
    del tmp_path
    pytest.importorskip("fastapi")
    from loom.testing import http_harness

    applications: list[Application] = []

    def captured(*args: Any, **kwargs: Any) -> Application:
        application = compose_application(*args, **kwargs)
        applications.append(application)
        return application

    monkeypatch.setattr(http_harness, "compose_application", captured)
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
        modules=composition.modules,
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
        modules=composition.modules,
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
    manifest = ModuleType(MANIFEST)
    manifest.MODELS = list(composition.models)  # type: ignore[attr-defined]
    manifest.USE_CASES = list(composition.use_cases)  # type: ignore[attr-defined]
    manifest.JOBS = list(composition.jobs)  # type: ignore[attr-defined]
    manifest.INTERFACES = list(composition.interfaces)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, MANIFEST, manifest)
    app = {
        "name": "acme",
        "entry": entry,
        "job_service": "inline",
        "discovery": {"mode": "manifest", "manifest": {"module": MANIFEST}},
    }
    base = {
        "app": app,
        "stores": {"default": SQLITE},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    written = _merged(_merged(base, document), composition.config)
    source = composition_source(
        ConfigContext.from_dict(written),
        environment=composition.environment,
        code_path=tmp_path,
        config_root=tmp_path,
    )
    application = compose_application(
        source, cast(Adapter[object], adapter), modules=composition.modules
    )
    return application.kernel.container


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
"""The paths through ``compose_application``, by name."""

STANDALONE: dict[str, StartupPath] = {
    "create_kernel": standalone_kernel,
    "bootstrap_app": standalone_bootstrap,
}
"""The paths calling the kernel directly, by name."""

UNCOMPOSED: dict[str, StartupPath] = {"harness": harness, **STANDALONE}
"""The paths that do not go through ``compose_application``, by name."""

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
