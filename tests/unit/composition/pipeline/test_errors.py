"""The start-up error contract (plan 3.4): what reaches the caller wrapped, and what as is.

``BootstrapError`` wraps, cause chained, a failing caller module, override,
repository or cache module, a use case's compilation, a structural refusal
of the dependency graph and the ``validate()`` seal runs; a
``BootstrapError`` subclass passes through. Everything else keeps
its own type. The recordings of ``snapshots/refusals`` hold what each path
raises (one per refusal, and one per declared variant), and are checked
against the contract here; the failures no recording provokes are provoked
below.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest

from loom.composition import Application, pipeline
from loom.composition.errors import ReservedBindingError
from loom.composition.pipeline import persistence, registration
from loom.composition.source import CompositionSource
from loom.core.bootstrap import BootstrapError
from loom.core.di import ContainerSealed
from loom.core.di.container import ResolutionError
from loom.core.logger import get_logger
from tests.helpers.composition import Serving
from tests.unit.composition.pipeline.components import (
    AcmeJob,
    AcmePing,
    AcmeRecord,
    binds_unbuildable,
    raises_at_registration,
)
from tests.unit.composition.pipeline.snapshot import SNAPSHOTS, path_recordings
from tests.unit.composition.pipeline.test_refusals import REFUSALS

_WRAPPED = {
    "failing-module": "RuntimeError",
    "use-case-compilation": "CompilationError",
    "store-beside-main-model": "CompilationError",
    "failing-application-provider": "ResolutionError",
    # Refusal 1 of the dependency graph, raised by factory.verify as is before T04.
    "unbound-dependency": "DependencyGraphError",
    "dependency-cycle": "DependencyGraphError",
    "captive-dependency": "DependencyGraphError",
    "caller-in-singleton": "DependencyGraphError",
    "input-marker-in-constructor": "DependencyGraphError",
    "unresolvable-parameter": "DependencyGraphError",
}
"""Refusals wrapped on every path, with the type of their cause."""

_RAW = {
    "reserved-binding": "ReservedBindingError",
    "job-backend": "JobBackendError",
    "job-backend-celery-extra": "JobBackendError",
    "job-backend-celery-section": "JobBackendError",
    "sqs-queue": "SQSQueueConfigError",
    "sql-invalid-section": "ConfigError",
    "sql-retired-key": "ConfigError",
    "undeclared-store": "ConfigError",
    "uncomposed-write-store": "UncomposedWriteStore",
    "joined-autocommit-store": "JoinedAutocommitStore",
    "unserved-model": "UnservedModelError",
    "deploy-check": "DeployCheckError",
    "agent-marker": "AgentCompilationError",
    "mcp-marker": "AgentCompilationError",
}
"""Refusals raised as is on every path, with their type."""

_JOB_COMPILATION_WRAPPED = {"celery", "lambda_jobs", "lambda_events"}
"""The paths compiling jobs inside the wrapper; the others raise ``CompilationError`` as is."""


def _recorded(refusal: str) -> list[tuple[str, dict[str, Any]]]:
    """What each path of *refusal* records, by path."""
    spec = REFUSALS[refusal]
    return path_recordings(f"refusals/{refusal}", spec.paths, spec.variants)


def _type(qualified: str) -> str:
    return qualified.rsplit(".", 1)[1]


def _failing_repositories(monkeypatch: pytest.MonkeyPatch) -> None:
    persist = pipeline.persist

    def failing(planned: Any) -> persistence.Persisted:
        persisted = persist(planned)
        wirings = {
            key: replace(wiring, repo_registration_module=raises_at_registration)
            for key, wiring in persisted.wirings.items()
        }
        return replace(persisted, wirings=wirings)

    monkeypatch.setattr(pipeline, "persist", failing)


def _failing_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(registration, "cache_module_for", lambda ctx: raises_at_registration)


def _unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    del monkeypatch


class TestStartupErrors:
    def test_the_recordings_cover_every_refusal(self) -> None:
        recorded = {path.name.split(".")[0] for path in (SNAPSHOTS / "refusals").iterdir()}
        assert recorded == {*_WRAPPED, *_RAW, "job-compilation"}

    @pytest.mark.parametrize("refusal", list(_WRAPPED))
    def test_a_wrapped_refusal_chains_its_cause(self, refusal: str) -> None:
        for path, recorded in _recorded(refusal):
            cause = recorded["cause"]
            assert recorded["wrapped"], path
            assert _type(cause["type"]) == _WRAPPED[refusal], path
            assert recorded["message"] == f"Bootstrap failed: {cause['message']}", path

    @pytest.mark.parametrize("refusal", list(_RAW))
    def test_a_raw_refusal_keeps_its_type(self, refusal: str) -> None:
        for path, recorded in _recorded(refusal):
            assert not recorded["wrapped"], path
            assert _type(recorded["type"]) == _RAW[refusal], path

    def test_a_bootstrap_error_subclass_passes_through_unwrapped(self) -> None:
        assert issubclass(ReservedBindingError, BootstrapError)
        assert all(recorded["cause"] is None for _, recorded in _recorded("reserved-binding"))

    def test_a_job_compilation_error_keeps_its_type_per_path(self) -> None:
        for path, recorded in _recorded("job-compilation"):
            assert recorded["wrapped"] == (path in _JOB_COMPILATION_WRAPPED), path
            compilation = recorded["cause"] if recorded["wrapped"] else recorded
            assert _type(compilation["type"]) == "CompilationError", path

    @pytest.mark.parametrize(
        ("overrides", "setup"),
        [
            ((raises_at_registration,), _unchanged),
            ((), _failing_repositories),
            ((), _failing_cache),
        ],
        ids=["override", "repository-module", "cache-module"],
    )
    def test_a_failing_registration_module_is_wrapped(
        self,
        overrides: tuple[Callable[..., None], ...],
        setup: Callable[[pytest.MonkeyPatch], None],
        overridden: Callable[..., Application],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        setup(monkeypatch)

        with pytest.raises(BootstrapError, match="Bootstrap failed: acme module failed") as caught:
            overridden(*overrides)

        assert type(caught.value) is BootstrapError
        assert isinstance(caught.value.__cause__, RuntimeError)

    def test_seal_validates_the_container_it_seals(self, source: CompositionSource) -> None:
        adapter = Serving(AcmeRecord, use_cases=(AcmePing,), jobs=(AcmeJob,))
        planned = pipeline.plan(source, adapter)
        registered = pipeline.register(
            pipeline.persist(planned), modules=(binds_unbuildable,), logger=get_logger("tests.acme")
        )

        with pytest.raises(BootstrapError, match="AcmeUnbuildable") as caught:
            pipeline.seal(registered)

        assert isinstance(caught.value.__cause__, ResolutionError)
        with pytest.raises(ContainerSealed):
            registered.kernel.container.register(AcmeRecord, AcmeRecord)
