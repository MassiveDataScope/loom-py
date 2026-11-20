"""How each start-up refusal reaches the caller on each path, on today's code.

Each snapshot ``refusals/<refusal>.json`` holds the type and message the
caller catches on every path, whether it is a plain ``BootstrapError``
wrapping the refusal, and the wrapped cause; a path that records something
else is a declared variant, ``refusals/<refusal>.<variant>.json``. The test
also checks the refusal is the one the scenario provokes, raw or as the
wrapped cause, so a recording never holds an unrelated failure.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.ai.errors import AgentCompilationError
from loom.composition import JobTransportError, Project
from loom.composition.errors import ReservedBindingError
from loom.core.bootstrap import BootstrapError
from loom.core.checks import DeployCheckError
from loom.core.config import ConfigError
from loom.core.di.container import ResolutionError
from loom.core.di.graph import DependencyGraphError
from loom.core.engine.compiler import CompilationError
from loom.core.environment import Environment
from loom.core.use_case import UnservedModelError
from loom.core.use_case.errors import JoinedAutocommitStore, UncomposedWriteStore
from loom.jobs.aws.errors import SQSQueueConfigError
from tests.helpers.jobs import BROKER
from tests.unit.composition.phases import paths
from tests.unit.composition.phases.components import (
    AcmeAgentUser,
    AcmeBesideModel,
    AcmeInputInConstructor,
    AcmeInterface,
    AcmeJob,
    AcmeJoinedLedger,
    AcmeLedgerEntry,
    AcmeLedgerWriter,
    AcmeMcpUser,
    AcmeMissingStore,
    AcmeNeedsCallerCache,
    AcmeNeedsCycle,
    AcmeNeedsLedger,
    AcmeNeedsService,
    AcmeNeedsServiceInterface,
    AcmePing,
    AcmeReadsUnserved,
    AcmeRecord,
    AcmeUncompilable,
    AcmeUncompilableJob,
    AcmeUnresolvable,
    binds_caller_cache,
    binds_captive,
    binds_cycle,
    binds_sql_service,
    binds_unbuildable,
    raises_at_registration,
)
from tests.unit.composition.phases.snapshot import (
    describe_container,
    describe_refusal,
    matches_path_snapshots,
    matches_snapshot,
)

_COMPOSED = tuple(paths.COMPOSED)
_EVERY = (*_COMPOSED, *paths.STANDALONE)
_PATHS = {**paths.COMPOSED, **paths.STANDALONE}
_LEDGER = {"stores": {"ledger": paths.SQLITE}}
_CLICKHOUSE = "clickhouse://acme:8123/acme"
_ANALYSED = ("fastapi",)
"""Every path analyses the same graph at the same point: one path records a graph refusal.

``dependency-cycle`` and ``unbound-dependency`` keep every composed path."""


@dataclass(frozen=True)
class _Refusal:
    """A composition provoking *refused* on each of *paths*; *variants* record their own."""

    composition: paths.Composition
    refused: type[BaseException]
    paths: tuple[str, ...] = _COMPOSED
    prepare: Callable[[pytest.MonkeyPatch], None] | None = None
    variants: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


def _without_celery_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "loom.celery.config", None)


_NEEDS_SERVICE = paths.Composition(
    use_cases=(AcmePing, AcmeNeedsService),
    interfaces=(AcmeInterface, AcmeNeedsServiceInterface),
)


REFUSALS: dict[str, _Refusal] = {  # test_precedence pins families of these too
    "reserved-binding": _Refusal(
        paths.Composition(overrides=(binds_sql_service,)), ReservedBindingError
    ),
    "failing-module": _Refusal(
        paths.Composition(overrides=(raises_at_registration,)), RuntimeError, _EVERY
    ),
    "failing-application-provider": _Refusal(
        paths.Composition(overrides=(binds_unbuildable,)), ResolutionError, _EVERY
    ),
    "use-case-compilation": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeUncompilable)), CompilationError, _EVERY
    ),
    "job-compilation": _Refusal(
        paths.Composition(jobs=(AcmeJob, AcmeUncompilableJob)),
        CompilationError,
        # The HTTP paths raise the refusal itself, the others a BootstrapError wrapping it.
        variants={"http": paths.HTTP},
    ),
    "store-beside-main-model": _Refusal(
        paths.Composition(
            models=(AcmeRecord, AcmeLedgerEntry),
            use_cases=(AcmePing, AcmeBesideModel),
            config=_LEDGER,
        ),
        CompilationError,
        _EVERY,
    ),
    "uncomposed-write-store": _Refusal(
        paths.Composition(
            jobs=(AcmeJob, AcmeLedgerWriter), config={"stores": {"ledger": {"backend": "none"}}}
        ),
        UncomposedWriteStore,
    ),
    "joined-autocommit-store": _Refusal(
        paths.Composition(
            use_cases=(AcmePing, AcmeJoinedLedger),
            config={"stores": {"ledger": {"backend": "mongo", "url": "mongodb://ledger"}}},
        ),
        JoinedAutocommitStore,
    ),
    "undeclared-store": _Refusal(paths.Composition(jobs=(AcmeJob, AcmeMissingStore)), ConfigError),
    "unserved-model": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeReadsUnserved)), UnservedModelError
    ),
    "agent-marker": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeAgentUser)), AgentCompilationError
    ),
    "mcp-marker": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeMcpUser)), AgentCompilationError
    ),
    "deploy-check": _Refusal(
        paths.Composition(environment=Environment.PRODUCTION),
        DeployCheckError,
        # The HTTP paths also report the REST deploy check.
        variants={"http": paths.HTTP},
    ),
    "job-transport": _Refusal(
        paths.Composition(config={"app": {"job_service": "rabbit"}}), JobTransportError
    ),
    "job-transport-celery-extra": _Refusal(
        paths.Composition(config={"app": {"job_service": "celery"}, "celery": BROKER}),
        JobTransportError,
        # The worker adapter module itself imports loom.celery.config, so the worker path cannot
        # run without the extra; stubbing the module only yields a test-side NameError.
        tuple(path for path in _COMPOSED if path != "celery"),
        prepare=_without_celery_extra,
    ),
    "job-transport-celery-section": _Refusal(
        paths.Composition(
            config={
                "app": {"job_service": "inline"},
                "celery": {**BROKER, "queues": ["emails"]},
            }
        ),
        JobTransportError,
    ),
    "unbound-dependency": _Refusal(_NEEDS_SERVICE, ResolutionError),
    "dependency-cycle": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeNeedsCycle), overrides=(binds_cycle,)),
        DependencyGraphError,
    ),
    "captive-dependency": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeNeedsLedger), overrides=(binds_captive,)),
        DependencyGraphError,
        _ANALYSED,
    ),
    "caller-in-singleton": _Refusal(
        paths.Composition(
            use_cases=(AcmePing, AcmeNeedsCallerCache), overrides=(binds_caller_cache,)
        ),
        DependencyGraphError,
        _ANALYSED,
    ),
    "input-marker-in-constructor": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeInputInConstructor)),
        DependencyGraphError,
        _ANALYSED,
    ),
    "unresolvable-parameter": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeUnresolvable)),
        DependencyGraphError,
        _ANALYSED,
    ),
    "sqs-queue": _Refusal(
        paths.Composition(
            config={
                "app": {"job_service": "sqs"},
                "aws": {"region": "eu-west-1"},
                "sqs": {"queues": {}},
            }
        ),
        SQSQueueConfigError,
    ),
    "sql-retired-key": _Refusal(
        paths.Composition(
            config={"sql": {"connections": {"main": {"url": _CLICKHOUSE, "pool_size": 2}}}}
        ),
        ConfigError,
        # Lambda HTTP validates the section first and reports the unknown field, not the rename.
        variants={"lambda_http": ("lambda_http",)},
    ),
    "sql-invalid-section": _Refusal(
        paths.Composition(
            config={"sql": {"connections": {"main": {"url": _CLICKHOUSE, "unknown": 1}}}}
        ),
        ConfigError,
    ),
}


_UNCOMPILABLE_JOB = """

from loom.core.command import Command
from loom.core.job.job import Job
from loom.core.use_case import Input


class AcmeNamed(Command, frozen=True):
    name: str


class AcmeUncompilableJob(Job[None]):
    def execute(self, first: AcmeNamed = Input(), second: AcmeNamed = Input()) -> None:
        del first, second
"""


class TestStartupRefusals:
    @pytest.mark.parametrize("refusal", list(REFUSALS))
    def test_each_refusal_reaches_the_caller_as_before(self, refusal: str, tmp_path: Path) -> None:
        spec = REFUSALS[refusal]
        caught = {
            path: paths.refusal(_PATHS, path, spec.composition, tmp_path, spec.prepare)
            for path in spec.paths
        }

        unrelated = [
            f"{path}: {error!r}"
            for path, error in caught.items()
            if not (isinstance(error, spec.refused) or isinstance(error.__cause__, spec.refused))
        ]
        assert unrelated == [], "\n".join(unrelated)
        recorded = {
            path: describe_refusal(error, tmp_path=tmp_path / path)
            for path, error in caught.items()
        }
        matches_path_snapshots(f"refusals/{refusal}", recorded, spec.variants)

    @pytest.mark.parametrize("path", ["harness", "bootstrap_app"])
    def test_an_unbound_dependency_is_accepted_by_paths_without_verification(
        self, path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        container = paths.UNCOMPOSED[path](_NEEDS_SERVICE, tmp_path, monkeypatch)

        matches_snapshot(f"accepted/unbound-dependency/{path}", describe_container(container))

    def test_standalone_kernel_refuses_an_unserved_model_at_start_up(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(BootstrapError, match="AcmePing injects main_repo"):
            paths.standalone_kernel(_NEEDS_SERVICE, tmp_path, monkeypatch)

    def test_a_job_discovered_from_modules_reaches_the_api_as_before(
        self, fixture_project: Callable[..., str], tmp_path: Path
    ) -> None:
        pytest.importorskip("fastapi")
        from loom.rest.fastapi.adapter import FastAPIAdapter

        config_path = Path(fixture_project("pipeline_modules_job"))
        config: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        package = config["app"]["packages"][0]
        with (config_path.parent / package / "__init__.py").open("a", encoding="utf-8") as written:
            written.write(_UNCOMPILABLE_JOB)
        config["app"]["job_service"] = "inline"
        config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

        with pytest.raises(CompilationError) as caught:
            Project.load(str(config_path), environment="test").compose(FastAPIAdapter())

        recorded = describe_refusal(caught.value, tmp_path=tmp_path)
        matches_snapshot("refusals/job-compilation.http", recorded)
