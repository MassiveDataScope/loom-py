"""How each start-up refusal reaches the caller on each path, on today's code.

Each snapshot ``refusals/<refusal>/<path>.json`` holds the type and message
the caller catches, whether it is a plain ``BootstrapError`` wrapping the
refusal, and the wrapped cause. The test also checks the refusal is the one
the scenario provokes, raw or as the wrapped cause, so a recording never
holds an unrelated failure.
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.ai.errors import AgentCompilationError
from loom.composition import JobBackendError, build_application
from loom.composition.errors import ReservedBindingError
from loom.core.checks import DeployCheckError
from loom.core.config import ConfigError
from loom.core.di.container import ResolutionError
from loom.core.engine.compiler import CompilationError
from loom.core.environment import Environment
from loom.core.use_case import UnservedModelError
from loom.core.use_case.errors import JoinedAutocommitStore, UncomposedWriteStore
from loom.jobs.aws.sqs import SQSQueueConfigError
from tests.helpers.jobs import BROKER
from tests.unit.composition.pipeline import paths
from tests.unit.composition.pipeline.components import (
    AcmeAgentUser,
    AcmeBesideModel,
    AcmeInterface,
    AcmeJob,
    AcmeJoinedLedger,
    AcmeLedgerEntry,
    AcmeLedgerWriter,
    AcmeMcpUser,
    AcmeMissingStore,
    AcmeNeedsService,
    AcmeNeedsServiceInterface,
    AcmePing,
    AcmeReadsUnserved,
    AcmeRecord,
    AcmeUncompilable,
    AcmeUncompilableJob,
    binds_sql_service,
    binds_unbuildable,
    raises_at_registration,
)
from tests.unit.composition.pipeline.snapshot import (
    describe_container,
    describe_refusal,
    matches_snapshot,
)

_COMPOSED = tuple(paths.COMPOSED)
_EVERY = (*_COMPOSED, *paths.STANDALONE)
_PATHS = {**paths.COMPOSED, **paths.STANDALONE}
_LEDGER = {"stores": {"ledger": paths.SQLITE}}
_CLICKHOUSE = "clickhouse://acme:8123/acme"


@dataclass(frozen=True)
class _Refusal:
    """A composition provoking *refused* on each of *paths*."""

    composition: paths.Composition
    refused: type[BaseException]
    paths: tuple[str, ...] = _COMPOSED
    prepare: Callable[[pytest.MonkeyPatch], None] | None = None


def _without_celery_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "loom.celery.config", None)


_NEEDS_SERVICE = paths.Composition(
    use_cases=(AcmePing, AcmeNeedsService),
    interfaces=(AcmeInterface, AcmeNeedsServiceInterface),
)


REFUSALS: dict[str, _Refusal] = {  # test_precedence pins families of these too
    "reserved-binding": _Refusal(
        paths.Composition(modules=(binds_sql_service,)), ReservedBindingError
    ),
    "failing-module": _Refusal(
        paths.Composition(modules=(raises_at_registration,)), RuntimeError, _EVERY
    ),
    "failing-application-provider": _Refusal(
        paths.Composition(modules=(binds_unbuildable,)), ResolutionError, _EVERY
    ),
    "use-case-compilation": _Refusal(
        paths.Composition(use_cases=(AcmePing, AcmeUncompilable)), CompilationError, _EVERY
    ),
    "job-compilation": _Refusal(
        paths.Composition(jobs=(AcmeJob, AcmeUncompilableJob)), CompilationError
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
        paths.Composition(environment=Environment.PRODUCTION), DeployCheckError
    ),
    "job-backend": _Refusal(
        paths.Composition(config={"app": {"job_service": "rabbit"}}), JobBackendError
    ),
    "job-backend-celery-extra": _Refusal(
        paths.Composition(config={"app": {"job_service": "celery"}, "celery": BROKER}),
        JobBackendError,
        # The worker adapter module itself imports loom.celery.config, so the worker path cannot
        # run without the extra; stubbing the module only yields a test-side NameError.
        tuple(path for path in _COMPOSED if path != "celery"),
        prepare=_without_celery_extra,
    ),
    "job-backend-celery-section": _Refusal(
        paths.Composition(
            config={
                "app": {"job_service": "inline"},
                "celery": {**BROKER, "queues": ["emails"]},
            }
        ),
        JobBackendError,
    ),
    "unbound-dependency": _Refusal(_NEEDS_SERVICE, ResolutionError),
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
    ),
    "sql-invalid-section": _Refusal(
        paths.Composition(
            config={"sql": {"connections": {"main": {"url": _CLICKHOUSE, "unknown": 1}}}}
        ),
        ConfigError,
    ),
}

_CASES = [
    pytest.param(refusal, path, id=f"{refusal}-{path}")
    for refusal, spec in REFUSALS.items()
    for path in spec.paths
]


@pytest.mark.parametrize(("refusal", "path"), _CASES)
def test_each_refusal_reaches_the_caller_as_before(
    refusal: str, path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = REFUSALS[refusal]
    if spec.prepare is not None:
        spec.prepare(monkeypatch)
    try:
        _PATHS[path](spec.composition, tmp_path, monkeypatch)
    except Exception as error:  # noqa: BLE001 - the recording is the assertion
        caught = error
    else:
        pytest.fail(f"{path} composed without refusing")

    assert isinstance(caught, spec.refused) or isinstance(caught.__cause__, spec.refused)
    matches_snapshot(f"refusals/{refusal}/{path}", describe_refusal(caught, tmp_path=tmp_path))


@pytest.mark.parametrize("path", list(paths.UNCOMPOSED))
def test_an_unbound_dependency_is_accepted_where_it_was_before(
    path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    container = paths.UNCOMPOSED[path](_NEEDS_SERVICE, tmp_path, monkeypatch)

    matches_snapshot(f"accepted/unbound-dependency/{path}", describe_container(container))


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


def test_a_job_discovered_from_modules_reaches_the_api_as_before(
    fixture_project: Callable[..., str], tmp_path: Path
) -> None:
    pytest.importorskip("fastapi")
    from loom.rest.fastapi.adapter import FastAPIAdapter

    config_path = Path(fixture_project("pipeline_modules_job"))
    config: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    module = config["app"]["discovery"]["interfaces"]["modules"][0]
    with (config_path.parent / f"{module}.py").open("a", encoding="utf-8") as written:
        written.write(_UNCOMPILABLE_JOB)
    config["app"]["discovery"] = {"mode": "modules", "modules": {"include": [module]}}
    config["app"]["job_service"] = "inline"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with pytest.raises(CompilationError) as caught:
        build_application(str(config_path), adapter=FastAPIAdapter(), environment="test")

    recorded = describe_refusal(caught.value, tmp_path=tmp_path)
    matches_snapshot("refusals/job-compilation/fastapi-modules", recorded)
