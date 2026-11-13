"""Which refusal reaches the caller when two are present at once.

Each scenario provokes two refusals that the phases raise at different
points, and the comment above it names them in order. The caller catches the
earlier one; a reordering of the phases would surface the other. Each
snapshot ``precedence/<scenario>/<path>.json`` records the caught refusal as
``refusals/`` does.

The scenarios of ``_SCENARIOS`` keep the order of the code before the
pipeline, but ``agent-marker-and-unbound-dependency``: since T04 the
dependency graph is analysed at the end of registration, ahead of the AI
phase. The ``validated-at-seal-after-<family>`` ones pin an intended T01d
change: the container is validated when it is sealed, so a failing
``APPLICATION`` provider now surfaces after the refusals registration and the
AI phase raise, where before T01d it surfaced ahead of them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest

from loom.ai.errors import AgentCompilationError
from loom.composition import JobBackendError
from loom.composition.errors import ReservedBindingError
from loom.core.checks import DeployCheckError
from loom.core.config import ConfigError
from loom.core.di.graph import DependencyGraphError
from loom.core.environment import Environment
from tests.unit.composition.pipeline import paths
from tests.unit.composition.pipeline.components import (
    AcmeAgentUser,
    AcmeInterface,
    AcmeJob,
    AcmeLedgerWriter,
    AcmeMcpUser,
    AcmeNeedsService,
    AcmeNeedsServiceInterface,
    AcmePing,
    binds_sql_service,
    binds_unbuildable,
)
from tests.unit.composition.pipeline.snapshot import describe_refusal, matches_snapshot
from tests.unit.composition.pipeline.test_refusals import REFUSALS

_INVALID_SQL = {
    "sql": {"connections": {"main": {"url": "clickhouse://acme:8123/acme", "unknown": 1}}}
}
_UNKNOWN_BACKEND = {"app": {"job_service": "rabbit"}}
_INVALID_JOB_SECTION = {"jobs": {AcmeJob.__name__: {"unknown": 1}}}
_INVALID_CURSORS = {"persistence": {"cursors": {"signing": "unknown"}}}
_UNCOMPOSED_LEDGER = {"stores": {"ledger": {"backend": "none"}}}


@dataclass(frozen=True)
class _Precedence:
    """A composition provoking two refusals; the caller catches *winner*, or *on_path*'s."""

    composition: paths.Composition
    winner: type[BaseException]
    on_path: Mapping[str, type[BaseException]] = field(default_factory=dict)


_SCENARIOS: dict[str, _Precedence] = {
    # wire_ai's AgentCompilationError before seal's UncomposedWriteStore.
    "agent-marker-and-uncomposed-write-store": _Precedence(
        paths.Composition(
            use_cases=(AcmePing, AcmeAgentUser),
            jobs=(AcmeJob, AcmeLedgerWriter),
            config=_UNCOMPOSED_LEDGER,
        ),
        AgentCompilationError,
    ),
    # wire_ai's AgentCompilationError (verify_mcp_markers) before seal's UncomposedWriteStore.
    "mcp-marker-and-uncomposed-write-store": _Precedence(
        paths.Composition(
            use_cases=(AcmePing, AcmeMcpUser),
            jobs=(AcmeJob, AcmeLedgerWriter),
            config=_UNCOMPOSED_LEDGER,
        ),
        AgentCompilationError,
    ),
    # T04 change: registration's graph analysis (refusal 1, a DependencyGraphError wrapped
    # in BootstrapError) before wire_ai's AgentCompilationError. Before T04 the unbound
    # dependency surfaced in seal, through factory.verify, after the AI phase.
    "agent-marker-and-unbound-dependency": _Precedence(
        paths.Composition(
            use_cases=(AcmePing, AcmeNeedsService, AcmeAgentUser),
            interfaces=(AcmeInterface, AcmeNeedsServiceInterface),
        ),
        DependencyGraphError,
    ),
    # configure_job_service's JobBackendError before resolve_sql's ConfigError.
    "job-backend-and-sql-section": _Precedence(
        paths.Composition(config={**_UNKNOWN_BACKEND, **_INVALID_SQL}),
        JobBackendError,
        # The HTTP adapters validate the sql section in their recipe, during plan.
        {"fastapi": ConfigError, "lambda_http": ConfigError},
    ),
    # A caller module's ReservedBindingError (create_kernel) before configure_job_service's
    # JobBackendError.
    "reserved-binding-and-job-backend": _Precedence(
        paths.Composition(modules=(binds_sql_service,), config=_UNKNOWN_BACKEND),
        ReservedBindingError,
    ),
    # plan's DeployCheckError before persist's ConfigError (the cursor codec).
    "deploy-check-and-cursor-codec": _Precedence(
        paths.Composition(environment=Environment.PRODUCTION, config=_INVALID_CURSORS),
        DeployCheckError,
    ),
    # JobSettings.from_config's ConfigError before persist's (the cursor codec).
    "job-section-and-cursor-codec": _Precedence(
        paths.Composition(config={**_INVALID_JOB_SECTION, **_INVALID_CURSORS}),
        ConfigError,
    ),
}

_CASES = [
    pytest.param(scenario, path, id=f"{scenario}-{path}")
    for scenario in _SCENARIOS
    for path in paths.COMPOSED
]


@pytest.mark.parametrize(("scenario", "path"), _CASES)
def test_the_earlier_refusal_reaches_the_caller_as_before(
    scenario: str, path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _SCENARIOS[scenario]
    try:
        paths.COMPOSED[path](spec.composition, tmp_path, monkeypatch)
    except Exception as error:  # noqa: BLE001 - the recording is the assertion
        caught = error
    else:
        pytest.fail(f"{path} composed without refusing")

    refused = spec.on_path.get(path, spec.winner)
    assert isinstance(caught, refused) or isinstance(caught.__cause__, refused)
    recorded = describe_refusal(caught, tmp_path=tmp_path)
    matches_snapshot(f"precedence/{scenario}/{path}", recorded)


_VALIDATED_AT_SEAL_AFTER = (
    "job-backend",
    "job-backend-celery-extra",
    "job-backend-celery-section",
    "sqs-queue",
    "sql-invalid-section",
    "agent-marker",
    "mcp-marker",
)
"""Refusal families raised before seal on some path, each now ahead of a failing provider."""

_VALIDATED_AT_PLAN = frozenset(
    {("sql-invalid-section", "fastapi"), ("sql-invalid-section", "lambda_http")}
)
"""The HTTP adapters validate the sql section in their recipe, during plan: unchanged."""

_AI_MARKERS_ON_CELERY = ("celery",)
"""Celery refuses a marker in ``reject_ai_markers``, the other paths in ``verify_*``."""


def _representatives(family: str) -> tuple[str, ...]:
    """The first path where *family* now precedes seal, plus Celery where it refuses otherwise."""
    paths_before_seal = [
        path for path in REFUSALS[family].paths if (family, path) not in _VALIDATED_AT_PLAN
    ]
    markers = family in ("agent-marker", "mcp-marker")
    return (paths_before_seal[0], *(_AI_MARKERS_ON_CELERY if markers else ()))


_SEAL_CASES = [
    pytest.param(family, path, id=f"{family}-{path}")
    for family in _VALIDATED_AT_SEAL_AFTER
    for path in _representatives(family)
]


@pytest.mark.parametrize(("family", "path"), _SEAL_CASES)
def test_a_failing_provider_surfaces_after_the_refusals_before_seal(
    family: str, path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refusal = REFUSALS[family]
    if refusal.prepare is not None:
        refusal.prepare(monkeypatch)
    composition = replace(
        refusal.composition, modules=(*refusal.composition.modules, binds_unbuildable)
    )
    with pytest.raises(refusal.refused) as caught:
        paths.COMPOSED[path](composition, tmp_path, monkeypatch)

    recorded = describe_refusal(caught.value, tmp_path=tmp_path)
    matches_snapshot(f"precedence/validated-at-seal-after-{family}/{path}", recorded)
