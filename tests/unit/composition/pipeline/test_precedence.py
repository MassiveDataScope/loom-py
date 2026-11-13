"""Which refusal reaches the caller when two are present at once, on today's code.

Each scenario provokes two refusals that today's phases raise at different
points, and the comment above it names them in today's order. The caller
catches the earlier one; a reordering of the phases would surface the
other. Each snapshot ``precedence/<scenario>/<path>.json`` records the
caught refusal as ``refusals/`` does.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from loom.ai.errors import AgentCompilationError
from loom.composition import JobBackendError
from loom.composition.errors import ReservedBindingError
from loom.core.checks import DeployCheckError
from loom.core.config import ConfigError
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
)
from tests.unit.composition.pipeline.snapshot import describe_refusal, matches_snapshot

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
    # wire_ai's AgentCompilationError before factory.verify's ResolutionError.
    "agent-marker-and-unbound-dependency": _Precedence(
        paths.Composition(
            use_cases=(AcmePing, AcmeNeedsService, AcmeAgentUser),
            interfaces=(AcmeInterface, AcmeNeedsServiceInterface),
        ),
        AgentCompilationError,
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
