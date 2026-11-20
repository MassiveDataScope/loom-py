"""Which refusal reaches the caller when two are present at once.

Each scenario provokes two refusals that the phases raise at different
points, and the comment above it names them in order. The caller catches the
earlier one; a reordering of the phases would surface the other. Each
snapshot ``precedence/<scenario>.json`` records the refusal every path
catches as ``refusals/`` does, a declared variant in
``precedence/<scenario>.<variant>.json``.

The dependency graph is analysed after registration, ahead of the AI
phase. The ``validated-at-seal-after-<family>`` scenarios pin that the
container is validated when it is sealed, so a failing ``APPLICATION``
provider surfaces after the refusals registration and the AI phase raise.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest

from loom.ai.errors import AgentCompilationError
from loom.composition import JobTransportError
from loom.composition.errors import ReservedBindingError
from loom.core.checks import DeployCheckError
from loom.core.config import ConfigError
from loom.core.di.graph import DependencyGraphError
from loom.core.environment import Environment
from tests.unit.composition.phases import paths
from tests.unit.composition.phases.components import (
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
from tests.unit.composition.phases.snapshot import describe_refusal, matches_path_snapshots
from tests.unit.composition.phases.test_refusals import REFUSALS

_INVALID_SQL = {
    "sql": {"connections": {"main": {"url": "clickhouse://acme:8123/acme", "unknown": 1}}}
}
_UNKNOWN_BACKEND = {"app": {"job_service": "rabbit"}}
_INVALID_JOB_SECTION = {"jobs": {AcmeJob.__name__: {"unknown": 1}}}
_INVALID_CURSORS = {"stores": {"settings": {"cursors": {"signing": "unknown"}}}}
_UNCOMPOSED_LEDGER = {"stores": {"ledger": {"backend": "none"}}}


@dataclass(frozen=True)
class _Precedence:
    """A composition provoking two refusals; the caller catches *winner*, or *on_path*'s.

    The paths of *variants* record their own snapshot.
    """

    composition: paths.Composition
    winner: type[BaseException]
    on_path: Mapping[str, type[BaseException]] = field(default_factory=dict)
    variants: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


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
    # wire_ai's AgentCompilationError (an Mcp() marker) before seal's UncomposedWriteStore.
    "mcp-marker-and-uncomposed-write-store": _Precedence(
        paths.Composition(
            use_cases=(AcmePing, AcmeMcpUser),
            jobs=(AcmeJob, AcmeLedgerWriter),
            config=_UNCOMPOSED_LEDGER,
        ),
        AgentCompilationError,
    ),
    # Recipe.analysed's DependencyGraphError, wrapped in BootstrapError, before wire_ai's
    # AgentCompilationError.
    "agent-marker-and-unbound-dependency": _Precedence(
        paths.Composition(
            use_cases=(AcmePing, AcmeNeedsService, AcmeAgentUser),
            interfaces=(AcmeInterface, AcmeNeedsServiceInterface),
        ),
        DependencyGraphError,
    ),
    # configure_job_service's JobTransportError before resolve_sql's ConfigError.
    "job-backend-and-sql-section": _Precedence(
        paths.Composition(config={**_UNKNOWN_BACKEND, **_INVALID_SQL}),
        JobTransportError,
        # The HTTP adapters validate the sql section in their recipe, during plan.
        {"fastapi": ConfigError, "lambda_http": ConfigError},
        {"http": paths.HTTP},
    ),
    # A caller module's ReservedBindingError (create_kernel) before configure_job_service's
    # JobTransportError.
    "reserved-binding-and-job-backend": _Precedence(
        paths.Composition(overrides=(binds_sql_service,), config=_UNKNOWN_BACKEND),
        ReservedBindingError,
    ),
    # plan's DeployCheckError before build_stores's ConfigError (the cursor codec).
    "deploy-check-and-cursor-codec": _Precedence(
        paths.Composition(environment=Environment.PRODUCTION, config=_INVALID_CURSORS),
        DeployCheckError,
        # The HTTP paths also report the REST deploy check.
        variants={"http": paths.HTTP},
    ),
    # JobSettings.from_config's ConfigError before build_stores's (the cursor codec).
    "job-section-and-cursor-codec": _Precedence(
        paths.Composition(config={**_INVALID_JOB_SECTION, **_INVALID_CURSORS}),
        ConfigError,
    ),
}


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
"""Celery hosts no agents and refuses a marker with nothing configured."""


def _representatives(family: str) -> tuple[str, ...]:
    """The first path where *family* now precedes seal, plus Celery where it refuses otherwise."""
    paths_before_seal = [
        path for path in REFUSALS[family].paths if (family, path) not in _VALIDATED_AT_PLAN
    ]
    markers = family in ("agent-marker", "mcp-marker")
    return (paths_before_seal[0], *(_AI_MARKERS_ON_CELERY if markers else ()))


class TestRefusalPrecedence:
    @pytest.mark.parametrize("scenario", list(_SCENARIOS))
    def test_the_earlier_refusal_reaches_the_caller_as_before(
        self, scenario: str, tmp_path: Path
    ) -> None:
        spec = _SCENARIOS[scenario]
        caught = {
            path: paths.refusal(paths.COMPOSED, path, spec.composition, tmp_path)
            for path in paths.COMPOSED
        }

        unexpected = [
            f"{path}: {error!r}"
            for path, error in caught.items()
            if not isinstance(error, refused := spec.on_path.get(path, spec.winner))
            and not isinstance(error.__cause__, refused)
        ]
        assert unexpected == [], "\n".join(unexpected)
        recorded = {
            path: describe_refusal(error, tmp_path=tmp_path / path)
            for path, error in caught.items()
        }
        matches_path_snapshots(f"precedence/{scenario}", recorded, spec.variants)

    @pytest.mark.parametrize("family", _VALIDATED_AT_SEAL_AFTER)
    def test_a_failing_provider_surfaces_after_the_refusals_before_seal(
        self, family: str, tmp_path: Path
    ) -> None:
        refusal = REFUSALS[family]
        composition = replace(
            refusal.composition, overrides=(*refusal.composition.overrides, binds_unbuildable)
        )
        caught = {
            path: paths.refusal(paths.COMPOSED, path, composition, tmp_path, refusal.prepare)
            for path in _representatives(family)
        }

        unexpected = [
            f"{path}: {error!r}"
            for path, error in caught.items()
            if not isinstance(error, refusal.refused)
        ]
        assert unexpected == [], "\n".join(unexpected)
        recorded = {
            path: describe_refusal(error, tmp_path=tmp_path / path)
            for path, error in caught.items()
        }
        matches_path_snapshots(f"precedence/validated-at-seal-after-{family}", recorded)
