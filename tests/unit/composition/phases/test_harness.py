"""``Recipe.lenient``, which the HTTP harness sets: the checks it skips (D15).

It skips the deploy checks, the use-case factory's verification, the
dependency graph's missing-binding refusal, the store refusal and the AI
marker refusal. Each is skipped only under a lenient
recipe: a strict one still refuses the same composition. A failing
``APPLICATION`` binding is refused either way.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.ai.errors import AgentCompilationError
from loom.composition import Project
from loom.core.bootstrap import BootstrapError
from loom.core.checks import DeployCheckError
from loom.core.config import ConfigContext
from loom.core.di.container import ResolutionError
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import Environment
from loom.core.use_case.errors import UncomposedWriteStore, UnservedModelError
from tests.helpers.composition import Serving, composition_project
from tests.unit.composition.phases.components import (
    AcmeAgentUser,
    AcmeLedgerWriter,
    AcmeNeedsService,
    AcmePing,
    AcmeReadsUnserved,
    AcmeRecord,
    binds_unbuildable,
)
from tests.unit.composition.phases.paths import SQLITE

_LEDGER: dict[str, Any] = {
    "app": {"name": "acme", "job_service": "inline"},
    "stores": {
        "default": SQLITE,
        "ledger": {"backend": "none"},
        "settings": {"cursors": {"signing": "ephemeral"}},
    },
}
# No store builds a cursor codec, so only a deploy check refuses the ephemeral keys;
# composed with no component, so no other lenient skip is involved.
_EPHEMERAL_KEYS: dict[str, Any] = {
    "app": {"name": "acme", "job_service": "inline"},
    "stores": {"settings": {"cursors": {"signing": "ephemeral"}}},
}
_SKIPPED: dict[str, tuple[dict[str, Any], type[Exception]]] = {
    "missing-binding": ({"use_cases": (AcmePing, AcmeNeedsService)}, BootstrapError),
    "factory-verify": ({"use_cases": (AcmePing, AcmeReadsUnserved)}, UnservedModelError),
    "unservable-store": ({"jobs": (AcmeLedgerWriter,)}, UncomposedWriteStore),
    "ai-marker": ({"use_cases": (AcmePing, AcmeAgentUser)}, AgentCompilationError),
}


class TestLenientHarness:
    @pytest.fixture
    def ledger_project(self) -> Project:
        """``acme`` over SQLite, beside a ``ledger`` store with no unit of work; jobs inline."""
        return composition_project(ConfigContext.from_dict(_LEDGER))

    @pytest.mark.parametrize("check", list(_SKIPPED))
    def test_a_strict_recipe_refuses_what_a_lenient_one_skips(
        self, check: str, ledger_project: Project
    ) -> None:
        components, refused = _SKIPPED[check]

        with pytest.raises(refused):
            ledger_project.compose(Serving(AcmeRecord, **components))

    @pytest.mark.parametrize("check", list(_SKIPPED))
    def test_a_lenient_recipe_skips_the_check(self, check: str, ledger_project: Project) -> None:
        components, _ = _SKIPPED[check]

        adapter = Serving(AcmeRecord, **components, lenient=True)

        kernel = ledger_project.compose(adapter).kernel

        assert kernel.container.resolve(RuntimeExecutor) is kernel.executor

    def test_a_lenient_recipe_still_refuses_an_unbuildable_singleton(
        self,
        ledger_project: Project,
    ) -> None:
        with pytest.raises(BootstrapError) as caught:
            ledger_project.compose(
                Serving(AcmeRecord, use_cases=(AcmePing,), lenient=True),
                modules=(binds_unbuildable,),
            )

        assert isinstance(caught.value.__cause__, ResolutionError)

    def test_a_strict_recipe_runs_the_deploy_checks(self) -> None:
        production = composition_project(
            ConfigContext.from_dict(_EPHEMERAL_KEYS), environment=Environment.PRODUCTION
        )

        with pytest.raises(DeployCheckError):
            production.compose(Serving())

    @pytest.mark.parametrize("environment", [Environment.PRODUCTION, Environment.TEST])
    def test_a_lenient_recipe_runs_no_deploy_check(
        self, environment: Environment, caplog: pytest.LogCaptureFixture
    ) -> None:
        project = composition_project(
            ConfigContext.from_dict(_EPHEMERAL_KEYS), environment=environment
        )

        with caplog.at_level(logging.DEBUG, logger="loom.core.checks"):
            project.compose(Serving(lenient=True))

        assert [r for r in caplog.records if r.name == "loom.core.checks"] == []
