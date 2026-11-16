"""The registration phase binds in one fixed order and compiles each use case once.

The order: the seed keys, the caller's modules, the recipe's overrides, each
store's repositories, the framework's keys. The use cases compile inside the
kernel's construction, among the framework's keys, never in a second pass;
a job compiles at seal, as before.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from loom.composition import AppConfig, Application
from loom.core.cache.calls import CachedCalls
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import Environment
from loom.core.job.service import JobService
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import SqlQueryService
from loom.core.stores.catalog import StoreCatalog
from loom.core.stores.repository.capabilities import Creatable
from loom.core.stores.repository.registry import RepositoryToken
from loom.core.use_case.factory import UseCaseFactory
from tests.unit.composition.phases import paths
from tests.unit.composition.phases.components import (
    FAKE_JOBS,
    FAKE_RECORDS,
    AcmeJob,
    AcmePing,
    AcmeRecord,
    overrides_repository_and_jobs,
)

Event = tuple[str, object]


class AcmeModuleService:
    """Bound by the caller's module."""


_STEPS: dict[Event, str] = {
    ("bind", AppConfig): "seed",
    ("bind", Environment): "seed",
    ("bind", ObservabilityRuntime): "seed",
    ("bind", StoreCatalog): "seed",
    ("bind", AcmeModuleService): "module",
    ("bind", RepositoryToken(AcmeRecord)): "override",
    ("bind", Creatable[AcmeRecord]): "repositories",
    ("bind", CachedCalls): "framework",
    ("compile", AcmePing): "compile",
    ("bind", UseCaseFactory): "framework",
    ("bind", RuntimeExecutor): "framework",
    ("bind", SqlQueryService): "framework",
    ("compile", AcmeJob): "jobs",
}


def _binds_module_service(container: LoomContainer) -> None:
    container.register(AcmeModuleService, AcmeModuleService, scope=Scope.APPLICATION)


def _steps(recorded: list[Event]) -> list[str]:
    """The step ``_STEPS`` names for each recorded event, consecutive repeats collapsed."""
    steps: list[str] = []
    for event in recorded:
        step = _STEPS.get(event)
        if step is not None and (not steps or steps[-1] != step):
            steps.append(step)
    return steps


class TestRegistrationOrder:
    @pytest.fixture
    def events(self, monkeypatch: pytest.MonkeyPatch) -> list[Event]:
        """Each binding a container makes and each compilation, in order."""
        recorded: list[Event] = []
        bind = LoomContainer._bind  # noqa: SLF001 - every register* binds through it
        build_plan = UseCaseCompiler._build_plan  # noqa: SLF001

        def binding(
            self: LoomContainer,
            interface: BindingKey,
            provider: Callable[[], Any] | type[Any],
            scope: Scope,
        ) -> None:
            recorded.append(("bind", interface))
            bind(self, interface, provider, scope)

        def compiling(self: UseCaseCompiler, use_case_type: type[Any]) -> Any:
            recorded.append(("compile", use_case_type))
            return build_plan(self, use_case_type)

        monkeypatch.setattr(LoomContainer, "_bind", binding)
        monkeypatch.setattr(UseCaseCompiler, "_build_plan", compiling)
        return recorded

    def test_registration_binds_in_the_fixed_order(
        self, overridden: Callable[..., Application], events: list[Event]
    ) -> None:
        container = overridden(
            overrides_repository_and_jobs, modules=(_binds_module_service,)
        ).kernel.container

        assert _steps(events) == [
            "seed",
            "module",
            "override",
            "repositories",
            "framework",
            "compile",
            "framework",
            "jobs",
        ]
        # Neither the repository module nor the job service default replaced the override.
        assert container.resolve_repo(AcmeRecord) is FAKE_RECORDS
        assert container.resolve(JobService) is FAKE_JOBS
        assert events.count(("bind", RepositoryToken(AcmeRecord))) == 1
        assert events.count(("bind", JobService)) == 1

    @pytest.mark.parametrize("name", list(paths.COMPOSED))
    def test_each_use_case_and_job_compiles_once(
        self, name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, events: list[Event]
    ) -> None:
        paths.COMPOSED[name](paths.Composition(), tmp_path, monkeypatch)

        compiled = Counter(subject for kind, subject in events if kind == "compile")
        assert compiled[AcmePing] == 1
        assert set(compiled.values()) == {1}
