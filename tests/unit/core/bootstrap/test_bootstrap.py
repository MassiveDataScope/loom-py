"""Unit tests for bootstrap_app pipeline."""

from __future__ import annotations

from functools import partial
from typing import Any

import pytest

from loom.core.bootstrap.bootstrap import BootstrapError, BootstrapResult, bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase


# Minimal plain config object — no framework base class required
class _FakeConfig:
    debug: bool = False


# ---------------------------------------------------------------------------
# Dummy domain objects
# ---------------------------------------------------------------------------


class IOrderRepo:
    pass


class FakeOrderRepo(IOrderRepo):
    pass


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "noop"


class RepoDepsUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo) -> None:
        self._repo = repo

    async def execute(self, **kwargs: Any) -> str:
        return "noop"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _repo_module(container: LoomContainer) -> None:
    container.register(IOrderRepo, FakeOrderRepo, scope=Scope.REQUEST)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_bootstrap_returns_result() -> None:
    cfg = _FakeConfig()
    result = bootstrap_app(config=cfg, use_cases=[NoDepsUseCase])
    assert isinstance(result, BootstrapResult)


def test_bootstrap_result_has_container() -> None:
    result = bootstrap_app(config=_FakeConfig(), use_cases=[NoDepsUseCase])
    assert isinstance(result.container, LoomContainer)


def test_bootstrap_result_has_compiler() -> None:
    from loom.core.engine.compiler import UseCaseCompiler

    result = bootstrap_app(config=_FakeConfig(), use_cases=[NoDepsUseCase])
    assert isinstance(result.compiler, UseCaseCompiler)


def test_bootstrap_result_has_factory() -> None:
    result = bootstrap_app(config=_FakeConfig(), use_cases=[NoDepsUseCase])
    assert isinstance(result.factory, UseCaseFactory)


def test_bootstrap_registers_runtime_executor() -> None:
    result = bootstrap_app(config=_FakeConfig(), use_cases=[NoDepsUseCase])
    assert result.container.is_registered(RuntimeExecutor)
    executor = result.container.resolve(RuntimeExecutor)
    assert isinstance(executor, RuntimeExecutor)


def test_bootstrap_compiles_all_use_cases() -> None:
    result = bootstrap_app(config=_FakeConfig(), use_cases=[NoDepsUseCase])
    plan = result.compiler.get_plan(NoDepsUseCase)
    assert plan is not None


def test_bootstrap_registers_config_in_container() -> None:
    cfg = _FakeConfig()
    result = bootstrap_app(config=cfg, use_cases=[])
    resolved = result.container.resolve(_FakeConfig)
    assert resolved is cfg


def test_bootstrap_executes_modules_in_order() -> None:
    order: list[int] = []

    def mod_a(c: LoomContainer) -> None:
        order.append(1)

    def mod_b(c: LoomContainer) -> None:
        order.append(2)

    bootstrap_app(config=_FakeConfig(), use_cases=[], modules=[mod_a, mod_b])
    assert order == [1, 2]


def test_bootstrap_modules_register_bindings() -> None:
    result = bootstrap_app(
        config=_FakeConfig(),
        use_cases=[RepoDepsUseCase],
        modules=[_repo_module],
    )
    assert result.container.is_registered(IOrderRepo)


def _register_repo(container: LoomContainer, scope: Scope) -> None:
    container.register(IOrderRepo, FakeOrderRepo, scope=scope)


def test_bootstrap_accepts_a_module_without_dunder_name() -> None:
    result = bootstrap_app(
        config=_FakeConfig(),
        use_cases=[RepoDepsUseCase],
        modules=[partial(_register_repo, scope=Scope.APPLICATION)],
    )
    assert result.container.is_registered(IOrderRepo)


def test_bootstrap_factory_can_build_use_case() -> None:
    result = bootstrap_app(
        config=_FakeConfig(),
        use_cases=[RepoDepsUseCase],
        modules=[_repo_module],
    )
    uc = result.factory.build(RepoDepsUseCase)
    assert isinstance(uc, RepoDepsUseCase)


def test_bootstrap_no_use_cases() -> None:
    result = bootstrap_app(config=_FakeConfig(), use_cases=[])
    assert isinstance(result, BootstrapResult)


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_bootstrap_compilation_error_raises_bootstrap_error() -> None:
    class BrokenUseCase(UseCase[Any, str]):
        # abstract — not overriding execute
        pass

    config = _FakeConfig()
    with pytest.raises(BootstrapError):
        bootstrap_app(config=config, use_cases=[BrokenUseCase])  # type: ignore[type-abstract]


def test_bootstrap_module_exception_raises_bootstrap_error() -> None:
    def bad_module(c: LoomContainer) -> None:
        raise RuntimeError("db connection refused")

    config = _FakeConfig()
    with pytest.raises(BootstrapError, match="Bootstrap failed"):
        bootstrap_app(config=config, use_cases=[], modules=[bad_module])


def test_bootstrap_with_metrics_adapter() -> None:
    from loom.core.engine.events import RuntimeEvent

    events: list[RuntimeEvent] = []

    class RecordingMetrics:
        def on_event(self, event: RuntimeEvent) -> None:
            events.append(event)

    bootstrap_app(
        config=_FakeConfig(),
        use_cases=[NoDepsUseCase],
        metrics=RecordingMetrics(),
    )
    assert any(e.use_case_name == "NoDepsUseCase" for e in events)
