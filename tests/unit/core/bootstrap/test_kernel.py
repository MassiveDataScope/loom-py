"""Unit tests for shared kernel runtime bootstrap."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.use_case.use_case import UseCase


class _Config:
    debug: bool = False


class _NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def test_create_kernel_returns_kernel_runtime() -> None:
    runtime = create_kernel(config=_Config(), use_cases=[_NoDepsUseCase])
    assert isinstance(runtime, KernelRuntime)
    assert isinstance(runtime.executor, RuntimeExecutor)


def test_create_kernel_registers_executor_in_container() -> None:
    runtime = create_kernel(config=_Config(), use_cases=[_NoDepsUseCase])
    resolved = runtime.container.resolve(RuntimeExecutor)
    assert resolved is runtime.executor


def test_create_kernel_executes_modules_before_validation() -> None:
    class _Token:
        pass

    def _module(container: LoomContainer) -> None:
        container.register(_Token, _Token, scope=Scope.APPLICATION)

    runtime = create_kernel(config=_Config(), use_cases=[_NoDepsUseCase], modules=[_module])
    assert isinstance(runtime.container.resolve(_Token), _Token)


def test_create_kernel_uses_provided_uow_factory() -> None:
    class _DummyFactory(UnitOfWorkFactory):
        def create(self) -> Any:
            return None

    uow_factory = _DummyFactory()
    runtime = create_kernel(
        config=_Config(),
        use_cases=[_NoDepsUseCase],
        uow_factory=uow_factory,
    )
    assert runtime.executor._uow_factory is uow_factory


def test_create_kernel_registers_the_environment_it_receives() -> None:
    runtime = create_kernel(
        config=_Config(), use_cases=[_NoDepsUseCase], environment=Environment.STAGING
    )

    assert runtime.container.resolve(Environment) is Environment.STAGING


def test_create_kernel_without_an_environment_resolves_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

    runtime = create_kernel(config=_Config(), use_cases=[_NoDepsUseCase])

    assert runtime.container.resolve(Environment) is Environment.DEVELOPMENT


class _Deployment:
    def __init__(self, environment: Environment) -> None:
        self.environment = environment


def test_a_module_resolves_the_environment_while_it_registers() -> None:
    def _module(container: LoomContainer) -> None:
        deployment = _Deployment(container.resolve(Environment))
        container.register(_Deployment, lambda: deployment, scope=Scope.APPLICATION)

    runtime = create_kernel(
        config=_Config(),
        use_cases=[_NoDepsUseCase],
        modules=[_module],
        environment=Environment.STAGING,
    )

    assert runtime.container.resolve(_Deployment).environment is Environment.STAGING
