"""Unit tests for shared kernel runtime bootstrap."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.bootstrap import KernelRuntime, create_kernel
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.stores.uow.coordinator import InvocationOutcome, OrderedCommits, ParticipantScope
from loom.core.use_case.use_case import UseCase
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory


class AcmeKernelConfig:
    debug: bool = False


class _NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class _FinishCounting(OrderedCommits):
    finished = 0

    async def finish(
        self, scope: ParticipantScope, error: BaseException | None
    ) -> InvocationOutcome:
        self.finished += 1
        return await super().finish(scope, error)


@pytest.fixture
def kernel() -> KernelRuntime:
    return create_kernel(config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase])


class TestCreateKernel:
    def test_create_kernel_returns_kernel_runtime(self, kernel: KernelRuntime) -> None:
        assert isinstance(kernel, KernelRuntime)
        assert isinstance(kernel.executor, RuntimeExecutor)

    def test_create_kernel_registers_executor_in_container(self, kernel: KernelRuntime) -> None:
        resolved = kernel.container.resolve(RuntimeExecutor)
        assert resolved is kernel.executor

    def test_create_kernel_executes_modules_before_validation(self) -> None:
        class _Token:
            pass

        def _module(container: LoomContainer) -> None:
            container.register(_Token, _Token, scope=Scope.APPLICATION)

        runtime = create_kernel(
            config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase], modules=[_module]
        )
        assert isinstance(runtime.container.resolve(_Token), _Token)

    async def test_create_kernel_runs_its_executor_through_the_coordinator_it_receives(
        self,
    ) -> None:
        coordinator = _FinishCounting({DEFAULT_STORE: StubUnitOfWorkFactory(Log())})

        runtime = create_kernel(
            config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase], coordinator=coordinator
        )
        await runtime.executor.execute(_NoDepsUseCase())

        assert coordinator.finished == 1

    def test_create_kernel_registers_the_environment_it_receives(self) -> None:
        runtime = create_kernel(
            config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase], environment=Environment.STAGING
        )

        assert runtime.container.resolve(Environment) is Environment.STAGING

    def test_create_kernel_without_an_environment_resolves_it(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "development")

        runtime = create_kernel(config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase])

        assert runtime.container.resolve(Environment) is Environment.DEVELOPMENT


class _Deployment:
    def __init__(self, environment: Environment) -> None:
        self.environment = environment


class TestModuleSeesTheEnvironment:
    def test_a_module_resolves_the_environment_while_it_registers(self) -> None:
        def _module(container: LoomContainer) -> None:
            deployment = _Deployment(container.resolve(Environment))
            container.register(_Deployment, lambda: deployment, scope=Scope.APPLICATION)

        runtime = create_kernel(
            config=AcmeKernelConfig(),
            use_cases=[_NoDepsUseCase],
            modules=[_module],
            environment=Environment.STAGING,
        )

        assert runtime.container.resolve(_Deployment).environment is Environment.STAGING
