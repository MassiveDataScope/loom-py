"""Unit tests for shared kernel runtime bootstrap."""

from __future__ import annotations

from functools import partial
from typing import Any

import pytest

from loom.core.bootstrap import BootstrapError, Kernel, create_kernel
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import ExecutionEvent
from loom.core.engine.executor import UseCaseExecutor
from loom.core.environment import ENVIRONMENT_VARIABLE, Environment
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import InvocationOutcome, OrderedCommits, ParticipantScope
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from tests.unit.core.doubles import FakeOrderRepo, IOrderRepo
from tests.unit.core.engine._lifecycle_doubles import Log, StubUnitOfWorkFactory


class AcmeKernelConfig:
    debug: bool = False


class _NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class _RepoDepsUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo) -> None:
        self._repo = repo

    async def execute(self, **kwargs: Any) -> str:
        return "noop"


def _register_repo(container: LoomContainer, scope: Scope = Scope.REQUEST) -> None:
    container.register(IOrderRepo, FakeOrderRepo, scope=scope)


class _FinishCounting(OrderedCommits):
    finished = 0

    async def finish(
        self, scope: ParticipantScope, error: BaseException | None
    ) -> InvocationOutcome:
        self.finished += 1
        return await super().finish(scope, error)


@pytest.fixture
def kernel() -> Kernel:
    return create_kernel(config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase])


class TestCreateKernel:
    def test_create_kernel_returns_kernel(self, kernel: Kernel) -> None:
        assert isinstance(kernel, Kernel)
        assert isinstance(kernel.executor, UseCaseExecutor)

    def test_create_kernel_carries_container_compiler_and_factory(self, kernel: Kernel) -> None:
        assert isinstance(kernel.container, LoomContainer)
        assert isinstance(kernel.compiler, UseCaseCompiler)
        assert isinstance(kernel.factory, UseCaseFactory)

    def test_create_kernel_compiles_all_use_cases(self, kernel: Kernel) -> None:
        assert kernel.compiler.get_plan(_NoDepsUseCase) is not None

    def test_create_kernel_registers_config_in_container(self) -> None:
        config = AcmeKernelConfig()

        built = create_kernel(config=config, use_cases=[])

        assert built.container.resolve(AcmeKernelConfig) is config

    def test_create_kernel_executes_modules_in_order(self) -> None:
        order: list[int] = []

        def first(container: LoomContainer) -> None:
            order.append(1)

        def second(container: LoomContainer) -> None:
            order.append(2)

        create_kernel(config=AcmeKernelConfig(), use_cases=[], modules=[first, second])

        assert order == [1, 2]

    def test_create_kernel_modules_register_bindings(self) -> None:
        built = create_kernel(
            config=AcmeKernelConfig(), use_cases=[_RepoDepsUseCase], modules=[_register_repo]
        )

        assert built.container.is_registered(IOrderRepo)

    def test_create_kernel_accepts_a_module_without_dunder_name(self) -> None:
        built = create_kernel(
            config=AcmeKernelConfig(),
            use_cases=[_RepoDepsUseCase],
            modules=[partial(_register_repo, scope=Scope.APPLICATION)],
        )

        assert built.container.is_registered(IOrderRepo)

    def test_create_kernel_factory_can_build_use_case(self) -> None:
        built = create_kernel(
            config=AcmeKernelConfig(), use_cases=[_RepoDepsUseCase], modules=[_register_repo]
        )

        assert isinstance(built.factory.build(_RepoDepsUseCase), _RepoDepsUseCase)

    def test_create_kernel_reports_compilation_to_the_metrics_recorder(self) -> None:
        events: list[ExecutionEvent] = []

        class RecordingMetrics:
            def on_event(self, event: ExecutionEvent) -> None:
                events.append(event)

        create_kernel(
            config=AcmeKernelConfig(), use_cases=[_NoDepsUseCase], metrics=RecordingMetrics()
        )

        assert any(event.use_case_name == "_NoDepsUseCase" for event in events)

    def test_create_kernel_registers_executor_in_container(self, kernel: Kernel) -> None:
        resolved = kernel.container.resolve(UseCaseExecutor)
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


class TestCreateKernelFailures:
    def test_a_compilation_error_raises_bootstrap_error(self) -> None:
        class BrokenUseCase(UseCase[Any, str]):
            pass

        with pytest.raises(BootstrapError):
            create_kernel(config=AcmeKernelConfig(), use_cases=[BrokenUseCase])  # type: ignore[type-abstract]

    def test_a_module_exception_raises_bootstrap_error(self) -> None:
        def bad_module(container: LoomContainer) -> None:
            raise RuntimeError("db connection refused")

        with pytest.raises(BootstrapError, match="Bootstrap failed"):
            create_kernel(config=AcmeKernelConfig(), use_cases=[], modules=[bad_module])


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
