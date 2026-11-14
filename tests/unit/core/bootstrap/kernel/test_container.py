"""``KernelRecipe.install`` adds the kernel to a filled container; ``create_kernel`` validates it.

``create_kernel`` has one mode: *config* is required.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.bootstrap import BootstrapError, create_kernel
from loom.core.bootstrap.kernel import KernelRecipe
from loom.core.di.container import LoomContainer
from loom.core.di.graph import KeyNode
from loom.core.di.scope import Scope
from loom.core.engine.compiler import CompilationError
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import Environment
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.use_case import Caller, Input
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase
from tests.unit.core.bootstrap.kernel.components import LOG, AcmeConfig, AcmePing


class AcmeUncompilable(UseCase[Any, None]):
    async def execute(self, first: int = Input(), second: int = Input()) -> None:
        del first, second


class AcmeUnbuildable:
    """An ``APPLICATION`` service whose constructor fails."""

    def __init__(self) -> None:
        raise RuntimeError("acme service cannot start")


class AcmeCallerHolder:
    def __init__(self, caller: Identity = Caller()) -> None:
        self.caller = caller


def _reads_the_holder_early(container: LoomContainer) -> None:
    container.register(AcmeCallerHolder, AcmeCallerHolder)
    container.graph.constructor(KeyNode(AcmeCallerHolder))


def _binds_unbuildable(container: LoomContainer) -> None:
    container.register(AcmeUnbuildable, AcmeUnbuildable, scope=Scope.APPLICATION)


class TestKernelContainer:
    def test_config_is_required(self) -> None:
        with pytest.raises(TypeError, match="'config'"):
            create_kernel(use_cases=[AcmePing])  # type: ignore[call-arg]

    def test_it_seeds_its_own_container(self) -> None:
        kernel = create_kernel(config=AcmeConfig(), use_cases=[AcmePing])

        assert kernel.container.is_registered(AcmeConfig)
        assert kernel.container.is_registered(Environment)

    def test_it_validates_the_container_it_built(self) -> None:
        with pytest.raises(BootstrapError, match="AcmeUnbuildable") as caught:
            create_kernel(config=AcmeConfig(), use_cases=[AcmePing], modules=[_binds_unbuildable])

        assert caught.value.__cause__ is not None

    @pytest.mark.parametrize(
        "key",
        [UseCaseFactory, RuntimeExecutor, UseCaseRegistry, ApplicationInvoker],
        ids=lambda key: key.__name__,
    )
    def test_a_module_binding_a_kernel_key_is_refused(self, key: type[Any]) -> None:
        def binds_kernel_key(container: LoomContainer) -> None:
            container.register_instance(key, object())

        expected = f"binds_kernel_key binds {key.__qualname__}, a key the kernel binds itself$"
        with pytest.raises(BootstrapError, match=expected) as caught:
            create_kernel(config=AcmeConfig(), use_cases=[AcmePing], modules=[binds_kernel_key])

        assert caught.value.__cause__ is None

    def test_a_kind_a_module_reads_before_the_kernel_is_installed_still_resolves(self) -> None:
        kernel = create_kernel(
            config=AcmeConfig(), use_cases=[AcmePing], modules=[_reads_the_holder_early]
        )
        ada = Identity(subject="acme-ada", mechanism="test")

        with ExecutionScope(ada):
            holder = kernel.container.resolve(AcmeCallerHolder)

        assert holder.caller is ada

    def test_installing_adds_the_kernel_without_seeding_or_validating(self) -> None:
        container = KernelRecipe.container()
        _binds_unbuildable(container)

        kernel = KernelRecipe([AcmePing], LOG).install(container)

        assert kernel.container is container
        assert set(container.bindings()) == {
            AcmeUnbuildable,
            UseCaseFactory,
            RuntimeExecutor,
            UseCaseRegistry,
            ApplicationInvoker,
        }
        assert container.resolve(RuntimeExecutor) is kernel.executor
        assert kernel.compiler.get_plan(AcmePing) is not None

    def test_installing_constructs_one_executor(self, executors: list[RuntimeExecutor]) -> None:
        kernel = KernelRecipe([AcmePing], LOG).install(KernelRecipe.container())

        assert executors == [kernel.executor]

    def test_installing_raises_a_compilation_error_as_is(self) -> None:
        with pytest.raises(CompilationError, match="AcmeUncompilable.execute"):
            KernelRecipe([AcmeUncompilable], LOG).install(KernelRecipe.container())
