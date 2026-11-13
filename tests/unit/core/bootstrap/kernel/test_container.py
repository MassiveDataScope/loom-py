"""``install_kernel`` adds the kernel to a filled container; ``create_kernel`` seeds and validates.

``create_kernel`` has one mode: *config* is required.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.bootstrap import BootstrapError, create_kernel
from loom.core.bootstrap.kernel import install_kernel
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compiler import CompilationError
from loom.core.engine.executor import RuntimeExecutor
from loom.core.environment import Environment
from loom.core.use_case import Input
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


def _binds_unbuildable(container: LoomContainer) -> None:
    container.register(AcmeUnbuildable, AcmeUnbuildable, scope=Scope.APPLICATION)


def test_config_is_required() -> None:
    with pytest.raises(TypeError, match="'config'"):
        create_kernel(use_cases=[AcmePing])  # type: ignore[call-arg]


def test_it_seeds_its_own_container() -> None:
    kernel = create_kernel(config=AcmeConfig(), use_cases=[AcmePing])

    assert kernel.container.is_registered(AcmeConfig)
    assert kernel.container.is_registered(Environment)


def test_it_validates_the_container_it_built() -> None:
    with pytest.raises(BootstrapError, match="AcmeUnbuildable") as caught:
        create_kernel(config=AcmeConfig(), use_cases=[AcmePing], modules=[_binds_unbuildable])

    assert caught.value.__cause__ is not None


def test_installing_adds_the_kernel_without_seeding_or_validating() -> None:
    container = LoomContainer()
    _binds_unbuildable(container)

    kernel = install_kernel(container, use_cases=[AcmePing], logger=LOG)

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


def test_installing_constructs_one_executor(executors: list[RuntimeExecutor]) -> None:
    kernel = install_kernel(LoomContainer(), use_cases=[AcmePing], logger=LOG)

    assert executors == [kernel.executor]


def test_installing_raises_a_compilation_error_as_is() -> None:
    with pytest.raises(CompilationError, match="AcmeUncompilable.execute"):
        install_kernel(LoomContainer(), use_cases=[AcmeUncompilable], logger=LOG)
