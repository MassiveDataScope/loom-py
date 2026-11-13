"""``create_kernel(container=...)`` adds the kernel to a filled container and validates nothing.

Without *container* it builds, seeds and validates its own, and *config* is
required; with it, *config*, *modules* and *environment* are refused.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.bootstrap import create_kernel
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
from tests.unit.core.bootstrap.kernel.components import AcmeConfig, AcmePing


class AcmeUncompilable(UseCase[Any, None]):
    async def execute(self, first: int = Input(), second: int = Input()) -> None:
        del first, second


class AcmeUnbuildable:
    """An ``APPLICATION`` service whose constructor fails."""

    def __init__(self) -> None:
        raise RuntimeError("acme service cannot start")


def _with_unbuildable() -> LoomContainer:
    container = LoomContainer()
    container.register(AcmeUnbuildable, AcmeUnbuildable, scope=Scope.APPLICATION)
    return container


def test_without_a_container_config_is_required() -> None:
    with pytest.raises(TypeError, match="'config'"):
        create_kernel(use_cases=[AcmePing])


def test_without_a_container_it_seeds_its_own() -> None:
    kernel = create_kernel(config=AcmeConfig(), use_cases=[AcmePing])

    assert kernel.container.is_registered(AcmeConfig)
    assert kernel.container.is_registered(Environment)


@pytest.mark.parametrize(
    "arguments",
    [
        {"config": AcmeConfig()},
        {"modules": [lambda container: None]},
        {"environment": Environment.TEST},
    ],
    ids=["config", "modules", "environment"],
)
def test_a_container_with_config_modules_or_environment_is_refused(
    arguments: dict[str, Any],
) -> None:
    with pytest.raises(TypeError, match=r"container=\.\.\.\) takes no config"):
        create_kernel(container=LoomContainer(), use_cases=[AcmePing], **arguments)


def test_on_a_container_it_adds_the_kernel_without_seeding_or_validating() -> None:
    container = _with_unbuildable()

    kernel = create_kernel(container=container, use_cases=[AcmePing])

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


def test_on_a_container_it_constructs_one_executor(executors: list[RuntimeExecutor]) -> None:
    kernel = create_kernel(container=LoomContainer(), use_cases=[AcmePing])

    assert executors == [kernel.executor]


def test_on_a_container_a_compilation_error_is_raised_as_is() -> None:
    with pytest.raises(CompilationError, match="AcmeUncompilable.execute"):
        create_kernel(container=LoomContainer(), use_cases=[AcmeUncompilable])
