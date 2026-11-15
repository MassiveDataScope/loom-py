"""Doubles shared by the composition unit tests."""

from __future__ import annotations

from typing import Any

from loom.core.bootstrap import KernelRuntime
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.job.job import Job
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase


class SendReceiptUseCase(UseCase[object, object]):
    """Declares no marker at all — must never be inspected for one."""

    async def execute(self) -> object:
        return None


class AcmePingJob(Job[int]):
    def execute(self) -> int:
        return 1


def kernel_runtime(*use_case_types: type[UseCase[Any, Any]]) -> KernelRuntime:
    """Build a real, minimal ``KernelRuntime`` with the given use cases compiled."""
    container = LoomContainer()
    compiler = UseCaseCompiler()
    for uc in use_case_types:
        compiler.compile(uc)
    factory = UseCaseFactory(container)
    for uc in use_case_types:
        factory.register(uc)
    executor = RuntimeExecutor(compiler)
    registry = UseCaseRegistry.build(list(use_case_types))
    app = AppInvoker(factory=factory, executor=executor, registry=registry)
    return KernelRuntime(
        container=container,
        compiler=compiler,
        factory=factory,
        executor=executor,
        registry=registry,
        app=app,
    )
