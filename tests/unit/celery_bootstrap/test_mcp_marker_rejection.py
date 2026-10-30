"""Worker start-up refusal of the ``Mcp()`` marker (spec 015, T202).

A Celery task worker never builds an AI runtime — no ``AgentRuntime`` is
constructed anywhere under ``loom.celery`` — so a use case declaring
``Mcp()`` must be refused at ``bootstrap_worker()`` time, not on its first
task execution. ``_reject_mcp_markers`` reuses
``loom.ai._startup.verify_mcp_markers``, wired here with no configured
servers at all, mirroring ``_reject_agent_markers``.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.ai.abc import McpHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.celery.adapter import _reject_mcp_markers
from loom.core.bootstrap import KernelRuntime
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.use_case import Mcp
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.invoker import AppInvoker
from loom.core.use_case.registry import UseCaseRegistry
from loom.core.use_case.use_case import UseCase


class LookUpKnowledgeUseCase(UseCase[object, object]):
    """Declares a server marker; unreachable from any worker in this suite."""

    async def execute(self, gateway: McpHandle = Mcp("knowledge", include=["search_*"])) -> object:
        return gateway


class SendReceiptUseCase(UseCase[object, object]):
    """Declares no marker at all — must never be inspected for one."""

    async def execute(self) -> object:
        return None


def _kernel_runtime(*use_case_types: type[UseCase[Any, Any]]) -> KernelRuntime:
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


class TestNoUseCaseDeclaresTheMarker:
    def test_no_marker_does_not_raise(self) -> None:
        kernel = _kernel_runtime(SendReceiptUseCase)

        _reject_mcp_markers((SendReceiptUseCase,), kernel)  # no raise


class TestOneUseCaseDeclaresTheMarker:
    def test_worker_startup_is_rejected_naming_use_case_and_parameter(self) -> None:
        kernel = _kernel_runtime(LookUpKnowledgeUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            _reject_mcp_markers((LookUpKnowledgeUseCase,), kernel)

        message = str(excinfo.value)
        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        assert "LookUpKnowledgeUseCase" in message
        assert "gateway" in message
        assert "knowledge" in message

    def test_no_configured_server_ever_reports_every_server_as_unknown(self) -> None:
        """No server is ever configured here (T402): every declared one is unknown."""
        kernel = _kernel_runtime(LookUpKnowledgeUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            _reject_mcp_markers((LookUpKnowledgeUseCase,), kernel)

        assert "none" in str(excinfo.value)
