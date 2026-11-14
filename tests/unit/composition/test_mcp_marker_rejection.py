"""Start-up refusal of the ``Mcp()`` marker without agents (spec 015, T202).

An entry point whose recipe declares ``agents=False`` (a Celery task worker,
for instance) never builds an AI runtime, so a use case declaring ``Mcp()``
must be refused when the application is composed, not on its first
execution. ``reject_ai_markers`` reuses
``loom.ai._startup.verify_mcp_markers``, wired here with no configured
servers at all, after the ``Agent()`` check.
"""

from __future__ import annotations

import pytest

from loom.ai.abc import McpHandle
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition._ai import reject_ai_markers
from loom.core.use_case import Mcp
from loom.core.use_case.use_case import UseCase
from tests.unit.composition.doubles import SendReceiptUseCase, kernel_runtime


class LookUpKnowledgeUseCase(UseCase[object, object]):
    """Declares a server marker; unreachable from any worker in this suite."""

    async def execute(self, gateway: McpHandle = Mcp("knowledge", include=["search_*"])) -> object:
        return gateway


class TestNoUseCaseDeclaresTheMarker:
    def test_no_marker_does_not_raise(self) -> None:
        kernel = kernel_runtime(SendReceiptUseCase)

        reject_ai_markers((SendReceiptUseCase,), kernel.compiler, kernel.registry)  # no raise


class TestOneUseCaseDeclaresTheMarker:
    def test_worker_startup_is_rejected_naming_use_case_and_parameter(self) -> None:
        kernel = kernel_runtime(LookUpKnowledgeUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            reject_ai_markers((LookUpKnowledgeUseCase,), kernel.compiler, kernel.registry)

        message = str(excinfo.value)
        codes = {issue.code for issue in excinfo.value.issues}
        assert codes == {AgentErrorCode.MCP_MARKER_UNKNOWN}
        assert "LookUpKnowledgeUseCase" in message
        assert "gateway" in message
        assert "knowledge" in message

    def test_no_configured_server_ever_reports_every_server_as_unknown(self) -> None:
        """No server is ever configured here (T402): every declared one is unknown."""
        kernel = kernel_runtime(LookUpKnowledgeUseCase)

        with pytest.raises(AgentCompilationError) as excinfo:
            reject_ai_markers((LookUpKnowledgeUseCase,), kernel.compiler, kernel.registry)

        assert "none" in str(excinfo.value)
