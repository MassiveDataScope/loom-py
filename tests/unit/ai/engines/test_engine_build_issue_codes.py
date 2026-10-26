"""Every failure the pydantic-ai engine raises while building carries its own code."""

from __future__ import annotations

import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.compiler._plan import CompiledUsecaseCapability
from loom.ai.engines.pydantic_ai._capabilities import _grant
from loom.ai.engines.pydantic_ai._schemas import reject_unusable_names
from loom.ai.errors import AgentCompilationError, AgentErrorCode

_AGENT = "searcher"


def _usecase(*keys: str) -> CompiledUsecaseCapability:
    return CompiledUsecaseCapability(keys=keys, use_cases=())


def _only_issue(failure: pytest.ExceptionInfo[AgentCompilationError]) -> tuple[str, str]:
    (issue,) = failure.value.issues
    return issue.code, issue.message


def test_two_grants_deriving_one_tool_name_are_a_collision() -> None:
    capabilities = (_usecase("orders.list"), _usecase("orders_list"))

    with pytest.raises(AgentCompilationError) as failure:
        reject_unusable_names(capabilities, _AGENT)

    assert _only_issue(failure) == (
        AgentErrorCode.TOOL_NAME_COLLISION,
        "searcher: grants 'orders.list' and 'orders_list' both derive the tool name "
        "'usecase_orders_list'",
    )


def test_a_tool_name_above_the_provider_bound_is_refused() -> None:
    key = "k" * 60
    capabilities = (_usecase(key),)

    with pytest.raises(AgentCompilationError) as failure:
        reject_unusable_names(capabilities, _AGENT)

    assert _only_issue(failure) == (
        AgentErrorCode.TOOL_NAME_TOO_LONG,
        f"searcher: grant '{key}' derives the tool name 'usecase_{key}' of 68 characters, "
        "above the 64-character bound providers accept",
    )


def test_a_use_case_without_an_execution_plan_is_refused() -> None:
    class _NeverCompiled:
        __execution_plan__ = None

    with pytest.raises(AgentCompilationError) as failure:
        _grant("orders.create", _NeverCompiled)  # type: ignore[arg-type]

    assert _only_issue(failure) == (
        AgentErrorCode.USECASE_NOT_COMPILED,
        "use case 'orders.create' carries no compiled execution plan; it was never compiled",
    )


def test_an_mcp_connection_the_runtime_never_opened_is_refused() -> None:
    pytest.importorskip(
        "pydantic_ai.mcp",
        reason="the MCP client is not installed: uv sync --group mcp-tests",
        exc_type=ImportError,
    )
    from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets

    capability = CompiledMcpCapability(
        server="orders", url="https://orders.example.com/mcp", timeout_ms=20000
    )

    toolsets = SharedMcpToolsets()

    with pytest.raises(AgentCompilationError) as failure:
        toolsets.for_build(capability, _AGENT)

    code, message = _only_issue(failure)
    assert code is AgentErrorCode.MCP_CONNECTION_NOT_OPENED
    assert message.startswith("searcher: the runtime never opened mcp server 'orders'")
