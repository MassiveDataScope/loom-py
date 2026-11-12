"""Which failed MCP call may be sent again (plan 6.5).

The mcp client makes only six JSON-RPC codes itself; any other code is an answer
the server sent. A code from the client's own set may come from a server too, so
it is never proof of an answer and is classified the same way either way.
"""

from __future__ import annotations

import httpx2
import pytest

from loom.ai.engines.pydantic_ai._failures import (
    CLIENT_MADE_CODES,
    McpCallInterrupted,
    Outcome,
    classify,
)
from loom.ai.errors import AgentRunError, AgentRunErrorCode, is_retriable

pytest.importorskip("mcp", reason="the MCP client is not installed: uv sync --group mcp-tests")

from mcp import MCPError  # noqa: E402

_SESSION_UNKNOWN = ("Session not found", "Session terminated")


def _sent_by_server(code: int, message: str) -> MCPError:
    """An error as the dispatcher builds it from a server's JSON-RPC answer, ``data`` set."""
    return MCPError(code=code, message=message, data={"from": "server"})


@pytest.mark.parametrize("code", [-32000, -32001, -32601, -32603, -32700])
@pytest.mark.parametrize("make", [MCPError, _sent_by_server])
def test_a_client_made_code_is_unsafe_whoever_sent_it(code: int, make: type[MCPError]) -> None:
    assert classify(make(code, "Connection closed")) is Outcome.UNSAFE


@pytest.mark.parametrize("make", [MCPError, _sent_by_server])
def test_an_invalid_request_is_unsafe(make: type[MCPError]) -> None:
    assert classify(make(-32600, "Unexpected content type")) is Outcome.UNSAFE


@pytest.mark.parametrize("message", _SESSION_UNKNOWN)
@pytest.mark.parametrize("make", [MCPError, _sent_by_server])
def test_a_session_the_server_does_not_know_is_safe(message: str, make: type[MCPError]) -> None:
    assert classify(make(-32600, message)) is Outcome.SAFE


@pytest.mark.parametrize("message", _SESSION_UNKNOWN)
@pytest.mark.parametrize("code", [-32000, -32603])
def test_the_session_messages_are_safe_only_under_invalid_request(code: int, message: str) -> None:
    assert classify(MCPError(code, message)) is Outcome.UNSAFE


def test_a_code_outside_the_client_set_is_an_answer_the_server_sent() -> None:
    assert classify(_sent_by_server(-32043, "middleware-sent JSON-RPC error")) is Outcome.COMPLETED


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionError("Connection closed"),
        RuntimeError("Client is not connected"),
        httpx2.ReadError("stale keep-alive"),
        ExceptionGroup("transport", [MCPError(-32043, "inside a group")]),
        ValueError("unknown"),
    ],
    ids=["connection", "runtime", "httpx", "group", "unknown"],
)
def test_anything_but_an_mcp_error_is_unsafe(exc: Exception) -> None:
    assert classify(exc) is Outcome.UNSAFE


def test_the_client_made_codes_are_pinned() -> None:
    """An upgrade of mcp reruns this: a new client-made code must be added on purpose."""
    assert frozenset({-32000, -32001, -32600, -32601, -32603, -32700}) == CLIENT_MADE_CODES
    assert isinstance(CLIENT_MADE_CODES, frozenset)


def test_an_interrupted_call_is_a_run_error_nobody_retries() -> None:
    interrupted = McpCallInterrupted("write_orders")

    assert isinstance(interrupted, AgentRunError)
    assert interrupted.code is AgentRunErrorCode.MCP_CALL_INTERRUPTED
    assert not is_retriable(AgentRunErrorCode.MCP_CALL_INTERRUPTED)
    assert "write_orders" in str(interrupted)


def _wrapped(cause: BaseException | None) -> Exception:
    """What pydantic-ai's ``MCPToolset`` raises: its own ``ModelRetry`` over the client's error."""
    from pydantic_ai.exceptions import ModelRetry

    retry = ModelRetry("the tool call failed")
    retry.__cause__ = cause
    return retry


@pytest.mark.parametrize(
    ("cause", "outcome"),
    [
        (MCPError(-32000, "Connection closed"), Outcome.UNSAFE),
        (MCPError(-32600, "Session terminated"), Outcome.SAFE),
        (_sent_by_server(-32043, "middleware-sent JSON-RPC error"), Outcome.COMPLETED),
        (None, Outcome.COMPLETED),
    ],
    ids=["cut", "forgotten", "answered", "the-tools-own-error"],
)
def test_a_model_retry_is_judged_by_what_caused_it(
    cause: BaseException | None, outcome: Outcome
) -> None:
    assert classify(_wrapped(cause)) is outcome
