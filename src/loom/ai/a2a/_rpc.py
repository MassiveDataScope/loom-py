"""JSON-RPC 2.0 envelope, error taxonomy and responses of the A2A endpoint.

The protocol layer, and nothing above it: nothing here knows what an agent is,
what a run is or how one is mounted. ``fasta2a.schema`` is the wire contract —
the envelope and the error objects, whose field names *are* the wire names, are
constructed as the ``fasta2a`` types themselves.

Nothing an external caller receives is derived from an exception's text: a
failed run answers its stable :class:`~loom.ai.errors.AgentRunErrorCode` and a
fixed detail drawn from :data:`RUN_ERROR_DETAILS`, never the failure message.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

import msgspec
from fasta2a.schema import (
    InternalError,
    InvalidParamsError,
    InvalidRequestError,
    JSONParseError,
    JSONRPCError,
    JSONRPCResponse,
    MethodNotFoundError,
    TaskNotFoundError,
    UnsupportedOperationError,
)
from starlette.responses import Response

from loom.ai.errors import AgentRunErrorCode
from loom.ai.fastapi.response import AgentJSONResponse
from loom.rest.errors import _INTERNAL_ERROR_MESSAGE

_JSONRPC_VERSION: Final = "2.0"

# Everything an external caller is ever told about a failed run, keyed by the
# stable code. Nothing here is derived from ``AgentRunError.message``: that
# text names capability keys, SQL connections, remote agent hosts and model
# bindings — exactly what the card and the stream redact (FR-030a, FR-038).
RUN_ERROR_DETAILS: Final[Mapping[AgentRunErrorCode, str]] = MappingProxyType(
    {
        AgentRunErrorCode.PROVIDER_UNAVAILABLE: "the model provider is unavailable",
        AgentRunErrorCode.PROVIDER_RATE_LIMITED: "the model provider rate limited this run",
        AgentRunErrorCode.TOOL_TIMEOUT: "a capability call exceeded its time limit",
        AgentRunErrorCode.TOOL_UNAVAILABLE: "a capability is unavailable",
        AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION: "the run produced an invalid output",
        AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED: "the run exceeded its step limit",
        AgentRunErrorCode.USAGE_LIMIT_EXCEEDED: "the run exceeded a declared spend or usage limit",
        AgentRunErrorCode.COST_NOT_MEASURABLE: (
            "the run's cost could not be measured, so a declared spend cap could not be enforced"
        ),
        AgentRunErrorCode.RUN_TIMEOUT: "the run exceeded its time limit",
        AgentRunErrorCode.TOO_MANY_RUNS: "the agent is at its concurrency limit",
        AgentRunErrorCode.UNAUTHORIZED: "the caller is not permitted to perform this run",
        AgentRunErrorCode.CANCELLED: "the run was cancelled",
        AgentRunErrorCode.HOOK_FAILED: "the output hook failed",
        AgentRunErrorCode.CONVERSATION_LOAD_FAILED: "the conversation could not be loaded",
        AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT: (
            "the conversation loader exceeded its time limit"
        ),
        # Unreachable over this surface: a stateful artefact never publishes
        # here (STATE_SURFACE_UNSUPPORTED, start-up). Present for the
        # catalogue's own totality test.
        AgentRunErrorCode.STATE_UNDECLARED: "the run declares no state, so it accepts none",
        # Unreachable over this surface for the same reason as
        # STATE_UNDECLARED above: a stateful artefact never publishes here.
        AgentRunErrorCode.STATE_REQUIRED: (
            "the run declares a required state field with no default, and no state was given"
        ),
        AgentRunErrorCode.MCP_GRANT_UNKNOWN: "the requested MCP grant is unknown",
        AgentRunErrorCode.SQL_GRANT_UNKNOWN: "the requested SQL grant is unknown",
        AgentRunErrorCode.TOOL_UNKNOWN: "the requested tool is unknown",
        AgentRunErrorCode.TOOL_UNTYPED: "the requested tool publishes no output schema",
        AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED: ("the tool returned no structured content"),
        AgentRunErrorCode.TOOL_DECODE_FAILED: "the tool result could not be decoded",
        AgentRunErrorCode.TOOL_CALL_FAILED: "the tool call failed",
        AgentRunErrorCode.MCP_CALL_INTERRUPTED: (
            "a tool call was cut off mid-flight and was not repeated"
        ),
        AgentRunErrorCode.AGENT_CALL_CYCLE: "the agent call graph contains a cycle",
        AgentRunErrorCode.AGENT_CALL_TOO_DEEP: "the agent call depth limit was exceeded",
        AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK: (
            "a per-run output shape cannot be combined with an output hook"
        ),
    }
)


class RpcEnvelope(msgspec.Struct, frozen=True, kw_only=True):
    """Permissive view of a JSON-RPC request, so a malformed one still echoes its id."""

    jsonrpc: str | None = None
    id: int | str | None = None
    method: str | None = None
    params: dict[str, Any] | None = None


_ENVELOPE_DECODER = msgspec.json.Decoder(RpcEnvelope)


class RpcFault(Exception):
    """A failure already shaped as the JSON-RPC error it answers with."""

    def __init__(self, error: JSONRPCError[Any, Any]) -> None:
        super().__init__(str(error["message"]))
        self.error = error


def rpc_response(request_id: int | str | None, result: object) -> JSONRPCResponse[Any, Any]:
    return JSONRPCResponse(jsonrpc=_JSONRPC_VERSION, id=request_id, result=result)


def _rpc_error(
    request_id: int | str | None, error: JSONRPCError[Any, Any]
) -> JSONRPCResponse[Any, Any]:
    return JSONRPCResponse(jsonrpc=_JSONRPC_VERSION, id=request_id, error=error)


def error_response(request_id: int | str | None, error: JSONRPCError[Any, Any]) -> Response:
    """Answer a JSON-RPC failure: HTTP 200 carrying the error object."""
    return AgentJSONResponse(content=_rpc_error(request_id, error))


def unsupported_error(method: str) -> UnsupportedOperationError:
    return UnsupportedOperationError(
        code=-32004,
        message="This operation is not supported",
        data={
            "method": method,
            "reason": "no task state is persisted, as the agent card advertises",
        },
    )


def method_not_found_error(method: str) -> MethodNotFoundError:
    return MethodNotFoundError(code=-32601, message="Method not found", data={"method": method})


def invalid_params_error(reason: str) -> InvalidParamsError:
    return InvalidParamsError(code=-32602, message="Invalid parameters", data={"reason": reason})


def task_not_found_error(reason: str) -> TaskNotFoundError:
    return TaskNotFoundError(code=-32001, message="Task not found", data={"reason": reason})


def _invalid_request_error(reason: str) -> InvalidRequestError:
    return InvalidRequestError(
        code=-32600, message="Request payload validation error", data={"reason": reason}
    )


def _parse_error() -> JSONParseError:
    return JSONParseError(code=-32700, message="Invalid JSON payload")


def internal_error(code: AgentRunErrorCode) -> InternalError:
    """Build the outward failure of one run: a stable code and a fixed detail."""
    return InternalError(
        code=-32603,
        message="Internal error",
        data={"code": str(code), "detail": RUN_ERROR_DETAILS.get(code, _INTERNAL_ERROR_MESSAGE)},
    )


def unexpected_error() -> InternalError:
    """Build the failure answering anything this endpoint did not anticipate."""
    return InternalError(
        code=-32603,
        message="Internal error",
        data={
            "code": str(AgentRunErrorCode.PROVIDER_UNAVAILABLE),
            "detail": _INTERNAL_ERROR_MESSAGE,
        },
    )


def decode_envelope(body: bytes) -> RpcEnvelope:
    """Decode the JSON-RPC envelope.

    Raises:
        RpcFault: ``-32700`` when the body is not JSON, ``-32600`` when it is
            JSON of an unusable shape.
    """
    try:
        return _ENVELOPE_DECODER.decode(body)
    except msgspec.ValidationError as exc:
        raise RpcFault(_invalid_request_error(str(exc))) from exc
    except msgspec.DecodeError as exc:
        raise RpcFault(_parse_error()) from exc


def require_method(envelope: RpcEnvelope) -> str:
    """Return the requested method, refusing anything that is not JSON-RPC 2.0.

    Validated after the id has been read, so the refusal still echoes it.

    Raises:
        RpcFault: ``-32600`` when the envelope is not a JSON-RPC 2.0 request.
    """
    if envelope.jsonrpc != _JSONRPC_VERSION or not envelope.method:
        raise RpcFault(_invalid_request_error("'jsonrpc' must be '2.0' and 'method' is required"))
    return envelope.method
