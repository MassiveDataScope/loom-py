"""Every code-keyed catalogue must cover every code.

Three catalogues map an :class:`~loom.ai.errors.AgentRunErrorCode` to
something each transport needs.  All three document themselves as total, and
"total" was a comment: two raise ``KeyError`` or degrade silently on a gap,
and the third — the HTTP status table — degrades to a 500 by design for the
codes it lists as deliberate defaults, but a code missing from *both* its
mapped entries and its declared defaults is not a deliberate choice, it is a
code nobody decided about.  These tests make the claim checkable, so CI
reports the gap instead of production.
"""

from __future__ import annotations

from loom.ai.a2a._rpc import RUN_ERROR_DETAILS
from loom.ai.errors import _RUN_ERROR_CLASSES, AgentRunErrorCode
from loom.ai.fastapi.endpoints import _STATUS_BY_CODE

# The remainder of ``AgentRunErrorCode`` deliberately falls back to the plain
# ``500`` in ``endpoints.py:_run_error_response``: these are wiring bugs (an
# unknown grant, a tool call that raised, a call cycle), client-side
# cancellation, or a deployment defect (a declared ``max_usd`` against a model
# the deployed price catalogue cannot price), none of which the HTTP contract
# names a status for. No production code reads this set — only this test does,
# to distinguish "deliberately a 500" from "nobody decided", so a code added to
# the enum without an entry in either table fails here instead of silently
# defaulting to 500.
_DEFAULT_500_CODES: frozenset[AgentRunErrorCode] = frozenset(
    {
        AgentRunErrorCode.CANCELLED,
        AgentRunErrorCode.MCP_GRANT_UNKNOWN,
        AgentRunErrorCode.SQL_GRANT_UNKNOWN,
        AgentRunErrorCode.TOOL_UNKNOWN,
        AgentRunErrorCode.TOOL_UNTYPED,
        AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED,
        AgentRunErrorCode.TOOL_DECODE_FAILED,
        AgentRunErrorCode.TOOL_CALL_FAILED,
        # Not a 503: that status is retried by default, and the interrupted
        # call may already have run (is_retriable(MCP_CALL_INTERRUPTED) is False).
        AgentRunErrorCode.MCP_CALL_INTERRUPTED,
        AgentRunErrorCode.AGENT_CALL_CYCLE,
        AgentRunErrorCode.AGENT_CALL_TOO_DEEP,
        AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK,
        # Kept out of the 503s despite its INFRASTRUCTURE class: 503 is
        # retried by default, and is_retriable(COST_NOT_MEASURABLE) is
        # False (see errors.py).
        AgentRunErrorCode.COST_NOT_MEASURABLE,
    }
)


def test_toda_clase_de_error_de_ejecucion_esta_mapeada() -> None:
    """``run_error_class`` raises ``KeyError`` for a code it does not know."""
    assert set(_RUN_ERROR_CLASSES) == set(AgentRunErrorCode)


def test_todo_detalle_a2a_esta_mapeado() -> None:
    """The A2A detail catalogue degrades silently, so a gap ships unnoticed."""
    assert set(RUN_ERROR_DETAILS) == set(AgentRunErrorCode)


def test_every_run_error_code_maps_to_a_status_or_is_declared_a_default_500() -> None:
    """Every code is either given a status or named as a deliberate default.

    A code present in neither set is not "deliberately a 500" — it is
    unmapped by omission, which is exactly the gap that let
    ``USAGE_LIMIT_EXCEEDED`` fall through to a 500 instead of the 422 every
    other ``LIMIT``-class code publishes.
    """
    assert not set(_STATUS_BY_CODE) & _DEFAULT_500_CODES
    assert set(_STATUS_BY_CODE) | _DEFAULT_500_CODES == set(AgentRunErrorCode)
