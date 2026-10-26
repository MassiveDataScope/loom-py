"""Limits phase: every policy value must sit inside its published range."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from loom.ai.declarative import PolicySpec
from loom.ai.declarative._v1 import (
    MAX_HISTORY_BYTES_MAX,
    MAX_HISTORY_BYTES_MIN,
    MAX_INPUT_TOKENS_PER_REQUEST_MAX,
    MAX_INPUT_TOKENS_PER_REQUEST_MIN,
    MAX_ITERATIONS_MAX,
    MAX_ITERATIONS_MIN,
    MAX_REQUESTS_MAX,
    MAX_REQUESTS_MIN,
    MAX_TOOL_CALLS_MAX,
    MAX_TOOL_CALLS_MIN,
    MAX_TOTAL_TOKENS_MAX,
    MAX_TOTAL_TOKENS_MIN,
    MAX_USD_MAX,
    MAX_USD_MIN,
    RETRIES_MAX,
    RETRIES_MIN,
    RUN_TIMEOUT_MS_MAX,
    RUN_TIMEOUT_MS_MIN,
    TOOL_TIMEOUT_MS_MAX,
    TOOL_TIMEOUT_MS_MIN,
)
from loom.ai.errors import AgentCompilationIssue, AgentErrorCode

# Every policy field whose value is an ``int``. ``max_usd`` is deliberately
# absent: it is a ``Decimal`` on ``PolicySpec`` and gets its own check below
# instead of widening this table's value type for one entry out of nine.
_POLICY_RANGES: Final[Mapping[str, tuple[int, int]]] = MappingProxyType(
    {
        "retries": (RETRIES_MIN, RETRIES_MAX),
        "tool_timeout_ms": (TOOL_TIMEOUT_MS_MIN, TOOL_TIMEOUT_MS_MAX),
        "max_iterations": (MAX_ITERATIONS_MIN, MAX_ITERATIONS_MAX),
        "run_timeout_ms": (RUN_TIMEOUT_MS_MIN, RUN_TIMEOUT_MS_MAX),
        "max_history_bytes": (MAX_HISTORY_BYTES_MIN, MAX_HISTORY_BYTES_MAX),
        "max_total_tokens": (MAX_TOTAL_TOKENS_MIN, MAX_TOTAL_TOKENS_MAX),
        "max_input_tokens_per_request": (
            MAX_INPUT_TOKENS_PER_REQUEST_MIN,
            MAX_INPUT_TOKENS_PER_REQUEST_MAX,
        ),
        "max_tool_calls": (MAX_TOOL_CALLS_MIN, MAX_TOOL_CALLS_MAX),
        "max_requests": (MAX_REQUESTS_MIN, MAX_REQUESTS_MAX),
    }
)


def validate_policies(policies: PolicySpec, component: str) -> list[AgentCompilationIssue]:
    """Check every policy value against its published inclusive range.

    An absent optional cap (``None``) is never out of range: FR-042 documents
    ``None`` as "disable that limit", so the guard clause below skips it
    rather than reporting a fault.

    Args:
        policies: Declared execution limits.
        component: Artifact path or agent name the issues point at.

    Returns:
        One ``POLICY_OUT_OF_RANGE`` issue per value outside its range.
    """
    issues: list[AgentCompilationIssue] = []
    for name, (minimum, maximum) in _POLICY_RANGES.items():
        value: int | None = getattr(policies, name)
        if value is None:
            continue
        if value < minimum or value > maximum:
            issues.append(
                AgentErrorCode.POLICY_OUT_OF_RANGE.issue(
                    component=component, policy=name, value=value, minimum=minimum, maximum=maximum
                )
            )
    if policies.max_usd is not None and not MAX_USD_MIN <= policies.max_usd <= MAX_USD_MAX:
        issues.append(
            AgentErrorCode.POLICY_OUT_OF_RANGE.issue(
                component=component,
                policy="max_usd",
                value=policies.max_usd,
                minimum=MAX_USD_MIN,
                maximum=MAX_USD_MAX,
            )
        )
    return issues
