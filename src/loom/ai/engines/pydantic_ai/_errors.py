"""Provider and engine failures → the coded run-time taxonomy (FR-028).

Classification is a lookup, not a chain of ``isinstance`` branches over
message text: the HTTP status decides the infrastructure sub-class, and the
exception type decides the rest. Only the ``INFRASTRUCTURE`` class is
retriable, and :func:`~loom.ai.errors.is_retriable` is the single authority on
that — this module never re-encodes the rule.

An :class:`~loom.ai.errors.AgentRunError` raised further down (a capability
refusing an ungranted operation, a tool timing out) already carries its code
and class, so it passes through unchanged instead of being reclassified.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from pydantic_ai import exceptions as pydantic_ai_exceptions
from pydantic_ai.exceptions import (
    CostCalculationFailedWarning,
    CostNotFoundWarning,
    ModelAPIError,
    ModelHTTPError,
    UnexpectedModelBehavior,
    UsageLimitExceeded,
)

from loom.ai.errors import AgentRunError, AgentRunErrorCode

_UNMEASURED_SPEND_WARNINGS: tuple[type[Warning], ...] = tuple(
    warning
    for warning in (
        CostNotFoundWarning,
        CostCalculationFailedWarning,
        getattr(pydantic_ai_exceptions, "UsageExtractionFailedWarning", None),
    )
    if warning is not None
)
"""pydantic-ai's bare ``Warning`` subclasses, all of them a spend the run could
not measure. ``UsageExtractionFailedWarning`` first ships in a pydantic-ai
release newer than the supported floor, so it is mapped only where it exists."""

_STATUS_CODES: Mapping[int, AgentRunErrorCode] = MappingProxyType(
    {
        401: AgentRunErrorCode.UNAUTHORIZED,
        403: AgentRunErrorCode.UNAUTHORIZED,
        408: AgentRunErrorCode.PROVIDER_UNAVAILABLE,
        429: AgentRunErrorCode.PROVIDER_RATE_LIMITED,
    }
)

_EXCEPTION_CODES: Mapping[type[Exception], AgentRunErrorCode] = MappingProxyType(
    {
        # The engine raises this one exception for every ``UsageLimits`` field:
        # the engine's own request-count default, and every ``policies`` spend
        # cap FR-040 declares, land on the same coded failure. Distinct from
        # ``MAX_ITERATIONS_EXCEEDED``, which is loom's own supervisor counting
        # tool calls over the event stream (``runtime/_limits.py``) — a
        # different counter, raised from a different place, never from here
        # (FR-046).
        UsageLimitExceeded: AgentRunErrorCode.USAGE_LIMIT_EXCEEDED,
        UnexpectedModelBehavior: AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION,
        ModelAPIError: AgentRunErrorCode.PROVIDER_UNAVAILABLE,
        TimeoutError: AgentRunErrorCode.RUN_TIMEOUT,
        # They subclass ``Warning``, not ``UserWarning``; see "Spend caps" in
        # docs/ai/artifacts.md for why that makes this mapping load-bearing
        # under ``-W error``. ``test_pydantic_ai_errors.py`` walks every
        # ``pydantic_ai._warnings`` subclass of ``Warning`` that is not a
        # ``UserWarning`` and fails the day a new one is left unmapped.
        **dict.fromkeys(_UNMEASURED_SPEND_WARNINGS, AgentRunErrorCode.COST_NOT_MEASURABLE),
    }
)


def _http_code(error: ModelHTTPError) -> AgentRunErrorCode:
    """Classify a provider HTTP failure; 5xx and the unknown are outages."""
    mapped = _STATUS_CODES.get(error.status_code)
    if mapped is not None:
        return mapped
    return AgentRunErrorCode.PROVIDER_UNAVAILABLE


def classify(error: BaseException) -> AgentRunErrorCode:
    """Return the coded classification of a failed run.

    Args:
        error: Exception raised by the engine, the provider or a capability.

    Returns:
        The run-time failure code; ``PROVIDER_UNAVAILABLE`` for an
        unrecognised failure, because an unclassified outage must stay
        retriable rather than be reported as model misbehaviour.
    """
    if isinstance(error, AgentRunError):
        return error.code
    if isinstance(error, ModelHTTPError):
        return _http_code(error)
    # Walk the error's own MRO rather than the mapping: the most specific
    # mapped ancestor wins by construction, so the day someone maps an
    # exception that is an ancestor of another — pydantic-ai's own
    # ``AgentRunError`` is the base of three entries here — the answer cannot
    # come to depend on dict insertion order. It is also cheaper: a dict
    # lookup per ancestor, against an ``isinstance`` per catalogue entry.
    for ancestor in type(error).__mro__:
        code = _EXCEPTION_CODES.get(ancestor)
        if code is not None:
            return code
    return AgentRunErrorCode.PROVIDER_UNAVAILABLE


def as_run_error(error: BaseException) -> AgentRunError:
    """Wrap a failure into the coded error the pillar's callers expect.

    Args:
        error: Exception raised by the engine, the provider or a capability.

    Returns:
        The original error when it is already an
        :class:`~loom.ai.errors.AgentRunError`, otherwise a new one carrying
        the classification and the original message.
    """
    if isinstance(error, AgentRunError):
        return error
    return AgentRunError(classify(error), f"{type(error).__name__}: {error}")
