"""Shielding one in-flight call from its own caller's cancellation.

An MCP session — wrapped in :class:`~loom.ai.runtime.SharedMcpSession` or
already declaring :class:`~loom.ai.abc.ConcurrentMcpSession` — makes the same
promise to every caller sharing it: a call already running finishes even if
the caller waiting on it is cancelled, so the session stays consistent for
whoever else is using it. :func:`shield_and_drain` is that promise, kept in
one place so neither the runtime's own lock nor a session that declares
itself already concurrency-safe has to restate it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any, TypeVar

from loom.core._async import run_to_completion

_logger = logging.getLogger(__name__)

_T = TypeVar("_T")


async def shield_and_drain(call: Coroutine[Any, Any, _T], *, label: str) -> _T:
    """Run *call* to completion even if the awaiting caller is cancelled.

    Args:
        call: The in-flight coroutine driving one round trip.
        label: Session name used in the drain's debug log.

    Returns:
        The call's own result, once it completes normally.

    Raises:
        asyncio.CancelledError: Re-raised to the caller once the call has
            finished draining underneath it; the call itself still ran to
            completion.
    """
    try:
        return await run_to_completion(call)
    except asyncio.CancelledError:
        _logger.debug("%r: drained a cancelled call", label)
        raise
