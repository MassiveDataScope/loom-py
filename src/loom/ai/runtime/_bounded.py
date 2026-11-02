"""Bounded, shielded execution of one use case inside an admitted run, and the
run context both the output hook and the conversation loader share.

Nothing here imports an engine or an optional extra.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, Final

from loom.ai.compiler._plan import AgentPlan
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core._async import discard_outcome
from loom.core.errors import Forbidden, Unauthenticated
from loom.core.identity import Identity
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError

_logger = logging.getLogger(__name__)

_DENIED_MESSAGE: Final[str] = "the caller is not allowed to perform this operation"
"""Fixed text of an authorization denial, the same the tool path answers."""

_DENIALS: Final[tuple[type[Exception], ...]] = (
    Forbidden,
    Unauthenticated,
    RoleNotAllowedError,
    RolesNotBoundError,
)
"""Application denials mapped to ``UNAUTHORIZED``, as the tool path maps them."""

_CANCEL_GRACE_S: Final[float] = 1.0
"""Time a bounded use case cut at its bound gets to observe its cancellation."""


@dataclass(frozen=True, slots=True)
class RunContext:
    """The run-owned context of one admitted run.

    Args:
        plan: Compiled plan being run.
        identity: Caller the run and its bounded use cases execute as.
        interaction_id: Identifier minted at admission.
        conversation_id: Opaque value the caller supplied, or ``None``.
    """

    plan: AgentPlan
    identity: Identity
    interaction_id: str
    conversation_id: str | None


async def bounded(coro: Coroutine[Any, Any, object], run: RunContext, *, what: str) -> object:
    """Await ``coro`` shielded from the consumer and bounded by ``tool_timeout_ms``.

    The coroutine runs as its own task and is only waited on, never cancelled
    by the consumer's cancellation, so a started use case finishes or fails
    cleanly even when the consumer leaves; its outcome is retrieved on every
    exit path, so nothing ends "never retrieved".  Transaction safety is not
    the shield's job: the executor exits the unit of work with the exception
    on any ``BaseException`` and the adapter rolls back.

    Args:
        coro: The use-case invocation to run.
        run: Context of the admitted run; its policies carry the bound.
        what: Name of the bounded step for log lines and errors
            (``"on_output hook"``, ``"conversation loader"``).

    Returns:
        The coroutine's return value.

    Raises:
        TimeoutError: When the task does not complete within the bound.
        asyncio.CancelledError: When the consumer was cancelled; re-raised
            once the task settled within the remaining bound.  A second
            cancellation during that wait cancels the task as well.
        RuntimeError: When the task ended cancelled on its own, without the
            consumer being cancelled: a use-case failure, not a consumer exit.
        Exception: Whatever the coroutine raised.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + run.plan.policies.tool_timeout_ms / 1000
    task = asyncio.ensure_future(coro)
    try:
        async with asyncio.timeout_at(deadline):
            # ``wait`` never cancels the task and returns once it is done,
            # cancelled included: a ``CancelledError`` here is the consumer's.
            await asyncio.wait({task})
    except asyncio.CancelledError:
        try:
            await _settle(task, max(deadline - loop.time(), 0.0), run, what)
        except asyncio.CancelledError:
            task.cancel()
            raise
        raise
    except TimeoutError:
        task.cancel()
        await _settle(task, _CANCEL_GRACE_S, run, what)
        raise
    if task.cancelled():
        raise RuntimeError(f"the {what} was cancelled internally")
    return task.result()


async def _settle(task: asyncio.Future[object], bound: float, run: RunContext, what: str) -> None:
    """Wait up to ``bound`` seconds for the task, never cancelling it; record how it ended."""
    if not task.done():
        try:
            async with asyncio.timeout(bound):
                await asyncio.wait({task})
        except TimeoutError:
            pass
    if not task.done():
        task.cancel()
        # The task runs detached from here on: retrieve a late outcome so an
        # exception raised after this point is never "never retrieved".
        task.add_done_callback(discard_outcome)
        _logger.warning(
            "%s of agent %r still running at its bound after the consumer "
            "left; cancelled (interaction %s)",
            what,
            run.plan.name,
            run.interaction_id,
        )
        return
    if task.cancelled():
        return
    if task.exception() is not None:
        _logger.error(
            "%s of agent %r failed after the consumer left (interaction %s)",
            what,
            run.plan.name,
            run.interaction_id,
            exc_info=task.exception(),
        )
        return
    _logger.info(
        "%s of agent %r completed after the consumer left (interaction %s)",
        what,
        run.plan.name,
        run.interaction_id,
    )


def failure_error(
    exc: BaseException,
    run: RunContext,
    *,
    code: AgentRunErrorCode,
    message: str,
    what: str,
) -> AgentRunError:
    """Map a bounded use case's failure to the coded error the caller receives.

    An ``AgentRunError`` keeps its own code and text and only gains the run's
    ``interaction_id``; an application denial becomes ``UNAUTHORIZED`` with
    the fixed denial text; anything else is logged server-side and answered
    with ``code`` and ``message``, never with the exception's detail.  The
    error's ``usage`` is left ``None``: the caller stamps what the run spent.

    Args:
        exc: What the bounded step raised.
        run: Context of the admitted run.
        code: Code of the generic failure branch.
        message: Fixed client text of the generic failure branch.
        what: Name of the failed step for the server-side log line.

    Returns:
        The error to raise or to turn into an ``ErrorEvent``.
    """
    if isinstance(exc, AgentRunError):
        return AgentRunError(exc.code, str(exc), interaction_id=run.interaction_id)
    if isinstance(exc, _DENIALS):
        return AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED, _DENIED_MESSAGE, interaction_id=run.interaction_id
        )
    _logger.exception(
        "%s of agent %r failed (interaction %s)", what, run.plan.name, run.interaction_id
    )
    return AgentRunError(code, message, interaction_id=run.interaction_id)

