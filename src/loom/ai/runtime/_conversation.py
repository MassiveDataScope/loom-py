"""The ``conversation`` loader: one use case before a run that continues a thread.

Runs in the runtime's single path, before any engine stream opens, inside the
run's execution — through the application's invoker, exactly as the output
hook — and hands the engine a neutral :class:`~loom.ai.abc.Conversation`.  The
history is opaque bytes the loader returned: nothing here decodes, inspects
or retains it, and nothing here imports an engine or an optional extra.
"""

from __future__ import annotations

from typing import Any, Final

from loom.ai.abc import Conversation
from loom.ai.compiler._plan import CompiledConversation
from loom.ai.errors import (
    CONVERSATION_LOAD_FAILED_MESSAGE,
    CONVERSATION_LOAD_TIMEOUT_MESSAGE,
    AgentRunErrorCode,
)
from loom.ai.runtime._bounded import RunContext, bounded, failure_error
from loom.core.di import LoomContainer
from loom.core.use_case.invoker import ApplicationInvoker

_LOADER: Final[str] = "conversation loader"
"""Name of the loader step in log lines and errors."""


def loader_command(run: RunContext, accepted: frozenset[str]) -> dict[str, Any]:
    """Build the command the loader use case receives, filtered to its Input's names.

    Filtering to ``accepted`` — the Input's declared names, computed once at
    compile — lets a ``forbid_unknown_fields`` Command decode the result.

    Args:
        run: Context of the admitted run; ``conversation_id`` is set.
        accepted: Internal names the Input declares.

    Returns:
        The payload ``from_payload`` will decode.
    """
    offered: dict[str, Any] = {
        "conversation_id": run.conversation_id,
        "interaction_id": run.interaction_id,
        "subject": run.identity.subject,
        "mechanism": run.identity.mechanism,
        "agent": run.plan.name,
    }
    return {name: value for name, value in offered.items() if name in accepted}


async def _invoke_loader(
    loader: CompiledConversation, run: RunContext, container: LoomContainer
) -> object:
    """Run the loader use case through the application's invoker, as the identity acting."""
    invoker: ApplicationInvoker = container.resolve(ApplicationInvoker)
    command = loader_command(run, loader.accepted)
    return await invoker.invoke(loader.use_case, params=None, payload=command)


def _as_history(value: object, bound: int) -> bytes | None:
    """Accept ``None`` or ``bytes`` no longer than ``bound``; refuse anything else.

    Args:
        value: What the loader use case returned.
        bound: ``policies.max_history_bytes`` of the plan; ``None`` is never measured.

    Returns:
        The history the engine receives, untouched.

    Raises:
        TypeError: ``value`` is neither ``bytes`` nor ``None``.
        ValueError: ``value`` is longer than ``bound``; the message carries both sizes.
    """
    if value is None:
        return None
    if not isinstance(value, bytes):
        raise TypeError(f"{_LOADER} returned {type(value).__name__}, expected bytes or None")
    if len(value) > bound:
        raise ValueError(
            f"{_LOADER} returned {len(value)} bytes, above max_history_bytes ({bound})"
        )
    return value


async def load_conversation(run: RunContext, container: LoomContainer) -> Conversation | None:
    """Load the conversation a run continues, or ``None`` when it is single-shot.

    The loader runs only when the plan declares one and the run carries a
    ``conversation_id``; it is bounded by ``tool_timeout_ms`` and shielded
    like the hook.  A failure is mapped to a coded, detail-free error before
    the engine starts, so no model tokens are spent.

    Args:
        run: Context of the admitted run.
        container: Application container, holding the application's invoker.

    Returns:
        The conversation the engine receives, or ``None``.

    Raises:
        AgentRunError: ``CONVERSATION_LOAD_TIMEOUT`` when the loader is cut
            at ``tool_timeout_ms`` or raises a ``TimeoutError`` of its own;
            ``CONVERSATION_LOAD_FAILED`` when it raises anything else,
            returns anything but ``bytes | None``, or returns more than
            ``policies.max_history_bytes``; ``UNAUTHORIZED`` on an
            application denial.  Its ``usage`` is ``None``: nothing was spent.
    """
    loader = run.plan.conversation
    if loader is None or run.conversation_id is None:
        return None
    try:
        raw = await bounded(_invoke_loader(loader, run, container), run, what=_LOADER)
        history = _as_history(raw, run.plan.policies.max_history_bytes)
    except TimeoutError as exc:
        raise failure_error(
            exc,
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT,
            message=CONVERSATION_LOAD_TIMEOUT_MESSAGE,
            what=_LOADER,
        ) from exc
    except Exception as exc:
        raise failure_error(
            exc,
            run,
            code=AgentRunErrorCode.CONVERSATION_LOAD_FAILED,
            message=CONVERSATION_LOAD_FAILED_MESSAGE,
            what=_LOADER,
        ) from exc
    return Conversation(conversation_id=run.conversation_id, history=history)
