"""The ``on_output`` hook: one use case per completed run, engine-neutral.

Sits outside the limit supervisor and outside every engine: when the
supervised stream produces its terminal event, the wrapper here runs the
plan's hook use case through the application's invoker, inside the run's
execution, so ``Caller()`` is the identity acting there — and re-creates the
terminal event with the run's ``interaction_id`` and the hook's result.  An
engine never sees the hook, so its own retry loop cannot replay it.

Nothing here imports an engine or an optional extra.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any, Final

import msgspec

from loom.ai.abc import AgentEvent, ErrorEvent, FinalEvent
from loom.ai.compiler._plan import HOOK_MESSAGES_FIELD, HOOK_OUTPUT_FIELD, CompiledOutputHook
from loom.ai.errors import AgentRunErrorCode
from loom.ai.runtime._bounded import RunContext, bounded, failure_error
from loom.core.di import LoomContainer
from loom.core.use_case.invoker import ApplicationInvoker

HOOK_FAILED_MESSAGE: Final[str] = "the output hook failed; the detail is recorded server-side"
"""Fixed text of a ``HOOK_FAILED`` error: the exception never reaches the caller."""

_HOOK: Final[str] = "on_output hook"
"""Name of the hook step in log lines and errors."""


def no_terminal_message(agent: str) -> str:
    """Return the text of the error closing a stream that ended without a terminal event.

    Args:
        agent: Name of the agent whose engine exhausted its stream silently.

    Returns:
        The ``PROVIDER_UNAVAILABLE`` message the stream and ``run()`` share.
    """
    return f"agent {agent!r} produced no terminal event"


def hook_command(
    output: object,
    run: RunContext,
    accepted: frozenset[str],
    *,
    messages: bytes | None = None,
) -> dict[str, Any]:
    """Build the command the hook use case receives, filtered to its Input's names.

    The validated output is nested under ``output``, the run's new messages
    under ``messages``, and the run context is offered beside them; nothing
    from the output can shadow a context name.  Filtering to ``accepted`` —
    the Input's declared names, computed once at compile — lets a
    ``forbid_unknown_fields`` Command decode the result.

    Args:
        output: Validated answer of the run.
        run: Context of the admitted run.
        accepted: Internal names the Input declares.
        messages: The run's new messages in the engine's serialised form;
            ``None`` when they are longer than ``policies.max_history_bytes``.

    Returns:
        The payload ``from_payload`` will decode.
    """
    offered: dict[str, Any] = {
        HOOK_MESSAGES_FIELD: messages,
        "interaction_id": run.interaction_id,
        "conversation_id": run.conversation_id,
        "subject": run.identity.subject,
        "mechanism": run.identity.mechanism,
        "agent": run.plan.name,
        "provider": run.plan.inference.provider,
        "model": run.plan.inference.model,
    }
    if HOOK_OUTPUT_FIELD in accepted:
        offered[HOOK_OUTPUT_FIELD] = run.plan.output.loom_type.to_builtins(output)
    return {name: value for name, value in offered.items() if name in accepted}


async def _invoke_hook(
    hook: CompiledOutputHook,
    final: FinalEvent,
    run: RunContext,
    container: LoomContainer,
) -> object:
    """Run the hook use case through the application's invoker, as the identity acting."""
    invoker: ApplicationInvoker = container.resolve(ApplicationInvoker)
    command = hook_command(final.output, run, hook.accepted, messages=final.messages)
    return await invoker.invoke(hook.use_case, params=None, payload=command)


async def hooked_events(
    events: AsyncIterator[AgentEvent],
    run: RunContext,
    container: LoomContainer,
) -> AsyncGenerator[AgentEvent, None]:
    """Forward a supervised stream, running the hook at its terminal event.

    Every terminal event leaves carrying ``interaction_id``; a ``final`` event
    additionally carries the hook's result, or becomes an ``error`` when the
    hook fails.  A consumer that closes the stream early never reaches the
    terminal event, so no hook runs for an abandoned run.  A supervised stream
    exhausted without a terminal event (engine misbehaviour) is closed with a
    ``PROVIDER_UNAVAILABLE`` error, so every admitted run ends named.

    Args:
        events: The supervised event stream of one run.
        run: Context of the admitted run.
        container: Application container, holding the application's invoker.

    Yields:
        The run's events, terminal event re-created.
    """
    async for event in events:
        if type(event) is ErrorEvent:
            yield msgspec.structs.replace(event, interaction_id=run.interaction_id)
            return
        if type(event) is FinalEvent:
            yield await _terminal(event, run, container)
            return
        yield event
    yield ErrorEvent(
        code=AgentRunErrorCode.PROVIDER_UNAVAILABLE,
        message=no_terminal_message(run.plan.name),
        interaction_id=run.interaction_id,
    )


async def _terminal(
    final: FinalEvent, run: RunContext, container: LoomContainer
) -> FinalEvent | ErrorEvent:
    """Run the hook, if any, and produce the stream's terminal event."""
    hook = run.plan.on_output
    if hook is None:
        return msgspec.structs.replace(final, interaction_id=run.interaction_id)
    try:
        result = await bounded(_invoke_hook(hook, final, run, container), run, what=_HOOK)
    except Exception as exc:
        error = failure_error(
            exc, run, code=AgentRunErrorCode.HOOK_FAILED, message=HOOK_FAILED_MESSAGE, what=_HOOK
        )
        return ErrorEvent(
            code=error.code,
            message=str(error),
            interaction_id=run.interaction_id,
            usage=final.usage,
        )
    return msgspec.structs.replace(final, interaction_id=run.interaction_id, hook_result=result)
