"""Task-local chain of in-flight agent names, for cycle and depth bounds.

Every run started through :meth:`~loom.ai.execution.AgentRuntime.run` — whether
the top-level call a transport makes or a nested call an
:class:`~loom.ai.abc.AgentHandle` makes from inside a use case — pushes its
own agent name onto this chain and pops it once the run is over, so the chain
always names the exact sequence of runs nested inside the current task.

A :class:`~contextvars.ContextVar` rather than a module-level variable: it is
local to the task carrying one run, not shared mutable state across the
worker's concurrent runs (module docstring rule: no global mutable state).
Reading it from a different task always sees the empty chain, which is
correct — that task did not open any of these runs.

Restoring the prior chain by *value* rather than by
:meth:`~contextvars.Token.reset` is deliberate, not a stylistic choice:
``_run_stream`` is an ``@asynccontextmanager`` async generator, and streaming
callers (the A2A and SSE surfaces) resume it across ``asend`` calls that do
not always run in the context that entered it. ``Token.reset`` raises
``ValueError`` when called outside the context that created the token;
``ContextVar.set`` carries no such restriction, so restoring by value is the
one operation this shape can perform safely — the same reason
:class:`~loom.core.observability.span.LoomSpan` exists instead of a lexical
``with`` for a span that opens and closes across the same kind of boundary.
"""

from __future__ import annotations

from contextvars import ContextVar

from loom.ai.errors import AgentRunError, AgentRunErrorCode

_chain: ContextVar[tuple[str, ...]] = ContextVar("loom_ai_agent_chain", default=())


def current_chain() -> tuple[str, ...]:
    """Return the agent names currently in flight in this task, outer first."""
    return _chain.get()


def enter_agent_call(name: str, *, max_depth: int) -> tuple[str, ...]:
    """Push *name* onto the chain, refusing a cycle or an over-deep nesting.

    Args:
        name: Agent about to run.
        max_depth: ``ai.max_agent_depth`` of this deployment; the chain
            (including *name*) may not exceed it.

    Returns:
        The chain as it was *before* this call, to hand back to
        :func:`exit_agent_call` once the run is over.

    Raises:
        AgentRunError: With ``AGENT_CALL_CYCLE`` when *name* is already in the
            chain, or ``AGENT_CALL_TOO_DEEP`` when adding it would exceed
            *max_depth*.
    """
    chain = _chain.get()
    if name in chain:
        raise AgentRunError(
            AgentRunErrorCode.AGENT_CALL_CYCLE,
            f"agent call cycle detected: {' -> '.join((*chain, name))}",
        )
    next_chain = (*chain, name)
    if len(next_chain) > max_depth:
        raise AgentRunError(
            AgentRunErrorCode.AGENT_CALL_TOO_DEEP,
            f"agent call chain {' -> '.join(next_chain)} exceeds ai.max_agent_depth={max_depth}",
        )
    _chain.set(next_chain)
    return chain


def exit_agent_call(previous: tuple[str, ...]) -> None:
    """Restore the chain to *previous*, whatever entered it since.

    Called unconditionally from a ``finally``. Setting the value back rather
    than resetting a token is what makes this safe to call from a context
    that only resumed, rather than created, the enclosing generator — see
    the module docstring.

    Args:
        previous: The chain :func:`enter_agent_call` returned when it pushed
            the name this call is popping.
    """
    _chain.set(previous)
