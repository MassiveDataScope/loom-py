"""The escape hatch: this engine's own objects, for what loom does not serve.

It lives in the engine package rather than in :mod:`loom.ai.abc` so that the
neutral surface keeps naming no engine: importing this module is the explicit
act of leaving loom's guarantees behind. What a run driven from here keeps and
loses is stated once, on :meth:`~loom.ai.abc.AgentHandle.native` — this module
carries the contract, not the reasoning.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

from loom.ai.abc import AgentHandle
from loom.ai.engines.pydantic_ai._errors import as_run_error


@dataclass(frozen=True, slots=True)
class NativeAgent:
    """What the pydantic-ai engine would itself have used to serve one run.

    Attributes:
        agent: The agent built from this plan's spec, carrying the artefact's
            ``output_check`` validator when it declares one. Shared by every
            run of this plan for the life of the worker: handed over as it
            is, not copied, so mutating it changes every run of that agent
            in this worker, including the ones loom itself drives. Refuses a
            run-level ``output_type`` when it holds a validator (pydantic-ai's
            own ``UserError``); use :attr:`shaped_agent` for a run that
            overrides the output shape.
        shaped_agent: The same agent spec, built with no ``output_check``
            validator registered, for a run overriding the plan's declared
            output shape. Identical to :attr:`agent` — the very same object —
            whenever the plan declares no ``output_check``.
        deps: The dependency bundle of this run: the application container
            and the run's state. Every capability call made with it acts as
            the identity acting where it is made, inside the artefact's own
            grants.
        usage_limits: The artefact's spend caps, projected onto the engine's
            own type and copied for this call: mutating the returned value
            never changes what the engine enforces on its own, loom-supervised
            runs. Passing it on to a driven run is the caller's own choice.
    """

    agent: Agent[Any, Any]
    shaped_agent: Agent[Any, Any]
    deps: object
    usage_limits: UsageLimits


@dataclass(frozen=True, slots=True)
class _NativeAccess:
    """The carrier :meth:`~loom.ai.engines.pydantic_ai._engine.PydanticAIEngine.native`
    returns: this engine's own objects, plus how to rejoin loom's bounds.

    Not published: :func:`native_agent` is the one accessor calling code
    reaches, and it is the one place this carrier is unpacked.
    """

    native: NativeAgent
    guard: Callable[[], AbstractAsyncContextManager[None]]


@asynccontextmanager
async def native_agent(
    handle: AgentHandle[Any], *, state: object | None = None
) -> AsyncIterator[NativeAgent]:
    """Drive one agent handle's engine-native objects inside loom's chain and admission bounds.

    Args:
        handle: Handle an ``Agent()`` marker filled; the run acts as the
            identity of the execution in progress.
        state: This run's state, resolved against the artefact's declared
            shape exactly as :meth:`~loom.ai.abc.AgentHandle.run` resolves it.

    Yields:
        This engine's own objects; see :class:`NativeAgent`.

    Raises:
        TypeError: When the agent behind *handle* is served by an engine
            other than pydantic-ai.
        NotImplementedError: When that engine publishes no native form at all.
        ExecutionRequired: Outside any execution.
        Unauthenticated: With no caller, or past the caller's deadline
            (``DelegationExpired``).
        AgentRunError: With ``UNAUTHORIZED`` when the identity acting is not
            authenticated; with ``STATE_UNDECLARED`` or ``STATE_REQUIRED`` when
            *state* does not match the artefact's declared shape; with
            ``AGENT_CALL_CYCLE`` or ``AGENT_CALL_TOO_DEEP`` when entering the
            guard refuses this run; with ``TOO_MANY_RUNS`` when no admission
            permit is free.
        RuntimeError: When the runtime serving *handle* was never entered.

    Example::

        async with (
            native_agent(handle) as access,
            access.agent.iter(
                "triage this", deps=access.deps, usage_limits=access.usage_limits
            ) as run,
        ):
            async for node in run:
                ...
    """
    access = handle.native(state=state)
    if not isinstance(access, _NativeAccess):
        raise TypeError(
            "this agent is not served by the pydantic-ai engine: its native form is "
            f"{type(access).__name__}, not the pydantic-ai engine's own carrier"
        )
    async with access.guard():
        yield access.native


__all__ = ["NativeAgent", "as_run_error", "native_agent"]
