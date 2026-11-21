"""The capability call boundary: the identity a tool runs as, its bound, and what escapes it.

Every tool acts as the identity of the execution in progress, refused past
its deadline and when anonymous
(:func:`~loom.ai.errors.require_authenticated`).  An application denial
leaves a tool as ``UNAUTHORIZED``; every other application failure leaves it
as a refusal value, logged server-side.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic_ai import ToolReturn
from pydantic_ai.tools import RunContext
from pydantic_ai.toolsets import AbstractToolset, ToolsetTool, WrapperToolset

from loom.ai.compiler import AgentPlan, CompiledMcpCapability
from loom.ai.engines.pydantic_ai._failures import PYDANTIC_AI_PASS_THROUGH, ToolsetBoundary
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.engines.pydantic_ai._returns import foreign_return, refusal
from loom.ai.errors import (
    DENIALS,
    DENIED_MESSAGE,
    AgentRunError,
    AgentRunErrorCode,
    require_authenticated,
)
from loom.ai.log_events import AiLogField
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability

_logger = logging.getLogger(__name__)


_MS_PER_SECOND = 1000.0


_FAILED_MESSAGE = "the operation failed; the detail is recorded server-side and is not shown here"
"""Generic cause: no driver text, no DSN, no schema name, no statement."""


_T = TypeVar("_T")


@dataclass(frozen=True)
class BuildContext:
    """Read-only facts shared by every toolset built for one plan.

    Attributes:
        agent: Name of the plan being built, reported on every span.
        container: Application container the toolsets are built from.
        observability: Runtime the capability spans open on, or ``None``.
        timeout_s: The plan's ``tool_timeout_ms``, in seconds.
        mcp: The worker's shared MCP toolsets.
        mcp_grants: The plan's ``mcp`` grants, in declaration order: the only
            servers a ``python`` factory may reach through its context.
    """

    agent: str
    container: LoomContainer
    observability: Observability | None
    timeout_s: float
    mcp: SharedMcpToolsets
    mcp_grants: tuple[CompiledMcpCapability, ...]

    def tool_span(self, kind: str, tool: str) -> AbstractContextManager[None]:
        """Open the ``Scope.TOOL`` span of one capability call, or a no-op."""
        if self.observability is None:
            return nullcontext()
        return self.observability.span(
            Scope.TOOL,
            tool,
            attributes={AiLogField.AGENT: self.agent, AiLogField.CAPABILITY: kind},
        )

    @classmethod
    def from_plan(
        cls, plan: AgentPlan, container: LoomContainer, mcp: SharedMcpToolsets
    ) -> BuildContext:
        """Derive the build context of one compiled plan.

        Args:
            plan: The compiled plan being built.
            container: Application container.
            mcp: The worker's shared MCP toolsets.

        Returns:
            The context every builder of this plan reads.
        """
        return cls(
            agent=plan.name,
            container=container,
            observability=container.resolve_or_none(Observability),
            timeout_s=plan.policies.tool_timeout_ms / _MS_PER_SECOND,
            mcp=mcp,
            mcp_grants=tuple(
                grant for grant in plan.capabilities if type(grant) is CompiledMcpCapability
            ),
        )


def bundle_container(run: RunContext[Any]) -> LoomContainer:
    """Return the container the run's dependency bundle carries.

    Args:
        run: The engine's run context of the tool call.

    Returns:
        The bundle's ``container``.

    Raises:
        AgentRunError: ``UNAUTHORIZED`` when the bundle exposes no container.
    """
    container = getattr(run.deps, "container", None)
    if not isinstance(container, LoomContainer):
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED,
            "a capability call requires a dependency bundle exposing 'container'",
        )
    return container


@asynccontextmanager
async def capability_call(
    context: BuildContext, kind: str, tool: str, identity: Identity
) -> AsyncIterator[None]:
    """Bound one capability call by the plan's tool timeout, inside its span."""
    with context.tool_span(kind, tool):
        try:
            async with asyncio.timeout(context.timeout_s):
                yield
        except TimeoutError as exc:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_TIMEOUT,
                f"tool '{tool}' exceeded the {context.timeout_s:.3f}s tool timeout",
            ) from exc


async def guarded(
    tool: str,
    operation: Callable[[], Awaitable[_T]],
    present: Callable[[_T], ToolReturn],
) -> ToolReturn:
    """Run one application call so that no raw application failure escapes the tool.

    Args:
        tool: Published tool name, named in the refusal and in the log record.
        operation: The application call, deferred so the guard encloses it.
        present: Turns the call's value into the tool return, bounds included.

    Returns:
        The presented value, or the generic refusal when the call failed.

    Raises:
        AgentRunError: ``UNAUTHORIZED`` on an application denial, or the coded
            error the body raised.
        Exception: An engine control-flow signal, untouched.
    """
    try:
        return present(await operation())
    except AgentRunError:
        raise
    except PYDANTIC_AI_PASS_THROUGH:
        raise
    except DENIALS as exc:
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED, f"tool '{tool}': {DENIED_MESSAGE}"
        ) from exc
    except Exception:
        _logger.exception("capability tool '%s' failed", tool)
        return refusal(f"tool '{tool}' could not complete: {_FAILED_MESSAGE}")


@dataclass
class _GuardedToolset(ToolsetBoundary, WrapperToolset[Any]):
    """Applies loom's call boundary to a toolset loom did not author.

    Every tool of the wrapped toolset acts as the authenticated identity
    acting (:func:`~loom.ai.errors.require_authenticated`), within the plan's
    ``tool_timeout_ms``, inside a ``Scope.TOOL`` span.

    Attributes:
        context: Build facts of the plan, carrying the tool timeout.
        kind: Capability kind, reported on the span.
    """

    context: BuildContext
    kind: str

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        """Guard, then bound, one call to a tool of the wrapped toolset, through :func:`guarded`."""
        identity = require_authenticated(f"tool '{name}'")
        async with capability_call(self.context, self.kind, name, identity):
            return await guarded(
                name,
                lambda: self.wrapped.call_tool(name, tool_args, ctx, tool),
                foreign_return,
            )


def guarded_toolset(
    toolset: AbstractToolset[Any], context: BuildContext, kind: str
) -> AbstractToolset[Any]:
    """Put a foreign toolset behind loom's capability call boundary."""
    return _GuardedToolset(wrapped=toolset, context=context, kind=kind)
