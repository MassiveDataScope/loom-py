"""The capability call boundary: who runs a tool, and what may escape it.

Two of the four invariants of a capability call hold here (US5):

* **The call runs as the caller, or it does not run.** The dependency bundle
  must carry a verified :class:`~loom.core.identity.Identity` *and* the
  application container. A bundle carrying neither fails closed with
  ``UNAUTHORIZED``; it is never degraded to ``ANONYMOUS``.
* **No raw application failure escapes a tool.** An authorisation denial is
  re-raised as ``UNAUTHORIZED`` — an ``AUTHORIZATION`` code the retry policy
  never replays — and every other failure is logged server-side and answered
  with a generic refusal value. Were it allowed to escape, the engine's
  classifier would read it as ``PROVIDER_UNAVAILABLE``, the run would be
  replayed, and an already-successful mutating use case would run again.

Every guard lives **inside** a tool body, never at engine build: a
pure-language agent holds no capability, so its dependency bundle may
legitimately be ``None`` and building its engine must not require one.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, Final, Protocol, TypeVar, cast

from pydantic_ai import ToolReturn
from pydantic_ai.exceptions import (
    ApprovalRequired,
    CallDeferred,
    ModelRetry,
    ToolFailed,
)
from pydantic_ai.tools import RunContext
from pydantic_ai.toolsets import AbstractToolset, ToolsetTool, WrapperToolset

from loom.ai.compiler import AgentPlan, CompiledMcpCapability
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.engines.pydantic_ai._returns import foreign_return, refusal
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.errors import Forbidden, Unauthenticated
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError
from loom.core.use_case.invoker import ApplicationInvoker

_logger = logging.getLogger(__name__)


_MS_PER_SECOND = 1000.0


_DENIED_MESSAGE = "the caller is not allowed to perform this operation"
"""Authorisation denials read alike, as ``core.sql.roles._deny`` already does."""

_FAILED_MESSAGE = "the operation failed; the detail is recorded server-side and is not shown here"
"""Generic cause: no driver text, no DSN, no schema name, no statement."""


_ENGINE_SIGNALS: Final[tuple[type[Exception], ...]] = (
    ModelRetry,
    ToolFailed,
    CallDeferred,
    ApprovalRequired,
)
"""Engine control-flow signals, which are never a capability failure.

All four subclass ``Exception``, so the guard's catch-all would otherwise turn
the model's own retry guidance and the deferred/approval protocols into silent
refusals.
"""


_T = TypeVar("_T")


class CapabilityDeps(Protocol):
    """Dependency bundle every capability call requires.

    Structural on purpose: the deployment owns its bundle type and only has to
    carry these attributes. A bundle that does not is refused inside the tool
    body rather than silently replaced by an anonymous caller.

    ``identity`` and ``container`` gate every capability; ``invoker`` is read
    by ``usecase`` grants alone, and is already bound to ``identity`` by the
    composition root that built the bundle — so a granted use case declaring
    ``Caller()`` receives the agent's caller instead of failing as
    unauthenticated.

    Attributes:
        identity: Verified caller this invocation runs as.
        container: Application container the capability resolves services from.
        invoker: Application invoker already bound to ``identity``.
    """

    identity: Identity
    container: LoomContainer
    invoker: ApplicationInvoker


@dataclass(frozen=True)
class BuildContext:
    """Read-only facts shared by every toolset built for one plan.

    Attributes:
        agent: Name of the plan being built, reported on every span.
        container: Application container a ``python`` factory receives.
        observability: Runtime the capability spans open on, or ``None``.
        timeout_s: The plan's ``tool_timeout_ms``, in seconds.
        mcp: The worker's MCP toolsets, so every agent granted a server works
            over the one connection the runtime opened for it. Required: a
            default would be an isolated store, and omitting it is exactly how
            per-agent connections would come back unnoticed.
        mcp_grants: The plan's ``mcp`` grants, in declaration order: the only
            servers a ``python`` factory may reach through its context.
            Required for the same reason ``mcp`` is: a default would make
            every ``remote()`` on a hand-built context miss silently.
    """

    agent: str
    container: LoomContainer
    observability: ObservabilityRuntime | None
    timeout_s: float
    mcp: SharedMcpToolsets
    mcp_grants: tuple[CompiledMcpCapability, ...]

    @classmethod
    def of(cls, plan: AgentPlan, container: LoomContainer, mcp: SharedMcpToolsets) -> BuildContext:
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
            observability=_observability(container),
            timeout_s=plan.policies.tool_timeout_ms / _MS_PER_SECOND,
            mcp=mcp,
            mcp_grants=tuple(
                grant for grant in plan.capabilities if type(grant) is CompiledMcpCapability
            ),
        )


def capability_deps(run: RunContext[Any]) -> CapabilityDeps:
    """Read the dependency bundle, fail-closed on an incomplete one."""
    bundle = run.deps
    identity = getattr(bundle, "identity", None)
    container = getattr(bundle, "container", None)
    if not isinstance(identity, Identity) or not isinstance(container, LoomContainer):
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED,
            "the invocation carries no verified caller: a capability call requires a "
            "dependency bundle exposing 'identity' and 'container'",
        )
    return cast(CapabilityDeps, bundle)


def require_authenticated(identity: Identity, tool: str) -> None:
    """Refuse an unauthenticated caller before any side effect."""
    if not identity.is_authenticated:
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED,
            f"tool '{tool}' requires an authenticated caller",
        )


def authenticated_caller(run: RunContext[Any], tool: str) -> Identity:
    """Identity strategy of a capability that must not run as nobody."""
    deps = capability_deps(run)
    require_authenticated(deps.identity, tool)
    return deps.identity


def _observability(container: LoomContainer) -> ObservabilityRuntime | None:
    """Resolve the observability runtime, or ``None`` when none is registered."""
    if not container.is_registered(ObservabilityRuntime):
        return None
    resolved = container.resolve(ObservabilityRuntime)
    return resolved if isinstance(resolved, ObservabilityRuntime) else None


def _span(
    context: BuildContext, kind: str, tool: str, identity: Identity
) -> AbstractContextManager[None]:
    """Open the ``Scope.TOOL`` span of one capability call, or a no-op."""
    if context.observability is None:
        return nullcontext()
    return context.observability.span(
        Scope.TOOL,
        tool,
        agent=context.agent,
        capability=kind,
        subject=identity.subject,
    )


@asynccontextmanager
async def capability_call(
    context: BuildContext, kind: str, tool: str, identity: Identity
) -> AsyncIterator[None]:
    """Bound one capability call by the plan's tool timeout, inside its span."""
    with _span(context, kind, tool, identity):
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
    """Run one application call so that no raw failure escapes the tool.

    A raw exception leaving a tool body is classified ``PROVIDER_UNAVAILABLE``
    by the engine's classifier, which the retry policy replays — re-invoking a
    mutating use case the model already ran. So an authorisation denial becomes
    ``UNAUTHORIZED`` (class ``AUTHORIZATION``, never retried) and every other
    failure becomes a refusal *value*: the model may self-correct, the run is
    not replayed, and no backend text reaches the caller.

    ``BaseException`` is deliberately not caught: ``asyncio.CancelledError``
    must keep propagating for the enclosing tool timeout to work.

    :data:`_ENGINE_SIGNALS` is re-raised for the opposite reason. Those four are
    plain ``Exception`` subclasses, but they are the engine's *control flow*,
    not failures: ``ModelRetry`` carries the guidance a tool wrote for the model
    ("bad argument, try X"), and ``CallDeferred`` / ``ApprovalRequired`` drive
    the deferred and approval protocols. Swallowing them into a generic refusal
    would lose the guidance and silently answer a protocol the engine is waiting
    on, so they pass through untouched.

    Args:
        tool: Published tool name, named in the refusal and in the log record.
        operation: The application call, deferred so the guard encloses it.
        present: Turns the call's value into the tool return, bounds included.

    Returns:
        The presented value, or the generic refusal.

    Raises:
        AgentRunError: When the call is refused by the application, or when the
            body already raised a coded error.
    """
    try:
        return present(await operation())
    except AgentRunError:
        raise
    except _ENGINE_SIGNALS:
        raise
    except (Forbidden, Unauthenticated, RoleNotAllowedError, RolesNotBoundError) as exc:
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED, f"tool '{tool}': {_DENIED_MESSAGE}"
        ) from exc
    except Exception:
        _logger.exception("capability tool '%s' failed", tool)
        return refusal(f"tool '{tool}' could not complete: {_FAILED_MESSAGE}")


@dataclass
class _GuardedToolset(WrapperToolset[Any]):
    """Applies loom's call boundary to a toolset loom did not author.

    ``mcp`` and ``python`` publish their own tools, so their calls would
    otherwise bypass the dependency bundle, the identity guard and the plan's
    ``tool_timeout_ms`` entirely. Wrapping the toolset puts every one of their
    tools behind the same boundary a ``usecase`` tool passes through.

    Attributes:
        context: Build facts of the plan, carrying the tool timeout.
        kind: Capability kind, reported on the span.
        caller: Identity strategy; ``authenticated_caller`` for every
            capability wrapped here, all of which can reach data or a remote
            server.
    """

    context: BuildContext
    kind: str
    caller: Callable[[RunContext[Any], str], Identity]

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        """Guard, then bound, one call to a tool of the wrapped toolset.

        The wrapped call goes through :func:`guarded` for the same reason a
        ``usecase`` call does: a raw exception from an MCP transport or from a
        first-party python toolset is classified ``PROVIDER_UNAVAILABLE`` and
        replayed by the retry policy, which would re-invoke any mutating
        operation the model already completed in that attempt.
        """
        identity = self.caller(ctx, name)
        async with capability_call(self.context, self.kind, name, identity):
            return await guarded(
                name,
                lambda: self.wrapped.call_tool(name, tool_args, ctx, tool),
                foreign_return,
            )


def guarded_toolset(
    toolset: AbstractToolset[Any],
    context: BuildContext,
    kind: str,
    caller: Callable[[RunContext[Any], str], Identity],
) -> AbstractToolset[Any]:
    """Put a foreign toolset behind loom's capability call boundary."""
    return _GuardedToolset(wrapped=toolset, context=context, kind=kind, caller=caller)
