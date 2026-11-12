"""The grant views an :class:`~loom.ai.abc.AgentHandle` exposes (T301–T303).

``McpGrantView`` and ``SqlGrantView`` reach exactly what the artefact already
grants and what the runtime already opened — never a second connection and
never a second filter:

* :class:`McpGrantView` filters the tools this server's shared session
  advertises with the same include/exclude rule
  :func:`~loom.ai.engines.pydantic_ai._capabilities._tool_predicate` applies
  to the model's own toolset, over the same
  :class:`~loom.ai.abc.McpSession` :class:`~loom.ai.runtime.AgentRuntime`
  opened at start-up — serialised behind
  :class:`~loom.ai.runtime._mcp.SharedMcpSession` unless it declares itself
  already safe for concurrent calls (see
  :func:`~loom.ai.runtime._mcp.mcp_session_for`). There is no second filter
  that could diverge from the model's, because there is no second filter.
* :class:`SqlGrantView` runs under the grant's own row and byte bounds, with
  roles resolved from the verified caller through
  :func:`~loom.ai._roles.bound_query_roles` — the one place that binding
  lives, called by the model's own ``sql`` capability too, with no
  caller-supplied role.

Both bound one call by a deadline, inside a span — the agent path by the
plan's own ``tool_timeout_ms``, the ``Mcp()`` marker path (T302) by the
server's own ``timeout_ms``, since a marker-driven call has no plan for
``tool_timeout_ms`` to belong to. The span carries ``agent`` or
``mcp_server`` depending on which path resolved the view
(``span_attributes``), never both — the neutral counterpart of
:func:`~loom.ai.engines.pydantic_ai._guards.capability_call`, since this
package stays free of any one engine. What is deliberately **not** carried
over from the model-facing path is turning a failure into a refusal *value*
(:func:`~loom.ai.engines.pydantic_ai._guards.guarded`): that exists so a model
can read a refusal and try again, and the consumer of a grant view is
application code, which reads an exception instead.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

import msgspec

from loom.ai._filters import admits
from loom.ai._roles import bound_query_roles
from loom.ai.abc import McpHandle, McpSession, McpToolInfo, SqlGrantHandle
from loom.ai.compiler import CompiledMcpCapability, CompiledSqlCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.errors import BoundaryValidationError, UnsupportedBoundaryType
from loom.core.identity import Identity
from loom.core.model import loom_type_of
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.abc import SqlQueryResult
from loom.core.sql.service import SqlQueryService

_T = TypeVar("_T")


def _tool_allowed(name: str, *, include: tuple[str, ...], exclude: tuple[str, ...]) -> bool:
    """Same include-then-exclude rule the model's own toolset filter applies."""
    return admits(name, include=include, exclude=exclude)


@dataclass(frozen=True, slots=True)
class McpGrant:
    """One agent's own ``mcp`` grant, resolved once at start-up.

    Carries exactly what :meth:`~loom.ai.abc.AgentHandle.mcp` needs to build
    its view: the compiled capability, the worker's shared session for that
    server and the tool catalogue start-up already listed — never a second
    connection, never a second listing.
    """

    capability: CompiledMcpCapability
    session: McpSession
    catalogue: tuple[McpToolInfo, ...]


@dataclass(frozen=True, slots=True)
class AgentGrants:
    """Every ``mcp``/``sql`` grant one agent was compiled with, resolved once.

    Built once per plan when the runtime is entered, so a marker-driven run
    never repeats the linear scan over ``plan.capabilities`` that finding
    one grant, or one policy, would otherwise cost on every call.

    Attributes:
        mcp: Live ``mcp`` grants by server name; a server declared under
            ``ai.remote_clients: optional`` whose connection was never
            opened has no entry here.
        sql: Compiled ``sql`` grants by connection name.
        mcp_names: Every ``mcp`` server this agent declares, in declaration
            order — including one whose connection never opened, so an
            "unknown grant" refusal can still name it.
        tool_timeout_s: The plan's own ``tool_timeout_ms``, in seconds.
        output_shape_bound: Whether the plan's output hook declares the
            ``output`` field, so a per-run shape override must be refused
            (T304).
    """

    mcp: Mapping[str, McpGrant]
    sql: Mapping[str, CompiledSqlCapability]
    mcp_names: tuple[str, ...]
    tool_timeout_s: float
    output_shape_bound: bool

    @property
    def names(self) -> tuple[str, ...]:
        """Every ``mcp`` server and ``sql`` connection this agent declares.

        Every ``mcp`` server first, then every ``sql`` connection, each group
        in declaration order. Grouped rather than interleaved so a server and a
        connection sharing a name stay distinguishable by position, which a
        single interleaved list cannot promise. This is the shape
        :meth:`~loom.ai.abc.AgentHandle.grants` returns, computed rather than
        stored, so the two underlying lists cannot drift apart from it.
        """
        return (*self.mcp_names, *self.sql)


class McpGrantView:
    """The concrete :class:`~loom.ai.abc.McpHandle` both paths return.

    ``include``/``exclude`` are taken as explicit arguments rather than read
    off ``capability`` — even though ``capability`` carries its own
    ``include``/``exclude`` fields — because the two callers of this
    constructor do not agree on which capability's filter applies. The
    agent path's capability *is* the filter: it was compiled from the
    artefact's own declaration. The use-case path's capability is the
    shared, server-keyed substrate every ``Mcp()`` binding on that server
    reads its session and catalogue from (FR-15); its own ``include`` is
    explicitly empty, and the caller's actual filter lives on its own
    binding instead. Reading ``capability.include`` here would silently
    admit the full catalogue for a use case that declared a narrower one —
    the authorization gap hostile verification measured during this
    feature's own planning (``plan.md``'s "Where the caller's include comes
    from") — so there is no shorter way to build this view that does not
    also state the filter.

    Args:
        span_attributes: The one caller-identifying attribute this view's
            tool spans carry — ``{"agent": name}`` on the agent path,
            ``{"mcp_server": name}`` on the ``Mcp()`` marker path — never
            both.
        capability: Compiled ``mcp`` capability this view calls through;
            only its ``server`` name and ``timeout_ms`` are read here, never
            its own ``include``/``exclude`` (see above).
        include: Glob patterns naming the tools this view admits.
        exclude: Glob patterns naming the tools this view refuses even if
            ``include`` would otherwise admit them.
        session: The server's shared, already-open session this view calls
            through — never a second connection.
        catalogue: The server's already-listed tool catalogue this view
            filters — never a second listing.
        timeout_s: Deadline bounding one call, in seconds.
        identity: Verified caller this view's queries and spans are bound
            to.
        observability: Runtime this view's own span opens on, or ``None``.
    """

    def __init__(
        self,
        *,
        span_attributes: Mapping[str, str],
        capability: CompiledMcpCapability,
        include: tuple[str, ...],
        exclude: tuple[str, ...],
        session: McpSession,
        catalogue: Sequence[McpToolInfo],
        timeout_s: float,
        identity: Identity,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._span_attributes = span_attributes
        self._capability = capability
        self._session = session
        self._identity = identity
        self._observability = observability
        self._timeout_s = timeout_s
        self._tools: dict[str, McpToolInfo] = {
            info.name: info
            for info in catalogue
            if _tool_allowed(info.name, include=include, exclude=exclude)
        }

    def tools(self) -> tuple[str, ...]:
        """Return the tool names this grant's own filter admits."""
        return tuple(self._tools)

    async def call(self, tool: str, arguments: Mapping[str, Any], *, expect: type[_T]) -> _T:
        """Call *tool* and decode its structured result into *expect*."""
        info = self._require_tool(tool)
        if not info.has_output_schema:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_UNTYPED,
                f"tool {tool!r} of mcp server {self._capability.server!r} publishes no "
                "output schema; call it with call_untyped() instead",
            )
        try:
            expect_type = loom_type_of(expect)
            structured = await self._call(tool, arguments)
            if structured is None:
                raise AgentRunError(
                    AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED,
                    f"tool {tool!r} of mcp server {self._capability.server!r} published an "
                    "output schema but returned no structured content",
                )
            return cast(_T, expect_type.from_builtins(structured))
        except (UnsupportedBoundaryType, BoundaryValidationError) as exc:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_DECODE_FAILED,
                f"tool {tool!r} of mcp server {self._capability.server!r}: {exc}",
            ) from exc

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Call *tool* and return the server's own structured content, undecoded.

        ``None`` — a server that returned no structured content at all — is
        the one shape folded into ``{}`` here: an absence, not a
        contradiction. Any other non-mapping content (a list, a scalar) is a
        server publishing a result its own protocol does not allow, and is
        refused rather than silently discarded — the same "a server that
        contradicts itself fails" rule :meth:`call` applies through
        ``TOOL_DECODE_FAILED``.
        """
        self._require_tool(tool)
        structured = await self._call(tool, arguments)
        if structured is None:
            return {}
        if not isinstance(structured, Mapping):
            raise AgentRunError(
                AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED,
                f"tool {tool!r} of mcp server {self._capability.server!r} returned "
                f"{type(structured).__name__} instead of a structured mapping",
            )
        return structured

    async def _call(self, tool: str, arguments: Mapping[str, Any]) -> object | None:
        async with self._guard(tool):
            result = await self._session.call_tool(tool, arguments)
        if not result.ok:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_CALL_FAILED,
                f"tool {tool!r} of mcp server {self._capability.server!r} reported a failure",
            )
        return result.structured

    def _require_tool(self, tool: str) -> McpToolInfo:
        info = self._tools.get(tool)
        if info is None:
            granted = ", ".join(sorted(self._tools)) or "none"
            raise AgentRunError(
                AgentRunErrorCode.TOOL_UNKNOWN,
                f"mcp server {self._capability.server!r} grants no tool named {tool!r}; "
                f"tools this grant admits: {granted}",
            )
        return info

    @asynccontextmanager
    async def _guard(self, tool: str) -> AsyncIterator[None]:
        """Bound one call by this view's own tool timeout, inside its own span.

        A call the session reports ``MCP_CALL_INTERRUPTED`` (it may have run,
        and is not repeated) reaches the use case as ``TOOL_UNAVAILABLE``, the
        code a marker already reads for a server it cannot use.
        """
        with self._span(tool):
            try:
                async with asyncio.timeout(self._timeout_s):
                    yield
            except TimeoutError as exc:
                raise AgentRunError(
                    AgentRunErrorCode.TOOL_TIMEOUT,
                    f"tool {tool!r} of mcp server {self._capability.server!r} exceeded "
                    f"the {self._timeout_s:.3f}s tool timeout",
                ) from exc
            except AgentRunError as exc:
                if exc.code is not AgentRunErrorCode.MCP_CALL_INTERRUPTED:
                    raise
                raise AgentRunError(
                    AgentRunErrorCode.TOOL_UNAVAILABLE,
                    f"tool {tool!r} of mcp server {self._capability.server!r} was interrupted "
                    "and may have run; it was not repeated",
                ) from exc

    def _span(self, tool: str) -> AbstractContextManager[None]:
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.TOOL,
            tool,
            **self._span_attributes,
            capability="mcp",
            subject=self._identity.subject,
        )


class SqlGrantView:
    """The concrete :class:`~loom.ai.abc.SqlGrantHandle` an ``AgentHandle.sql()`` returns.

    Args:
        agent: Agent this grant belongs to, named in its own span.
        capability: Compiled ``sql`` capability this view is bounded by.
        sql_query_service: The application's single, already-resolved query
            service — resolved once by whoever builds the marker resolver,
            never per call and never by reaching into a container this view
            holds no reference to.
        identity: Verified caller whose roles bind this view's queries.
        observability: Runtime this view's own span opens on, or ``None``.
    """

    def __init__(
        self,
        *,
        agent: str,
        capability: CompiledSqlCapability,
        sql_query_service: SqlQueryService,
        identity: Identity,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._agent = agent
        self._capability = capability
        self._sql_query_service = sql_query_service
        self._identity = identity
        self._observability = observability

    async def query(
        self,
        statement: str,
        *,
        parameters: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Run *statement* with the caller's roles, under this grant's own bounds."""
        roles = bound_query_roles(self._capability, self._identity)
        with self._span(roles):
            result = await self._sql_query_service.execute(
                statement,
                connection=self._capability.connection,
                roles=roles,
                parameters=parameters,
                limit=self._capability.max_rows,
            )
        return _bounded_rows(self._capability, result)

    def _span(self, roles: tuple[str, ...]) -> AbstractContextManager[None]:
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.READ,
            f"sql:{self._capability.connection}",
            agent=self._agent,
            connection=self._capability.connection,
            roles=",".join(roles),
            subject=self._identity.subject,
        )


_JSON_ARRAY_OVERHEAD = len(b"[]")
"""Bytes an empty JSON array costs; every row added past the first also costs
one byte for the ``,`` joining it to the previous one."""


def _bounded_rows(
    capability: CompiledSqlCapability, result: SqlQueryResult
) -> tuple[Mapping[str, Any], ...]:
    """Turn tabular rows into mappings, truncated to the grant's byte bound.

    The row bound is already enforced upstream, by passing
    ``limit=capability.max_rows`` into the query itself; this only trims
    trailing rows so the encoded payload never exceeds ``max_result_bytes`` —
    the same "truncate, don't refuse" contract
    :meth:`~loom.ai.abc.SqlGrantHandle.query` documents.

    Each row is encoded exactly once and its size accumulated, rather than
    re-encoding the whole kept list on every row: the running total is
    arithmetically identical to ``len(msgspec.json.encode(kept))`` for a
    compact JSON array (no whitespace between elements), so the bound this
    reaches is the same one the naive re-encode would have found, at O(n)
    instead of O(n^2) in the number of rows.
    """
    columns = tuple(column.name for column in result.columns)
    kept: list[Mapping[str, Any]] = []
    total = _JSON_ARRAY_OVERHEAD
    for row in result.rows:
        mapped = dict(zip(columns, row, strict=True))
        separator = 1 if kept else 0
        total += len(msgspec.json.encode(mapped)) + separator
        if total > capability.max_result_bytes:
            break
        kept.append(mapped)
    return tuple(kept)


if TYPE_CHECKING:  # each view satisfies the public Protocol it stands in for (A3)

    def _mcp_grant_view_satisfies_mcp_handle(view: McpGrantView) -> McpHandle:
        return view

    def _sql_grant_view_satisfies_sql_grant_handle(view: SqlGrantView) -> SqlGrantHandle:
        return view


__all__ = ["AgentGrants", "McpGrant", "McpGrantView", "SqlGrantView"]
