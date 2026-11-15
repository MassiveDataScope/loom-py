"""The grant views an :class:`~loom.ai.abc.AgentHandle` exposes.

:class:`McpGrantView` calls the server's shared session through the grant's
own include/exclude filter; :class:`SqlGrantView` queries under the grant's
row and byte bounds with roles resolved from the identity acting.  Both bound
one call by a deadline, inside a ``Scope.TOOL`` span, and raise their
failures.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractContextManager, asynccontextmanager, nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

import msgspec

from loom.ai._filters import admits
from loom.ai.abc import McpHandle, McpSession, McpToolInfo, SqlGrantHandle
from loom.ai.compiler import CompiledMcpCapability, CompiledSqlCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode, require_authenticated
from loom.core.errors import BoundaryValidationError, UnsupportedBoundaryType
from loom.core.identity import Identity
from loom.core.model import loom_type_of
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.abc import SqlQueryResult
from loom.core.sql.service import SqlQueryService

_T = TypeVar("_T")


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

    Built once per plan when the runtime is entered.

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
            ``output`` field, so a per-run shape override must be refused.
    """

    mcp: Mapping[str, McpGrant]
    sql: Mapping[str, CompiledSqlCapability]
    mcp_names: tuple[str, ...]
    tool_timeout_s: float
    output_shape_bound: bool

    @property
    def names(self) -> tuple[str, ...]:
        """Every ``mcp`` server, then every ``sql`` connection, each in declaration order."""
        return (*self.mcp_names, *self.sql)


class McpGrantView:
    """The concrete :class:`~loom.ai.abc.McpHandle` both paths return.

    Args:
        span_attributes: The one caller-identifying attribute this view's
            tool spans carry — ``{"agent": name}`` on the agent path,
            ``{"mcp_server": name}`` on the ``Mcp()`` marker path — never
            both.
        capability: Compiled ``mcp`` capability this view calls through;
            only its ``server`` name and ``timeout_ms`` are read here, never
            its own ``include``/``exclude``.
        include: Glob patterns naming the tools this view admits.
        exclude: Glob patterns naming the tools this view refuses even if
            ``include`` would otherwise admit them.
        session: The server's shared, already-open session this view calls
            through — never a second connection.
        catalogue: The server's already-listed tool catalogue this view
            filters — never a second listing.
        timeout_s: Deadline bounding one call, in seconds.
        observability: Runtime this view's own span opens on, or ``None``.

    Each call acts as the execution identity of the moment
    (:func:`~loom.ai.errors.require_authenticated`).
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
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._span_attributes = span_attributes
        self._capability = capability
        self._who = f"mcp server {capability.server!r}"
        self._session = session
        self._observability = observability
        self._timeout_s = timeout_s
        self._tools: dict[str, McpToolInfo] = {
            info.name: info
            for info in catalogue
            if admits(info.name, include=include, exclude=exclude)
        }

    def tools(self) -> tuple[str, ...]:
        """Return the tool names this grant's own filter admits, to the identity acting.

        Returns:
            The admitted tool names.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no subject.
        """
        require_authenticated(self._who)
        return tuple(self._tools)

    async def call(self, tool: str, arguments: Mapping[str, Any], *, expect: type[_T]) -> _T:
        """Call *tool* as the identity acting and decode its structured result into *expect*.

        Args:
            tool: Tool name.
            arguments: Tool arguments.
            expect: Type the structured result decodes into.

        Returns:
            The decoded result.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no
                subject; with ``TOOL_UNKNOWN``, ``TOOL_UNTYPED``,
                ``TOOL_RESULT_UNSTRUCTURED``, ``TOOL_DECODE_FAILED``,
                ``TOOL_TIMEOUT`` or the call's own failure code.
        """
        caller = require_authenticated(self._who)
        info = self._require_tool(tool)
        if not info.has_output_schema:
            raise AgentRunError(
                AgentRunErrorCode.TOOL_UNTYPED,
                f"tool {tool!r} of mcp server {self._capability.server!r} publishes no "
                "output schema; call it with call_untyped() instead",
            )
        try:
            expect_type = loom_type_of(expect)
            structured = await self._call(tool, arguments, caller)
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
        """Call *tool* as the identity acting and return its structured content, undecoded.

        No structured content at all is returned as ``{}``; any other
        non-mapping content is refused.

        Args:
            tool: Tool name.
            arguments: Tool arguments.

        Returns:
            The structured content, ``{}`` for none.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no
                subject; with ``TOOL_UNKNOWN``, ``TOOL_RESULT_UNSTRUCTURED``,
                ``TOOL_TIMEOUT`` or the call's own failure code.
        """
        caller = require_authenticated(self._who)
        self._require_tool(tool)
        structured = await self._call(tool, arguments, caller)
        if structured is None:
            return {}
        if not isinstance(structured, Mapping):
            raise AgentRunError(
                AgentRunErrorCode.TOOL_RESULT_UNSTRUCTURED,
                f"tool {tool!r} of mcp server {self._capability.server!r} returned "
                f"{type(structured).__name__} instead of a structured mapping",
            )
        return structured

    async def _call(
        self, tool: str, arguments: Mapping[str, Any], caller: Identity
    ) -> object | None:
        async with self._guard(tool, caller):
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
    async def _guard(self, tool: str, caller: Identity) -> AsyncIterator[None]:
        """Bound one call by this view's own tool timeout, inside its own span."""
        with self._span(tool, caller):
            try:
                async with asyncio.timeout(self._timeout_s):
                    yield
            except TimeoutError as exc:
                raise AgentRunError(
                    AgentRunErrorCode.TOOL_TIMEOUT,
                    f"tool {tool!r} of mcp server {self._capability.server!r} exceeded "
                    f"the {self._timeout_s:.3f}s tool timeout",
                ) from exc

    def _span(self, tool: str, caller: Identity) -> AbstractContextManager[None]:
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.TOOL,
            tool,
            **self._span_attributes,
            capability="mcp",
            subject=caller.subject,
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
        observability: Runtime this view's own span opens on, or ``None``.

    Each query runs with the roles of the execution identity of the moment.
    """

    def __init__(
        self,
        *,
        agent: str,
        capability: CompiledSqlCapability,
        sql_query_service: SqlQueryService,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._agent = agent
        self._capability = capability
        self._sql_query_service = sql_query_service
        self._observability = observability

    async def query(
        self,
        statement: str,
        *,
        parameters: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Run *statement* with the caller's roles, under this grant's own bounds.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller is anonymous
                or holds no role this grant allows.
        """
        caller = require_authenticated(f"sql grant {self._capability.connection!r}")
        roles = self._capability.roles_of(caller)
        with self._span(roles, caller):
            result = await self._sql_query_service.execute(
                statement,
                connection=self._capability.connection,
                roles=roles,
                parameters=parameters,
                limit=self._capability.max_rows,
            )
        return _bounded_rows(self._capability, result)

    def _span(self, roles: tuple[str, ...], caller: Identity) -> AbstractContextManager[None]:
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.READ,
            f"sql:{self._capability.connection}",
            agent=self._agent,
            connection=self._capability.connection,
            roles=",".join(roles),
            subject=caller.subject,
        )


_JSON_ARRAY_OVERHEAD = len(b"[]")
"""Bytes an empty JSON array costs; every row added past the first also costs
one byte for the ``,`` joining it to the previous one."""


def _bounded_rows(
    capability: CompiledSqlCapability, result: SqlQueryResult
) -> tuple[Mapping[str, Any], ...]:
    """Turn tabular rows into mappings, dropping trailing rows above ``max_result_bytes``."""
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


if TYPE_CHECKING:

    def _mcp_grant_view_satisfies_mcp_handle(view: McpGrantView) -> McpHandle:
        return view

    def _sql_grant_view_satisfies_sql_grant_handle(view: SqlGrantView) -> SqlGrantHandle:
        return view


__all__ = ["AgentGrants", "McpGrant", "McpGrantView", "SqlGrantView"]
