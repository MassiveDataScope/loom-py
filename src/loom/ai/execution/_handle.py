"""The handles an ``Agent()`` or ``Mcp()`` marker resolves to, and the resolvers building them.

:class:`AgentMarkerResolver` and :class:`McpMarkerResolver` implement
:class:`~loom.core.execution.ExecutionResolver`: each returns one handle per
declaration, built on its first resolve and holding no identity. Every call on
a handle acts as the execution identity of the moment
(:func:`~loom.core.execution.execution_identity`), so a handle held by a
singleton, or by a task that outlived its execution, serves each caller as
itself and is refused past the caller's delegation. The ``Mcp()`` handle looks
its server's grant up on every call.

Running an ``Agent()`` handle goes through :meth:`AgentHost.run`, under the
same admission and policies as a transport-driven run. Three run modes:
``run(prompt)`` decodes into the artefact's declared output;
``run(prompt, expect=X)`` decodes this call only into ``X``, without the
artefact's own output check; ``run_text`` decodes nothing. Both overrides are
refused before the model is called when the artefact's output hook declares
the ``output`` field (:meth:`AgentHost.output_hook_shape_bound`).

``mcp()`` and ``sql()`` on an ``Agent()`` handle return the artefact's own
granted views (:mod:`~loom.ai.execution._grants`), over the session and
connection the model's own capabilities use.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Hashable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from loom.ai.abc import AgentAnswer, AgentHandle, McpHandle, Prompt, SqlGrantHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode, require_authenticated
from loom.ai.execution._grants import (
    AgentGrants,
    McpGrant,
    McpGrantView,
    SqlGrantView,
)
from loom.ai.execution._lifecycle import AgentHost
from loom.core.errors import IdentityFixed
from loom.core.execution import ExecutionResolver, current_execution, execution_identity
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.observability.span import LoomSpan
from loom.core.stores.sql.service import SqlQueryService

_K = TypeVar("_K", bound=Hashable)
_H = TypeVar("_H")


class _AgentHandle:
    """One named agent; each call acts as the execution identity of the moment.

    Args:
        name: Agent name as this deployment's host knows it.
        host: Live host the handle runs through.
        observability: Runtime the handle's own span opens on, or ``None``.
        sql_query_service: The application's query service, handed to every
            :class:`~loom.ai.execution._grants.SqlGrantView` this handle builds.
    """

    def __init__(
        self,
        *,
        name: str,
        host: AgentHost,
        observability: Observability | None,
        sql_query_service: SqlQueryService,
    ) -> None:
        self._name = name
        self._who = f"agent {name!r}"
        self._host = host
        self._observability = observability
        self._sql_query_service = sql_query_service

    async def run(
        self,
        prompt: Prompt,
        *,
        expect: type[Any] | None = None,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[Any]:
        """Run the agent once, decoded into ``expect`` or its declared output.

        Args:
            prompt: Prompt for this run.
            expect: When given, decode this run only into this type instead
                of the artefact's declared output; the artefact's own output
                check does not run for this call.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.
            state: This run's state, forwarded to
                :meth:`~loom.ai.execution.AgentHost.run` unchanged.

        Returns:
            The decoded answer, this run's own usage and its interaction id.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no
                subject; with ``AGENT_RUN_SHAPE_WITH_HOOK`` when ``expect`` is
                given and the artefact's output hook declares the ``output``
                field; both before the model is called; with
                ``STATE_UNDECLARED`` when ``state`` is given and the artefact
                declares no state shape; or with whatever code the run itself
                failed with.
        """
        caller = require_authenticated(self._who)
        if expect is not None:
            self._require_shape_allowed()
        span = self._open_span(caller)
        try:
            result = await self._host.run(
                self._name,
                prompt,
                identity=caller,
                conversation_id=conversation_id,
                output_type=expect,
                state=state,
            )
        except AgentRunError as exc:
            self._close_span_on_failure(span, exc)
            raise
        self._close_span_on_success(span, result.interaction_id)
        return AgentAnswer(
            output=result.output,
            usage=result.usage,
            interaction_id=result.interaction_id,
            provider_details=result.provider_details,
        )

    async def run_text(
        self,
        prompt: Prompt,
        *,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[str]:
        """Run the agent for open prose, with no declared or overridden shape.

        Args:
            prompt: Prompt for this run.
            conversation_id: Identifier of the conversation this run
                continues; ``None`` runs single-shot.
            state: This run's state, forwarded to :meth:`run` unchanged.

        Returns:
            The model's own prose, this run's usage and its interaction id.

        Raises:
            AgentRunError: With ``UNAUTHORIZED``, ``AGENT_RUN_SHAPE_WITH_HOOK``
                or ``STATE_UNDECLARED``, for the same reasons :meth:`run`
                raises them with ``expect`` and ``state``.
        """
        return await self.run(prompt, expect=str, conversation_id=conversation_id, state=state)

    def native(self, *, state: object | None = None) -> object:
        """Return the engine's own objects for this agent, refused for no authenticated caller.

        Opens no span and takes no admission slot. See
        :meth:`~loom.ai.abc.AgentHandle.native`.

        Args:
            state: This run's state, resolved against the artefact's declared
                shape exactly as :meth:`run` resolves it.

        Returns:
            The engine-native objects, in the engine's own types.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no
                subject, as :meth:`run`; with ``STATE_UNDECLARED`` or
                ``STATE_REQUIRED`` when *state* does not match the artefact's
                declared shape.
            RuntimeError: When this handle's runtime was never entered.
            NotImplementedError: When the engine serving this agent declares
                no native form.
        """
        caller = require_authenticated(self._who)
        return self._host.native(self._name, identity=caller, state=state)

    def mcp(self, server: str) -> McpHandle:
        """Return this artefact's own filtered view of one ``mcp`` grant.

        Raises:
            AgentRunError: With ``MCP_GRANT_UNKNOWN`` when the artefact
                declares no ``mcp`` grant on that server name.
        """
        grants = self._host.grants(self._name)
        grant = grants.mcp.get(server)
        if grant is None:
            raise self._mcp_grant_unknown(server, grants)
        return McpGrantView(
            span_attributes={"agent": self._name},
            capability=grant.capability,
            include=grant.capability.include,
            exclude=grant.capability.exclude,
            session=grant.session,
            catalogue=grant.catalogue,
            timeout_s=grants.tool_timeout_s,
            observability=self._observability,
        )

    def sql(self, connection: str) -> SqlGrantHandle:
        """Return this artefact's own bounded view of one ``sql`` grant.

        Raises:
            AgentRunError: With ``SQL_GRANT_UNKNOWN`` when the artefact
                declares no ``sql`` grant on that connection name.
        """
        grants = self._host.grants(self._name)
        capability = grants.sql.get(connection)
        if capability is None:
            raise self._sql_grant_unknown(connection, grants)
        return SqlGrantView(
            agent=self._name,
            capability=capability,
            sql_query_service=self._sql_query_service,
            observability=self._observability,
        )

    def grants(self) -> tuple[str, ...]:
        """Return every grant name reachable through :meth:`mcp` and :meth:`sql`."""
        return self._host.grants(self._name).names

    def _mcp_grant_unknown(self, server: str, grants: AgentGrants) -> AgentRunError:
        granted = grants.mcp_names
        if granted:
            detail = f"mcp servers this agent grants: {', '.join(granted)}"
        else:
            detail = (
                f"agent {self._name!r} grants no mcp server; add one to its "
                "artefact's 'mcp' capability"
            )
        return AgentRunError(
            AgentRunErrorCode.MCP_GRANT_UNKNOWN,
            f"agent {self._name!r} grants no mcp server named {server!r}; {detail}",
        )

    def _sql_grant_unknown(self, connection: str, grants: AgentGrants) -> AgentRunError:
        granted = tuple(grants.sql)
        if granted:
            detail = f"sql connections this agent grants: {', '.join(granted)}"
        else:
            detail = (
                f"agent {self._name!r} grants no sql connection; add one to its "
                "artefact's 'sql' capability"
            )
        return AgentRunError(
            AgentRunErrorCode.SQL_GRANT_UNKNOWN,
            f"agent {self._name!r} grants no sql connection named {connection!r}; {detail}",
        )

    def _require_shape_allowed(self) -> None:
        """Refuse a per-run shape when the output hook's command needs the declared one."""
        if self._host.grants(self._name).output_shape_bound:
            raise AgentRunError(
                AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK,
                f"agent {self._name!r} declares an output hook that reads the run's "
                "output, so this run cannot use a per-run shape; call run(prompt) "
                "for the artefact's own declared output instead",
            )

    def _open_span(self, caller: Identity) -> LoomSpan | None:
        """Open this run's own span, annotated with the interaction id when it closes."""
        if self._observability is None:
            return None
        return self._observability.open_span(
            Scope.AGENT,
            "agent_run",
            agent=self._name,
            mechanism=caller.mechanism,
        )

    @staticmethod
    def _close_span_on_success(span: LoomSpan | None, interaction_id: str | None) -> None:
        if span is None:
            return
        if interaction_id is not None:
            span.annotate({"interaction_id": interaction_id})
        span.end()

    @staticmethod
    def _close_span_on_failure(span: LoomSpan | None, exc: AgentRunError) -> None:
        if span is None:
            return
        if exc.interaction_id is not None:
            span.annotate({"interaction_id": exc.interaction_id})
        span.fail(exc)


class _MarkerResolver(ABC, Generic[_K, _H]):
    """One handle per marker declaration, built on its first resolve or call."""

    def __init__(self) -> None:
        self._handles: dict[_K, _H] = {}

    def resolve(self, declaration: Any, annotation: object) -> _H:
        """Return the handle of *declaration*, built on its first resolve.

        Args:
            declaration: The marker of the parameter.
            annotation: The parameter's annotation; not read.

        Returns:
            The declaration's handle.
        """
        key = self._key(declaration)
        handle = self._handles.get(key)
        return self._built(key) if handle is None else handle

    def _handle(self, key: _K, identity: Identity | None, what: str) -> _H:
        """Return the handle of *key*, refusing an identity other than the execution's.

        Args:
            key: The declaration's key.
            identity: The identity the caller gave, or ``None``.
            what: The declaration the handle serves, named in the error.

        Returns:
            The declaration's handle.

        Raises:
            IdentityFixed: When *identity* is given inside an execution acting
                as another identity.
        """
        if (
            identity is not None
            and current_execution() is not None
            and identity != execution_identity()
        ):
            raise IdentityFixed(
                f"{what} acts as the execution identity of each call, not as the "
                "identity given; act as another caller with executor.run(UseCase, "
                "identity=...) or ExecutionScope(...)"
            )
        handle = self._handles.get(key)
        return self._built(key) if handle is None else handle

    def _built(self, key: _K) -> _H:
        handle = self._handles[key] = self._build(key)
        return handle

    @staticmethod
    @abstractmethod
    def _key(declaration: Any) -> _K:
        """Return the key of a marker declaration."""

    @abstractmethod
    def _build(self, key: _K) -> _H:
        """Build the handle of *key*."""


class AgentMarkerResolver(_MarkerResolver[str, "_AgentHandle"]):
    """Resolves each ``Agent(name)`` declaration to its one handle, in either resolver shape.

    Args:
        host: Live agent host serving this deployment.
        sql_query_service: The application's query service, handed to every
            ``sql`` grant view a handle builds.
        observability: Runtime every handle's span opens on, or ``None``.
    """

    def __init__(
        self,
        host: AgentHost,
        *,
        sql_query_service: SqlQueryService,
        observability: Observability | None,
    ) -> None:
        super().__init__()
        self._host = host
        self._sql_query_service = sql_query_service
        self._observability = observability

    def __call__(self, name: str, identity: Identity | None = None) -> AgentHandle[Any]:
        """Return the handle of agent *name*, built on its first call.

        Args:
            name: Agent name as this deployment's host knows it.
            identity: Checked, inside an execution, against the execution
                identity; every call on the handle acts as the execution
                identity of the moment.

        Returns:
            The agent's handle.

        Raises:
            IdentityFixed: When *identity* is given inside an execution acting
                as another identity.
        """
        return self._handle(name, identity, f"agent {name!r}")

    @staticmethod
    def _key(declaration: Any) -> str:
        return cast(str, declaration.name)

    def _build(self, key: str) -> _AgentHandle:
        return _AgentHandle(
            name=key,
            host=self._host,
            observability=self._observability,
            sql_query_service=self._sql_query_service,
        )


class _McpHandle:
    """One ``Mcp(server, include=...)`` declaration over the server's shared grant.

    Every call looks the grant up (:meth:`AgentHost.use_case_mcp_grant`)
    and goes through an :class:`~loom.ai.execution._grants.McpGrantView` of it
    filtered by this declaration's ``include``, built once per grant.

    Args:
        server: MCP server name the declaration names.
        include: Glob patterns of the tools the declaration admits.
        host: Live agent host serving this deployment.
        observability: Runtime the view's span opens on, or ``None``.
    """

    def __init__(
        self,
        *,
        server: str,
        include: tuple[str, ...],
        host: AgentHost,
        observability: Observability | None,
    ) -> None:
        self._server = server
        self._include = include
        self._who = f"mcp server {server!r}"
        self._host = host
        self._observability = observability
        self._view: tuple[McpGrant, McpGrantView] | None = None

    def tools(self) -> tuple[str, ...]:
        """Return the tools this declaration admits; none while the server is unavailable.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no
                subject; with ``MCP_SERVER_NOT_GRANTED`` when no use case was
                granted the server at startup.
        """
        require_authenticated(self._who)
        view = self._current_view()
        return () if view is None else view.tools()

    async def call(self, tool: str, arguments: Mapping[str, Any], *, expect: type[Any]) -> Any:
        """Call *tool* as the caller of the moment and decode its result into *expect*.

        Raises:
            ExecutionRequired: Outside any execution.
            Unauthenticated: With no caller, or past the caller's delegation.
            AgentRunError: With ``UNAUTHORIZED`` when the caller has no
                subject; with ``MCP_SERVER_NOT_GRANTED`` when no use case was
                granted the server at startup; with ``TOOL_UNAVAILABLE`` when
                it never connected; or as :meth:`McpGrantView.call`.
        """
        require_authenticated(self._who)
        view = self._current_view()
        if view is None:
            raise self._unavailable(tool)
        return await view.call(tool, arguments, expect=expect)

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Call *tool* as the caller of the moment and return its structured content.

        Raises:
            AgentRunError: As :meth:`call`.
        """
        require_authenticated(self._who)
        view = self._current_view()
        if view is None:
            raise self._unavailable(tool)
        return await view.call_untyped(tool, arguments)

    def _current_view(self) -> McpGrantView | None:
        """Look the server's grant up and return this declaration's view of it, or ``None``."""
        grant = self._host.use_case_mcp_grant(self._server)
        if grant is None:
            return None
        cached = self._view
        if cached is None or cached[0] is not grant:
            cached = self._view = (
                grant,
                McpGrantView(
                    span_attributes={"mcp_server": self._server},
                    capability=grant.capability,
                    include=self._include,
                    exclude=(),
                    session=grant.session,
                    catalogue=grant.catalogue,
                    timeout_s=grant.capability.timeout_ms / 1000,
                    observability=self._observability,
                ),
            )
        return cached[1]

    def _unavailable(self, tool: str) -> AgentRunError:
        return AgentRunError(
            AgentRunErrorCode.TOOL_UNAVAILABLE,
            f"tool {tool!r} of mcp server {self._server!r} is unavailable: the "
            "server never connected at start-up",
        )


class McpMarkerResolver(_MarkerResolver[tuple[str, tuple[str, ...]], "_McpHandle"]):
    """Resolves each ``Mcp(server, include=...)`` declaration to its one handle, in either shape.

    A ``Scope.TOOL`` span of a call carries ``mcp_server``, never ``agent``.

    Args:
        host: Live agent host serving this deployment.
        observability: Runtime every handle's span opens on, or ``None``.
    """

    def __init__(self, host: AgentHost, *, observability: Observability | None) -> None:
        super().__init__()
        self._host = host
        self._observability = observability

    def __call__(
        self, server: str, include: tuple[str, ...], identity: Identity | None = None
    ) -> McpHandle:
        """Return the handle of one ``Mcp()`` declaration, built on its first call.

        Args:
            server: MCP server name the declaration names.
            include: Glob patterns of the tools the declaration admits.
            identity: Checked, inside an execution, against the execution
                identity; every call on the handle acts as the execution
                identity of the moment.

        Returns:
            The declaration's handle.

        Raises:
            IdentityFixed: When *identity* is given inside an execution acting
                as another identity.
        """
        return self._handle((server, include), identity, f"mcp server {server!r}")

    @staticmethod
    def _key(declaration: Any) -> tuple[str, tuple[str, ...]]:
        return declaration.server, declaration.include

    def _build(self, key: tuple[str, tuple[str, ...]]) -> _McpHandle:
        server, include = key
        return _McpHandle(
            server=server,
            include=include,
            host=self._host,
            observability=self._observability,
        )


if TYPE_CHECKING:

    def _agent_handle_satisfies_agent_handle(handle: _AgentHandle) -> AgentHandle[Any]:
        return handle

    def _mcp_handle_satisfies_mcp_handle(handle: _McpHandle) -> McpHandle:
        return handle

    def _agent_resolver_satisfies_resolver(resolver: AgentMarkerResolver) -> ExecutionResolver:
        return resolver

    def _mcp_resolver_satisfies_resolver(resolver: McpMarkerResolver) -> ExecutionResolver:
        return resolver


__all__ = ["AgentMarkerResolver", "McpMarkerResolver"]
