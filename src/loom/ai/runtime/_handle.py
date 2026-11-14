"""The handles an ``Agent()`` or ``Mcp()`` marker resolves to, and the resolvers building them.

:class:`AgentMarkerResolver` and :class:`McpMarkerResolver` implement
:class:`~loom.core.execution.ExecutionResolver`: each returns one handle per
declaration, built on its first resolve and holding no identity. Every call on
a handle acts as the execution identity of the moment
(:func:`~loom.core.execution.execution_identity`), so a handle held by a
singleton, or by a task that outlived its execution, serves each caller as
itself and is refused past the caller's delegation. The ``Mcp()`` handle looks
its server's grant up on every call.

Running an ``Agent()`` handle goes through :meth:`AgentRuntime.run`, under the
same admission and policies as a transport-driven run. Three run modes:
``run(prompt)`` decodes into the artefact's declared output;
``run(prompt, expect=X)`` decodes this call only into ``X``, without the
artefact's own output check; ``run_text`` decodes nothing. Both overrides are
refused before the model is called when the artefact's output hook declares
the ``output`` field (:meth:`AgentRuntime.output_hook_shape_bound`).

``mcp()`` and ``sql()`` on an ``Agent()`` handle return the artefact's own
granted views (:mod:`~loom.ai.runtime._grants`), over the session and
connection the model's own capabilities use.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.ai.abc import AgentAnswer, AgentHandle, McpHandle, Prompt, SqlGrantHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode, require_authenticated
from loom.ai.runtime._grants import (
    AgentGrants,
    McpGrant,
    McpGrantView,
    SqlGrantView,
)
from loom.ai.runtime._lifecycle import AgentRuntime
from loom.core.errors import IdentityFixed
from loom.core.execution import ExecutionResolver, current_execution, execution_identity
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.core.sql.service import SqlQueryService


class _AgentHandle:
    """One named agent; each call acts as the execution identity of the moment.

    Args:
        name: Agent name as this deployment's runtime knows it.
        runtime: Live runtime the handle runs through.
        observability: Runtime the handle's own span opens on, or ``None``.
        sql_query_service: The application's query service, handed to every
            :class:`~loom.ai.runtime._grants.SqlGrantView` this handle builds.
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: AgentRuntime,
        observability: ObservabilityRuntime | None,
        sql_query_service: SqlQueryService,
    ) -> None:
        self._name = name
        self._who = f"agent {name!r}"
        self._runtime = runtime
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
                :meth:`~loom.ai.runtime.AgentRuntime.run` unchanged.

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
        caller = require_authenticated(execution_identity(), self._who)
        if expect is not None:
            self._require_shape_allowed()
        span = self._open_span(caller)
        try:
            result = await self._runtime.run(
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
        """Return the engine's own objects for this agent, bound to the caller of the moment.

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
        caller = require_authenticated(execution_identity(), self._who)
        return self._runtime.native(self._name, identity=caller, state=state)

    def mcp(self, server: str) -> McpHandle:
        """Return this artefact's own filtered view of one ``mcp`` grant.

        Raises:
            AgentRunError: With ``MCP_GRANT_UNKNOWN`` when the artefact
                declares no ``mcp`` grant on that server name.
        """
        grants = self._runtime.grants(self._name)
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
        grants = self._runtime.grants(self._name)
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
        return self._runtime.grants(self._name).names

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
        if self._runtime.grants(self._name).output_shape_bound:
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
            subject=caller.subject,
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


class AgentMarkerResolver:
    """Resolves each ``Agent(name)`` declaration to its one handle, in either resolver shape.

    Args:
        runtime: Live agent runtime serving this deployment.
        sql_query_service: The application's query service, handed to every
            ``sql`` grant view a handle builds.
        observability: Runtime every handle's span opens on, or ``None``.
    """

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        sql_query_service: SqlQueryService,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._runtime = runtime
        self._sql_query_service = sql_query_service
        self._observability = observability
        self._handles: dict[str, _AgentHandle] = {}

    def __call__(self, name: str, identity: Identity | None = None) -> AgentHandle[Any]:
        """Return the handle of agent *name*, built on its first call.

        Args:
            name: Agent name as this deployment's runtime knows it.
            identity: Checked, inside an execution, against the execution
                identity; every call on the handle acts as the execution
                identity of the moment.

        Returns:
            The agent's handle.

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
                f"agent {name!r} acts as the execution identity of each call, not as the "
                "identity given; act as another caller with executor.run(UseCase, "
                "identity=...) or ExecutionScope(...)"
            )
        handle = self._handles.get(name)
        if handle is None:
            handle = self._handles[name] = _AgentHandle(
                name=name,
                runtime=self._runtime,
                observability=self._observability,
                sql_query_service=self._sql_query_service,
            )
        return handle

    def resolve(self, declaration: Any, annotation: object) -> AgentHandle[Any]:
        """Return the handle of *declaration*, built on its first resolve.

        Args:
            declaration: The ``Agent(name)`` marker of the parameter.
            annotation: The parameter's annotation; not read.

        Returns:
            The declaration's handle.
        """
        return self._handles.get(declaration.name) or self(declaration.name)


class _McpHandle:
    """One ``Mcp(server, include=...)`` declaration over the server's shared grant.

    Every call looks the grant up (:meth:`AgentRuntime.use_case_mcp_grant`)
    and goes through an :class:`~loom.ai.runtime._grants.McpGrantView` of it
    filtered by this declaration's ``include``, built once per grant.

    Args:
        server: MCP server name the declaration names.
        include: Glob patterns of the tools the declaration admits.
        runtime: Live agent runtime serving this deployment.
        observability: Runtime the view's span opens on, or ``None``.
    """

    def __init__(
        self,
        *,
        server: str,
        include: tuple[str, ...],
        runtime: AgentRuntime,
        observability: ObservabilityRuntime | None,
    ) -> None:
        self._server = server
        self._include = include
        self._who = f"mcp server {server!r}"
        self._runtime = runtime
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
        require_authenticated(execution_identity(), self._who)
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
                it never connected; or as :meth:`McpGrantView.call_as`.
        """
        caller = require_authenticated(execution_identity(), self._who)
        view = self._current_view()
        if view is None:
            raise self._unavailable(tool)
        return await view.call_as(caller, tool, arguments, expect=expect)

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Call *tool* as the caller of the moment and return its structured content.

        Raises:
            AgentRunError: As :meth:`call`.
        """
        caller = require_authenticated(execution_identity(), self._who)
        view = self._current_view()
        if view is None:
            raise self._unavailable(tool)
        return await view.call_untyped_as(caller, tool, arguments)

    def _current_view(self) -> McpGrantView | None:
        """Look the server's grant up and return this declaration's view of it, or ``None``."""
        grant = self._runtime.use_case_mcp_grant(self._server)
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


class McpMarkerResolver:
    """Resolves each ``Mcp(server, include=...)`` declaration to its one handle, in either shape.

    A ``Scope.TOOL`` span of a call carries ``mcp_server``, never ``agent``.

    Args:
        runtime: Live agent runtime serving this deployment.
        observability: Runtime every handle's span opens on, or ``None``.
    """

    def __init__(
        self, runtime: AgentRuntime, *, observability: ObservabilityRuntime | None
    ) -> None:
        self._runtime = runtime
        self._observability = observability
        self._handles: dict[tuple[str, tuple[str, ...]], _McpHandle] = {}

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
        if (
            identity is not None
            and current_execution() is not None
            and identity != execution_identity()
        ):
            raise IdentityFixed(
                f"mcp server {server!r} acts as the execution identity of each call, not as the "
                "identity given; act as another caller with executor.run(UseCase, "
                "identity=...) or ExecutionScope(...)"
            )
        handle = self._handles.get((server, include))
        if handle is None:
            handle = self._handles[server, include] = _McpHandle(
                server=server,
                include=include,
                runtime=self._runtime,
                observability=self._observability,
            )
        return handle

    def resolve(self, declaration: Any, annotation: object) -> McpHandle:
        """Return the handle of *declaration*, built on its first resolve.

        Args:
            declaration: The ``Mcp(server, include=...)`` marker of the parameter.
            annotation: The parameter's annotation; not read.

        Returns:
            The declaration's handle.
        """
        return self._handles.get((declaration.server, declaration.include)) or self(
            declaration.server, declaration.include
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
