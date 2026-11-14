"""The concrete :class:`~loom.ai.abc.McpHandle`/:class:`~loom.ai.abc.AgentHandle`
an ``Mcp()`` or ``Agent()`` marker resolves to.

Two markers, two resolvers, never one constructed by application code:

* ``Agent()`` resolves through :func:`agent_marker_resolver`, handed to the
  executor via
  :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver`.
  Running the resolved handle reuses :meth:`AgentRuntime.run` unchanged, so
  the same concurrency admission and the same policies a transport-driven
  run goes through apply here too; nothing in this module opens a parallel
  path around them.
* ``Mcp()`` resolves through :func:`mcp_marker_resolver`, handed to
  the executor via
  :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_mcp_resolver` — a
  second, differently-typed resolver, not an overload of the first.
  It builds a caller-bound :class:`~loom.ai.runtime._grants.McpGrantView`
  over the server's shared substrate grant
  (:meth:`AgentRuntime.use_case_mcp_grant`), filtered by the *resolving
  binding's own* ``include`` — never by the shared grant's, which carries
  none (see :mod:`~loom.ai.runtime._grants`'s module docstring). A server
  tolerated
  unreachable under ``ai.remote_clients: optional`` resolves to
  :class:`_UnavailableMcpHandle` instead.

Three run modes (``Agent()`` path only): ``run(prompt)`` decodes into
the artefact's own declared output; ``run(prompt, expect=X)`` decodes this
call only into ``X``, skipping the artefact's own output check, which is
compiled against the declared schema and cannot be asked to validate another
one; ``run_text`` decodes nothing at all. Both overrides are refused before
the model is called when the artefact's output hook declares the ``output``
field — see :meth:`AgentRuntime.output_hook_shape_bound` — because that
hook's command is compiled against the declared shape and cannot be handed
another one. A hook declaring only conversation-bookkeeping fields is
unaffected and keeps running with every mode.

``mcp()`` and ``sql()`` on the ``Agent()`` path return the
artefact's own granted views, built by :mod:`~loom.ai.runtime._grants`: the
same filtered session and the same bounded connection the model's own
capabilities run over, never a second one.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from loom.ai.abc import AgentAnswer, AgentHandle, McpHandle, Prompt, SqlGrantHandle
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._grants import AgentGrants, McpGrantView, SqlGrantView
from loom.ai.runtime._lifecycle import AgentRuntime
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.observability.span import LoomSpan
from loom.core.sql.service import SqlQueryService


class _BoundAgentHandle:
    """One named agent, bound to the caller of a single use-case execution.

    Args:
        name: Agent name as this deployment's runtime knows it.
        runtime: Live runtime the handle runs through.
        identity: Verified caller of the execution that resolved this
            handle. Never read from anywhere else, and never reassignable —
            an ``AgentHandle`` closes over one identity for its whole life.
        observability: Runtime the handle's own span opens on, or ``None`` to
            open no span. ``None`` when no observability runtime is
            registered, matching every other optional-observability
            collaborator in this codebase (e.g.
            :class:`~loom.core.sql.caller_bound.CallerBoundSql`).
        sql_query_service: The application's single, already-resolved query
            service, handed to every :class:`~loom.ai.runtime._grants.SqlGrantView`
            this handle builds — this handle never reaches into the runtime
            for it, and the runtime holds no container to reach into.
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: AgentRuntime,
        identity: Identity,
        observability: ObservabilityRuntime | None,
        sql_query_service: SqlQueryService,
    ) -> None:
        self._name = name
        self._runtime = runtime
        self._identity = identity
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
            AgentRunError: With ``UNAUTHORIZED`` when this handle's identity
                is anonymous — checked before the model is called; with
                ``AGENT_RUN_SHAPE_WITH_HOOK`` when ``expect`` is given and the
                artefact's output hook declares the ``output`` field — also
                checked before the model is called; with ``STATE_UNDECLARED``
                when ``state`` is given and the artefact declares no state
                shape; or with whatever code the run itself failed with.
        """
        self._require_authenticated()
        if expect is not None:
            self._require_shape_allowed()
        span = self._open_span()
        try:
            result = await self._runtime.run(
                self._name,
                prompt,
                identity=self._identity,
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
        answer = await self.run(prompt, expect=str, conversation_id=conversation_id, state=state)
        return answer

    def native(self, *, state: object | None = None) -> object:
        """Return the engine's own objects for this agent, bound to this caller.

        Opens no span and takes no admission slot: nothing is run here, and
        what the caller then does with the returned objects is outside this
        runtime's supervision. See :meth:`~loom.ai.abc.AgentHandle.native`.

        Args:
            state: This run's state, resolved against the artefact's declared
                shape exactly as :meth:`run` resolves it.

        Returns:
            The engine-native objects, in the engine's own types.

        Raises:
            AgentRunError: With ``UNAUTHORIZED`` when this handle's identity is
                anonymous — the same refusal :meth:`run` makes, so the hatch
                never yields a bundle the run path would have refused; with
                ``STATE_UNDECLARED`` or ``STATE_REQUIRED`` when *state* does
                not match the artefact's declared shape.
            RuntimeError: When this handle's runtime was never entered.
            NotImplementedError: When the engine serving this agent declares
                no native form.
        """
        self._require_authenticated()
        return self._runtime.native(self._name, identity=self._identity, state=state)

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
            identity=self._identity,
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
            identity=self._identity,
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

    def _require_authenticated(self) -> None:
        """Refuse an anonymous caller before any network call.

        Mirrors :func:`loom.ai.engines.pydantic_ai._guards.require_authenticated`:
        the same refusal, for the same reason, on the path a use case opens
        instead of the path a tool call opens.
        """
        if not self._identity.is_authenticated:
            raise AgentRunError(
                AgentRunErrorCode.UNAUTHORIZED,
                f"agent {self._name!r} requires an authenticated caller",
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

    def _open_span(self) -> LoomSpan | None:
        """Open this run's own span; the transport opens none for this path.

        ``AgentRuntime`` itself emits no span (its own docstring states the
        transport owns that), and a marker-driven run has no transport at
        all — so, unlike the HTTP and A2A surfaces, this is the only place
        that can open one. Uses :meth:`~ObservabilityRuntime.open_span`
        rather than the lexical-scope :meth:`~ObservabilityRuntime.span`
        because the interaction id, annotated on close, is only known once
        the run — awaited further down in :meth:`run` — has returned.
        """
        if self._observability is None:
            return None
        return self._observability.open_span(
            Scope.AGENT,
            "agent_run",
            agent=self._name,
            subject=self._identity.subject,
            mechanism=self._identity.mechanism,
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


def agent_marker_resolver(
    runtime: AgentRuntime,
    *,
    sql_query_service: SqlQueryService,
    observability: ObservabilityRuntime | None,
) -> Any:
    """Build the resolver ``RuntimeExecutor.bind_agent_resolver`` takes.

    Kept a plain factory function, not a class, because it closes over
    nothing but the collaborators every handle it builds needs — the live
    runtime, the application's single query service and the observability
    runtime the handle's span opens on. ``sql_query_service`` is resolved by
    the caller, once, the same way the composition root resolves it for
    every other consumer: a handle never reaches into a container for
    it, and ``AgentRuntime`` holds no container to reach into.

    Args:
        runtime: Live agent runtime serving this deployment.
        sql_query_service: The application's single query service, handed to
            every ``sql`` grant view a resolved handle builds.
        observability: Runtime every handle's span opens on, or ``None``.

    Returns:
        A callable of ``(agent_name, identity) -> AgentHandle``, the shape
        :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_agent_resolver`
        requires. Returned as ``Any`` because that boundary method lives in
        ``loom.core`` and is typed to stay ignorant of this pillar.
    """

    def _resolve(name: str, identity: Identity) -> _BoundAgentHandle:
        return _BoundAgentHandle(
            name=name,
            runtime=runtime,
            identity=identity,
            observability=observability,
            sql_query_service=sql_query_service,
        )

    return _resolve


class _UnavailableMcpHandle:
    """The handle an ``Mcp()`` marker resolves to when its server never connected.

    Reachable only under ``ai.remote_clients: optional``: a server
    the marker granted but whose connection start-up tolerated as
    unreachable (:meth:`AgentRuntime.use_case_mcp_grant` returning ``None``).
    This is a different failure from a call naming a tool outside the
    marker's own ``include`` — that one is ``TOOL_UNKNOWN``, raised by
    :class:`~loom.ai.runtime._grants.McpGrantView` against a live catalogue.
    Here there is no catalogue to check a tool name against: the server is
    configured and granted, it simply never opened, so every call raises the
    same ``TOOL_UNAVAILABLE`` regardless of which tool was named.
    """

    def __init__(self, server: str) -> None:
        self._server = server

    def tools(self) -> tuple[str, ...]:
        """Return no tools: an unreachable server publishes none to check against."""
        return ()

    async def call(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        *,
        expect: type[Any],
    ) -> Any:
        """Raise ``TOOL_UNAVAILABLE``; *arguments* and *expect* are never read."""
        del arguments, expect
        raise self._unavailable(tool)

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Raise ``TOOL_UNAVAILABLE``; *arguments* is never read."""
        del arguments
        raise self._unavailable(tool)

    def _unavailable(self, tool: str) -> AgentRunError:
        return AgentRunError(
            AgentRunErrorCode.TOOL_UNAVAILABLE,
            f"tool {tool!r} of mcp server {self._server!r} is unavailable: the "
            "server never connected at start-up",
        )


def mcp_marker_resolver(
    runtime: AgentRuntime,
    *,
    observability: ObservabilityRuntime | None,
) -> Any:
    """Build the resolver ``RuntimeExecutor.bind_mcp_resolver`` takes.

    Mirrors :func:`agent_marker_resolver`'s shape, over a different runtime
    query: each call reads the server-keyed shared substrate grant
    (:meth:`AgentRuntime.use_case_mcp_grant`) and filters it by *this call's
    own* ``include`` — the resolving ``Mcp()`` binding's, passed in by the
    executor — never by the shared grant's own capability, whose
    ``include``/``exclude`` are explicitly empty (see
    :mod:`~loom.ai.runtime._grants`). Two markers naming the same server
    therefore share one live session and one catalogue, and still resolve
    to two independently filtered views.

    A server tolerated unreachable resolves to :class:`_UnavailableMcpHandle`
    instead of raising: the marker is granted, the connection simply never
    opened, and that is a call-time failure, not a bind-time one.

    Args:
        runtime: Live agent runtime serving this deployment.
        observability: Runtime the resolved view's own span opens on, or
            ``None``.

    Returns:
        A callable of ``(server, include, identity) -> McpHandle``, the
        shape :meth:`~loom.core.engine.executor.RuntimeExecutor.bind_mcp_resolver`
        requires. Returned as ``Any`` for the same boundary reason
        :func:`agent_marker_resolver` is.

    Raises:
        RuntimeError: From the returned callable, when the runtime was
            never entered — :meth:`AgentRuntime.use_case_mcp_grant`'s own
            guard, distinct from the tolerated-unreachable case above.

    Note:
        The resolved view's ``span_attributes`` carry ``{"mcp_server": ...}``,
        named for what the value actually is. The design sketched
        ``{"use_case": ...}``, but this resolver is typed to receive only the
        marker's server, its ``include`` and the caller: the executor
        never crosses the declaring use case's own name over that boundary,
        so that key could only have carried the server under a name that
        denies it. Attributing a tool span to the use case that asked for it
        would take the executor passing that name through, a change to the
        resolver's signature that this does not make.

        Either way this path's spans stay distinguishable from the agent
        path's, which carry ``{"agent": ...}``. A ``Scope.TOOL`` span of an
        MCP call therefore carries one key or the other, never both — see
        "What the spans carry" in ``docs/ai/observability.md``.
    """

    def _resolve(server: str, include: tuple[str, ...], identity: Identity) -> McpHandle:
        grant = runtime.use_case_mcp_grant(server)
        if grant is None:
            return _UnavailableMcpHandle(server)
        return McpGrantView(
            span_attributes={"mcp_server": server},
            capability=grant.capability,
            include=include,
            exclude=(),
            session=grant.session,
            catalogue=grant.catalogue,
            timeout_s=grant.capability.timeout_ms / 1000,
            identity=identity,
            observability=observability,
        )

    return _resolve


if TYPE_CHECKING:

    def _bound_handle_satisfies_agent_handle(handle: _BoundAgentHandle) -> AgentHandle[Any]:
        return handle

    def _unavailable_handle_satisfies_mcp_handle(handle: _UnavailableMcpHandle) -> McpHandle:
        return handle


__all__ = ["agent_marker_resolver", "mcp_marker_resolver"]
