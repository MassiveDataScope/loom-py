from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import msgspec

from loom.ai._filters import select_names
from loom.ai.abc import (
    AgentAnswer,
    AgentHandle,
    AgentUsage,
    DepsFactory,
    McpHandle,
    Prompt,
    SqlGrantHandle,
)
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.engine.plan import ExecutionPlan
from loom.core.errors import InvalidUse
from loom.core.execution import current_execution, execution_identity
from loom.core.identity import Identity
from loom.core.stores.repository import RepoFor
from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")

_DEFAULT_AGENT_USAGE = AgentUsage(input_tokens=0, output_tokens=0, requests=1, duration_ms=0)
"""Usage stamped on a scripted answer when the test does not name one: zero
cost, one request, so a double never claims work it did not do."""


@dataclass(frozen=True, slots=True)
class RecordedRun:
    """One call :meth:`AgentHandleDouble.run` received."""

    prompt: Prompt
    expect: type[Any] | None
    conversation_id: str | None
    state: object | None = None


@dataclass(frozen=True, slots=True)
class RecordedRunText:
    """One call :meth:`AgentHandleDouble.run_text` received."""

    prompt: Prompt
    conversation_id: str | None
    state: object | None = None


@dataclass(frozen=True, slots=True)
class RecordedToolCall:
    """One call :meth:`McpHandleDouble.call` or :meth:`McpHandleDouble.call_untyped` received."""

    tool: str
    arguments: Mapping[str, Any]
    typed: bool


@dataclass(frozen=True, slots=True)
class RecordedQuery:
    """One call :meth:`SqlGrantHandleDouble.query` received."""

    statement: str
    parameters: Mapping[str, Any] | None


_ScriptedT = TypeVar("_ScriptedT")
_DoubleT = TypeVar("_DoubleT")


def _next_scripted(queue: list[_ScriptedT], failure: str) -> _ScriptedT:
    """Return the next scripted value, refusing an unscripted call.

    A single scheduled value is never consumed: it keeps answering every
    call, the common case of a use case running the same grant more than
    once. Two or more scheduled values are consumed in call order.
    """
    if not queue:
        raise AssertionError(failure)
    if len(queue) > 1:
        return queue.pop(0)
    return queue[0]


class McpHandleDouble:
    """In-memory double for :class:`~loom.ai.abc.McpHandle`.

    No network call is ever made. :meth:`call` and :meth:`call_untyped`
    return a result scripted per tool name through :meth:`on_call` /
    :meth:`on_call_untyped`; every invocation is recorded on :attr:`calls`
    so a test can assert what the use case asked of it.

    Args:
        server: Server name this double stands in for, named in its
            refusal messages.
    """

    def __init__(self, server: str) -> None:
        self._server = server
        self._tools: tuple[str, ...] = ()
        self._typed_results: dict[str, Any] = {}
        self._untyped_results: dict[str, Mapping[str, Any]] = {}
        self.calls: list[RecordedToolCall] = []

    def with_tools(self, *tools: str) -> McpHandleDouble:
        """Script the tool names :meth:`tools` returns.

        Returns:
            ``self`` for chaining.
        """
        self._tools = tools
        return self

    def on_call(self, tool: str, result: Any) -> McpHandleDouble:
        """Script the decoded result the next typed :meth:`call` for *tool* returns.

        Returns:
            ``self`` for chaining.
        """
        self._typed_results[tool] = result
        return self

    def on_call_untyped(self, tool: str, result: Mapping[str, Any]) -> McpHandleDouble:
        """Script the raw result the next :meth:`call_untyped` for *tool* returns.

        Returns:
            ``self`` for chaining.
        """
        self._untyped_results[tool] = result
        return self

    def tools(self) -> tuple[str, ...]:
        """Return the tool names scripted through :meth:`with_tools`."""
        return self._tools

    async def call(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        *,
        expect: type[Any],
    ) -> Any:
        """Record the call and return the result scripted for *tool*.

        Raises:
            AssertionError: If no result was scheduled for *tool* through
                :meth:`on_call`.
        """
        del expect
        self.calls.append(RecordedToolCall(tool=tool, arguments=arguments, typed=True))
        if tool not in self._typed_results:
            raise AssertionError(
                f"mcp {self._server!r} double received call({tool!r}, ...) but no result "
                f"was scheduled; call .on_call({tool!r}, ...) before running the use case"
            )
        return self._typed_results[tool]

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Record the call and return the raw result scripted for *tool*.

        Raises:
            AssertionError: If no result was scheduled for *tool* through
                :meth:`on_call_untyped`.
        """
        self.calls.append(RecordedToolCall(tool=tool, arguments=arguments, typed=False))
        if tool not in self._untyped_results:
            raise AssertionError(
                f"mcp {self._server!r} double received call_untyped({tool!r}, ...) but no "
                f"result was scheduled; call .on_call_untyped({tool!r}, ...) before running "
                "the use case"
            )
        return self._untyped_results[tool]


class _FilteredMcpDouble:
    """Narrows a published :class:`McpHandleDouble` by one binding's ``include``.

    Bound to an ``Mcp(server, include=...)`` marker parameter through
    :meth:`UseCaseTest.with_mcp`. Production refuses a tool call on two
    distinct grounds before ever reaching the network
    (``McpGrantView._require_tool``): the tool falls outside the
    resolving signature's own ``include``, or it falls inside it but the
    server's live catalogue never published it. This wrapper enforces
    both against the double it wraps, so a test that calls an excluded or
    unpublished tool fails the same way production would, instead of
    passing against a double that never checked.

    Args:
        double: Published double whose scripted tools and results this
            view narrows and delegates to.
        server: Server name the resolving ``Mcp(server, ...)`` marker
            declared, named in the refusal message — not necessarily the
            name the double itself was constructed with.
        include: The resolving binding's own ``include`` glob patterns.
    """

    def __init__(self, double: McpHandleDouble, *, server: str, include: tuple[str, ...]) -> None:
        self._double = double
        self._server = server
        self._admitted = select_names(double.tools(), include=include, exclude=())

    def tools(self) -> tuple[str, ...]:
        """Return the scripted tool names this binding's ``include`` admits."""
        return self._admitted

    async def call(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        *,
        expect: type[Any],
    ) -> Any:
        """Refuse *tool* outside this view, else delegate to the wrapped double.

        Raises:
            AgentRunError: With code ``TOOL_UNKNOWN`` if *tool* is not
                among :meth:`tools`, naming the admitted tools — the same
                refusal production makes before any network call.
            AssertionError: If *tool* is admitted but the wrapped double
                has no result scheduled for it through :meth:`McpHandleDouble.on_call`.
        """
        self._require_admitted(tool)
        return await self._double.call(tool, arguments, expect=expect)

    async def call_untyped(self, tool: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        """Refuse *tool* outside this view, else delegate to the wrapped double.

        Raises:
            AgentRunError: With code ``TOOL_UNKNOWN`` if *tool* is not
                among :meth:`tools`, naming the admitted tools — the same
                refusal production makes before any network call.
            AssertionError: If *tool* is admitted but the wrapped double
                has no result scheduled for it through
                :meth:`McpHandleDouble.on_call_untyped`.
        """
        self._require_admitted(tool)
        return await self._double.call_untyped(tool, arguments)

    def _require_admitted(self, tool: str) -> None:
        if tool in self._admitted:
            return
        granted = ", ".join(sorted(self._admitted)) or "none"
        raise AgentRunError(
            AgentRunErrorCode.TOOL_UNKNOWN,
            f"mcp server {self._server!r} grants no tool named {tool!r}; "
            f"tools this grant admits: {granted}",
        )


class SqlGrantHandleDouble:
    """In-memory double for :class:`~loom.ai.abc.SqlGrantHandle`.

    No connection is ever opened. :meth:`query` returns rows scripted
    through :meth:`on_query`, and every call is recorded on :attr:`calls`.

    Args:
        connection: Connection name this double stands in for, named in its
            refusal message.
    """

    def __init__(self, connection: str) -> None:
        self._connection = connection
        self._rows: Sequence[Mapping[str, Any]] | None = None
        self.calls: list[RecordedQuery] = []

    def on_query(self, rows: Sequence[Mapping[str, Any]]) -> SqlGrantHandleDouble:
        """Script the rows every :meth:`query` call returns.

        Returns:
            ``self`` for chaining.
        """
        self._rows = rows
        return self

    async def query(
        self,
        statement: str,
        *,
        parameters: Mapping[str, Any] | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Record the call and return the rows scripted through :meth:`on_query`.

        Raises:
            AssertionError: If no rows were scheduled through :meth:`on_query`.
        """
        self.calls.append(RecordedQuery(statement=statement, parameters=parameters))
        if self._rows is None:
            raise AssertionError(
                f"sql {self._connection!r} double received query(...) but no rows were "
                "scheduled; call .on_query(...) before running the use case"
            )
        return self._rows


@dataclass(frozen=True, slots=True)
class DepsFactoryBundleDouble:
    """Dependency bundle :class:`DepsFactoryDouble` builds for one invocation.

    Args:
        identity: Verified caller of the invocation.
        container: Application container holding the singleton services.
        state: The invocation's state, a mapping normalised against the
            artefact's declared shape, or ``None`` for an artefact that
            declares no state or a call that supplies none.
    """

    identity: Identity
    container: LoomContainer
    state: Mapping[str, Any] | None = None


class DepsFactoryDouble:
    """In-memory double for :class:`~loom.ai.abc.DepsFactory`.

    Builds a :class:`DepsFactoryBundleDouble` exposing ``identity``,
    ``container`` and ``state`` in the same shape the composition root's own
    factory builds, so a test exercising a capability call directly against
    an engine sees the caller and the container exactly where a guard reads
    them structurally.
    """

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the dependency bundle for one invocation.

        Args:
            identity: Verified caller of this invocation.
            container: Application container holding the singleton services.
            state: The invocation's state, a mapping normalised against the
                artefact's declared shape, or ``None`` for an artefact that
                declares no state or a call that supplies none.

        Returns:
            The bundle a capability call reads ``identity``, ``container``
            and ``state`` off of.
        """
        return DepsFactoryBundleDouble(identity=identity, container=container, state=state)


class AgentHandleDouble:
    """In-memory double for :class:`~loom.ai.abc.AgentHandle`.

    Bound to a use case's parameter through :meth:`UseCaseTest.with_agent`.

    Stands in for the handle a real ``Agent()`` marker resolves to: no
    network, model or database call is ever made. Every run mode returns an
    answer scripted through :meth:`on_run` / :meth:`on_run_text`; every
    grant view is a further double reached through :meth:`mcp` / :meth:`sql`
    and cached, so scheduling its answers once configures every call the use
    case makes through it. Every call this handle itself receives is
    recorded on :attr:`run_calls` / :attr:`run_text_calls`, so a test can
    assert what the use case asked of it.

    Args:
        name: Agent name as the ``Agent(name)`` marker declares it, named in
            this double's refusal messages.

    Example::

        triage = AgentHandleDouble("incident-triage").on_run(SeverityAssessment(severity=5))
        result = await (
            UseCaseTest(TriageIncidentUseCase())
            .with_caller(identity)
            .with_agent("incident-triage", triage)
            .with_params(incident_id="INC-1")
            .run()
        )
        assert triage.run_calls[0].prompt == "Assess INC-1."
    """

    def __init__(self, name: str) -> None:
        self._name = name
        self._run_answers: list[AgentAnswer[Any]] = []
        self._run_text_answers: list[AgentAnswer[str]] = []
        self._mcp: dict[str, McpHandleDouble] = {}
        self._sql: dict[str, SqlGrantHandleDouble] = {}
        self._declared: tuple[str, ...] | None = None
        self.run_calls: list[RecordedRun] = []
        self.run_text_calls: list[RecordedRunText] = []

    def on_run(
        self,
        output: Any,
        *,
        usage: AgentUsage | None = None,
        interaction_id: str | None = None,
        provider_details: Mapping[str, Any] | None = None,
    ) -> AgentHandleDouble:
        """Schedule the next :meth:`run` call's answer, declared or per-run shape alike.

        Args:
            output: The answer the next call returns.
            usage: Its accounting; a default when omitted.
            interaction_id: Its interaction id, when the test pins one.
            provider_details: What the provider reported about the answer,
                for a use case that reads a decision model's confidence.

        Returns:
            ``self`` for chaining.
        """
        self._run_answers.append(
            AgentAnswer(
                output=output,
                usage=usage or _DEFAULT_AGENT_USAGE,
                interaction_id=interaction_id,
                provider_details=provider_details,
            )
        )
        return self

    def on_run_text(
        self,
        text: str,
        *,
        usage: AgentUsage | None = None,
        interaction_id: str | None = None,
    ) -> AgentHandleDouble:
        """Schedule the next :meth:`run_text` call's answer.

        Returns:
            ``self`` for chaining.
        """
        self._run_text_answers.append(
            AgentAnswer(
                output=text, usage=usage or _DEFAULT_AGENT_USAGE, interaction_id=interaction_id
            )
        )
        return self

    async def run(
        self,
        prompt: Prompt,
        *,
        expect: type[Any] | None = None,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[Any]:
        """Record the call and return the answer scripted through :meth:`on_run`.

        Raises:
            AssertionError: If no answer was scheduled through :meth:`on_run`.
        """
        self.run_calls.append(
            RecordedRun(prompt=prompt, expect=expect, conversation_id=conversation_id, state=state)
        )
        return _next_scripted(
            self._run_answers,
            f"agent {self._name!r} double received run(...) but no answer was scheduled; "
            "call .on_run(...) before running the use case",
        )

    async def run_text(
        self,
        prompt: Prompt,
        *,
        conversation_id: str | None = None,
        state: object | None = None,
    ) -> AgentAnswer[str]:
        """Record the call and return the answer scripted through :meth:`on_run_text`.

        Raises:
            AssertionError: If no answer was scheduled through :meth:`on_run_text`.
        """
        self.run_text_calls.append(
            RecordedRunText(prompt=prompt, conversation_id=conversation_id, state=state)
        )
        return _next_scripted(
            self._run_text_answers,
            f"agent {self._name!r} double received run_text(...) but no answer was scheduled; "
            "call .on_run_text(...) before running the use case",
        )

    def native(self, *, state: object | None = None) -> object:
        """Refuse the escape hatch: a double has no engine behind it.

        :meth:`~loom.ai.abc.AgentHandle.native` hands back the objects of a
        live engine, and there is none here. A use case that reaches for it
        is driving the underlying library itself, which is exactly the code
        this double cannot stand in for: test it against a real runtime.

        Raises:
            NotImplementedError: Always.
        """
        del state
        raise NotImplementedError(
            f"agent {self._name!r} double received native(); a double has no engine behind it, "
            "so a use case driving the engine itself needs a real runtime to test against"
        )

    def mcp(self, server: str) -> McpHandleDouble:
        """Return this agent's own double for the ``mcp`` grant named *server*.

        Built once and cached: repeated calls with the same name return the
        same double.
        """
        return self._mcp.setdefault(server, McpHandleDouble(server))

    def sql(self, connection: str) -> SqlGrantHandleDouble:
        """Return this agent's own double for the ``sql`` grant named *connection*.

        Built once and cached: repeated calls with the same name return the
        same double.
        """
        return self._sql.setdefault(connection, SqlGrantHandleDouble(connection))

    def with_grants(self, *names: str) -> AgentHandleDouble:
        """Declare the grant names this double reports, and refuse the rest.

        Without it the double reports what has been reached, which is not what
        the real handle promises: there, the listing names what the artefact
        declares, whether or not anything used it. A test that asserts a grant
        is available would pass against the double and prove nothing about
        production, so declaring the set here makes the two agree.

        Args:
            names: Every server and connection this agent declares.

        Returns:
            This double, for chaining.
        """
        self._declared = names
        return self

    def grants(self) -> tuple[str, ...]:
        """Return the declared grant names, matching what the real handle reports.

        Falls back to what has been reached when nothing was declared, so a
        test that does not care keeps working.
        """
        if self._declared is not None:
            return self._declared
        return (*self._mcp, *self._sql)


if TYPE_CHECKING:

    def _mcp_contract(double: McpHandleDouble) -> McpHandle:
        return double

    def _filtered_mcp_contract(double: _FilteredMcpDouble) -> McpHandle:
        return double

    def _sql_contract(double: SqlGrantHandleDouble) -> SqlGrantHandle:
        return double

    def _deps_contract(double: DepsFactoryDouble) -> DepsFactory:
        return double

    def _agent_contract(double: AgentHandleDouble) -> AgentHandle[Any]:
        return double


class _Doubles(Generic[_DoubleT]):
    """The doubles a harness registered for one marker kind, keyed by the declared name.

    Args:
        declarer: What declares the markers, named in a refusal.
        harness: The harness class whose method registers a double, named in a refusal.
        doubles: The registered doubles by declared name.
    """

    __slots__ = ("_declarer", "_doubles", "_harness")

    def __init__(self, declarer: str, harness: str, doubles: Mapping[str, _DoubleT]) -> None:
        self._declarer = declarer
        self._harness = harness
        self._doubles = doubles

    def _registered(self, key: str, spelling: str, method: str) -> _DoubleT:
        """Return the double registered under *key*.

        Inside an execution the execution's caller is read first; a singleton
        built outside any execution reads none.

        Args:
            key: The name the marker declares.
            spelling: The marker as written, named in the refusal.
            method: The harness method registering a double for *key*.

        Returns:
            The registered double.

        Raises:
            loom.core.errors.Unauthenticated: Inside an execution acting with no caller.
            RuntimeError: When no double is registered under *key*.
        """
        if current_execution() is not None:
            execution_identity()
        double = self._doubles.get(key)
        if double is None:
            raise InvalidUse(
                f"{self._declarer} declares {spelling} but no double was registered; "
                f"call {self._harness}.{method}({key!r}, ...) before running it."
            )
        return double


class _AgentDoubles(_Doubles[AgentHandleDouble]):
    """Resolves each ``Agent(name)`` declaration to the double registered for its name."""

    __slots__ = ()

    def resolve(self, declaration: Any, annotation: object) -> AgentHandleDouble:
        """Return the double registered for ``declaration.name``.

        Raises:
            loom.core.errors.Unauthenticated: Inside an execution acting with no caller.
            RuntimeError: When no double is registered for the name.
        """
        name = declaration.name
        return self._registered(name, f"Agent({name!r})", "with_agent")


class _McpDoubles(_Doubles[McpHandleDouble]):
    """Resolves each ``Mcp(server, include=...)`` declaration to its server's double, narrowed."""

    __slots__ = ()

    def resolve(self, declaration: Any, annotation: object) -> _FilteredMcpDouble:
        """Return the double registered for ``declaration.server``, narrowed to its ``include``.

        Raises:
            loom.core.errors.Unauthenticated: Inside an execution acting with no caller.
            RuntimeError: When no double is registered for the server.
        """
        server = declaration.server
        double = self._registered(server, f"Mcp({server!r}, ...)", "with_mcp")
        return _FilteredMcpDouble(double, server=server, include=declaration.include)


class UseCaseTest(Generic[ResultT]):
    """Fluent test harness for executing UseCases without HTTP or framework overhead.

    Builds and runs the real ExecutionPlan — no shortcuts or mocking of the
    pipeline. Designed for unit and integration tests that must exercise
    computes, rules, and load steps in full.

    Args:
        use_case: Constructed UseCase instance to test.

    Example::

        result = await (
            UseCaseTest(UpdateUserUseCase(repo=fake_repo))
            .with_params(user_id=1)
            .with_input(email="new@example.com")
            .run()
        )
    """

    def __init__(self, use_case: UseCase[Any, ResultT]) -> None:
        self._use_case = use_case
        self._params: dict[str, Any] = {}
        self._payload: dict[str, Any] | None = None
        self._load_overrides: dict[type[Any], Any] = {}
        self._dependencies: dict[type[Any], Any] = {}
        self._identity: Identity | None = None
        self._agent_doubles: dict[str, AgentHandleDouble] = {}
        self._mcp_doubles: dict[str, McpHandleDouble] = {}

    def with_params(self, **kwargs: Any) -> UseCaseTest[ResultT]:
        """Set primitive parameter values bound by name.

        Args:
            **kwargs: Parameter names and values matching the UseCase's
                non-Input, non-Load parameters.

        Returns:
            ``self`` for chaining.
        """
        self._params.update(kwargs)
        return self

    def with_input(self, **kwargs: Any) -> UseCaseTest[ResultT]:
        """Set raw payload fields for command construction.

        The payload is passed to the Command's ``from_payload`` method.
        Use ``with_command`` if you have a pre-built Command instance.

        Args:
            **kwargs: Payload fields matching the Command struct.

        Returns:
            ``self`` for chaining.
        """
        if self._payload is None:
            self._payload = {}
        self._payload.update(kwargs)
        return self

    def with_command(self, cmd: Any) -> UseCaseTest[ResultT]:
        """Set a pre-built Command instance as the execution payload.

        Serializes the command via ``msgspec.to_builtins`` so it is
        compatible with the standard ``from_payload`` pipeline.

        Args:
            cmd: A ``Command`` (msgspec.Struct) instance.

        Returns:
            ``self`` for chaining.
        """
        self._payload = msgspec.to_builtins(cmd)
        return self

    def with_loaded(self, entity_type: type[Any], entity: Any) -> UseCaseTest[ResultT]:
        """Pre-load an entity, bypassing repository calls for this type.

        Args:
            entity_type: The entity class used in the ``LoadById()`` marker.
            entity: The pre-loaded entity instance.

        Returns:
            ``self`` for chaining.
        """
        self._load_overrides[entity_type] = entity
        return self

    def with_deps(self, entity_type: type[Any], repo: Any) -> UseCaseTest[ResultT]:
        """Register a repository for a given entity type.

        Used when the UseCase has ``LoadById()`` steps that require a repo.
        ``with_loaded`` takes precedence over ``with_deps`` for the same type.

        Args:
            entity_type: The entity class used in the ``LoadById()`` marker.
            repo: Repository implementing ``get_by_id``.

        Returns:
            ``self`` for chaining.
        """
        self._dependencies[entity_type] = repo
        return self

    def with_caller(self, identity: Identity) -> UseCaseTest[ResultT]:
        """Run the use case as *identity*, filling its ``Caller()`` parameter.

        Without this call a use case declaring ``Caller()`` fails closed, which
        is the point: an authorization test must state whose request it is.

        Args:
            identity: Caller the execution runs as.  Pass
                :data:`~loom.core.identity.identity.ANONYMOUS` to exercise the
                unauthenticated path explicitly.

        Returns:
            ``self`` for chaining.
        """
        self._identity = identity
        return self

    def with_agent(self, name: str, double: AgentHandleDouble) -> UseCaseTest[ResultT]:
        """Bind *double* to the ``Agent(name)`` marker parameter named *name*.

        Without this call a use case declaring ``Agent(name)`` fails closed
        when run, naming the use case and the agent — the same fail-closed
        design :meth:`with_caller` applies to ``Caller()``, extended to the
        whole handle: every run mode and every grant view, not only the
        execution itself, so nothing that reaches the agent can run without
        a network, a model or a database standing in.

        Args:
            name: Agent name exactly as the ``Agent(name)`` marker declares
                it in the use case under test.
            double: Pre-built :class:`AgentHandleDouble` — script its
                answers with :meth:`~AgentHandleDouble.on_run` /
                :meth:`~AgentHandleDouble.on_run_text` and its grant views
                with :meth:`~AgentHandleDouble.mcp` / :meth:`~AgentHandleDouble.sql`
                before passing it here.

        Returns:
            ``self`` for chaining.
        """
        self._agent_doubles[name] = double
        return self

    def with_mcp(self, server: str, double: McpHandleDouble) -> UseCaseTest[ResultT]:
        """Bind *double* to the ``Mcp(server, include=...)`` marker parameter named *server*.

        Without this call a use case declaring ``Mcp(server, ...)`` fails
        closed when run, naming the use case and the server — the same
        fail-closed design :meth:`with_agent` applies to ``Agent(name)``.

        The double is narrowed to the resolving binding's own ``include``
        before the use case ever sees it: a scripted tool outside
        ``include``, or inside it but never scripted through
        :meth:`~McpHandleDouble.with_tools`, is refused with the same
        ``AgentRunError(TOOL_UNKNOWN)`` production raises before any
        network call.

        Args:
            server: Server name exactly as the ``Mcp(server, ...)`` marker
                declares it in the use case under test.
            double: Pre-built :class:`McpHandleDouble` — script its tools
                and results with :meth:`~McpHandleDouble.with_tools` /
                :meth:`~McpHandleDouble.on_call` /
                :meth:`~McpHandleDouble.on_call_untyped` before passing it
                here.

        Returns:
            ``self`` for chaining.
        """
        self._mcp_doubles[server] = double
        return self

    def with_main_repo(self, repo: RepoFor[Any]) -> UseCaseTest[ResultT]:
        """Inject the main repository dependency into the UseCase instance.

        This is useful for unit tests of ``UseCase[TModel, TResult]`` where
        the core logic reads from ``self.main_repo``.

        Args:
            repo: Repository instance compatible with the UseCase's main model.

        Returns:
            ``self`` for chaining.
        """
        self._use_case.main_repo = repo
        return self

    async def run(self) -> ResultT:
        """Compile and execute the UseCase through the full pipeline.

        Returns:
            The result produced by the UseCase.

        Raises:
            loom.core.errors.RuleViolationsError: If one or more rule steps fail.
            NotFound: If a Load step finds no entity.
            loom.core.errors.Unauthenticated: If the UseCase declares
                ``Caller()``, ``Agent(name)`` or ``Mcp(server, ...)`` and
                no :meth:`with_caller` was set.
            RuntimeError: If the UseCase declares ``Agent(name)`` and no
                matching :meth:`with_agent` call registered a double for
                *name*, or declares ``Mcp(server, ...)`` and no matching
                :meth:`with_mcp` call registered a double for *server*.
            loom.core.engine.compiler.CompilationError: If the UseCase fails
                structural validation.
        """
        declarer = f"{type(self._use_case).__qualname__}.execute"
        harness = type(self).__name__
        executor = UseCaseExecutor(
            UseCaseCompiler(),
            agent_resolver=_AgentDoubles(declarer, harness, self._agent_doubles),
            mcp_resolver=_McpDoubles(declarer, harness, self._mcp_doubles),
        )
        return await executor.execute(  # type: ignore[no-any-return]
            self._use_case,
            params=self._params,
            payload=self._payload,
            dependencies=self._dependencies if self._dependencies else None,
            load_overrides=self._load_overrides if self._load_overrides else None,
            identity=self._identity,
        )

    @property
    def plan(self) -> ExecutionPlan:
        """Compile and return the ExecutionPlan for the UseCase.

        Useful for asserting plan structure in advanced test scenarios.

        Returns:
            The compiled ``ExecutionPlan``.
        """
        cached = type(self._use_case).__execution_plan__
        if cached is not None:
            return cached
        return UseCaseCompiler().compile(type(self._use_case))
