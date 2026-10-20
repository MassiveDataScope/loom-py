"""``_BoundAgentHandle``: the AgentHandle a marker resolves to (T203/T205/T304).

Covers the anonymous-caller refusal, the handle's own span (nothing else
opens one on this code path — see the module docstring of ``_handle.py``),
the three run modes, the per-run-shape refusal when the output hook needs the
declared shape, and the grant lookups' not-yet-implemented failure mode.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import msgspec
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.ai.abc import AgentEvent, AgentResult, Conversation, FinalEvent, HealthStatus, StateShape
from loom.ai.compiler._plan import CompiledOutputHook
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import _BoundAgentHandle, agent_marker_resolver
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.identity import ANONYMOUS, Identity
from loom.core.model import msgspec_type
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.service import NullSqlQueryService
from loom.core.use_case import Caller, Input, UseCase
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    ConversationRecorder,
    CountingEngineProvider,
    RecordingDepsFactory,
    RecordingMcpSession,
    RecordTurn,
    StubDepsFactory,
    StubMcpClient,
    conversational_plan,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_sql_capability,
    make_sql_config,
    mcp_client_factory,
)


class _MessageOnlyCommand(Command, frozen=True, kw_only=True):
    """A hook command that never reads the run's output — only its bookkeeping."""

    interaction_id: str
    conversation_id: str | None = None


class _RecordMessageOnly(UseCase[Any, None]):
    """An output hook that declares no ``output`` field at all.

    The regression fixture for the narrow refusal (T304): a hook shaped like
    conversation persistence, which never touches the decoded answer, must
    keep working under both new run modes.
    """

    def __init__(self, recorder: ConversationRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: _MessageOnlyCommand = Input(), caller: Identity = Caller()
    ) -> None:
        del caller
        self._recorder.timeline.append("hook")


def _messages_only_plan(name: str) -> Any:
    """Build a plan whose ``on_output`` command never declares ``output``."""
    hook = CompiledOutputHook(
        usecase="messages-only",
        use_case=_RecordMessageOnly,
        accepted=frozenset(info.name for info in msgspec.structs.fields(_MessageOnlyCommand)),
    )
    return msgspec.structs.replace(make_plan(name), on_output=hook)


_AUTHENTICATED = Identity(subject="ada", mechanism="test")
_AGENT_NAME = "triage"


class _DefaultedState(msgspec.Struct, forbid_unknown_fields=True):
    """A state shape whose every field carries a declared default."""

    marca: str = ""
    km: int = 0


_DEFAULTED_STATE = StateShape(schema={"type": "object"}, loom_type=msgspec_type(_DefaultedState))


class _RequiredState(msgspec.Struct, forbid_unknown_fields=True):
    """A state shape with one field carrying no declared default."""

    marca: str


_REQUIRED_STATE = StateShape(
    schema={"type": "object", "properties": {"marca": {"type": "string"}}, "required": ["marca"]},
    loom_type=msgspec_type(_RequiredState),
)


@pytest.fixture
def deps() -> StubDepsFactory:
    """Per-invocation dependency factory carrying only the caller identity."""
    return StubDepsFactory()


@pytest.fixture
def container() -> LoomContainer:
    """Empty application container; no test here resolves anything from it."""
    return LoomContainer()


class _OneShotEngine:
    """A single successful turn, for tests that only need the happy path."""

    def __init__(self, provider_details: Mapping[str, Any] | None = None) -> None:
        self.run_stream_calls = 0
        self.identities: list[Identity] = []
        self.provider_details = provider_details

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> object:
        del prompt, conversation, state
        self.run_stream_calls += 1
        self.identities.append(identity)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            async def _events() -> AsyncIterator[AgentEvent]:
                yield FinalEvent(
                    output={"ok": True},
                    usage=DEFAULT_USAGE,
                    provider_details=self.provider_details,
                )

            yield _events()

        return _stream()

    async def run(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        del prompt, identity, conversation, state
        return AgentResult(
            output={"ok": True}, usage=DEFAULT_USAGE, provider_details=self.provider_details
        )

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


class _NativeRecordingEngine:
    """An engine whose ``native()`` records the resolved state it received.

    A mutation to :meth:`AgentRuntime.native` that passes the caller's raw
    ``state`` straight through instead of the value ``_resolve_state``
    computed leaves every test pinning ``STATE_UNDECLARED`` alone green: that
    refusal fires whichever value is forwarded. This double is what a test
    needs to tell the two apart.
    """

    def __init__(self) -> None:
        self.states: list[object | None] = []

    def native(
        self, *, identity: Identity, state: object | None = None, guard: Any = None
    ) -> object:
        del identity, guard
        self.states.append(state)
        return object()

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


def _tracing_runtime() -> tuple[ObservabilityRuntime, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return ObservabilityRuntime([], tracer=provider.get_tracer("loom.ai")), exporter


async def _agent_runtime(deps: StubDepsFactory, container: LoomContainer) -> AgentRuntime:
    provider = CountingEngineProvider(engines={_AGENT_NAME: _OneShotEngine()})  # type: ignore[dict-item]
    return AgentRuntime(
        plans=[make_plan(_AGENT_NAME)],
        config=make_ai_config(),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


class TestAnonymousIdentity:
    """The anonymous caller is rejected before ever touching the model (design R3)."""

    async def test_an_anonymous_caller_is_rejected_with_unauthorized(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=ANONYMOUS,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola")

        assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED

    async def test_the_message_matches_the_one_shown_in_use_case_dsl_md(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for ``UNAUTHORIZED``.

        Edit one without the other and this test is the gap the next review
        catches.
        """
        runtime = await _agent_runtime(deps, container)
        # The anonymous refusal runs before the runtime is ever asked to run
        # this name, so a name absent from the runtime's own plans is fine
        # here — it matches the agent name ``markers.md`` and the example
        # in ``docs/rest/use-case-dsl.md`` both use.
        handle = _BoundAgentHandle(
            name="incident-triage",
            runtime=runtime,
            identity=ANONYMOUS,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola")

        assert str(excinfo.value) == "agent 'incident-triage' requires an authenticated caller"

    async def test_an_anonymous_caller_never_reaches_the_model(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The engine is never invoked: the run is cut before the runtime's 'run'."""
        engine = _OneShotEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=ANONYMOUS,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError):
                await handle.run("hola")
            assert engine.run_stream_calls == 0

    async def test_an_authenticated_caller_runs_as_itself(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The engine's run receives exactly the handle's identity, never another:

        the runtime authorises every capability with this identity, so
        forcing a different one inside the adapter would leave the capability
        running as a third party.
        """
        engine = _OneShotEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}
        assert engine.identities == [_AUTHENTICATED]


class TestProviderDetailsReachTheAnswer:
    async def test_the_final_events_provider_details_come_back_on_the_answer(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """What the provider said about the answer travels from ``final`` to ``AgentResult``."""
        details = {"confidence": {"ok": 0.93}}
        engine = _OneShotEngine(provider_details=details)
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.provider_details == details


class TestTheHandlesSpan:
    """The handle opens its own span; the runtime opens none on its own."""

    async def test_a_successful_run_opens_and_closes_an_agent_span(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        observability, exporter = _tracing_runtime()
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME)],
            config=make_ai_config(),
            engine_provider=CountingEngineProvider(  # type: ignore[arg-type]
                engines={_AGENT_NAME: _OneShotEngine()}  # type: ignore[dict-item]
            ),
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=observability,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola")

        spans = exporter.get_finished_spans()
        assert [span.name for span in spans] == ["agent:agent_run"]
        assert spans[0].attributes is not None
        assert spans[0].attributes["subject"] == "ada"
        assert spans[0].attributes["interaction_id"] == answer.interaction_id

    async def test_no_span_exists_without_an_observability_runtime(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``observability=None`` is an explicit no-op, not a silent failure."""
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}


class _ShapedEngine(_OneShotEngine):
    """A one-shot engine that also serves a per-run shape override (T304).

    Records every ``output_type`` it was asked to run with, so a test can
    assert the artefact's own declared shape was bypassed for an overridden
    call and used for a plain one.
    """

    def __init__(self, *, shaped_output: object = "prose") -> None:
        super().__init__()
        self._shaped_output = shaped_output
        self.shaped_calls: list[type[Any] | None] = []

    def run_stream_shaped(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        output_type: type[Any],
        state: object | None = None,
    ) -> object:
        del prompt, state
        self.shaped_calls.append(output_type)
        self.identities.append(identity)

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            async def _events() -> AsyncIterator[AgentEvent]:
                yield FinalEvent(output=self._shaped_output, usage=DEFAULT_USAGE)

            yield _events()

        return _stream()


async def _shaped_runtime(
    deps: StubDepsFactory, container: LoomContainer, engine: _ShapedEngine
) -> AgentRuntime:
    provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
    return AgentRuntime(
        plans=[make_plan(_AGENT_NAME)],
        config=make_ai_config(),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
    )


class TestTheThreeModes:
    """T304: the declared shape, the per-run shape and open-ended text."""

    async def test_run_without_expect_uses_the_artifacts_declared_shape(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}

    async def test_run_with_expect_types_only_that_runs_response(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine(shaped_output={"severity": 5})
        runtime = await _shaped_runtime(deps, container, engine)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola", expect=dict)

        assert answer.output == {"severity": 5}
        assert engine.shaped_calls == [dict]

    async def test_run_text_returns_prose_with_no_declared_shape(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine(shaped_output="a plain sentence")
        runtime = await _shaped_runtime(deps, container, engine)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run_text("hola")

        assert answer.output == "a plain sentence"
        assert engine.shaped_calls == [str]

    async def test_expect_does_not_invoke_the_artifacts_output_check(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The declared shape uses run_stream; a per-run shape uses run_stream_shaped."""
        engine = _ShapedEngine(shaped_output={"anything": True})
        runtime = await _shaped_runtime(deps, container, engine)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            await handle.run("hola", expect=dict)

        assert engine.run_stream_calls == 0
        assert engine.shaped_calls == [dict]


class TestRejectionByOutputHook:
    """T304: a per-run shape is rejected before the model if the hook needs it."""

    async def test_expect_is_rejected_when_the_hook_declares_output(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        plan = conversational_plan(_AGENT_NAME, hook=True)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((RecordTurn,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola", expect=dict)

        assert excinfo.value.code is AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK
        assert engine.shaped_calls == []
        assert engine.run_stream_calls == 0

    async def test_the_message_matches_the_one_shown_in_use_case_dsl_md_for_hooks(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Pins the exact message ``docs/rest/use-case-dsl.md`` quotes for
        ``AGENT_RUN_SHAPE_WITH_HOOK``, under the same agent name the page's
        example uses.
        """
        agent_name = "incident-triage"
        engine = _ShapedEngine()
        provider = CountingEngineProvider(engines={agent_name: engine})  # type: ignore[dict-item]
        plan = conversational_plan(agent_name, hook=True)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((RecordTurn,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=agent_name,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run("hola", expect=dict)

        assert str(excinfo.value) == (
            "agent 'incident-triage' declares an output hook that reads the run's "
            "output, so this run cannot use a per-run shape; call run(prompt) for "
            "the artefact's own declared output instead"
        )

    async def test_run_text_is_rejected_when_the_hook_declares_output(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine()
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        plan = conversational_plan(_AGENT_NAME, hook=True)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((RecordTurn,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await handle.run_text("hola")

        assert excinfo.value.code is AgentRunErrorCode.AGENT_RUN_SHAPE_WITH_HOOK

    async def test_run_without_a_shape_still_works_with_a_hook_declaring_only_messages(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        provider = CountingEngineProvider(engines={_AGENT_NAME: _OneShotEngine()})  # type: ignore[dict-item]
        plan = _messages_only_plan(_AGENT_NAME)
        recorder = ConversationRecorder()
        container.register_instance(ConversationRecorder, recorder)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((_RecordMessageOnly,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}
        assert recorder.timeline == ["hook"]

    async def test_expect_still_works_with_a_hook_declaring_only_messages(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _ShapedEngine(shaped_output={"custom": True})
        provider = CountingEngineProvider(engines={_AGENT_NAME: engine})  # type: ignore[dict-item]
        plan = _messages_only_plan(_AGENT_NAME)
        recorder = ConversationRecorder()
        container.register_instance(ConversationRecorder, recorder)
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=RecordingDepsFactory((_RecordMessageOnly,)),  # type: ignore[arg-type]
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            answer = await handle.run("hola", expect=dict)

        assert answer.output == {"custom": True}
        assert recorder.timeline == ["hook"]


_RUNBOOKS_SERVER = "runbooks"


async def _runtime_with_one_mcp_grant(
    deps: StubDepsFactory, container: LoomContainer
) -> AgentRuntime:
    """One agent, one live ``mcp`` grant on :data:`_RUNBOOKS_SERVER` — the only one."""
    capability = make_mcp_capability(_RUNBOOKS_SERVER)
    session = RecordingMcpSession(label=_RUNBOOKS_SERVER, tools=("search",))
    client = StubMcpClient(label=_RUNBOOKS_SERVER, session=session, log=[])
    provider = CountingEngineProvider(engines={_AGENT_NAME: _OneShotEngine()})  # type: ignore[dict-item]
    return AgentRuntime(
        plans=[make_plan(_AGENT_NAME, capabilities=(capability,))],
        config=make_ai_config(mcp_servers=make_mcp_servers(_RUNBOOKS_SERVER)),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory({_RUNBOOKS_SERVER: client}),  # type: ignore[arg-type]
    )


class TestUngrantedGrants:
    """T301/T303: name the missing grant before ever touching the network."""

    async def test_an_unknown_mcp_grant_names_the_agents_mcp_grants(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The message names the one granted server, not an arbitrary one."""
        runtime = await _runtime_with_one_mcp_grant(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                handle.mcp("not-granted")

        assert excinfo.value.code is AgentRunErrorCode.MCP_GRANT_UNKNOWN
        assert str(excinfo.value) == (
            "agent 'triage' grants no mcp server named 'not-granted'; mcp servers "
            f"this agent grants: {_RUNBOOKS_SERVER}"
        )

    async def test_an_unknown_sql_grant_names_the_agents_sql_grants(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The same rejection on the ``sql`` side: an agent with no ``sql`` grant at all."""
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                handle.sql("reporting")

        assert excinfo.value.code is AgentRunErrorCode.SQL_GRANT_UNKNOWN
        assert str(excinfo.value) == (
            "agent 'triage' grants no sql connection named 'reporting'; agent "
            "'triage' grants no sql connection; add one to its artefact's 'sql' "
            "capability"
        )


class TestPublishedGrants:
    """T301/T303: ``grants()`` names every granted permission, the mitigation the
    specification promises for the one concession the design makes — that the
    ``mcp`` server name is not verified at start-up.
    """

    async def test_grants_names_the_granted_mcp_server(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _runtime_with_one_mcp_grant(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            assert handle.grants() == (_RUNBOOKS_SERVER,)

    async def test_grants_groups_servers_ahead_of_connections(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The order is grouped, not interleaved, even though the artifact alternates them.

        The artifact declares mcp, sql, mcp: if the listing followed
        declaration order they would come out interleaved. They come out
        grouped, which is what the public contract promises and what keeps a
        server and a connection sharing a name distinguishable by position.
        """
        second = "playbooks"
        provider = CountingEngineProvider(engines={_AGENT_NAME: _OneShotEngine()})  # type: ignore[dict-item]
        sessions = {
            name: StubMcpClient(
                label=name,
                session=RecordingMcpSession(label=name, tools=("search",)),
                log=[],
            )
            for name in (_RUNBOOKS_SERVER, second)
        }
        runtime = AgentRuntime(
            plans=[
                make_plan(
                    _AGENT_NAME,
                    capabilities=(
                        make_mcp_capability(_RUNBOOKS_SERVER),
                        make_sql_capability("analytics"),
                        make_mcp_capability(second),
                    ),
                )
            ],
            # The stubs answer without delay, so only wall-clock time on a
            # loaded runner can spend the start-up budget, a real loop-time
            # deadline; this test checks order, not that budget, so it gets
            # ten seconds instead of the 500 ms default.
            config=make_ai_config(
                mcp_servers=make_mcp_servers(_RUNBOOKS_SERVER, second),
                startup_timeout_ms=10_000,
            ),
            sql_config=make_sql_config("analytics"),
            engine_provider=provider,  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory(sessions),  # type: ignore[arg-type]
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            assert handle.grants() == (_RUNBOOKS_SERVER, second, "analytics")


class TestTheMarkerResolver:
    """``agent_marker_resolver`` builds the callable the executor takes."""

    async def test_the_resolver_binds_the_name_and_the_caller(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        runtime = await _agent_runtime(deps, container)
        resolve = agent_marker_resolver(
            runtime, sql_query_service=NullSqlQueryService(), observability=None
        )

        handle = resolve(_AGENT_NAME, _AUTHENTICATED)

        async with runtime:
            answer = await handle.run("hola")

        assert answer.output == {"ok": True}


class TestTheNativeEscapeHatch:
    """``AgentHandle.native``: the engine's own objects, on the handle's terms."""

    async def test_an_engine_without_a_native_form_says_so(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """``_OneShotEngine`` implements the contract and nothing else."""
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(NotImplementedError) as excinfo:
                handle.native()

        assert "no 'native'" in str(excinfo.value)

    async def test_an_anonymous_caller_is_refused_before_the_engine_is_asked(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The hatch never hands out what ``run`` would have refused."""
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=ANONYMOUS,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                handle.native()

        assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED

    async def test_state_is_resolved_against_the_artefacts_shape(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """Undeclared state is refused here exactly as it is on a run."""
        runtime = await _agent_runtime(deps, container)
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                handle.native(state={"ticket": "INC-1"})

        assert excinfo.value.code is AgentRunErrorCode.STATE_UNDECLARED

    async def test_a_none_state_carries_the_shapes_declared_defaults(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A ``None`` state on a stateful artefact resolves to its declared defaults.

        Pins the exact value :meth:`AgentRuntime.native` must forward: a
        mutation that passes the caller's raw ``state`` straight through
        instead of the shape's resolved defaults would leave this green only
        by accident, so it is checked against the recorded value, not merely
        against "no error".
        """
        engine = _NativeRecordingEngine()
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME, state=_DEFAULTED_STATE)],
            config=make_ai_config(),
            engine_provider=CountingEngineProvider(  # type: ignore[arg-type]
                engines={_AGENT_NAME: engine}  # type: ignore[dict-item]
            ),
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            handle.native()

        assert engine.states == [{"marca": "", "km": 0}]

    async def test_a_none_state_against_a_required_field_raises_state_required(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = _NativeRecordingEngine()
        runtime = AgentRuntime(
            plans=[make_plan(_AGENT_NAME, state=_REQUIRED_STATE)],
            config=make_ai_config(),
            engine_provider=CountingEngineProvider(  # type: ignore[arg-type]
                engines={_AGENT_NAME: engine}  # type: ignore[dict-item]
            ),
            deps=deps,
            container=container,
        )
        handle = _BoundAgentHandle(
            name=_AGENT_NAME,
            runtime=runtime,
            identity=_AUTHENTICATED,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )

        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                handle.native()

        assert excinfo.value.code is AgentRunErrorCode.STATE_REQUIRED
        assert engine.states == []
