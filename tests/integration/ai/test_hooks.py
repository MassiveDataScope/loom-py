"""The ``on_output`` hook at run time (002 T5: AC5-AC12, AC14).

Every run goes through ``AgentHost`` over a ``ScriptedEngine``; the hook
use cases are real use cases executed by a real ``UseCaseExecutor`` over a
recording unit of work.  No network, no database, no model.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import msgspec
import pytest

from loom.ai.abc import (
    AgentEvent,
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler._plan import AgentPlan, CompiledOutputHook
from loom.ai.errors import (
    HOOK_FAILED_MESSAGE,
    AgentCompilationError,
    AgentErrorCode,
    AgentRunErrorCode,
)
from loom.ai.execution import AgentHost, AgentRunError
from loom.ai.execution._turn import TurnCallbacks
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.errors import Forbidden
from loom.core.execution import ExecutionScope
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case import Caller, Input, UseCase
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.keys import use_case_key
from tests.integration.ai.conftest import (
    DEFAULT_USAGE,
    CountingEngineProvider,
    RecordingDepsFactory,
    RecordingMcpSession,
    ScriptedEngine,
    StubDepsFactory,
    StubMcpClient,
    default_script,
    error_script,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_policies,
    mcp_client_factory,
)

_AGENT = "incident-triage"
_ACME_OTHER = Identity(subject="acme-other", roles=("analyst",), mechanism="test")
_DENIED_MESSAGE = "the caller is not allowed to perform this operation"


# ---------------------------------------------------------------------------
# Incident-triage hook use cases
# ---------------------------------------------------------------------------


class TriageReport(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Structured triage verdict an agent returns."""

    incident_ref: str
    severity: str
    confidence: float
    alerts: list[str] = []


class TriageRecorded(msgspec.Struct, frozen=True):
    """Result of recording one triage."""

    triage_id: str


@dataclass(frozen=True)
class RecordedCall:
    """One execution of a hook use case: the command it received and its caller."""

    command: Command
    caller: Identity


@dataclass
class Recorder:
    """Shared observer of every hook execution, resolved from the container.

    Attributes:
        calls: One entry per execution, in order.
        timeline: ``"hook"`` appended when a hook runs; tests append events.
        entered: Set when the first execution starts.
        gate: When set, an execution waits on it before completing.
        sleep_s: Time an execution sleeps before completing.
        failure: Exception an execution raises after its waits; a
            ``CancelledError`` here models a hook cancelling itself.
        cancelled: Whether an execution observed ``CancelledError``.
    """

    calls: list[RecordedCall] = field(default_factory=list)
    timeline: list[str] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    gate: asyncio.Event | None = None
    sleep_s: float = 0.0
    failure: BaseException | None = None
    cancelled: bool = False

    async def record(self, command: Command, caller: Identity) -> None:
        """Record one execution, honouring the configured gate, sleep and failure."""
        self.calls.append(RecordedCall(command=command, caller=caller))
        self.timeline.append("hook")
        self.entered.set()
        try:
            if self.gate is not None:
                await self.gate.wait()
            if self.sleep_s:
                await asyncio.sleep(self.sleep_s)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        if self.failure is not None:
            raise self.failure


class TriageCommand(Command, frozen=True, kw_only=True):
    """Typed output plus every context name the host offers."""

    output: TriageReport
    interaction_id: str
    subject: str
    mechanism: str
    agent: str
    provider: str
    model: str
    conversation_id: str | None = None


class AnswerCommand(Command, frozen=True, kw_only=True):
    """Untyped output plus the context names a spoofing test needs."""

    output: dict[str, Any]
    interaction_id: str
    subject: str
    conversation_id: str | None = None


class StrictCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Declares only ``output`` and ``interaction_id`` and refuses anything else."""

    output: dict[str, Any]
    interaction_id: str


@use_case_key("incidents.record_triage")
class RecordTriage(UseCase[Any, TriageRecorded]):
    """Records a typed triage report."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: TriageCommand = Input(), caller: Identity = Caller()
    ) -> TriageRecorded:
        await self._recorder.record(cmd, caller)
        return TriageRecorded(triage_id=cmd.interaction_id)


@use_case_key("incidents.record_answer")
class RecordAnswer(UseCase[Any, TriageRecorded]):
    """Records an untyped answer."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: AnswerCommand = Input(), caller: Identity = Caller()
    ) -> TriageRecorded:
        await self._recorder.record(cmd, caller)
        return TriageRecorded(triage_id=cmd.interaction_id)


@use_case_key("incidents.record_strict")
class RecordStrict(UseCase[Any, TriageRecorded]):
    """Records through a strict command."""

    def __init__(self, recorder: Recorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: StrictCommand = Input(), caller: Identity = Caller()
    ) -> TriageRecorded:
        await self._recorder.record(cmd, caller)
        return TriageRecorded(triage_id=cmd.interaction_id)


_COMMANDS: dict[type[UseCase[Any, Any]], type[Command]] = {
    RecordTriage: TriageCommand,
    RecordAnswer: AnswerCommand,
    RecordStrict: StrictCommand,
}

REPORT = TriageReport(incident_ref="INC-1", severity="high", confidence=0.71, alerts=["A-7"])


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _hooked_plan(use_case: type[UseCase[Any, Any]], **plan_kwargs: Any) -> AgentPlan:
    """Build a plan whose ``on_output`` names ``use_case``, as the compiler would."""
    command = _COMMANDS[use_case]
    hook = CompiledOutputHook(
        usecase=str(getattr(use_case, "__use_case_key__", use_case.__name__)),
        use_case=use_case,
        accepted=frozenset(info.name for info in msgspec.structs.fields(command)),
    )
    return msgspec.structs.replace(make_plan(_AGENT, **plan_kwargs), on_output=hook)


def _host(
    engine: ScriptedEngine,
    plan: AgentPlan,
    *,
    deps: object,
    container: LoomContainer,
    max_concurrent_runs: int = 8,
) -> AgentHost:
    """Build a host serving one scripted agent."""
    return AgentHost(
        plans=[plan],
        config=make_ai_config(max_concurrent_runs=max_concurrent_runs),
        engine_plugin=CountingEngineProvider(engines={_AGENT: engine}),  # type: ignore[arg-type]
        deps=deps,  # type: ignore[arg-type]
        container=container,
    )


def _tool_script(output: object) -> tuple[AgentEvent, ...]:
    """A run with one full tool cycle before its ``final``."""
    return (
        TextDeltaEvent(text="thinking"),
        ToolCallEvent(tool="sql_observability", call_id="c1", arguments={}),
        ToolResultEvent(call_id="c1", ok=True, summary="3 rows"),
        FinalEvent(output=output, usage=DEFAULT_USAGE),
    )


async def _collect(host: AgentHost, identity: Identity, **kwargs: Any) -> list[AgentEvent]:
    """Consume a whole stream and return its events."""
    events: list[AgentEvent] = []
    async with host.run_stream(_AGENT, "prompt", identity=identity, **kwargs) as stream:
        async for event in stream:
            events.append(event)
    return events


@pytest.fixture
def recorder(container: LoomContainer) -> Recorder:
    """Recorder the hook use cases resolve from the container."""
    recorder = Recorder()
    container.register_instance(Recorder, recorder)
    return recorder


@pytest.fixture
def hook_deps(container: LoomContainer) -> RecordingDepsFactory:
    """Deps factory serving the three hook use cases through a real executor."""
    return RecordingDepsFactory((RecordTriage, RecordAnswer, RecordStrict)).registered_in(container)


# ---------------------------------------------------------------------------
# AC5, AC6 — execution, command shape, caller
# ---------------------------------------------------------------------------


class TestHookExecution:
    async def test_runs_the_hook_once_with_the_typed_output_when_the_run_completes(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The command carries the validated output as the declared type plus the context."""
        engine = ScriptedEngine(script=default_script(REPORT))
        plan = _hooked_plan(RecordTriage)
        host = _host(engine, plan, deps=hook_deps, container=container)

        async with host:
            result = await host.run(_AGENT, "prompt", identity=identity)

        assert len(recorder.calls) == 1
        command = recorder.calls[0].command
        assert isinstance(command, TriageCommand)
        assert command.output == REPORT
        assert command.subject == identity.subject
        assert command.mechanism == identity.mechanism
        assert command.agent == plan.name
        assert command.provider == plan.inference.provider
        assert command.model == plan.inference.model
        assert result.interaction_id is not None
        assert command.interaction_id == result.interaction_id
        assert result.hook_result == TriageRecorded(triage_id=result.interaction_id)
        assert result.output == REPORT

    async def test_delivers_the_output_as_a_dict_when_the_command_declares_it_so(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A dict output reaches a ``dict[str, Any]`` field unchanged."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            await host.run(_AGENT, "prompt", identity=identity)

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.output == {"answer": "42"}

    async def test_a_subject_field_on_the_output_does_not_override_the_context(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``subject`` inside the output stays nested; the command's is the caller's."""
        engine = ScriptedEngine(script=default_script({"answer": "42", "subject": "spoofed"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            await host.run(_AGENT, "prompt", identity=identity)

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.subject == identity.subject
        assert command.output["subject"] == "spoofed"

    async def test_feeds_a_strict_command_when_it_declares_only_output_and_interaction_id(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The offered dict is filtered to the declared names before ``from_payload``."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordStrict), deps=hook_deps, container=container)

        async with host:
            result = await host.run(_AGENT, "prompt", identity=identity)

        command = recorder.calls[0].command
        assert isinstance(command, StrictCommand)
        assert result.interaction_id is not None
        assert command == StrictCommand(
            output={"answer": "42"}, interaction_id=result.interaction_id
        )

    async def test_runs_the_hook_once_at_the_final_event_even_when_the_script_includes_tool_events(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Tool events pass through untouched; the hook runs once, at the final event."""
        engine = ScriptedEngine(script=_tool_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host, host.run_stream(_AGENT, "prompt", identity=identity) as stream:
            async for event in stream:
                recorder.timeline.append(type(event).__name__)

        assert len(recorder.calls) == 1
        assert recorder.timeline == [
            "TextDeltaEvent",
            "ToolCallEvent",
            "ToolResultEvent",
            "hook",
            "FinalEvent",
        ]

    async def test_runs_the_hook_as_the_verified_caller_when_there_is_identity(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``Caller()`` inside the use case is the run's identity."""
        engine = ScriptedEngine(script=default_script(REPORT))
        host = _host(engine, _hooked_plan(RecordTriage), deps=hook_deps, container=container)

        async with host:
            await host.run(_AGENT, "prompt", identity=identity)

        assert recorder.calls[0].caller == identity

    async def test_runs_the_hook_as_the_identity_acting_not_the_run_s_caller(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A run context retained for one caller runs its hook as whoever acts."""
        turn = TurnCallbacks(
            plan=_hooked_plan(RecordAnswer),
            identity=identity,
            interaction_id="acme-interaction",
            conversation_id=None,
            invoker=container.resolve(ApplicationInvoker),
        )

        async def final() -> AsyncIterator[AgentEvent]:
            yield FinalEvent(output={"answer": "42"}, usage=DEFAULT_USAGE)

        with ExecutionScope(_ACME_OTHER):
            events = [event async for event in turn.events(final())]

        assert [type(event) for event in events] == [FinalEvent]
        assert [call.caller for call in recorder.calls] == [_ACME_OTHER]

    async def test_runs_the_hook_as_anonymous_when_the_caller_is_anonymous(
        self,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An anonymous run still records, with ``Caller()`` bound to ``ANONYMOUS``."""
        engine = ScriptedEngine(script=default_script(REPORT))
        host = _host(engine, _hooked_plan(RecordTriage), deps=hook_deps, container=container)

        async with host:
            await host.run(_AGENT, "prompt", identity=ANONYMOUS)

        call = recorder.calls[0]
        assert call.caller == ANONYMOUS
        assert isinstance(call.command, TriageCommand)
        assert call.command.subject == ""

    async def test_names_the_interaction_id_with_no_hook_result_when_there_is_no_hook(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Without a hook the run is still named; nothing is executed."""
        engine = ScriptedEngine()
        host = _host(engine, make_plan(_AGENT), deps=hook_deps, container=container)

        async with host:
            result = await host.run(_AGENT, "prompt", identity=identity)

        assert result.interaction_id is not None
        assert len(result.interaction_id) == 32
        assert result.hook_result is None
        assert recorder.calls == []


# ---------------------------------------------------------------------------
# AC7, AC8 — failure mode and transaction boundary
# ---------------------------------------------------------------------------


class TestHookFailures:
    async def test_fails_with_hook_failed_without_the_detail_when_the_hook_raises(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The caller gets the coded error and the id; the exception text stays server-side."""
        recorder.failure = ValueError("secret detail")
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            with pytest.raises(AgentRunError) as failure:
                await host.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.HOOK_FAILED
        assert failure.value.interaction_id is not None
        assert str(failure.value) == HOOK_FAILED_MESSAGE
        assert "secret detail" not in str(failure.value)
        assert hook_deps.uow.log == ["begin", "rollback"]
        assert engine.stream_count == 1

    async def test_fails_with_hook_failed_when_the_hook_cancels_itself(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A hook ending in its own ``CancelledError`` is a hook failure, not a consumer exit."""
        recorder.failure = asyncio.CancelledError()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            with pytest.raises(AgentRunError) as failure:
                await host.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.HOOK_FAILED
        assert failure.value.interaction_id is not None
        assert str(failure.value) == HOOK_FAILED_MESSAGE
        assert len(recorder.calls) == 1

    async def test_emits_a_single_error_with_no_final_when_the_hook_cancels_itself_in_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The stream closes with one ``error`` carrying the id and no ``final``."""
        recorder.failure = asyncio.CancelledError()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            events = await _collect(host, identity)

        terminal = [event for event in events if isinstance(event, ErrorEvent | FinalEvent)]
        assert len(terminal) == 1
        error = terminal[0]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.HOOK_FAILED
        assert error.interaction_id is not None
        assert error.message == HOOK_FAILED_MESSAGE

    async def test_emits_a_single_error_with_no_final_when_the_hook_raises_in_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The stream ends in one ``error`` event carrying the id, and no ``final``."""
        recorder.failure = ValueError("secret detail")
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            events = await _collect(host, identity)

        terminal = [event for event in events if isinstance(event, ErrorEvent | FinalEvent)]
        assert len(terminal) == 1
        error = terminal[0]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.HOOK_FAILED
        assert error.interaction_id is not None
        assert "secret detail" not in error.message
        assert engine.stream_count == 1

    async def test_maps_forbidden_to_unauthorized_when_the_hooks_rules_refuse(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An authorization denial keeps its meaning and its fixed text."""
        recorder.failure = Forbidden("triage of INC-1 is restricted")
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            with pytest.raises(AgentRunError) as failure:
                await host.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(failure.value) == _DENIED_MESSAGE
        assert failure.value.interaction_id is not None

    async def test_fails_with_hook_failed_when_the_hook_exceeds_tool_timeout_ms(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A hook sleeping past the bound is cut and reported as ``HOOK_FAILED``."""
        recorder.sleep_s = 5.0
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        plan = _hooked_plan(RecordAnswer, policies=make_policies(tool_timeout_ms=50))
        host = _host(engine, plan, deps=hook_deps, container=container)
        loop = asyncio.get_running_loop()

        async with host:
            started = loop.time()
            with pytest.raises(AgentRunError) as failure:
                await host.run(_AGENT, "prompt", identity=identity)
            elapsed = loop.time() - started

        assert failure.value.code is AgentRunErrorCode.HOOK_FAILED
        assert elapsed < 2.0, f"the hook was not bounded: {elapsed:.3f}s"
        assert recorder.cancelled is True
        # The cut hook is cancelled and the executor rolls back on ``BaseException``,
        # so the unit of work it had begun is closed.
        assert hook_deps.uow.log == ["begin", "rollback"]

    async def test_commits_the_transaction_when_the_hook_finishes(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A successful hook is one ``begin → commit`` through the executor."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            await host.run(_AGENT, "prompt", identity=identity)

        assert hook_deps.uow.log == ["begin", "commit"]
        assert len(recorder.calls) == 1


# ---------------------------------------------------------------------------
# AC9 — only completed runs fire the hook
# ---------------------------------------------------------------------------


class TestIncompleteRuns:
    async def test_does_not_run_the_hook_when_the_engine_ends_in_error(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An engine failure passes through with the id and runs no hook."""
        engine = ScriptedEngine(script=error_script(AgentRunErrorCode.PROVIDER_UNAVAILABLE))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            events = await _collect(host, identity)
            with pytest.raises(AgentRunError) as failure:
                await host.run(_AGENT, "prompt", identity=identity)

        error = events[-1]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
        assert error.interaction_id is not None
        assert failure.value.interaction_id is not None
        assert recorder.calls == []

    async def test_does_not_run_the_hook_when_the_run_exceeds_its_budget(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A ``RUN_TIMEOUT`` breach is terminal, named, and runs no hook."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}), delays_ms=(0, 400))
        plan = _hooked_plan(RecordAnswer, policies=make_policies(run_timeout_ms=30))
        host = _host(engine, plan, deps=hook_deps, container=container)

        async with host:
            events = await _collect(host, identity)

        error = events[-1]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.RUN_TIMEOUT
        assert error.interaction_id is not None
        assert recorder.calls == []

    async def test_does_not_run_the_hook_when_the_consumer_abandons_the_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Leaving after the first delta closes the generators; nothing is recorded."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host, host.run_stream(_AGENT, "prompt", identity=identity) as stream:
            async for event in stream:
                assert isinstance(event, TextDeltaEvent)
                break

        assert recorder.calls == []
        assert hook_deps.uow.log == []

    async def test_closes_with_provider_unavailable_when_the_engine_emits_no_terminal_event(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A stream exhausted without a terminal event still ends named; no hook runs."""
        engine = ScriptedEngine(script=(TextDeltaEvent(text="ok"),))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            with pytest.raises(AgentRunError) as failure:
                await host.run(_AGENT, "prompt", identity=identity)

        assert failure.value.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
        assert failure.value.interaction_id is not None
        assert _AGENT in str(failure.value)
        assert recorder.calls == []

    async def test_emits_a_single_error_when_the_engine_emits_no_terminal_event_in_stream(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Exactly one ``error`` event closes the stream, carrying the id."""
        engine = ScriptedEngine(script=(TextDeltaEvent(text="ok"),))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            events = await _collect(host, identity)

        terminal = [event for event in events if isinstance(event, ErrorEvent | FinalEvent)]
        assert len(terminal) == 1
        error = terminal[0]
        assert isinstance(error, ErrorEvent)
        assert error.code is AgentRunErrorCode.PROVIDER_UNAVAILABLE
        assert error.interaction_id is not None
        assert recorder.calls == []


# ---------------------------------------------------------------------------
# AC10 — cancellation during the hook
# ---------------------------------------------------------------------------


class TestCancellationDuringTheHook:
    async def test_completes_the_record_once_when_the_consumer_is_cancelled(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The shielded hook commits, the consumer sees its cancellation, the permit frees."""
        recorder.gate = asyncio.Event()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        plan = _hooked_plan(RecordAnswer)
        host = _host(engine, plan, deps=hook_deps, container=container, max_concurrent_runs=1)

        async with host:
            consumer = asyncio.create_task(host.run(_AGENT, "prompt", identity=identity))
            await recorder.entered.wait()
            consumer.cancel()
            recorder.gate.set()
            with pytest.raises(asyncio.CancelledError):
                await consumer
            assert len(recorder.calls) == 1
            assert hook_deps.uow.log == ["begin", "commit"]
            assert recorder.cancelled is False

            recorder.gate = None
            result = await host.run(_AGENT, "prompt", identity=identity)

        assert result.hook_result is not None
        assert len(recorder.calls) == 2

    async def test_cancels_the_hook_when_the_consumer_is_cancelled_a_second_time(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A second cancel while waiting for the hook to settle cuts the hook, not just the wait."""
        recorder.gate = asyncio.Event()
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            consumer = asyncio.create_task(host.run(_AGENT, "prompt", identity=identity))
            await recorder.entered.wait()
            consumer.cancel()
            await asyncio.sleep(0)
            consumer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consumer
            await asyncio.sleep(0)

        assert recorder.cancelled is True
        assert hook_deps.uow.log == ["begin", "rollback"]


# ---------------------------------------------------------------------------
# AC11 — streaming order
# ---------------------------------------------------------------------------


class TestOrderInStream:
    async def test_runs_the_hook_before_delivering_final_when_it_streams(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The record exists before the consumer sees ``final``, which carries the result."""
        engine = ScriptedEngine(script=default_script(REPORT))
        host = _host(engine, _hooked_plan(RecordTriage), deps=hook_deps, container=container)

        final: object = None
        async with host, host.run_stream(_AGENT, "prompt", identity=identity) as stream:
            async for event in stream:
                recorder.timeline.append(type(event).__name__)
                final = event

        assert recorder.timeline == ["TextDeltaEvent", "hook", "FinalEvent"]
        assert isinstance(final, FinalEvent)
        assert final.interaction_id is not None
        assert final.hook_result == TriageRecorded(triage_id=final.interaction_id)


# ---------------------------------------------------------------------------
# AC12 (runtime half) — conversation_id
# ---------------------------------------------------------------------------


class TestConversationId:
    async def test_delivers_the_conversation_id_to_the_command_when_it_is_given(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The opaque value is copied verbatim into the command."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            await host.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.conversation_id == "c-42"

    async def test_delivers_none_when_no_conversation_id_is_given(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Absent means ``None``; the host never invents one."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            events = await _collect(host, identity)

        command = recorder.calls[0].command
        assert isinstance(command, AnswerCommand)
        assert command.conversation_id is None
        assert isinstance(events[-1], FinalEvent)

    async def test_refuses_with_value_error_when_the_conversation_id_exceeds_the_limit(
        self,
        identity: Identity,
        recorder: Recorder,
        hook_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """An out-of-bound value is a programming error, refused before admission."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        host = _host(engine, _hooked_plan(RecordAnswer), deps=hook_deps, container=container)

        async with host:
            with pytest.raises(ValueError, match="conversation_id"):
                await host.run(_AGENT, "prompt", identity=identity, conversation_id="x" * 129)
            with pytest.raises(ValueError, match="conversation_id"):
                async with host.run_stream(_AGENT, "prompt", identity=identity, conversation_id=""):
                    pass
            await host.run(_AGENT, "prompt", identity=identity, conversation_id="x" * 128)

        assert engine.stream_count == 1
        assert len(recorder.calls) == 1


# ---------------------------------------------------------------------------
# AC14 — start-up probe
# ---------------------------------------------------------------------------


class TestStartupProbe:
    async def test_refuses_startup_when_there_is_a_hook_and_the_container_holds_no_invoker(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        lifecycle_log: list[str],
    ) -> None:
        """The probe names the agents and runs before any client opens."""
        clients = {
            "tools": StubMcpClient(label="tools", session=RecordingMcpSession(), log=lifecycle_log)
        }
        plan = _hooked_plan(RecordAnswer, capabilities=(make_mcp_capability("tools"),))
        host = AgentHost(
            plans=[plan],
            config=make_ai_config(mcp_servers=make_mcp_servers("tools")),
            engine_plugin=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with host:
                pass

        issues = failure.value.issues
        assert [issue.code for issue in issues] == [AgentErrorCode.ON_OUTPUT_INVOKER_MISSING]
        assert _AGENT in issues[0].message
        assert lifecycle_log == []

    async def test_starts_normally_when_there_is_no_hook(
        self,
        identity: Identity,
        deps: StubDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The dict bundle stays valid for a deployment without hooks."""
        engine = ScriptedEngine()
        host = _host(engine, make_plan(_AGENT), deps=deps, container=container)

        async with host:
            result = await host.run(_AGENT, "prompt", identity=identity)

        assert result.interaction_id is not None
