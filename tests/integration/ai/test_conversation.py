"""The ``conversation`` loader at run time (006 T5: AC6-AC10).

Every run goes through ``AgentRuntime`` over a ``ScriptedEngine``; the loader
and hook use cases are real use cases executed by a real ``RuntimeExecutor``
over a recording unit of work.  No network, no database, no model.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from dataclasses import dataclass, field
from typing import Any

import msgspec
import pytest

from loom.ai.abc import AgentEvent, Conversation, FinalEvent, TextDeltaEvent
from loom.ai.compiler._plan import AgentPlan, CompiledConversation, CompiledOutputHook
from loom.ai.errors import (
    CONVERSATION_LOAD_FAILED_MESSAGE,
    CONVERSATION_LOAD_TIMEOUT_MESSAGE,
    AgentCompilationError,
    AgentErrorCode,
    AgentRunErrorClass,
    AgentRunErrorCode,
    is_retriable,
    run_error_class,
)
from loom.ai.runtime import AgentRunError, AgentRuntime
from loom.core.command import Command
from loom.core.di import LoomContainer
from loom.core.errors import Forbidden
from loom.core.identity import Identity
from loom.core.use_case import Caller, Input, UseCase
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
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_policies,
    mcp_client_factory,
)

_AGENT = "incident-triage"
_DENIED_MESSAGE = "the caller is not allowed to perform this operation"
_HISTORY = b'[{"kind": "request"}, {"kind": "response"}]'
_NEW_MESSAGES = b'[{"kind": "request", "conversation_id": "c-42"}]'


# ---------------------------------------------------------------------------
# Loader and hook use cases
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecordedCall:
    """One execution of a use case: the command it received and its caller."""

    command: Command
    caller: Identity


@dataclass
class LoaderRecorder:
    """Shared observer of every loader execution, resolved from the container.

    Attributes:
        calls: One entry per execution, in order.
        results: Values returned by successive executions; the last one repeats.
        gate: When set, an execution waits on it before completing.
        sleep_s: Time an execution sleeps before completing.
        failure: Exception an execution raises after its waits.
        cancelled: Whether an execution observed ``CancelledError``.
    """

    calls: list[RecordedCall] = field(default_factory=list)
    results: list[object] = field(default_factory=lambda: [_HISTORY])
    gate: asyncio.Event | None = None
    sleep_s: float = 0.0
    failure: BaseException | None = None
    cancelled: bool = False

    async def load(self, command: Command, caller: Identity) -> object:
        """Record one execution, honouring the configured gate, sleep and failure."""
        self.calls.append(RecordedCall(command=command, caller=caller))
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
        return self.results[min(len(self.calls), len(self.results)) - 1]


@dataclass
class HookRecorder:
    """Shared observer of every hook execution."""

    calls: list[RecordedCall] = field(default_factory=list)


class LoadCommand(Command, frozen=True, kw_only=True):
    """Every context name the loader offers."""

    conversation_id: str
    interaction_id: str
    subject: str
    mechanism: str
    agent: str


class StrictLoadCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Declares only ``conversation_id`` and refuses anything else."""

    conversation_id: str


class TurnCommand(Command, frozen=True, kw_only=True):
    """Untyped output plus the run's new messages."""

    output: dict[str, Any]
    interaction_id: str
    conversation_id: str | None = None
    messages: bytes | None = None


class StrictTurnCommand(Command, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Declares neither ``messages`` nor ``conversation_id`` and refuses anything else."""

    output: dict[str, Any]
    interaction_id: str


class TurnRecorded(msgspec.Struct, frozen=True):
    """Result of recording one turn."""

    interaction_id: str


@use_case_key("conversations.load")
class LoadConversation(UseCase[Any, bytes | None]):
    """Returns the prior history of a conversation."""

    def __init__(self, recorder: LoaderRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: LoadCommand = Input(), caller: Identity = Caller()
    ) -> bytes | None:
        result = await self._recorder.load(cmd, caller)
        return result  # type: ignore[return-value]  # a misbehaving loader is under test


@use_case_key("conversations.load_strict")
class LoadStrict(UseCase[Any, bytes | None]):
    """Returns the prior history through a strict command."""

    def __init__(self, recorder: LoaderRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: StrictLoadCommand = Input(), caller: Identity = Caller()
    ) -> bytes | None:
        result = await self._recorder.load(cmd, caller)
        return result  # type: ignore[return-value]


@use_case_key("incidents.record_turn")
class RecordTurn(UseCase[Any, TurnRecorded]):
    """Records the answer and the turn's new messages."""

    def __init__(self, recorder: HookRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: TurnCommand = Input(), caller: Identity = Caller()
    ) -> TurnRecorded:
        self._recorder.calls.append(RecordedCall(command=cmd, caller=caller))
        return TurnRecorded(interaction_id=cmd.interaction_id)


@use_case_key("incidents.record_strict_turn")
class RecordStrictTurn(UseCase[Any, TurnRecorded]):
    """Records through a strict command."""

    def __init__(self, recorder: HookRecorder) -> None:
        self._recorder = recorder

    async def execute(
        self, cmd: StrictTurnCommand = Input(), caller: Identity = Caller()
    ) -> TurnRecorded:
        self._recorder.calls.append(RecordedCall(command=cmd, caller=caller))
        return TurnRecorded(interaction_id=cmd.interaction_id)


_COMMANDS: dict[type[UseCase[Any, Any]], type[Command]] = {
    LoadConversation: LoadCommand,
    LoadStrict: StrictLoadCommand,
    RecordTurn: TurnCommand,
    RecordStrictTurn: StrictTurnCommand,
}


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _accepted(use_case: type[UseCase[Any, Any]]) -> frozenset[str]:
    return frozenset(info.name for info in msgspec.structs.fields(_COMMANDS[use_case]))


def _key(use_case: type[UseCase[Any, Any]]) -> str:
    return str(getattr(use_case, "__use_case_key__", use_case.__name__))


def _plan(
    *,
    loader: type[UseCase[Any, Any]] | None = LoadConversation,
    hook: type[UseCase[Any, Any]] | None = None,
    **plan_kwargs: Any,
) -> AgentPlan:
    """Build a plan declaring ``loader`` and ``hook``, as the compiler would."""
    plan = make_plan(_AGENT, **plan_kwargs)
    if loader is not None:
        conversation = CompiledConversation(
            usecase=_key(loader), use_case=loader, accepted=_accepted(loader)
        )
        plan = msgspec.structs.replace(plan, conversation=conversation)
    if hook is not None:
        on_output = CompiledOutputHook(usecase=_key(hook), use_case=hook, accepted=_accepted(hook))
        plan = msgspec.structs.replace(plan, on_output=on_output)
    return plan


def _runtime(
    engine: ScriptedEngine, plan: AgentPlan, *, deps: object, container: LoomContainer
) -> AgentRuntime:
    """Build a runtime serving one scripted agent."""
    return AgentRuntime(
        plans=[plan],
        config=make_ai_config(),
        engine_provider=CountingEngineProvider(engines={_AGENT: engine}),  # type: ignore[arg-type]
        deps=deps,  # type: ignore[arg-type]
        container=container,
    )


def _script(messages: bytes | None) -> tuple[AgentEvent, ...]:
    """A success script whose ``final`` carries ``messages``."""
    return (
        TextDeltaEvent(text="ok"),
        FinalEvent(output={"answer": "42"}, usage=DEFAULT_USAGE, messages=messages),
    )


def _holds_conversation(value: object, *, excluded: object, seen: set[int]) -> bool:
    """Walk ``value`` and its attributes and containers looking for a ``Conversation``."""
    if id(value) in seen or value is excluded:
        return False
    seen.add(id(value))
    if isinstance(value, Conversation):
        return True
    children: list[object] = []
    if isinstance(value, dict):
        children.extend(value.values())  # type: ignore[arg-type]  # scanned heterogeneously
    elif isinstance(value, list | tuple | set | frozenset):
        children.extend(value)  # type: ignore[arg-type]
    elif dataclasses.is_dataclass(value) and not isinstance(value, type):
        children.extend(getattr(value, f.name) for f in dataclasses.fields(value))
    elif hasattr(value, "__dict__"):
        children.extend(vars(value).values())
    return any(_holds_conversation(child, excluded=excluded, seen=seen) for child in children)


@pytest.fixture
def loader(container: LoomContainer) -> LoaderRecorder:
    """Recorder the loader use cases resolve from the container."""
    recorder = LoaderRecorder()
    container.register_instance(LoaderRecorder, recorder)
    return recorder


@pytest.fixture
def hooks(container: LoomContainer) -> HookRecorder:
    """Recorder the hook use cases resolve from the container."""
    recorder = HookRecorder()
    container.register_instance(HookRecorder, recorder)
    return recorder


@pytest.fixture
def conversation_deps() -> RecordingDepsFactory:
    """Deps factory serving the loader and hook use cases through a real executor."""
    return RecordingDepsFactory((LoadConversation, LoadStrict, RecordTurn, RecordStrictTurn))


# ---------------------------------------------------------------------------
# AC6 — the loader runs once, before the engine, as the caller
# ---------------------------------------------------------------------------


class TestLoaderExecution:
    async def test_runs_the_loader_once_with_the_context_when_the_run_carries_a_conversation_id(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The command carries the conversation and the run context; the engine gets the bytes."""
        engine = ScriptedEngine()
        plan = _plan()
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert len(loader.calls) == 1
        command = loader.calls[0].command
        assert isinstance(command, LoadCommand)
        assert command.conversation_id == "c-42"
        assert command.interaction_id == result.interaction_id
        assert command.subject == identity.subject
        assert command.mechanism == identity.mechanism
        assert command.agent == plan.name
        assert loader.calls[0].caller == identity
        assert engine.conversations == [Conversation(conversation_id="c-42", history=_HISTORY)]
        assert conversation_deps.uow.log == ["begin", "commit"]

    async def test_delivers_a_none_history_when_the_loader_returns_none(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A first turn has an id but no history."""
        loader.results = [None]
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert engine.conversations == [Conversation(conversation_id="c-42", history=None)]

    async def test_delivers_a_none_history_when_the_loader_returns_none_under_a_cap(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A ``None`` history is never measured against ``max_history_bytes``."""
        loader.results = [None]
        engine = ScriptedEngine()
        plan = _plan(policies=make_policies(max_history_bytes=1024))
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert engine.conversations == [Conversation(conversation_id="c-42", history=None)]

    async def test_delivers_the_history_when_it_measures_exactly_max_history_bytes(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The ceiling is inclusive: a history of exactly the bound reaches the engine intact."""
        loader.results = [b"x" * 2048]
        engine = ScriptedEngine()
        plan = _plan(policies=make_policies(max_history_bytes=2048))
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert engine.conversations == [Conversation(conversation_id="c-42", history=b"x" * 2048)]

    async def test_feeds_a_strict_command_when_it_declares_only_conversation_id(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The offered context is filtered to the Input's names before decoding."""
        engine = ScriptedEngine()
        runtime = _runtime(
            engine, _plan(loader=LoadStrict), deps=conversation_deps, container=container
        )

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert loader.calls[0].command == StrictLoadCommand(conversation_id="c-42")
        assert engine.conversations == [Conversation(conversation_id="c-42", history=_HISTORY)]

    async def test_does_not_run_the_loader_when_there_is_no_conversation_id(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Without a conversation the run is single-shot: no loader, ``None`` to the engine."""
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity)

        assert loader.calls == []
        assert engine.conversations == [None]
        assert conversation_deps.uow.log == []

    async def test_delivers_none_to_the_engine_when_the_plan_declares_no_conversation(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A ``conversation_id`` on a plan without a loader reaches no use case and no engine."""
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(loader=None), deps=conversation_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert loader.calls == []
        assert engine.conversations == [None]
        assert conversation_deps.uow.log == []


# ---------------------------------------------------------------------------
# AC7 — a loader failure fails the run before the engine starts
# ---------------------------------------------------------------------------


class TestLoaderFailures:
    async def test_fails_with_conversation_load_failed_without_the_detail_when_the_loader_raises(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        hooks: HookRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The caller gets the coded error and the id; no model runs, no hook runs."""
        loader.failure = ValueError("secret detail")
        engine = ScriptedEngine()
        plan = _plan(hook=RecordTurn)
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        error = failure.value
        assert error.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert error.interaction_id is not None
        assert str(error) == CONVERSATION_LOAD_FAILED_MESSAGE
        assert "secret detail" not in str(error)
        assert error.usage is None
        assert engine.stream_count == 0
        assert hooks.calls == []
        assert conversation_deps.uow.log == ["begin", "rollback"]
        assert is_retriable(error.code) is False
        assert run_error_class(error.code) is AgentRunErrorClass.APPLICATION

    async def test_maps_forbidden_to_unauthorized_when_the_loaders_rules_refuse(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A thread owned by another subject is a denial with the fixed text."""
        loader.failure = Forbidden("thread c-42 belongs to another subject")
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED
        assert str(failure.value) == _DENIED_MESSAGE
        assert failure.value.interaction_id is not None
        assert engine.stream_count == 0

    async def test_fails_with_conversation_load_timeout_when_the_loader_exceeds_tool_timeout_ms(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A loader sleeping past the bound is cut and reported as a retriable timeout."""
        loader.sleep_s = 5.0
        engine = ScriptedEngine()
        plan = _plan(policies=make_policies(tool_timeout_ms=50))
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)
        loop = asyncio.get_running_loop()

        async with runtime:
            started = loop.time()
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")
            elapsed = loop.time() - started

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT
        assert str(failure.value) == CONVERSATION_LOAD_TIMEOUT_MESSAGE
        assert failure.value.usage is None
        assert elapsed < 2.0, f"the loader was not bounded: {elapsed:.3f}s"
        assert loader.cancelled is True
        assert engine.stream_count == 0
        assert conversation_deps.uow.log == ["begin", "rollback"]
        assert is_retriable(failure.value.code) is True
        assert run_error_class(failure.value.code) is AgentRunErrorClass.INFRASTRUCTURE

    async def test_fails_with_conversation_load_timeout_when_the_loader_raises_timeout_error(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A ``TimeoutError`` from the loader's own I/O gets the same code as a bound cut."""
        loader.failure = TimeoutError()
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_TIMEOUT
        assert str(failure.value) == CONVERSATION_LOAD_TIMEOUT_MESSAGE
        assert failure.value.usage is None
        assert engine.stream_count == 0
        assert conversation_deps.uow.log == ["begin", "rollback"]

    async def test_fails_with_conversation_load_failed_when_the_loader_returns_a_str(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Only ``bytes`` or ``None`` cross the boundary; a ``str`` never reaches the engine."""
        loader.results = ["[]"]
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(failure.value) == CONVERSATION_LOAD_FAILED_MESSAGE
        assert failure.value.usage is None
        assert engine.stream_count == 0
        assert engine.conversations == []

    async def test_fails_with_conversation_load_failed_when_the_history_exceeds_max_history_bytes(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A history over the ceiling fails closed; the size and the bound stay server-side."""
        loader.results = [b"x" * 2049]
        engine = ScriptedEngine()
        plan = _plan(policies=make_policies(max_history_bytes=2048))
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            with (
                caplog.at_level(logging.ERROR, logger="loom.ai.runtime._bounded"),
                pytest.raises(AgentRunError) as failure,
            ):
                await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        error = failure.value
        assert error.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert str(error) == CONVERSATION_LOAD_FAILED_MESSAGE
        assert error.usage is None
        assert error.interaction_id is not None
        assert engine.stream_count == 0
        records = [
            record
            for record in caplog.records
            if record.name == "loom.ai.runtime._bounded" and record.levelno == logging.ERROR
        ]
        assert len(records) == 1
        assert records[0].exc_text is not None
        assert "2049" in records[0].exc_text
        assert "2048" in records[0].exc_text
        assert "2049" not in str(error)
        assert "2048" not in str(error)

    async def test_raises_on_entry_when_the_loader_fails_in_stream(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``run_stream`` raises before any event, so no stream ever opens."""
        loader.failure = ValueError("secret detail")
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            with pytest.raises(AgentRunError) as failure:
                async with runtime.run_stream(
                    _AGENT, "prompt", identity=identity, conversation_id="c-42"
                ):
                    pytest.fail("the stream opened despite the loader failure")

        assert failure.value.code is AgentRunErrorCode.CONVERSATION_LOAD_FAILED
        assert failure.value.interaction_id is not None
        assert engine.stream_count == 0


# ---------------------------------------------------------------------------
# AC8 — no caching, no cross-run state
# ---------------------------------------------------------------------------


class TestNoCache:
    async def test_loads_twice_as_each_caller_when_two_runs_share_a_conversation_id(
        self,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """Each run loads as its own caller and the engine receives each run's own bytes."""
        first = Identity(subject="user-1", roles=("analyst",), mechanism="test")
        second = Identity(subject="user-2", roles=("analyst",), mechanism="test")
        loader.results = [b"[1]", b"[2]"]
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            await runtime.run(_AGENT, "prompt", identity=first, conversation_id="c-42")
            await runtime.run(_AGENT, "prompt", identity=second, conversation_id="c-42")
            slots = vars(runtime)["_slots"]
            slot = slots[_AGENT]
            retained = _holds_conversation(vars(runtime), excluded=engine, seen=set()) or any(
                _holds_conversation(getattr(slot, f.name), excluded=engine, seen=set())
                for f in dataclasses.fields(slot)
            )

        assert len(loader.calls) == 2
        assert [call.caller for call in loader.calls] == [first, second]
        subjects = [
            command.subject
            for call in loader.calls
            if isinstance(command := call.command, LoadCommand)
        ]
        assert subjects == ["user-1", "user-2"]
        assert engine.conversations == [
            Conversation(conversation_id="c-42", history=b"[1]"),
            Conversation(conversation_id="c-42", history=b"[2]"),
        ]
        assert retained is False


# ---------------------------------------------------------------------------
# AC9 — the run's new messages reach the hook and the result
# ---------------------------------------------------------------------------


class TestMessagesInTheHook:
    async def test_delivers_the_messages_to_the_hook_when_the_final_carries_them(
        self,
        identity: Identity,
        hooks: HookRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """The hook command and the result carry the event's bytes verbatim."""
        engine = ScriptedEngine(script=_script(_NEW_MESSAGES))
        plan = _plan(loader=None, hook=RecordTurn)
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        command = hooks.calls[0].command
        assert isinstance(command, TurnCommand)
        assert command.messages == _NEW_MESSAGES
        assert command.conversation_id == "c-42"
        assert result.messages == _NEW_MESSAGES
        assert result.hook_result == TurnRecorded(interaction_id=command.interaction_id)

    async def test_delivers_none_messages_when_the_final_carries_none(
        self,
        identity: Identity,
        hooks: HookRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A run whose engine reports no messages offers ``None``; the result carries ``None``."""
        engine = ScriptedEngine(script=default_script({"answer": "42"}))
        plan = _plan(loader=None, hook=RecordTurn)
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        command = hooks.calls[0].command
        assert isinstance(command, TurnCommand)
        assert command.messages is None
        assert result.messages is None

    async def test_feeds_a_strict_command_when_it_declares_no_messages(
        self,
        identity: Identity,
        hooks: HookRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """``messages`` is filtered out before a strict Command decodes."""
        engine = ScriptedEngine(script=_script(_NEW_MESSAGES))
        plan = _plan(loader=None, hook=RecordStrictTurn)
        runtime = _runtime(engine, plan, deps=conversation_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity, conversation_id="c-42")

        assert isinstance(hooks.calls[0].command, StrictTurnCommand)
        assert result.interaction_id is not None
        assert result.hook_result == TurnRecorded(interaction_id=result.interaction_id)
        assert result.messages == _NEW_MESSAGES


# ---------------------------------------------------------------------------
# AC10 — the start-up probe covers conversational plans
# ---------------------------------------------------------------------------


class TestStartupProbe:
    async def test_refuses_startup_when_there_is_a_loader_and_the_bundle_carries_no_invoker(
        self,
        deps: StubDepsFactory,
        container: LoomContainer,
        lifecycle_log: list[str],
    ) -> None:
        """The probe names the agent and runs before any client opens."""
        clients = {
            "tools": StubMcpClient(label="tools", session=RecordingMcpSession(), log=lifecycle_log)
        }
        plan = _plan(capabilities=(make_mcp_capability("tools"),))
        runtime = AgentRuntime(
            plans=[plan],
            config=make_ai_config(mcp_servers=make_mcp_servers("tools")),
            engine_provider=CountingEngineProvider(),  # type: ignore[arg-type]
            deps=deps,
            container=container,
            mcp_client_factory=mcp_client_factory(clients),  # type: ignore[arg-type]
        )

        with pytest.raises(AgentCompilationError) as failure:
            async with runtime:
                pass

        issues = failure.value.issues
        assert [issue.code for issue in issues] == [AgentErrorCode.CONVERSATION_INVOKER_MISSING]
        assert _AGENT in issues[0].message
        assert lifecycle_log == []

    async def test_reports_both_codes_when_the_plan_declares_a_hook_and_a_loader(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """A plan declaring both surfaces both issues in one start-up failure."""
        runtime = _runtime(ScriptedEngine(), _plan(hook=RecordTurn), deps=deps, container=container)

        with pytest.raises(AgentCompilationError) as failure:
            async with runtime:
                pass

        issues = failure.value.issues
        assert [issue.code for issue in issues] == [
            AgentErrorCode.ON_OUTPUT_INVOKER_MISSING,
            AgentErrorCode.CONVERSATION_INVOKER_MISSING,
        ]
        assert all(_AGENT in issue.message for issue in issues)

    async def test_starts_normally_when_the_bundle_carries_a_bound_invoker(
        self,
        identity: Identity,
        loader: LoaderRecorder,
        conversation_deps: RecordingDepsFactory,
        container: LoomContainer,
    ) -> None:
        """A bound invoker satisfies the probe for a conversational plan."""
        engine = ScriptedEngine()
        runtime = _runtime(engine, _plan(), deps=conversation_deps, container=container)

        async with runtime:
            result = await runtime.run(_AGENT, "prompt", identity=identity)

        assert result.interaction_id is not None
        assert loader.calls == []
