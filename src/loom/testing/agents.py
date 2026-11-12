"""Deterministic AI-agent test double and the shared engine contract suite.

:class:`FakeAgentEngine` replays a fixed event script with no network, no
credentials, no clocks and no randomness, so runs are reproducible byte for
byte.  :func:`agent_engine_contract_suite` is the engine-agnostic contract
suite (FR-048): it exercises only the :class:`~loom.ai.abc.AgentEngine`
protocol surface and the ``loom.ai`` value types, so the same suite runs
unmodified against the fake and against any real engine adapter.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import MappingProxyType

from loom.ai.abc import (
    AgentEngine,
    AgentEvent,
    AgentResult,
    AgentUsage,
    Conversation,
    ErrorEvent,
    FinalEvent,
    HealthStatus,
    Prompt,
    TextDeltaEvent,
)
from loom.ai.errors import AgentRunErrorClass, AgentRunErrorCode, is_retriable, run_error_class
from loom.core.identity import Identity
from loom.core.model import LoomFrozenStruct

_DEFAULT_USAGE = AgentUsage(input_tokens=0, output_tokens=0, requests=1, duration_ms=0)


class FakeAgentRunError(Exception):
    """Terminal failure of a scripted :class:`FakeAgentEngine` run.

    Raised by :meth:`FakeAgentEngine.run` when the script ends in an
    ``ErrorEvent``.  Defined on the testing surface — not in ``loom.ai`` —
    because it is a detail of the fake: real engines raise their own errors.

    Args:
        code: Failure code of the terminal ``ErrorEvent`` that ended the run.
        message: Its description; prefixed with the code in the exception
            text, so a failed assertion names both.

    Attributes:
        code: Failure code carried by the terminal ``ErrorEvent``.
    """

    code: AgentRunErrorCode

    def __init__(self, code: AgentRunErrorCode, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def _default_script(output: object | None) -> tuple[AgentEvent, ...]:
    return (
        TextDeltaEvent(text="ok"),
        FinalEvent(output=output, usage=_DEFAULT_USAGE),
    )


def _validated_terminal(events: tuple[AgentEvent, ...]) -> FinalEvent | ErrorEvent:
    """Return the script's terminal event, rejecting malformed scripts."""
    if not events:
        raise ValueError("script must contain at least one event")
    terminal = events[-1]
    if not isinstance(terminal, FinalEvent | ErrorEvent):
        raise ValueError(
            "script must end in a terminal event (FinalEvent or ErrorEvent); "
            f"last event is {type(terminal).__name__}"
        )
    for position, event in enumerate(events[:-1]):
        if isinstance(event, FinalEvent | ErrorEvent):
            raise ValueError(
                f"script has a terminal {type(event).__name__} at position {position}; "
                "only the last event may be terminal"
            )
    return terminal


class FakeAgentEngine:
    """Deterministic, offline :class:`~loom.ai.abc.AgentEngine` test double.

    Replays a fixed event script: no network, no credentials, no clocks and
    no randomness, so two instances built from the same arguments produce
    byte-for-byte identical results and streams.

    Args:
        script: Event sequence to replay.  Must end in exactly one terminal
            event (``FinalEvent`` or ``ErrorEvent``), with no terminal event
            before the last position.  When omitted, a fixed default script
            ending in a ``FinalEvent`` carrying ``output`` is replayed.
        output: Output of the default script's ``FinalEvent``.  Ignored when
            ``script`` is provided.

    Raises:
        ValueError: If ``script`` is empty, does not end in a terminal
            event, or contains a terminal event before the last position.

    Example::

        engine = FakeAgentEngine(output={"answer": 42})
        result = await engine.run("question", identity=identity)
    """

    def __init__(
        self, *, script: Sequence[AgentEvent] | None = None, output: object | None = None
    ) -> None:
        events = _default_script(output) if script is None else tuple(script)
        self._terminal = _validated_terminal(events)
        self._script = events

    async def run(
        self,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AgentResult:
        """Replay the script to completion.

        Args:
            prompt: Caller prompt; ignored, the script is fixed.
            identity: Verified caller; ignored, the script is fixed.
            conversation: Conversation to continue; ignored, the script is fixed.
            state: This run's state; ignored, the script is fixed.

        Returns:
            The terminal ``FinalEvent``'s output, usage, messages and
            provider details.

        Raises:
            FakeAgentRunError: If the script ends in an ``ErrorEvent``.
        """
        del prompt, identity, conversation, state
        if isinstance(self._terminal, ErrorEvent):
            raise FakeAgentRunError(self._terminal.code, self._terminal.message)
        return AgentResult(
            output=self._terminal.output,
            usage=self._terminal.usage,
            messages=self._terminal.messages,
            provider_details=self._terminal.provider_details,
        )

    def run_stream(
        self,
        prompt: Prompt,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[AgentEvent]]:
        """Replay the script as an event stream.

        The returned context manager closes the iterator on exit via
        ``aclose()`` — deterministically, never left to the garbage
        collector — mirroring how a real engine must release its provider
        connection.

        Args:
            prompt: Caller prompt; ignored, the script is fixed.
            identity: Verified caller; ignored, the script is fixed.
            conversation: Conversation to continue; ignored, the script is fixed.
            state: This run's state; ignored, the script is fixed.

        Returns:
            An async context manager yielding the scripted event stream.
        """
        del prompt, identity, conversation, state
        return self._stream()

    async def health(self) -> HealthStatus:
        """Report a fixed ``"ok"`` status without any I/O.

        Returns:
            Always ``"ok"``: the fake has no dependency that could degrade.
        """
        return HealthStatus(status="ok")

    @asynccontextmanager
    async def _stream(self) -> AsyncIterator[AsyncIterator[AgentEvent]]:
        iterator = self._replay()
        try:
            yield iterator
        finally:
            await iterator.aclose()

    async def _replay(self) -> AsyncGenerator[AgentEvent, None]:
        for event in self._script:
            yield event


class ContractScenario(LoomFrozenStruct, frozen=True, kw_only=True):
    """Engine behaviour one contract check requires.

    An ``AgentPlan`` declares structure, not behaviour, while every contract
    check needs a scripted behaviour: a success run with its events, or a
    failure with its coded error.  The scenario is therefore the right seam —
    the suite hands this neutral description and the adapter under test
    builds an engine exhibiting it: the fake maps it onto a script, and a
    real-engine adapter can map it onto a stubbed provider (FR-048).

    Attributes:
        expected_output: Output ``run()`` and the terminal ``FinalEvent``
            must produce in a success scenario.
        events: Events the engine may replay in a success scenario,
            ending in a ``FinalEvent``; the suite checks stream structure
            only, never that these exact events come back.  ``None`` lets
            the engine choose its own events.
        error_code: When set, the scenario is a failure: the stream must end
            in an ``ErrorEvent`` with this code, and ``events`` is ignored.
    """

    expected_output: object = None
    events: tuple[AgentEvent, ...] | None = None
    error_code: AgentRunErrorCode | None = None


_SUITE_IDENTITY = Identity(subject="contract-suite")
_SUITE_PROMPT = "contract-suite"
_SUITE_OUTPUT: Mapping[str, str] = MappingProxyType({"answer": "contract"})
_SUITE_USAGE = AgentUsage(input_tokens=3, output_tokens=5, requests=1, duration_ms=7)


def _success_scenario() -> ContractScenario:
    events: tuple[AgentEvent, ...] = (
        TextDeltaEvent(text="contract "),
        FinalEvent(output=_SUITE_OUTPUT, usage=_SUITE_USAGE),
    )
    return ContractScenario(expected_output=_SUITE_OUTPUT, events=events)


def _error_scenario(code: AgentRunErrorCode) -> ContractScenario:
    return ContractScenario(error_code=code)


def _assert_valid_usage(usage: object) -> None:
    assert isinstance(usage, AgentUsage), "usage must be an AgentUsage"
    counters = (
        usage.input_tokens,
        usage.output_tokens,
        usage.requests,
        usage.duration_ms,
        usage.cache_read_tokens,
        usage.cache_write_tokens,
        usage.tool_calls,
    )
    assert all(value >= 0 for value in counters), "every usage counter must be >= 0"
    # An engine that cannot price a run reports no cost: a zero would be read
    # as a free run by anything comparing two models on what they spent.
    assert usage.cost is None or usage.cost >= 0, "cost must be absent or >= 0"


async def _collect_events(engine: AgentEngine) -> list[AgentEvent]:
    events: list[AgentEvent] = []
    async with engine.run_stream(_SUITE_PROMPT, identity=_SUITE_IDENTITY) as stream:
        async for event in stream:
            events.append(event)
    return events


async def _check_run_returns_result(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    engine = factory(_success_scenario())
    result = await engine.run(_SUITE_PROMPT, identity=_SUITE_IDENTITY)
    assert isinstance(result, AgentResult), "run() must return an AgentResult"
    assert result.output == _SUITE_OUTPUT, "run() output must match the scenario"
    _assert_valid_usage(result.usage)


async def _check_stream_success_ends_in_final(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    events = await _collect_events(factory(_success_scenario()))
    finals = [event for event in events if isinstance(event, FinalEvent)]
    assert len(finals) == 1, "a success stream must contain exactly one FinalEvent"
    assert isinstance(events[-1], FinalEvent), "the FinalEvent must be the last event"
    assert not any(isinstance(event, ErrorEvent) for event in events), (
        "a success stream must not contain an ErrorEvent"
    )


async def _check_stream_error_ends_in_error(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    events = await _collect_events(factory(_error_scenario(AgentRunErrorCode.PROVIDER_UNAVAILABLE)))
    errors = [event for event in events if isinstance(event, ErrorEvent)]
    assert len(errors) == 1, "an error stream must contain exactly one ErrorEvent"
    assert isinstance(events[-1], ErrorEvent), "the ErrorEvent must be the last event"
    assert not any(isinstance(event, FinalEvent) for event in events), (
        "an error stream must not contain a FinalEvent"
    )


async def _check_usage_only_on_final(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    events = await _collect_events(factory(_success_scenario()))
    for event in events:
        if isinstance(event, FinalEvent):
            _assert_valid_usage(event.usage)
            continue
        assert "usage" not in type(event).__struct_fields__, (
            f"{type(event).__name__} must not carry a usage field; only FinalEvent does"
        )


_NEVER_RETRIED_DESPITE_INFRASTRUCTURE: frozenset[AgentRunErrorCode] = frozenset(
    {AgentRunErrorCode.COST_NOT_MEASURABLE, AgentRunErrorCode.MCP_CALL_INTERRUPTED}
)
"""Codes FR-028's own class-implies-retriable rule does not hold for.

Kept as a closed, documented list rather than only inferred from
:func:`~loom.ai.errors.is_retriable`'s behaviour, so a future code added to
that function's private carve-out without updating this contract fails the
assertion below."""


async def _check_error_code_taxonomy(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    for code in AgentRunErrorCode:
        events = await _collect_events(factory(_error_scenario(code)))
        terminal = events[-1]
        assert isinstance(terminal, ErrorEvent), f"the stream for {code} must end in an ErrorEvent"
        assert terminal.code is code, f"ErrorEvent.code must be {code}, got {terminal.code}"
        retriable = (
            run_error_class(code) is AgentRunErrorClass.INFRASTRUCTURE
            and code not in _NEVER_RETRIED_DESPITE_INFRASTRUCTURE
        )
        assert is_retriable(code) == retriable, (
            f"is_retriable({code}) must be True iff its class is INFRASTRUCTURE (FR-028), "
            f"except the documented codes in _NEVER_RETRIED_DESPITE_INFRASTRUCTURE"
        )


async def _check_stream_close_is_deterministic(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    engine = factory(_success_scenario())
    async with engine.run_stream(_SUITE_PROMPT, identity=_SUITE_IDENTITY) as stream:
        await anext(stream)
    # A closed async generator raises StopAsyncIteration from __anext__;
    # engines wrapping the stream in another object may surface the interpreter's
    # RuntimeError("cannot reuse already closed ...") instead.  Both prove the
    # iterator was closed on exit, so both are accepted.
    try:
        await anext(stream)
    except (StopAsyncIteration, RuntimeError):
        return
    raise AssertionError("the iterator must be closed after leaving run_stream()")


async def _check_health_reports_known_status(
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    status = await factory(_success_scenario()).health()
    assert isinstance(status, HealthStatus), "health() must return a HealthStatus"
    assert status.status in {"ok", "degraded", "unavailable"}, (
        f"unknown health status {status.status!r}"
    )


_CONTRACT_CHECKS: tuple[
    tuple[str, Callable[[Callable[[ContractScenario], AgentEngine]], Awaitable[None]]], ...
] = (
    ("run_returns_result", _check_run_returns_result),
    ("stream_success_ends_in_final", _check_stream_success_ends_in_final),
    ("stream_error_ends_in_error", _check_stream_error_ends_in_error),
    ("usage_only_on_final", _check_usage_only_on_final),
    ("error_code_taxonomy", _check_error_code_taxonomy),
    ("stream_close_is_deterministic", _check_stream_close_is_deterministic),
    ("health_reports_known_status", _check_health_reports_known_status),
)


async def _run_check(
    check: Callable[[Callable[[ContractScenario], AgentEngine]], Awaitable[None]],
    factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    await check(factory)


def agent_engine_contract_suite(
    engine_factory: Callable[[ContractScenario], AgentEngine],
) -> None:
    """Run the shared :class:`~loom.ai.abc.AgentEngine` contract suite (FR-048).

    Every check exercises only the protocol surface and the ``loom.ai`` value
    types, never an engine's internals, so the same suite validates the fake
    and any real engine adapter.  Checks: ``run()`` result and usage shape,
    exactly-one-terminal streams for success and failure, usage carried only
    by ``FinalEvent``, the full run-time error-code taxonomy with FR-028
    retriability, deterministic stream closure on early exit, and a known
    ``health()`` status.

    The function is synchronous by design: each check runs in its own
    fresh event loop via ``asyncio.run``, so streams closed by one check can
    never leak into the next.  Call it from a synchronous test; calling it
    from an async test with a running loop raises ``RuntimeError``.

    Args:
        engine_factory: Builds one engine exhibiting the behaviour a
            :class:`ContractScenario` describes; called once per check
            invocation.

    Raises:
        AssertionError: If a check fails; the message names the check.
    """
    for name, check in _CONTRACT_CHECKS:
        try:
            asyncio.run(_run_check(check, engine_factory))
        except AssertionError as error:
            raise AssertionError(f"contract check '{name}' failed: {error}") from error
