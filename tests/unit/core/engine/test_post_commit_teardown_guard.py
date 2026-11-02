"""A teardown failure after a commit must not discard the queued invalidations.

``invocation()`` resets the invocation scope's contextvar token in its
``finally``, after every store has already committed.  If that reset raises —
a token created in a different context, in principle — the exception used
to propagate straight into ``_run_lifecycle``'s ``except BaseException``,
which called ``channel.discard()`` unconditionally: a write that had just
committed lost every queued invalidation to an unrelated bookkeeping
failure.  ``_ExecutionState.committed`` closes this: once set, the handler
drains instead of discarding, and only logs a further drain failure so the
teardown error is what actually propagates.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

import loom.core.uow.scope as scope_module
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitChannel, active_channel
from loom.core.use_case.use_case import UseCase

from ._lifecycle_doubles import Log, StubUnitOfWorkFactory


class _FailingResetScopeVar:
    """Stands in for the invocation scope's ContextVar, whose first ``reset`` fails.

    A real ``contextvars.Token`` cannot be monkeypatched (it is a built-in
    type), so the ContextVar-shaped object ``invocation()`` talks to is
    replaced instead; ``set``/``get`` still delegate to the real one so the
    rest of the executor's bookkeeping (nested-execution detection) is
    unaffected. The real token is reset before the failure is raised, and
    only the outermost execution's reset fails: an execution the drain
    starts then opens and closes its own scope normally.
    """

    def __init__(self, real: Any) -> None:
        self._real = real
        self._failed = False

    def set(self, value: Any) -> Any:
        return self._real.set(value)

    def get(self) -> Any:
        return self._real.get()

    def reset(self, token: Any) -> None:
        self._real.reset(token)
        if not self._failed:
            self._failed = True
            raise RuntimeError("token reset boom")


class _RecordingPriorityAction(UseCase[Any, str]):
    """Commits, then queues a priority action that records that it ran."""

    def __init__(self, log: Log) -> None:
        self._log = log

    async def execute(self, value: str) -> str:
        channel = active_channel()
        assert channel is not None
        channel.enqueue_priority(lambda: self._log("priority.ran"))
        return value


async def test_a_teardown_failure_after_commit_still_drains_instead_of_discarding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = Log()
    factory = StubUnitOfWorkFactory(log)
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=factory)
    monkeypatch.setattr(scope_module, "_scope", _FailingResetScopeVar(scope_module._scope))
    action = _RecordingPriorityAction(log)

    with pytest.raises(RuntimeError, match="token reset boom"):
        await executor.execute(action, params={"value": "x"})

    # The commit happened and the teardown failure is what propagated (not
    # some other error), but the queued invalidation still ran rather than
    # being thrown away with it.
    assert "uow.commit" in log.entries
    assert "priority.ran" in log.entries


class _NestedEnqueue(UseCase[Any, str]):
    """A use case that enqueues onto whatever channel is active when it runs."""

    def __init__(self, log: Log) -> None:
        self._log = log

    async def execute(self, value: str) -> str:
        channel = active_channel()
        assert channel is not None
        channel.enqueue(lambda: self._log("nested.ran"))
        return value


class _DispatchStartsNestedExecution(UseCase[Any, str]):
    """Commits, then queues a plain dispatch that runs a nested execution.

    The nested execution is exactly what an inline-mode job dispatch does
    (``_PendingDispatch.run`` calls ``self.executor.execute(...)``); this
    double reproduces that shape directly instead of wiring up the whole
    job-dispatch machinery.
    """

    def __init__(self, executor: RuntimeExecutor, log: Log) -> None:
        self._executor = executor
        self._log = log

    async def execute(self, value: str) -> str:
        channel = active_channel()
        assert channel is not None

        async def _dispatch() -> None:
            self._log("dispatch.started")
            await self._executor.execute(_NestedEnqueue(self._log), params={"value": "nested"})
            self._log("dispatch.finished")

        channel.enqueue(_dispatch)
        return value


async def test_a_late_enqueue_from_a_nested_execution_during_the_teardown_drain_is_not_lost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The channel must be unbound before this exceptional drain, same as the success path.

    Otherwise the nested execution a plain-lane dispatch starts sees the
    channel already mid-drain as its own ``active_channel()``, opens no
    channel of its own, and its enqueue lands on lists ``drain`` has
    already emptied into local deques — dropped without a trace.
    """
    log = Log()
    factory = StubUnitOfWorkFactory(log)
    executor = RuntimeExecutor(UseCaseCompiler(), uow_factory=factory)
    monkeypatch.setattr(scope_module, "_scope", _FailingResetScopeVar(scope_module._scope))
    action = _DispatchStartsNestedExecution(executor, log)

    with pytest.raises(RuntimeError, match="token reset boom"):
        await executor.execute(action, params={"value": "x"})

    assert "uow.commit" in log.entries
    assert "dispatch.finished" in log.entries
    assert "nested.ran" in log.entries


class _StubChannel:
    """Stands in for a channel whose ``drain`` raises whatever the test wants.

    Exercises ``_drain_committed_channel_logging_failure`` directly: a
    per-action failure is already wrapped into ``PostCommitError`` by
    ``PostCommitChannel.drain`` itself (``_run_all`` catches ``Exception``
    per action), so provoking a *different* ``Exception`` from ``drain()``
    needs a double, not a misbehaving action.
    """

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def drain(self, *, committed: bool) -> None:
        _ = committed
        raise self._error


class TestDrainCommittedChannelLoggingFailure:
    """The widened catch: any ``Exception``, not just ``PostCommitError``, is logged."""

    async def test_an_ordinary_exception_from_drain_is_logged_and_swallowed(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        executor = RuntimeExecutor(UseCaseCompiler())
        channel = _StubChannel(RuntimeError("shield machinery unavailable"))

        with caplog.at_level("ERROR"):
            await executor._drain_committed_channel_logging_failure(
                cast(PostCommitChannel, channel)
            )

        assert "PostCommitDrainFailedDuringTeardown" in caplog.text

    async def test_a_cancellation_from_drain_is_not_swallowed(self) -> None:
        executor = RuntimeExecutor(UseCaseCompiler())
        channel = cast(PostCommitChannel, _StubChannel(asyncio.CancelledError()))

        with pytest.raises(asyncio.CancelledError):
            await executor._drain_committed_channel_logging_failure(channel)
