"""US1 s3: dispatches run after the close, in every execution shape, on every adapter."""

from __future__ import annotations

import pytest

from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitError, active_channel

from .._lifecycle_doubles import (
    Broker,
    Dispatching,
    DispatchThenFail,
    Log,
    Metrics,
    Ok,
    context_is_clean,
)
from .conftest import LifecycleCase


async def test_dispatch_is_sent_after_the_close_and_after_exec_done(
    executor: RuntimeExecutor, broker: Broker, log: Log
) -> None:
    await executor.execute(Dispatching(broker), params={"value": "job-A"})

    assert broker.sent == ["job-A"]
    assert log.entries == [
        "event.EXEC_START",
        "uow.enter",
        "dispatch.queued(job-A)",
        "uow.exit",
        "event.EXEC_DONE",
        "broker.send(job-A) uow=False",
    ]


async def test_broker_failure_is_a_post_commit_error_after_a_committed_transaction(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker, metrics: Metrics, log: Log
) -> None:
    broker.failing.add("job-B")
    action = Dispatching(broker)

    with pytest.raises(PostCommitError) as info:
        await executor.execute(action, params={"value": "job-B"})

    assert info.value.committed is True
    assert [type(failure) for failure in info.value.failures] == [ConnectionError]
    assert case.factory.created[0].ended == ["commit"]
    assert case.probe.rollbacks() == 0
    assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_DONE]
    assert log.index("event.EXEC_DONE") < log.index("broker.send(job-B) uow=False")
    assert context_is_clean()


async def test_read_only_execution_opens_no_unit_of_work_and_sends_at_the_end(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker, metrics: Metrics, log: Log
) -> None:
    await executor.execute(Dispatching(broker), params={"value": "job-C"}, read_only=True)

    assert broker.sent == ["job-C"]
    assert case.factory.created == []
    assert metrics.only(EventKind.EXEC_DONE).commit_ms is None
    assert log.entries == [
        "event.EXEC_START",
        "dispatch.queued(job-C)",
        "event.EXEC_DONE",
        "broker.send(job-C) uow=False",
    ]
    assert active_channel() is None


async def test_a_read_only_dispatch_failure_is_reported_as_not_committed(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker, metrics: Metrics
) -> None:
    """A1: no unit of work was owned, so nothing committed and a retry is safe."""
    broker.failing.add("job-E")
    action = Dispatching(broker)

    with pytest.raises(PostCommitError) as info:
        await executor.execute(action, params={"value": "job-E"}, read_only=True)

    assert info.value.committed is False
    assert case.factory.created == []
    assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_DONE]
    assert context_is_clean()


async def test_failed_execution_discards_and_the_next_one_sends_nothing_stale(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker, log: Log
) -> None:
    action = DispatchThenFail(broker)
    with pytest.raises(RuntimeError, match="after dispatch"):
        await executor.execute(action, params={"value": "job-D"})
    await executor.execute(Ok(), params={"value": "later"})

    assert broker.sent == []
    assert not any(entry.startswith("broker.send") for entry in log.entries)
    assert case.factory.created[0].ended == ["rollback"]
    assert context_is_clean()
