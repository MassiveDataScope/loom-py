"""US1 s3: dispatches run after the close, in every execution shape, on every adapter."""

from __future__ import annotations

import pytest

from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitError
from loom.core.uow.scope import UndeclaredStoreWrite

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


async def test_a_read_only_execution_may_not_dispatch_a_job(
    case: LifecycleCase, executor: RuntimeExecutor, broker: Broker, metrics: Metrics
) -> None:
    """T120: a read-only execution writes nothing, through a job neither."""
    action = Dispatching(broker)

    with pytest.raises(UndeclaredStoreWrite, match="a job dispatch is refused"):
        await executor.execute(action, params={"value": "job-C"}, read_only=True)

    assert broker.sent == []
    assert case.factory.created == []
    assert metrics.kinds() == [EventKind.EXEC_START, EventKind.EXEC_ERROR]
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
