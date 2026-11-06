"""Pending-dispatch helpers, inside and outside an execution."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

from loom.core.engine.post_commit import PostCommitChannel, bind_channel, reset_channel
from loom.core.errors import PostCommitError
from loom.core.job.context import (
    add_pending_dispatch,
    clear_pending_dispatches,
    flush_pending_dispatches,
)


@pytest.fixture
def bound_channel() -> Iterator[PostCommitChannel]:
    """Stand in for the channel the executor binds for the duration of an execution."""
    channel = PostCommitChannel()
    token = bind_channel(channel)
    try:
        yield channel
    finally:
        reset_channel(token)


async def test_add_and_flush_sync_callable() -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("a"))
    add_pending_dispatch(lambda: calls.append("b"))
    await flush_pending_dispatches()
    assert calls == ["a", "b"]


async def test_flush_clears_queue() -> None:
    add_pending_dispatch(lambda: None)
    await flush_pending_dispatches()
    # second flush should be a no-op
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("x"))
    await flush_pending_dispatches()
    assert calls == ["x"]


async def test_flush_empty_queue_is_noop() -> None:
    await flush_pending_dispatches()  # must not raise


async def test_flush_async_callable_is_awaited() -> None:
    calls: list[str] = []

    async def _async_fn() -> None:
        await asyncio.sleep(0)
        calls.append("async")

    add_pending_dispatch(_async_fn)
    await flush_pending_dispatches()
    assert calls == ["async"]


async def test_clear_discards_without_executing() -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("should-not-run"))
    clear_pending_dispatches()
    await flush_pending_dispatches()
    assert calls == []


async def test_contextvar_isolated_between_tasks() -> None:
    """Each asyncio task must have its own independent pending queue."""
    task_calls: dict[str, list[str]] = {"a": [], "b": []}

    async def task_a() -> None:
        add_pending_dispatch(lambda: task_calls["a"].append("a"))
        await flush_pending_dispatches()

    async def task_b() -> None:
        add_pending_dispatch(lambda: task_calls["b"].append("b"))
        await flush_pending_dispatches()

    await asyncio.gather(task_a(), task_b())

    assert task_calls["a"] == ["a"]
    assert task_calls["b"] == ["b"]


async def test_contextvar_default_not_shared() -> None:
    """Appending in one context must not affect a fresh context."""
    add_pending_dispatch(lambda: None)

    calls: list[str] = []

    async def fresh_context() -> None:
        # A task spawned before any dispatch exists starts with no channel of its own.
        await flush_pending_dispatches()
        add_pending_dispatch(lambda: calls.append("fresh"))
        await flush_pending_dispatches()

    task = asyncio.ensure_future(fresh_context())
    await task
    assert calls == ["fresh"]


# ---------------------------------------------------------------------------
# A2 — inside an execution the executor owns the channel
# ---------------------------------------------------------------------------


async def test_flush_inside_an_execution_refuses_instead_of_doing_nothing(
    bound_channel: PostCommitChannel,
) -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("queued"))

    with pytest.raises(RuntimeError, match="RuntimeExecutor owns the post-commit channel"):
        await flush_pending_dispatches()

    await bound_channel.drain(committed=False)
    assert calls == ["queued"]


async def test_clear_inside_an_execution_discards_the_bound_channel(
    bound_channel: PostCommitChannel,
) -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("must-not-run"))

    clear_pending_dispatches()

    await bound_channel.drain(committed=False)
    assert calls == []


async def test_clear_outside_an_execution_still_discards_the_fallback() -> None:
    calls: list[str] = []
    add_pending_dispatch(lambda: calls.append("must-not-run"))

    clear_pending_dispatches()
    await flush_pending_dispatches()

    assert calls == []


async def test_flush_outside_an_execution_reports_failures_as_not_committed() -> None:
    """A1: no unit of work was involved, so the caller may retry the whole operation."""

    def _fail() -> None:
        raise ConnectionError("broker down")

    add_pending_dispatch(_fail)

    with pytest.raises(PostCommitError) as info:
        await flush_pending_dispatches()

    assert info.value.committed is False
