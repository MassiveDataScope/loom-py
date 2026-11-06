"""Tests for the post-commit channel (spec 005, design D1)."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.engine.post_commit import (
    PostCommitChannel,
    active_channel,
    bind_channel,
    reset_channel,
)
from loom.core.errors import LoomError, PostCommitError


@pytest.mark.parametrize(("committed", "retryable"), [(True, False), (False, True)])
def test_a_post_commit_error_is_retryable_only_without_a_commit(
    committed: bool, retryable: bool
) -> None:
    error = PostCommitError(committed=committed, failures=(ConnectionError("broker down"),))

    assert error.retryable is retryable


async def test_drain_runs_actions_in_enqueue_order() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("a"))
    channel.enqueue(lambda: calls.append("b"))
    channel.enqueue(lambda: calls.append("c"))

    await channel.drain(committed=True)

    assert calls == ["a", "b", "c"]


async def test_drain_awaits_sync_returning_awaitable_and_async_callables() -> None:
    calls: list[str] = []

    async def _async_action() -> None:
        await asyncio.sleep(0)
        calls.append("async")

    def _sync_returning_awaitable() -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        loop.call_soon(lambda: (calls.append("future"), future.set_result(None)))
        return future

    channel = PostCommitChannel()
    channel.enqueue(_async_action)
    channel.enqueue(_sync_returning_awaitable)
    channel.enqueue(lambda: calls.append("sync"))

    await channel.drain(committed=True)

    assert calls == ["async", "future", "sync"]


async def test_drain_continues_after_failure_then_raises_with_all_failures() -> None:
    calls: list[str] = []
    first = RuntimeError("first")
    second = ValueError("second")

    def _fail(exc: Exception) -> None:
        raise exc

    channel = PostCommitChannel()
    channel.enqueue(lambda: _fail(first))
    channel.enqueue(lambda: calls.append("middle"))
    channel.enqueue(lambda: _fail(second))
    channel.enqueue(lambda: calls.append("last"))

    with pytest.raises(PostCommitError) as info:
        await channel.drain(committed=True)

    assert calls == ["middle", "last"]
    assert info.value.committed is True
    assert info.value.failures == (first, second)
    assert isinstance(info.value, LoomError)
    assert info.value.code == "post_commit_failure"


async def test_drain_empties_the_channel_so_a_second_one_runs_nothing() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("once"))

    await channel.drain(committed=True)
    await channel.drain(committed=True)

    assert calls == ["once"]


async def test_drain_reports_the_committed_flag_its_owner_passed() -> None:
    """A1: only the owner knows whether a transaction of its own committed."""

    def _fail() -> None:
        raise ConnectionError("broker down")

    channel = PostCommitChannel()
    channel.enqueue(_fail)

    with pytest.raises(PostCommitError) as info:
        await channel.drain(committed=False)

    assert info.value.committed is False


async def test_discard_drops_actions_without_running_them() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()
    channel.enqueue(lambda: calls.append("never"))

    channel.discard()
    await channel.drain(committed=True)

    assert calls == []


async def test_the_owner_unbinds_before_draining_so_actions_see_no_channel() -> None:
    """A6: unbinding is the owner's job, and the only mechanism."""
    seen: list[PostCommitChannel | None] = []
    channel = PostCommitChannel()
    token = bind_channel(channel)
    channel.enqueue(lambda: seen.append(active_channel()))
    assert active_channel() is channel

    reset_channel(token)
    await channel.drain(committed=True)

    assert seen == [None]
    assert active_channel() is None


async def test_nested_drain_from_an_action_does_not_recurse() -> None:
    calls: list[str] = []
    channel = PostCommitChannel()

    async def _drain_again() -> None:
        calls.append("outer")
        await channel.drain(committed=True)

    channel.enqueue(_drain_again)
    channel.enqueue(lambda: calls.append("second"))

    await channel.drain(committed=True)

    assert calls == ["outer", "second"]


def test_bind_and_reset_restore_previous_channel() -> None:
    outer = PostCommitChannel()
    inner = PostCommitChannel()
    outer_token = bind_channel(outer)
    inner_token = bind_channel(inner)

    assert active_channel() is inner
    reset_channel(inner_token)
    assert active_channel() is outer
    reset_channel(outer_token)
    assert active_channel() is None


class TestEnqueuePriority:
    """F2: a cache invalidation must precede the job dispatches it might invalidate."""

    async def test_a_priority_action_runs_before_a_plain_one_queued_earlier(self) -> None:
        """The F2 regression: dispatch queued during the pipeline, write follows it.

        A use case body dispatches a job, then writes: the dispatch is
        queued first, the bump second — exactly the interleaving F2 named.
        The bump still runs first.
        """
        calls: list[str] = []
        channel = PostCommitChannel()
        channel.enqueue(lambda: calls.append("dispatch"))
        channel.enqueue_priority(lambda: calls.append("bump"))

        await channel.drain(committed=True)

        assert calls == ["bump", "dispatch"]

    async def test_two_priority_actions_keep_their_relative_order(self) -> None:
        calls: list[str] = []
        channel = PostCommitChannel()
        channel.enqueue_priority(lambda: calls.append("first_bump"))
        channel.enqueue_priority(lambda: calls.append("second_bump"))
        channel.enqueue(lambda: calls.append("dispatch"))

        await channel.drain(committed=True)

        assert calls == ["first_bump", "second_bump", "dispatch"]

    async def test_discard_drops_priority_actions_too(self) -> None:
        calls: list[str] = []
        channel = PostCommitChannel()
        channel.enqueue_priority(lambda: calls.append("bump"))
        channel.enqueue(lambda: calls.append("dispatch"))

        channel.discard()
        await channel.drain(committed=True)

        assert calls == []


@pytest.mark.parametrize(("requeue", "after"), [(True, ["after"]), (False, [])])
async def test_cancellation_propagates_and_requeues_only_for_a_later_drain(
    requeue: bool, after: list[str]
) -> None:
    calls: list[str] = []
    channel = PostCommitChannel(requeue_on_cancel=requeue)

    def _cancel() -> None:
        raise asyncio.CancelledError()

    channel.enqueue(_cancel)
    channel.enqueue(lambda: calls.append("after"))

    with pytest.raises(asyncio.CancelledError):
        await channel.drain(committed=True)
    assert calls == []

    await channel.drain(committed=True)
    assert calls == after
