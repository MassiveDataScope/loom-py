"""Pending job dispatches, delegated to the post-commit channel.

The public names are kept for existing callers; the queue itself is the
:class:`~loom.core.engine.post_commit.PostCommitChannel` that
:func:`~loom.core.engine.post_commit.store_channel` returns for the store the
running execution writes to.  When no channel is bound, a per-context fallback
channel holds the dispatches so :func:`flush_pending_dispatches` and
:func:`clear_pending_dispatches` keep working for hand-driven callers.

Inside an execution the executor owns the drain: :func:`flush_pending_dispatches`
refuses to run then, and :func:`clear_pending_dispatches` discards the
execution's dispatches rather than the fallback channel's. A read-only
execution writes nothing, through a job neither: queueing a dispatch from one
raises :class:`~loom.core.errors.UndeclaredStoreWrite`.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable
from contextvars import ContextVar
from typing import Any

from loom.core.engine.post_commit import ActionT, PostCommitChannel, active_channel, store_channel
from loom.core.uow.scope import current_scope

# default=None, never a channel instance: a shared default would leak
# dispatches across contexts.  Each context creates its own on first use.
_fallback: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_pending_dispatches", default=None
)


def _fallback_channel() -> PostCommitChannel:
    channel = _fallback.get()
    if channel is None:
        channel = PostCommitChannel(requeue_on_cancel=True)
        _fallback.set(channel)
    return channel


def add_pending_dispatch(fn: Callable[[], Any]) -> None:
    """Register a dispatch callable to run after the current UoW commits.

    In inline mode ``fn`` is an async coroutine method.
    :func:`flush_pending_dispatches` handles both transparently.

    Args:
        fn: Zero-argument callable.  May return ``None`` (sync) or a
            coroutine (async).
    """
    _target_channel().enqueue(fn)


def add_pending_publish(fn: Callable[[], Any]) -> None:
    """Register a broker send to run, shielded, after the current UoW commits."""
    _target_channel().enqueue_publish(fn)


def pending_publish_group(key: Hashable, create: Callable[[], ActionT]) -> ActionT:
    """The active channel's publish action under ``key``; see ``publish_group``."""
    return _target_channel().publish_group(key, create)


def _target_channel() -> PostCommitChannel:
    bound = store_channel()
    return _fallback_channel() if bound is None else bound


async def flush_pending_dispatches() -> None:
    """Execute all pending dispatches queued outside an execution and clear them.

    Sync callables (Celery ``send_task``) are called directly.
    Async callables (inline runner) are awaited.  Nothing committed here:
    the failures are reported as ``committed=False``.

    Raises:
        RuntimeError: If called inside an execution.  The executor owns the
            drain of its post-commit channel and runs it once the unit of
            work has closed.
        loom.core.errors.PostCommitError: If any dispatch raised;
            the remaining dispatches still run.
    """
    if active_channel() is not None:
        raise RuntimeError(
            "flush_pending_dispatches() cannot run inside an execution: "
            "RuntimeExecutor owns the post-commit channel and drains it "
            "after the unit of work closes."
        )
    channel = _fallback.get()
    if channel is None:
        return
    await channel.drain(committed=False)


def clear_pending_dispatches() -> None:
    """Discard the job dispatches and publishes queued in this context, never a cache invalidation.

    Inside an invocation the executor owns, those of every store of the
    invocation and those no store holds; elsewhere those of the bound channel,
    or of the fallback one. The priority lane stays: its cache invalidations
    describe writes that may still commit. Jobs registered during a failed
    transaction must not be sent to the broker.
    """
    bound = active_channel()
    channels = [_fallback.get() if bound is None else bound]
    scope = current_scope()
    if scope is not None:
        channels.extend(scope.channels())
    for channel in channels:
        if channel is not None:
            channel.discard_dispatches()
