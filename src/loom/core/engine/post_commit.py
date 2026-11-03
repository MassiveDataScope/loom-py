"""Post-commit channel: actions that run once a transaction has committed.

The executor binds one :class:`PostCommitChannel` per execution that owns a
unit of work (or when no channel is bound) and drains it after the commit.
Actions enqueued while a channel is bound belong to that channel; a
failed transaction discards them.  The owner unbinds the channel before
draining, so an execution started from an action opens its own lifecycle.
"""

from __future__ import annotations

import asyncio
import collections
import inspect
from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode
from loom.core.logger import get_logger

PostCommitAction = Callable[[], Awaitable[None] | None]
"""Zero-argument callable: sync (returning ``None`` or an awaitable) or async."""

_channel: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_post_commit_channel", default=None
)
_shielded_tasks: set[asyncio.Task[None]] = set()


class PostCommitError(LoomError):
    """Raised when one or more post-commit actions failed after a commit.

    Every action runs even when an earlier one fails; the error carries all
    failures in priority, publish, then plain order.

    Args:
        committed: Whether the transaction had committed when the actions ran.
        failures: Exceptions in priority, publish, then plain order.
    """

    def __init__(self, *, committed: bool, failures: tuple[Exception, ...]) -> None:
        self.committed = committed
        # A retry once the transaction committed would apply it twice.
        self.retryable = not committed
        self.failures = failures
        super().__init__(
            f"{len(failures)} post-commit action(s) failed (committed={committed})",
            code=ErrorCode.POST_COMMIT_FAILURE,
        )


class PostCommitChannel:
    """Ordered queue of actions to run after the owning transaction commits.

    Three FIFO lanes run in priority, publish, then plain order, regardless
    of enqueue order. This only orders actions queued on *this*
    channel: a caller with more than one path to the same kind of action —
    :class:`~loom.core.cache.repository.CachedRepository` queues its own
    write's bump through :meth:`enqueue_priority` but the ``@transactional``
    hook walk queues the mixins' tagged bump through plain :meth:`enqueue` —
    decides per call site which lane it belongs in; the channel does not
    infer that from what the action does.

    Example::

        channel = PostCommitChannel()
        channel.enqueue_publish(lambda: broker.send(message))
        await channel.drain(committed=True)
    """

    __slots__ = ("_actions", "_priority_actions", "_publish_actions", "_requeue_on_cancel")

    def __init__(self, *, requeue_on_cancel: bool = True) -> None:
        self._priority_actions: list[PostCommitAction] = []
        self._publish_actions: list[PostCommitAction] = []
        self._actions: list[PostCommitAction] = []
        self._requeue_on_cancel = requeue_on_cancel

    def enqueue(self, action: PostCommitAction) -> None:
        """Append an action after priority and publish actions.

        Args:
            action: Sync callable returning ``None`` or an awaitable, or an
                async callable.
        """
        self._actions.append(action)

    def enqueue_priority(self, action: PostCommitAction) -> None:
        """Queue an action ahead of publish and plain actions.

        Use this for an action other queued actions may depend on having
        already run — a cache invalidation ahead of the job dispatches that
        might read the cache it invalidates. Two actions queued this way
        still run in the order they were queued.

        Args:
            action: Sync callable returning ``None`` or an awaitable, or an
                async callable.
        """
        self._priority_actions.append(action)

    def enqueue_publish(self, action: PostCommitAction) -> None:
        """Queue a cheap broker send after priority actions and before plain actions."""
        self._publish_actions.append(action)

    def discard(self) -> None:
        """Drop every queued action without running it."""
        self._priority_actions.clear()
        self._publish_actions.clear()
        self._actions.clear()

    async def drain(self, *, committed: bool) -> None:
        """Run priority and publish under one shield, then plain actions cancellably.

        The owner unbinds the channel before draining, so an execution
        started from an action opens its own lifecycle. A failing action
        does not stop the others, in any lane; their failures collect
        into one :class:`PostCommitError`.

        The lanes carry different durability stories:

        * A priority action (:meth:`enqueue_priority`) describes a write
          that has *already committed* — that is the whole point of
          deferring it here rather than running it inline. It runs under
          ``asyncio.shield``: a cancellation reaching this call cannot
          interrupt it, so it always completes even if the caller gives up
          on waiting. It is expected to be cheap (a handful of cache
          ``incr`` calls), which is what makes the shield safe to take
          unconditionally.
        * A publish action (:meth:`enqueue_publish`) is a cheap broker send.
          It runs in the same shield as priority actions, so cancellation
          during a cache bump cannot skip a committed job send. Every publish
          now runs before every inline job; previously they shared one FIFO.
        * A plain action (:meth:`enqueue`) stays interruptible. On cancellation
          its remainder is requeued only when this channel has a later drain;
          otherwise that remainder is dropped with a warning. In inline job
          mode a dispatched job's body is awaited here
          as a full nested use-case execution
          (:class:`~loom.core.job.service._PendingDispatch.run`), with no
          framework-imposed timeout — shielding it, as the priority lane
          is shielded, would make it uncancellable for as long as it ran;
          this is why the plain lane is not shielded.

        Args:
            committed: Whether a transaction of this owner's had committed
                when the actions ran.  ``False`` when the owner held no unit
                of work, so a caller may safely retry the whole operation.

        Raises:
            PostCommitError: If any action raised; carries every failure.
        """
        failures: list[Exception] = []
        shielded_failures: list[tuple[PostCommitAction, Exception]] = []
        shielded = asyncio.create_task(self._drain_shielded(failures, shielded_failures))
        _shielded_tasks.add(shielded)
        shielded.add_done_callback(_shielded_tasks.discard)
        try:
            await asyncio.shield(shielded)
        except asyncio.CancelledError:
            shielded.add_done_callback(lambda _: _log_shielded_failures(shielded_failures))
            self._abandon_plain(collections.deque())
            raise

        pending = collections.deque(self._actions)
        self._actions = []
        try:
            await _run_all(pending, failures)
        except asyncio.CancelledError:
            _log_shielded_failures(shielded_failures)
            self._abandon_plain(pending)
            raise
        except BaseException:
            self._actions = [*pending, *self._actions]
            raise
        if failures:
            raise PostCommitError(committed=committed, failures=tuple(failures))

    def _abandon_plain(self, pending: collections.deque[PostCommitAction]) -> None:
        """Keep the plain actions a cancellation left for a later drain, or drop them loudly."""
        remaining = [*pending, *self._actions]
        if self._requeue_on_cancel:
            self._actions = remaining
            return
        self._actions = []
        if remaining:
            get_logger(__name__).warning("PostCommitPlainActionsDropped", count=len(remaining))

    async def _drain_shielded(
        self,
        failures: list[Exception],
        failed_actions: list[tuple[PostCommitAction, Exception]],
    ) -> None:
        priority_pending = collections.deque(self._priority_actions)
        self._priority_actions = []
        await _run_all(priority_pending, failures, failed_actions)
        publish_pending = collections.deque(self._publish_actions)
        self._publish_actions = []
        await _run_all(publish_pending, failures, failed_actions)


def _log_shielded_failures(failed_actions: list[tuple[PostCommitAction, Exception]]) -> None:
    """Log what a shielded action raised once a cancellation left no caller to raise it to."""
    logger = get_logger(__name__)
    for action, error in failed_actions:
        logger.error(
            "PostCommitShieldedActionFailedAfterCancellation",
            job_name=getattr(action, "task_name", None),
            task_id=getattr(action, "task_id", None),
            error=repr(error),
        )


async def _run_all(
    pending: collections.deque[PostCommitAction],
    failures: list[Exception],
    failed_actions: list[tuple[PostCommitAction, Exception]] | None = None,
) -> None:
    """Pop and run ``pending`` in order; an interrupted action is not retried."""
    while pending:
        action = pending.popleft()
        try:
            await _run_one(action)
        except Exception as exc:  # collected and re-raised together by drain
            failures.append(exc)
            if failed_actions is not None:
                failed_actions.append((action, exc))


async def _run_one(action: PostCommitAction) -> None:
    result = action()
    if inspect.isawaitable(result):
        await result


def bind_channel(channel: PostCommitChannel) -> Token[PostCommitChannel | None]:
    """Make ``channel`` the active channel for the current context.

    Args:
        channel: Channel that receives subsequent enqueues.

    Returns:
        Token to pass to :func:`reset_channel`.
    """
    return _channel.set(channel)


def reset_channel(token: Token[PostCommitChannel | None]) -> None:
    """Restore the channel that was active before :func:`bind_channel`.

    Args:
        token: Token returned by :func:`bind_channel`.
    """
    _channel.reset(token)


def active_channel() -> PostCommitChannel | None:
    """Return the channel bound to the current context, if any."""
    return _channel.get()
