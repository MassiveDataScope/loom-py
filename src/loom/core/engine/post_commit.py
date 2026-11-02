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

PostCommitAction = Callable[[], Awaitable[None] | None]
"""Zero-argument callable: sync (returning ``None`` or an awaitable) or async."""

_channel: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_post_commit_channel", default=None
)


class PostCommitError(LoomError):
    """Raised when one or more post-commit actions failed after a commit.

    Every action runs even when an earlier one fails; the error carries all
    failures in enqueue order.

    Args:
        committed: Whether the transaction had committed when the actions ran.
        failures: Exceptions raised by the failed actions, in enqueue order.
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

    Two priorities, each FIFO among itself: :meth:`enqueue_priority` runs an
    action ahead of every plain :meth:`enqueue` action, regardless of the
    order the two calls were made in. Two actions of the same priority keep
    the order they were queued in. This only orders actions queued on *this*
    channel: a caller with more than one path to the same kind of action —
    :class:`~loom.core.cache.repository.CachedRepository` queues its own
    write's bump through :meth:`enqueue_priority` but the ``@transactional``
    hook walk queues the mixins' tagged bump through plain :meth:`enqueue` —
    decides per call site which lane it belongs in; the channel does not
    infer that from what the action does.

    Example::

        channel = PostCommitChannel()
        channel.enqueue(lambda: broker.send(message))
        await channel.drain(committed=True)
    """

    __slots__ = ("_actions", "_priority_actions")

    def __init__(self) -> None:
        self._priority_actions: list[PostCommitAction] = []
        self._actions: list[PostCommitAction] = []

    def enqueue(self, action: PostCommitAction) -> None:
        """Append an action; it runs after every :meth:`enqueue_priority` action.

        Args:
            action: Sync callable returning ``None`` or an awaitable, or an
                async callable.
        """
        self._actions.append(action)

    def enqueue_priority(self, action: PostCommitAction) -> None:
        """Queue an action ahead of every plain :meth:`enqueue` action.

        Use this for an action other queued actions may depend on having
        already run — a cache invalidation ahead of the job dispatches that
        might read the cache it invalidates. Two actions queued this way
        still run in the order they were queued.

        Args:
            action: Sync callable returning ``None`` or an awaitable, or an
                async callable.
        """
        self._priority_actions.append(action)

    def discard(self) -> None:
        """Drop every queued action without running it."""
        self._priority_actions.clear()
        self._actions.clear()

    async def drain(self, *, committed: bool) -> None:
        """Run every queued action: the priority lane shielded, then the plain lane.

        The owner unbinds the channel before draining, so an execution
        started from an action opens its own lifecycle. A failing action
        does not stop the others, in either lane; their failures collect
        into one :class:`PostCommitError`.

        The two lanes carry different durability stories, so they are run
        differently:

        * A priority action (:meth:`enqueue_priority`) describes a write
          that has *already committed* — that is the whole point of
          deferring it here rather than running it inline. It runs under
          ``asyncio.shield``: a cancellation reaching this call cannot
          interrupt it, so it always completes even if the caller gives up
          on waiting. It is expected to be cheap (a handful of cache
          ``incr`` calls), which is what makes the shield safe to take
          unconditionally.
        * A plain action (:meth:`enqueue`) — a job dispatch — has its own
          durability story and no such requirement, so it stays exactly as
          interruptible as before the priority lane existed: a cancellation
          arriving while the plain lane runs stops the drain at once, and
          the plain actions not yet run stay queued so a later drain can run
          them. In inline job mode a dispatched job's body is awaited here
          as a full nested use-case execution
          (:class:`~loom.core.job.service._PendingDispatch.run`), with no
          framework-imposed timeout — shielding it, as the priority lane
          is shielded, would make it uncancellable for as long as it ran;
          this is why the two lanes are not shielded alike.

        Args:
            committed: Whether a transaction of this owner's had committed
                when the actions ran.  ``False`` when the owner held no unit
                of work, so a caller may safely retry the whole operation.

        Raises:
            PostCommitError: If any action raised; carries every failure.
        """
        priority_pending = collections.deque(self._priority_actions)
        self._priority_actions = []
        failures: list[Exception] = []
        await asyncio.shield(_run_all(priority_pending, failures))

        pending = collections.deque(self._actions)
        self._actions = []
        try:
            await _run_all(pending, failures)
        except BaseException:
            # The priority lane cannot appear here: it already ran to
            # completion, shielded, above. Only the plain lane's remainder
            # needs to survive a cancellation for a later drain to run it.
            self._actions = [*pending, *self._actions]
            raise
        if failures:
            raise PostCommitError(committed=committed, failures=tuple(failures))


async def _run_all(pending: collections.deque[PostCommitAction], failures: list[Exception]) -> None:
    """Pop and run ``pending`` in order; an interrupted action is not retried."""
    while pending:
        action = pending.popleft()
        try:
            await _run_one(action)
        except Exception as exc:  # collected and re-raised together by drain
            failures.append(exc)


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
