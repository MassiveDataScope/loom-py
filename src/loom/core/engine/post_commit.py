"""Post-commit channel: actions that run once a transaction has committed.

The executor binds one :class:`PostCommitChannel` per execution that owns a
unit of work (or when no channel is bound) and drains it after the commit.
Actions enqueued while a channel is bound belong to that channel; a
failed transaction discards them.  The owner unbinds the channel before
draining, so an execution started from an action opens its own lifecycle.
Inside an invocation scope the executor owns, :func:`store_channel` holds
each action on the channel of the store that owns it until that store has
ended (ADR 0008).
"""

from __future__ import annotations

import asyncio
import collections
import inspect
from collections.abc import Awaitable, Callable, Hashable, Iterable, Mapping
from contextvars import ContextVar, Token
from typing import Protocol, TypeVar, cast, runtime_checkable

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode
from loom.core.logger import get_logger
from loom.core.persistence.stores import StoreKey
from loom.core.uow.scope import current_scope

PostCommitAction = Callable[[], Awaitable[None] | None]
"""Zero-argument callable: sync (returning ``None`` or an awaitable) or async."""
ActionT = TypeVar("ActionT", bound=PostCommitAction)


@runtime_checkable
class LoggedAction(Protocol):
    """A post-commit action naming what it sends, for the logs: never its payload."""

    def log_fields(self) -> Iterable[Mapping[str, object]]: ...  # job_name, task_id per send


_channel: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_post_commit_channel", default=None
)
_shielded_tasks: set[asyncio.Task[None]] = set()
_logger = get_logger(__name__)


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
    of enqueue order. The caller picks the lane per call site; the channel
    does not infer it from what the action does.

    Example::

        channel = PostCommitChannel()
        channel.enqueue_publish(lambda: broker.send(message))
        await channel.drain(committed=True)
    """

    __slots__ = (
        "_actions",
        "_priority_actions",
        "_publish_actions",
        "_publish_groups",
        "_requeue_on_cancel",
    )

    def __init__(self, *, requeue_on_cancel: bool = False) -> None:
        self._priority_actions: list[PostCommitAction] = []
        self._publish_actions: list[PostCommitAction] = []
        self._actions: list[PostCommitAction] = []
        self._publish_groups: dict[Hashable, PostCommitAction] = {}
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
        """Queue a cheap broker send, after priority actions and before plain ones.

        Args:
            action: Sync callable returning ``None`` or an awaitable, or an
                async callable.
        """
        self._publish_actions.append(action)

    def publish_group(self, key: Hashable, create: Callable[[], ActionT]) -> ActionT:
        """The publish action queued under ``key`` until the next drain; ``create`` makes it."""
        queued = self._publish_groups.get(key)
        if queued is not None:
            return cast(ActionT, queued)
        action = create()
        self._publish_groups[key] = action
        self.enqueue_publish(action)
        return action

    def adopt(self, other: PostCommitChannel, *, priority_only: bool = False) -> None:
        """Move ``other``'s actions to the end of this channel's lanes, emptying ``other``.

        Args:
            other: Channel whose actions move here.
            priority_only: Move only its priority actions; the others are
                logged as never ran (:func:`log_never_ran`) and dropped.
        """
        self._priority_actions += other._priority_actions
        if priority_only:
            for action in (*other._publish_actions, *other._actions):
                log_never_ran(action)
        else:
            self._publish_actions += other._publish_actions
            self._actions += other._actions
        other.discard()

    def discard_dispatches(self) -> None:
        """Drop the publish and plain actions, keeping the priority ones (cache invalidations)."""
        self._publish_groups.clear()
        self._publish_actions.clear()
        self._actions.clear()

    def discard(self) -> None:
        """Drop every queued action without running it."""
        self.discard_dispatches()
        self._priority_actions.clear()

    async def drain(self, *, committed: bool) -> None:
        """Run every queued action; a failing one does not stop the others.

        * Priority and publish actions run in one shielded task: they report a
          write that has already committed (a cache bump, a broker send), are
          cheap, and must not be skipped by a cancellation.
        * Plain actions run cancellably: in inline mode one awaits a whole
          nested job execution, which a shield would make uncancellable. What an
          interruption leaves is re-queued when a later drain will run it
          (``requeue_on_cancel=True``), or dropped with a warning.

        Args:
            committed: Whether a transaction of this owner's had committed
                when the actions ran.  ``False`` when the owner held no unit
                of work, so a caller may safely retry the whole operation.

        Raises:
            PostCommitError: If any action raised; carries every failure, in
                priority, publish, then plain order.
        """
        shielded_failures: list[_Failure] = []
        plain_failures: list[_Failure] = []
        shielded_pending = collections.deque([*self._priority_actions, *self._publish_actions])
        self._priority_actions, self._publish_actions = [], []
        self._publish_groups.clear()
        shielded = asyncio.create_task(_run_all(shielded_pending, shielded_failures))
        _shielded_tasks.add(shielded)
        shielded.add_done_callback(_shielded_tasks.discard)
        pending: collections.deque[PostCommitAction] = collections.deque()
        try:
            await asyncio.shield(shielded)
            pending.extend(self._actions)
            self._actions = []
            await _run_all(pending, plain_failures)
        except BaseException:
            shielded.add_done_callback(
                lambda done: _log_unreported(done, shielded_pending, shielded_failures)
            )
            _log_failures(plain_failures)
            self._settle_interrupted(pending)
            raise
        failures = [error for _, error in (*shielded_failures, *plain_failures)]
        if failures:
            raise PostCommitError(committed=committed, failures=tuple(failures))

    def _settle_interrupted(self, pending: collections.deque[PostCommitAction]) -> None:
        remaining = [*pending, *self._actions]
        if self._requeue_on_cancel:
            self._actions = remaining
            return
        self._actions = []
        if remaining:
            _logger.warning(_PLAIN_DROPPED, count=len(remaining))


_Failure = tuple[PostCommitAction, Exception]
_PLAIN_DROPPED = "PostCommitPlainActionsDropped"
_SHIELDED_UNSENT = "PostCommitShieldedActionNeverRan"
_UNREPORTED_FAILURE = "PostCommitActionFailedAfterInterruption"


def _log_unreported(
    shielded: asyncio.Task[None],
    unsent: collections.deque[PostCommitAction],
    failures: list[_Failure],
) -> None:
    """Log what the shielded lanes could no longer report to a caller.

    The failures it collected, and, when the task itself died (the loop closing
    cancels it), every action it never ran.
    """
    _log_failures(failures)
    if shielded.cancelled() or shielded.exception() is not None:
        for action in unsent:
            log_never_ran(action)


def log_never_ran(action: PostCommitAction) -> None:
    """Log a shielded action the interruption stopped, by job name and task id only."""
    for fields in _describe(action):
        _logger.error(_SHIELDED_UNSENT, **fields)


def _log_failures(failures: list[_Failure]) -> None:
    for action, error in failures:
        for fields in _describe(action):
            _logger.error(_UNREPORTED_FAILURE, **fields, error=repr(error))


def _describe(action: PostCommitAction) -> Iterable[Mapping[str, object]]:
    """What a log may name of ``action``, never its repr: that would log the payload."""
    if isinstance(action, LoggedAction):
        return action.log_fields()
    return ({"job_name": None, "task_id": None},)


async def _run_all(pending: collections.deque[PostCommitAction], failures: list[_Failure]) -> None:
    """Pop and run ``pending`` in order; an interrupted action is not retried."""
    while pending:
        action = pending.popleft()
        try:
            await _run_one(action)
        except Exception as exc:  # collected and re-raised together by drain
            failures.append((action, exc))


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


def store_channel(store: StoreKey | None = None) -> PostCommitChannel | None:
    """Return the channel an action that depends on ``store`` is queued on.

    In an invocation scope that settles its actions by store, owned by this
    task, that is the channel of the stores the action depends on: ``store``
    alone (a cache invalidation), or with ``None`` every store of the running
    execution (a job dispatch: its primary and ``Joined`` stores), once
    enlisted. Anywhere else it is :func:`active_channel`.

    Args:
        store: Store the action depends on; ``None`` for the running
            execution's stores.
    """
    scope = current_scope()
    if scope is None or scope.post_commit is None or not scope.owned_here():
        return active_channel()
    running = next((f.stores for f in reversed(scope.frames) if f.primary), ())
    owners = frozenset((store,) if store else running)
    if not owners or not owners <= scope.participants.keys():
        return active_channel()
    channel = scope.store_channels.get(owners)
    if channel is None:
        channel = scope.store_channels[owners] = PostCommitChannel()
    return channel
