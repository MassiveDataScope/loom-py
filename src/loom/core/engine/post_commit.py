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
from collections.abc import Awaitable, Callable, Hashable
from contextvars import ContextVar, Token
from typing import TypeVar, cast

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode
from loom.core.logger import get_logger

PostCommitAction = Callable[[], Awaitable[None] | None]
"""Zero-argument callable: sync (returning ``None`` or an awaitable) or async."""
ActionT = TypeVar("ActionT", bound=PostCommitAction)

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
        """The publish action queued under ``key`` since the last drain, created on first use.

        Lets a broker gather the sends of one transaction into a single action,
        such as one batch call.

        Args:
            key: Who owns the group, such as the job service.
            create: Builds the action and is called once per drain.
        """
        queued = self._publish_groups.get(key)
        if queued is not None:
            return cast(ActionT, queued)
        action = create()
        self._publish_groups[key] = action
        self.enqueue_publish(action)
        return action

    def discard(self) -> None:
        """Drop every queued action without running it."""
        self._publish_groups.clear()
        self._priority_actions.clear()
        self._publish_actions.clear()
        self._actions.clear()

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
    _logger.error(_SHIELDED_UNSENT, **_describe(action))


def _log_failures(failures: list[_Failure]) -> None:
    for action, error in failures:
        _logger.error(_UNREPORTED_FAILURE, **_describe(action), error=repr(error))


def _describe(action: PostCommitAction) -> dict[str, object]:
    """A broker send's job name and task id, never its repr: that would log the payload."""
    return {
        "job_name": getattr(action, "task_name", None),
        "task_id": getattr(action, "task_id", None),
    }


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
