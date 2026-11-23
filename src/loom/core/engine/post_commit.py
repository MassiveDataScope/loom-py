"""Post-commit channel: actions that run once a transaction has committed.

The executor binds one :class:`PostCommitChannel` per execution that owns a
unit of work (or when no channel is bound) and drains it after the commit.
Actions enqueued while a channel is bound belong to that channel; a
failed transaction discards them.  The owner unbinds the channel before
draining, so an execution started from an action opens its own lifecycle.
Inside an invocation scope the executor owns, :func:`store_channel` holds
each action on the channel of the store that owns it until that store has
ended.
"""

from __future__ import annotations

import asyncio
import collections
import inspect
import logging
from collections.abc import Awaitable, Callable, Hashable, Iterable
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Protocol, TypeVar, cast, runtime_checkable

from loom.core.concurrency import held_task
from loom.core.errors import PostCommitError
from loom.core.errors.summary import ErrorSummary
from loom.core.execution import execution_ended
from loom.core.log_names import LogEvent, LogField, LogKey
from loom.core.stores.catalog import StoreKey
from loom.core.stores.uow.scope import current_scope, refuse_write_under_read_only

PostCommitAction = Callable[[], Awaitable[None] | None]
"""Zero-argument callable: sync (returning ``None`` or an awaitable) or async."""
ActionT = TypeVar("ActionT", bound=PostCommitAction)


@dataclass(frozen=True)
class ActionSubject:
    """What a log may name of a post-commit action: the job and task it sends, never its payload."""

    job: str | None = None
    task_id: str | None = None

    def fields(self) -> dict[str, object]:
        """The subject as log fields, ``None`` for what the action does not name."""
        return {LogField.JOB: self.job, LogField.TASK_ID: self.task_id}


@runtime_checkable
class LoggedAction(Protocol):
    """A post-commit action naming what it sends, for the logs: never its payload."""

    def subjects(self) -> Iterable[ActionSubject]: ...


_channel: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_post_commit_channel", default=None
)
_logger = logging.getLogger(__name__)


class PostCommitEvent(LogEvent):
    """What the post-commit channel logs."""

    PLAIN_ACTIONS_DROPPED = "engine.post_commit_plain_actions_dropped"
    DRAIN_FAILED_DURING_TEARDOWN = "engine.post_commit_drain_failed_during_teardown"
    ACTION_FAILED = "engine.post_commit_action_failed"
    ACTION_FAILED_AFTER_INTERRUPTION = "engine.post_commit_action_failed_after_interruption"
    SHIELDED_ACTION_NEVER_RAN = "engine.post_commit_shielded_action_never_ran"


class PostCommitKey(LogKey):
    """The fields of a post-commit record."""

    DROPPED_ACTIONS = "loom.post_commit.dropped_actions"


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
            other.discard_dispatches(log=True)
        else:
            self._publish_actions += other._publish_actions
            self._actions += other._actions
        other.discard()

    def discard_dispatches(self, *, log: bool = False) -> None:
        """Drop the publish and plain actions, keeping the priority ones (cache invalidations).

        Args:
            log: Log each dropped action as never ran (:func:`log_never_ran`).
        """
        if log:
            for action in (*self._publish_actions, *self._actions):
                log_never_ran(action)
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
        if not (self._priority_actions or self._publish_actions or self._actions):
            return
        shielded_failures: list[_Failure] = []
        plain_failures: list[_Failure] = []
        shielded_pending = collections.deque([*self._priority_actions, *self._publish_actions])
        self._priority_actions, self._publish_actions = [], []
        self._publish_groups.clear()
        shielded = held_task(_run_all(shielded_pending, shielded_failures))
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
            _log_failures(plain_failures, PostCommitEvent.ACTION_FAILED_AFTER_INTERRUPTION)
            self._settle_interrupted(pending)
            raise
        failed = [*shielded_failures, *plain_failures]
        if failed:
            _log_failures(failed, PostCommitEvent.ACTION_FAILED)
            raise PostCommitError(committed=committed, failures=tuple(e for _, e in failed))

    def _settle_interrupted(self, pending: collections.deque[PostCommitAction]) -> None:
        remaining = [*pending, *self._actions]
        if self._requeue_on_cancel:
            self._actions = remaining
            return
        self._actions = []
        if remaining:
            _logger.warning(
                PostCommitEvent.PLAIN_ACTIONS_DROPPED,
                extra={PostCommitKey.DROPPED_ACTIONS: len(remaining)},
            )


_Failure = tuple[PostCommitAction, Exception]


def _log_unreported(
    shielded: asyncio.Task[None],
    unsent: collections.deque[PostCommitAction],
    failures: list[_Failure],
) -> None:
    """Log what the shielded lanes could no longer report to a caller.

    The failures it collected, and, when the task itself died (the loop closing
    cancels it), every action it never ran.
    """
    _log_failures(failures, PostCommitEvent.ACTION_FAILED_AFTER_INTERRUPTION)
    if shielded.cancelled() or shielded.exception() is not None:
        for action in unsent:
            log_never_ran(action)


async def drain_after_failure(channel: PostCommitChannel) -> None:
    """Drain the actions of the stores that may have written, in a failed invocation.

    The caller re-raises the failure that brought it here, so a drain
    failure is logged rather than raised: it would hide that failure. A
    cancellation is not caught: it is a terminal outcome of its own.
    """
    try:
        await channel.drain(committed=True)
    except Exception:
        _logger.exception(PostCommitEvent.DRAIN_FAILED_DURING_TEARDOWN)


def log_never_ran(action: PostCommitAction) -> None:
    """Log a shielded action the interruption stopped, by job name and task id only."""
    for subject in _describe(action):
        _logger.error(PostCommitEvent.SHIELDED_ACTION_NEVER_RAN, extra=subject.fields())


def _log_failures(failures: list[_Failure], event: PostCommitEvent) -> None:
    for action, error in failures:
        for subject in _describe(action):
            _logger.error(event, extra={**subject.fields(), **ErrorSummary.of(error).fields()})


def _describe(action: PostCommitAction) -> Iterable[ActionSubject]:
    """What a log may name of ``action``, never its repr: that would log the payload."""
    if isinstance(action, LoggedAction):
        return action.subjects()
    return (ActionSubject(),)


async def _run_all(pending: collections.deque[PostCommitAction], failures: list[_Failure]) -> None:
    """Pop and run ``pending`` in order; an interrupted action is not retried."""
    while pending:
        action = pending.popleft()
        try:
            await _run_one(action)
        except Exception as exc:
            failures.append((action, exc))


async def _run_one(action: PostCommitAction) -> None:
    result = action()
    if inspect.isawaitable(result):
        await result


def bind_channel(channel: PostCommitChannel | None) -> Token[PostCommitChannel | None]:
    """Make ``channel`` the active channel for the current context.

    Args:
        channel: Channel that receives subsequent enqueues; ``None`` binds none.

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
    task, that is the channel of the stores
    :meth:`~loom.core.stores.uow.scope.InvocationScope.owners_of` names. Anywhere
    else a job dispatch (``store`` omitted) is queued on
    :func:`active_channel`, and a cache invalidation gets ``None``: its write
    is already durable, so it runs now. A job dispatch from a task whose
    execution ended gets ``None`` too: that execution's channel is drained,
    so the dispatch goes out detached, as at top level, acting as the same
    caller.

    Args:
        store: Store the action depends on; ``None`` for a job dispatch of
            the running execution.

    Raises:
        UndeclaredStoreWrite: For a job dispatch while a read-only execution runs.
    """
    if store is None:
        if execution_ended():
            return None
        refuse_write_under_read_only("a job dispatch")
    scope = current_scope()
    owners = None if scope is None else scope.owners_of(store)
    if scope is None or owners is None:
        return active_channel() if store is None else None
    channel = scope.store_channels.get(owners)
    if channel is None:
        channel = scope.store_channels[owners] = PostCommitChannel()
    return channel
