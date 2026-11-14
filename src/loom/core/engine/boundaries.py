"""The unit of work one execution runs in, and the post-commit actions it settles.

An execution runs in exactly one of four boundaries, chosen by
:meth:`Boundaries.of` from the invocation scope it finds and whether it
writes:

- :class:`Owned`: a writer at top level with a coordinator opens an
  invocation scope, commits it on success and rolls it back on any
  exception, cancellation included.
- :class:`Joined`: a writer inside this task's scope pushes its frame on it
  and never commits.
- :class:`Unmanaged`: a writer at top level with no coordinator runs with no
  unit of work.
- :class:`ReadOnly`: an execution writing to no store pushes a read-only
  frame on the scope of the task that owns it, else opens a read-only
  invocation of its own.

Post-commit actions (job dispatches, cache invalidations) are held by the
store that owns them and, once the scope has closed, run on the
:class:`~loom.core.engine.post_commit.PostCommitChannel` of the execution
that owns the scope as that store's outcome allows (or on the outermost
execution's channel when there is no scope).

A task that outlived the execution it was started in finds that
execution's scope and channel still set; :func:`detached` hides both from
the execution it starts.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, ClassVar, TypeAlias

from loom.core.authz.elevation import ElevationSink, elevated_scopes, elevation_scope
from loom.core.engine.observation import COMMIT, Attempt
from loom.core.engine.plan import ExecutionPlan
from loom.core.engine.post_commit import (
    PostCommitChannel,
    active_channel,
    bind_channel,
    drain_after_failure,
    reset_channel,
)
from loom.core.errors import NestedElevation, NestedIdentityMismatch
from loom.core.identity import current_identity
from loom.core.uow.coordinator import Coordinator, InvocationOutcome, OrderedCommits
from loom.core.uow.scope import (
    Frame,
    InvocationScope,
    current_scope,
    invocation,
    invocation_error,
    unscoped,
)
from loom.core.use_case.errors import JoinedAutocommitStore, UseCaseErrorCode

_READ_ONLY = Frame(None)

Work: TypeAlias = Callable[[], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class Owned:
    """A writer at top level: opens an invocation scope over ``coordinator`` and commits it."""

    coordinator: Coordinator
    owns_transaction: ClassVar[bool] = True
    read_only: ClassVar[bool] = False


@dataclass(frozen=True, slots=True)
class Joined:
    """A writer that pushes its frame on ``scope``, this task's, and never commits."""

    scope: InvocationScope
    owns_transaction: ClassVar[bool] = False
    read_only: ClassVar[bool] = False


@dataclass(frozen=True, slots=True)
class Unmanaged:
    """A writer at top level with no coordinator: no unit of work."""

    owns_transaction: ClassVar[bool] = False
    read_only: ClassVar[bool] = False


@dataclass(frozen=True, slots=True)
class ReadOnly:
    """An execution writing to no store, under ``scope`` if this task owns it, else on its own."""

    scope: InvocationScope | None
    owns_transaction: ClassVar[bool] = False
    read_only: ClassVar[bool] = True


_UNMANAGED = Unmanaged()

Boundary: TypeAlias = Owned | Joined | Unmanaged | ReadOnly


class Boundaries:
    """Runs each execution of one executor in its unit of work and settles what it queued.

    Args:
        coordinator: The application's stores, which a top-level writer
            commits; ``None`` runs writers with no unit of work.
        elevation_sink: Receives the elevations an execution owning its
            transaction grants.
    """

    __slots__ = ("_coordinator", "_sink", "_top_level")

    def __init__(self, coordinator: Coordinator | None, elevation_sink: ElevationSink | None):
        self._coordinator = coordinator
        # Preallocated: the common top-level writer allocates no boundary.
        self._top_level: Owned | Unmanaged = (
            _UNMANAGED if coordinator is None else Owned(coordinator)
        )
        self._sink = elevation_sink

    def of(self, scope: InvocationScope | None, read_only: bool) -> Boundary:
        """The boundary of an execution finding *scope* published, writing unless *read_only*."""
        if read_only:
            return ReadOnly(scope)
        if scope is None:
            return self._top_level
        return Joined(scope)

    async def run(
        self, boundary: Boundary, plan: ExecutionPlan, attempt: Attempt, work: Work
    ) -> Any:
        """Run *work* in *boundary*, report its one outcome on *attempt*, then drain.

        A failure drains what may be durable before it is reported; a
        success is reported before its actions run, in a ``finally`` so a
        failing metrics adapter never skips the actions of a committed
        invocation.
        """
        channel = _Channel(boundary.owns_transaction or active_channel() is None)
        try:
            _refuse_nesting(attempt.use_case_name)
            # The elevation frame wraps the coordinator's flush and commit too:
            # the session-settings provider reads ``elevated_scopes()`` when a
            # transaction begins, which may be at that flush.
            async with elevation_scope(
                owns_transaction=boundary.owns_transaction,
                sink=self._sink,
                read_only=boundary.read_only,
            ):
                result = await self._enter(boundary, plan, attempt, channel, work)
        except BaseException as exc:
            # ``BaseException``: a cancellation is a terminal outcome too,
            # accounted for and re-raised.
            await channel.after_failure()
            attempt.failed(exc)
            raise
        finally:
            channel.unbind()
        try:
            attempt.succeeded()
        finally:
            await channel.after_success(committed=boundary.owns_transaction)
        return result

    async def _enter(
        self,
        boundary: Boundary,
        plan: ExecutionPlan,
        attempt: Attempt,
        channel: _Channel,
        work: Work,
    ) -> Any:
        match boundary:
            case Owned(coordinator):
                return await _commit(coordinator, plan, attempt, channel, work)
            case Joined(scope):
                return await _in_frame(scope, plan, work)
            case Unmanaged():
                return await work()
            case ReadOnly(scope):
                async with self._read_only_frame(scope):
                    return await work()

    @asynccontextmanager
    async def _read_only_frame(self, scope: InvocationScope | None) -> AsyncIterator[None]:
        """Push a frame writing to no store: on ``scope`` in the task that owns it.

        Anywhere else it opens a read-only invocation of its own, enlisting no
        store, unpublished before the execution's post-commit actions run.
        """
        if scope is not None and scope.owned_here():
            async with scope.frame(_READ_ONLY):
                yield
            return
        coordinator = self._coordinator or OrderedCommits({})
        with nullcontext() if scope is None else unscoped():
            async with invocation(coordinator, read_only=True) as own, own.frame(_READ_ONLY):
                yield


@contextmanager
def detached() -> Iterator[None]:
    """Hide the invocation scope and post-commit channel a departed execution left in this task.

    For the one execution a task that outlived its execution starts: it
    joins no scope and drains on no channel of the departed one.
    """
    channel_token = bind_channel(None)
    try:
        with unscoped():
            yield
    finally:
        reset_channel(channel_token)


class _Channel:
    """The post-commit channel one execution opened, if any, and the one its outcome settled.

    ``settled`` is set once the coordinator has ended the invocation scope
    this execution owns: the actions its stores' outcomes let run, drained
    even on failure — a write already committed must never lose its queued
    invalidations to another store's failure or an unrelated teardown one.
    Without it, nothing durable happened and the opened channel is discarded.
    """

    __slots__ = ("_token", "opened", "settled")

    def __init__(self, opens: bool) -> None:
        self.opened = PostCommitChannel() if opens else None
        self._token = None if self.opened is None else bind_channel(self.opened)
        self.settled: PostCommitChannel | None = None

    def unbind(self) -> None:
        if self._token is not None:
            reset_channel(self._token)
            self._token = None

    async def after_failure(self) -> None:
        # Unbound first: a drain must never run with its own channel still
        # bound (post_commit module docstring) — a nested execution started
        # from an action would otherwise see this channel as
        # ``active_channel()``, get no channel of its own, and enqueue onto one
        # already mid-drain, silently lost.
        self.unbind()
        if self.settled is not None:
            await drain_after_failure(self.settled)
        elif self.opened is not None:
            self.opened.discard()

    async def after_success(self, *, committed: bool) -> None:
        settled = self.settled if self.settled is not None else self.opened
        if settled is not None:
            await settled.drain(committed=committed)


async def _commit(
    coordinator: Coordinator,
    plan: ExecutionPlan,
    attempt: Attempt,
    channel: _Channel,
    work: Work,
) -> Any:
    """Open an invocation scope, run *work* in it, end it, raise what it ended with.

    The scope is unpublished before this returns, so the drain that
    follows (an inline job dispatch among its actions) opens its own.
    """
    result: Any = None
    error: BaseException | None = None
    async with invocation(coordinator) as scope:
        scope.post_commit = active_channel()
        try:
            result = await _in_frame(scope, plan, work)
        except BaseException as exc:  # NOSONAR S5754 - raised below, once the scope ended
            error = exc
        try:
            outcome = await _finish(scope, error, attempt)
        finally:
            # ``finish`` records the outcome before it raises an
            # interruption caught while ending a store (a cancelled close).
            channel.settled = PostCommitChannel()
            scope.settle_post_commit(channel.settled, error)
    failure = invocation_error(outcome, error or scope.poisoned)
    if failure is not None:
        raise failure
    return result


async def _in_frame(scope: InvocationScope, plan: ExecutionPlan, work: Work) -> Any:
    """Push *plan*'s frame on *scope* and run *work* under it.

    Raises:
        JoinedAutocommitStore: When a ``Joined`` store cannot roll back.
    """
    async with scope.frame(Frame(plan.primary, plan.joined)):
        _refuse_autocommit_joined(scope, plan)
        return await work()


async def _finish(
    scope: InvocationScope, error: BaseException | None, attempt: Attempt
) -> InvocationOutcome:
    """End every participant of ``scope``; from the flush on, the phase is the commit."""
    if error is not None:
        return await scope.coordinator.finish(scope, error)
    attempt.phase = COMMIT
    started = time.perf_counter()
    try:
        return await scope.coordinator.finish(scope, None)
    finally:
        attempt.commit_ms = (time.perf_counter() - started) * 1000


def _refuse_autocommit_joined(scope: InvocationScope, plan: ExecutionPlan) -> None:
    """Refuse a ``Joined`` store of ``plan`` whose enlisted unit of work cannot roll back.

    What composition refuses at startup for the components it discovers,
    for any plan, compiled then or on its first execution.
    """
    for key in sorted(plan.joined):
        if not scope.participants[key].transactional:
            issue = UseCaseErrorCode.JOINED_AUTOCOMMIT_STORE.issue(
                component=plan.use_case_type, store=key
            )
            raise JoinedAutocommitStore(issue)


def _refuse_nesting(use_case: str) -> None:
    """Refuse ``use_case`` inside an elevated execution, or joining this task's scope as another.

    Its identity is compared with the scope's identity, never the ambient one a parent set.
    """
    scopes = elevated_scopes()
    if scopes:
        raise NestedElevation.opening(use_case, scopes)
    scope = current_scope()
    if (
        scope is not None
        and not scope.read_only
        and scope.owned_here()
        and current_identity() != scope.identity
    ):
        raise NestedIdentityMismatch.joining(use_case)
