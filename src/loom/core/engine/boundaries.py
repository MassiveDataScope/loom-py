"""The unit of work one execution runs in, and the post-commit actions it settles.

An execution runs in exactly one of four boundaries, chosen by
:meth:`Boundaries.of` from the invocation scope it finds and whether it
writes; each runs the execution itself:

- :class:`OpensScope`: a writer at top level with a coordinator opens an
  invocation scope, commits it on success and rolls it back on any
  exception, cancellation included.
- :class:`JoinsScope`: an execution inside this task's scope pushes its
  frame on it, a read-only frame for one writing to no store, and never
  commits.
- :class:`WithoutUnitOfWork`: a writer at top level with no coordinator runs
  with no unit of work.
- :class:`ReadsOnItsOwn`: an execution writing to no store, outside a scope
  this task owns, opens a read-only invocation of its own.

Post-commit actions (job dispatches, cache invalidations) are held by the
store that owns them and, once the scope has closed, run on the
:class:`~loom.core.engine.post_commit.PostCommitChannel` of the execution
that owns the scope as that store's outcome allows (or on the outermost
execution's channel when there is no scope).

A task that outlived the execution it was started in finds that
execution's scope and channel still set; :func:`outside_departed_execution` hides both from
the execution it starts.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from typing import Any, ClassVar, TypeAlias

from loom.core.authz.elevation import ElevationSink, elevated_scopes, elevation_scope
from loom.core.engine.observation import StartedExecution
from loom.core.engine.plan import ExecutionPlan
from loom.core.engine.post_commit import (
    PostCommitChannel,
    active_channel,
    bind_channel,
    drain_after_failure,
    reset_channel,
)
from loom.core.errors import NestedElevation
from loom.core.stores.uow.coordinator import Coordinator, OrderedCommits
from loom.core.stores.uow.scope import (
    Frame,
    InvocationScope,
    invocation,
    invocation_error,
    unscoped,
)

_READ_ONLY = Frame(None)

Work: TypeAlias = Callable[[], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class OpensScope:
    """A writer at top level: opens an invocation scope over ``coordinator`` and commits it."""

    coordinator: Coordinator
    owns_transaction: ClassVar[bool] = True
    read_only: ClassVar[bool] = False

    async def run(self, execution: _Run) -> Any:
        """Open an invocation scope, run the work in it, end it, raise what it ended with.

        The scope is unpublished before this returns.

        Raises:
            JoinedAutocommitStore: When a joined store cannot roll back.
        """
        plan = execution.plan
        result: Any = None
        error: BaseException | None = None
        async with invocation(self.coordinator) as scope:
            scope.post_commit = active_channel()
            try:
                async with scope.frame(plan.frame):
                    if plan.joined:
                        plan.refuse_autocommit_joined(scope.participants)
                    result = await execution.work()
            except BaseException as exc:
                error = exc
            try:
                outcome = await execution.started.committing(scope, error)
            finally:
                execution.settled = PostCommitChannel()
                scope.settle_post_commit(execution.settled, error)
        failure = invocation_error(outcome, error or scope.poisoned)
        if failure is not None:
            raise failure
        return result


@dataclass(frozen=True, slots=True)
class JoinsScope:
    """An execution that pushes *frame* on ``scope``, this task's, and never commits.

    ``frame`` is the plan's for a writer, the read-only frame for an
    execution writing to no store.
    """

    scope: InvocationScope
    frame: Frame
    read_only: bool
    owns_transaction: ClassVar[bool] = False

    async def run(self, execution: _Run) -> Any:
        """Run the work under the frame, as the identity that opened the scope.

        Raises:
            NestedIdentityMismatch: When the execution acts as another identity.
            JoinedAutocommitStore: When a joined store cannot roll back.
        """
        scope = self.scope
        scope.refuse_other_identity(execution.started.use_case_name)
        async with scope.frame(self.frame):
            if self.frame.joined:
                execution.plan.refuse_autocommit_joined(scope.participants)
            return await execution.work()


@dataclass(frozen=True, slots=True)
class WithoutUnitOfWork:
    """A writer at top level with no coordinator: no unit of work."""

    owns_transaction: ClassVar[bool] = False
    read_only: ClassVar[bool] = False

    async def run(self, execution: _Run) -> Any:
        """Run the work as is."""
        return await execution.work()


@dataclass(frozen=True, slots=True)
class ReadsOnItsOwn:
    """An execution writing to no store, outside a scope this task owns.

    It opens a read-only invocation over ``coordinator``, enlisting no store,
    hiding the scope of another task when ``hides_scope``.
    """

    coordinator: Coordinator
    hides_scope: bool
    owns_transaction: ClassVar[bool] = False
    read_only: ClassVar[bool] = True

    async def run(self, execution: _Run) -> Any:
        """Run the work under a read-only frame of an invocation of its own."""
        with unscoped() if self.hides_scope else nullcontext():
            async with (
                invocation(self.coordinator, read_only=True) as own,
                own.frame(_READ_ONLY),
            ):
                return await execution.work()


Boundary: TypeAlias = OpensScope | JoinsScope | WithoutUnitOfWork | ReadsOnItsOwn


class Boundaries:
    """Runs each execution of one executor in its unit of work and settles what it queued.

    Args:
        coordinator: The application's stores, which a top-level writer
            commits; ``None`` runs writers with no unit of work.
        elevation_sink: Receives the elevations an execution owning its
            transaction grants.
    """

    __slots__ = ("_hiding_reads", "_reads", "_sink", "_top_level")

    def __init__(self, coordinator: Coordinator | None, elevation_sink: ElevationSink | None):
        self._top_level: OpensScope | WithoutUnitOfWork = (
            WithoutUnitOfWork() if coordinator is None else OpensScope(coordinator)
        )
        reads_over = OrderedCommits({}) if coordinator is None else coordinator
        self._reads = ReadsOnItsOwn(reads_over, hides_scope=False)
        self._hiding_reads = ReadsOnItsOwn(reads_over, hides_scope=True)
        self._sink = elevation_sink

    def boundary_for(
        self, scope: InvocationScope | None, plan: ExecutionPlan, read_only: bool
    ) -> Boundary:
        """The boundary of a run of *plan* finding *scope* published, writing unless *read_only*."""
        if scope is None:
            return self._reads if read_only else self._top_level
        if not read_only:
            return JoinsScope(scope, plan.frame, False)
        if scope.is_owned_here():
            return JoinsScope(scope, _READ_ONLY, True)
        return self._hiding_reads

    async def run(
        self, boundary: Boundary, plan: ExecutionPlan, started: StartedExecution, work: Work
    ) -> Any:
        """Run *work* in *boundary*, report its one outcome on *started*, then drain.

        A failure drains what may be durable before it is reported; a
        success is reported before its actions run, and they run even when
        reporting it raises.

        Raises:
            NestedElevation: Inside an execution that elevated a scope.
        """
        execution = _Run(
            plan, started, work, opens=boundary.owns_transaction or active_channel() is None
        )
        try:
            elevated = elevated_scopes()
            if elevated:
                raise NestedElevation.opening(started.use_case_name, elevated)
            async with elevation_scope(
                owns_transaction=boundary.owns_transaction,
                sink=self._sink,
                read_only=boundary.read_only,
            ):
                result = await boundary.run(execution)
        except BaseException as exc:
            await execution.after_failure()
            started.failed(exc)
            raise
        finally:
            execution.unbind()
        try:
            started.succeeded()
        finally:
            await execution.after_success(committed=boundary.owns_transaction)
        return result


@contextmanager
def outside_departed_execution() -> Iterator[None]:
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


class _Run:
    """One execution of *plan* running *work* in its boundary, observed by *started*.

    It holds the post-commit channel the execution opened, when *opens*, and
    the one its outcome settled. ``settled`` is set once the coordinator has
    ended the invocation scope this execution owns: the actions its stores'
    outcomes let run, drained even on failure. Without it the opened channel
    is discarded.
    """

    __slots__ = ("_token", "opened", "plan", "settled", "started", "work")

    def __init__(
        self, plan: ExecutionPlan, started: StartedExecution, work: Work, *, opens: bool
    ) -> None:
        self.plan = plan
        self.started = started
        self.work = work
        self.opened = PostCommitChannel() if opens else None
        self._token = None if self.opened is None else bind_channel(self.opened)
        self.settled: PostCommitChannel | None = None

    def unbind(self) -> None:
        if self._token is not None:
            reset_channel(self._token)
            self._token = None

    async def after_failure(self) -> None:
        self.unbind()
        if self.settled is not None:
            await drain_after_failure(self.settled)
        elif self.opened is not None:
            self.opened.discard()

    async def after_success(self, *, committed: bool) -> None:
        settled = self.settled if self.settled is not None else self.opened
        if settled is not None:
            await settled.drain(committed=committed)
