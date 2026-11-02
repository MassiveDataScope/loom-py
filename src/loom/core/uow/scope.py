"""The invocation scope: the stores one invocation enlisted, and who may touch them (ADR 0008).

One :class:`InvocationScope` lives as long as one top-level invocation (an
HTTP request, a job, a message). It is published in a single ``ContextVar``
and holds:

- the task that owns it, the only one allowed to touch its participants;
- its :class:`~loom.core.uow.coordinator.Coordinator`, which enlists stores
  and ends them together;
- the participants, one per store, in enlistment order;
- a stack of :class:`Frame`, one per execution, naming the stores that
  execution uses.

Every access to a participant goes through one checkpoint,
:meth:`InvocationScope.participant_for`. The scope never commits: the
:class:`~loom.core.uow.coordinator.Coordinator` does, and the caller turns its
:class:`~loom.core.uow.coordinator.InvocationOutcome` into the invocation's
error with :func:`invocation_error`.

Example::

    async with invocation(OrderedCommits({DEFAULT_STORE: factory})) as scope:
        async with scope.frame(Frame(DEFAULT_STORE)):
            ...  # run the use case
        outcome = await scope.coordinator.finish(scope, error=None)
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from loom.core.errors.codes import ErrorCode
from loom.core.errors.errors import ForeignSessionError, SystemError
from loom.core.persistence.stores import StoreKey
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import (
    Committed,
    Coordinator,
    InvocationOutcome,
    NonTransactional,
    OrderedCommits,
    Participant,
    RolledBack,
    StoreOutcome,
    Uncertain,
)


class ScopeOwnershipError(SystemError):
    """A participant was touched from outside the task that owns the invocation scope.

    Raised from another task (a ``gather`` child, or a task that outlives its
    use case), from a thread, or once the scope has closed.

    Args:
        message: Which of those happened.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.INVOCATION_SCOPE_FOREIGN_TASK)


class UnitOfWorkInsideScope(SystemError):
    """``async with uow:`` was entered inside an invocation scope.

    The unit of work would open a second transaction that commits on its
    own, outside the invocation. Build it with :meth:`entering`.

    Args:
        message: Names the unit of work.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.UOW_INSIDE_SCOPE)

    @classmethod
    def entering(cls, unit_of_work: object) -> UnitOfWorkInsideScope:
        """The error for entering ``unit_of_work`` inside a scope.

        Args:
            unit_of_work: The unit of work being entered.
        """
        return cls(
            f"`async with {type(unit_of_work).__qualname__}` inside an invocation scope would "
            "commit on its own; the executor already runs the use case in its store's "
            "unit of work"
        )


class StoresNotCommitted(SystemError):
    """The stores of one invocation did not all end the same way.

    Some store committed, or wrote without a transaction, while another did
    not commit, or a store's commit result is unknown. loom never claims to
    have undone what committed: the outcome names every store for
    reconciliation. ``__cause__`` is the business error or the first failure.
    Build it with :meth:`of`.

    Args:
        message: Names every store and how it ended.
        outcome: The outcome of every store, in commit order.

    Attributes:
        outcome: The outcome of every store; ``None`` once the error crossed
            a process boundary: the causes it holds may carry driver details
            and may not pickle, so only the message travels.
    """

    retryable = False
    """A store may have committed: a retry would apply its write twice."""

    def __init__(self, message: str, outcome: InvocationOutcome | None = None) -> None:
        self.outcome = outcome
        super().__init__(message, code=ErrorCode.STORES_NOT_COMMITTED)

    @classmethod
    def of(cls, outcome: InvocationOutcome) -> StoresNotCommitted:
        """The error for ``outcome``, whose message names every store and how it ended.

        Args:
            outcome: The outcome of every store, in commit order.
        """
        stores = ", ".join(f"{store.key}={type(store).__name__}" for store in outcome.stores)
        return cls(f"the stores of this invocation did not all commit: {stores}", outcome)

    def __reduce__(self) -> tuple[type[StoresNotCommitted], tuple[str]]:
        return type(self), (self.message,)


@dataclass(frozen=True, slots=True)
class Frame:
    """The stores one execution uses.

    Attributes:
        primary: The store the execution writes to.
        joined: Further stores it may write to (``Joined[...]``, T046).
        stores: The primary store, then the joined stores in name order.
    """

    primary: StoreKey
    joined: frozenset[StoreKey] = frozenset()
    stores: tuple[StoreKey, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        stores = (self.primary, *sorted(self.joined - {self.primary}))
        object.__setattr__(self, "stores", stores)


@dataclass(eq=False)
class InvocationScope:
    """The participants of one invocation and the task that owns them.

    Satisfies :class:`~loom.core.uow.coordinator.ParticipantScope`.

    Attributes:
        owner_task: The only task allowed to touch the participants.
        coordinator: Enlists stores and ends them together.
        closed: Set once the coordinator has ended every participant.
        participants: Participants by store, in enlistment order.
        frames: One frame per execution running in the scope, innermost last.
        outcome: Set by the coordinator when it ends the participants.
    """

    owner_task: asyncio.Task[Any] | None
    coordinator: Coordinator
    closed: bool = False
    participants: dict[StoreKey, Participant] = field(default_factory=dict)
    frames: list[Frame] = field(default_factory=list)
    outcome: InvocationOutcome | None = None
    _writes: itertools.count[int] = field(default_factory=itertools.count, init=False, repr=False)

    @property
    def committed(self) -> bool:
        """Whether the coordinator recorded an outcome in which every store succeeded."""
        return self.outcome is not None and self.outcome.succeeded

    def require_owner(self) -> None:
        """Check that the caller may touch this scope's participants.

        Raises:
            ScopeOwnershipError: From a thread with no running event loop, once
                the scope is closed, when it has no owner task, or from a task
                other than the owner.
        """
        problem = self._ownership_problem()
        if problem is not None:
            raise ScopeOwnershipError(problem)

    def owned_here(self) -> bool:
        """Whether the caller may touch this scope's participants (:meth:`require_owner` passes)."""
        return self._ownership_problem() is None

    def _ownership_problem(self) -> str | None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # ``current_task()`` itself raises without a running loop.
            return "the invocation scope was used from a thread without its event loop"
        if self.closed:
            return "the invocation scope is already closed"
        if self.owner_task is None or asyncio.current_task() is not self.owner_task:
            return "the invocation scope was used from a task other than the one that owns it"
        return None

    def participant_for(self, key: StoreKey, owner: object, *, write: bool) -> Participant | None:
        """Return the participant of ``key`` a repository of ``owner`` joins, if any.

        The single checkpoint every access to a participant goes through.
        Ownership is checked only when ``key`` is enlisted or a write is
        asked for; a read of a store that is not enlisted returns ``None``
        from any task, and the repository reads with a session per call. A
        write must target a store of the innermost frame, when one is pushed:
        a write anywhere else would commit on its own, outside the invocation.

        Args:
            key: Store the repository belongs to.
            owner: The repository's session manager or client.
            write: Whether the repository is about to write.

        Returns:
            The participant of ``key``, or ``None`` when it is not enlisted.

        Raises:
            ScopeOwnershipError: When the caller may not touch the scope
                (:meth:`require_owner`).
            ForeignSessionError: When the participant is phased and its
                sessions come from another owner, or a write targets a store
                outside the innermost frame (until ``Joined`` declares it).
        """
        participant = self.participants.get(key)
        if participant is None and not write:
            return None
        self.require_owner()
        if participant is not None and not participant.accepts(owner):
            raise ForeignSessionError.between(
                ForeignSessionError.side(key, repr(owner)),
                ForeignSessionError.side(key, participant.physical.target),
            )
        if write:
            self._require_in_frame(key, owner)
        if write and participant is not None:
            participant.record_write(next(self._writes))
        return participant

    def _require_in_frame(self, key: StoreKey, owner: object) -> None:
        if self.frames and key not in self.frames[-1].stores:
            raise ForeignSessionError.between(
                ForeignSessionError.side(key, repr(owner)),
                ForeignSessionError.side(
                    self.frames[-1].primary, "the store this execution writes to"
                ),
            )

    @asynccontextmanager
    async def frame(self, frame: Frame) -> AsyncIterator[None]:
        """Push ``frame`` for one execution and enlist the stores it names that are not yet.

        Args:
            frame: The stores the execution uses.

        Raises:
            ScopeOwnershipError: When the caller may not touch the scope
                (:meth:`require_owner`).
        """
        self.require_owner()
        self.frames.append(frame)
        try:
            for key in frame.stores:
                await self.coordinator.enlist(self, key)
            yield
        finally:
            self.frames.pop()


_scope: ContextVar[InvocationScope | None] = ContextVar("loom_invocation_scope", default=None)


def current_scope() -> InvocationScope | None:
    """Return the invocation scope published in this context, closed or not, or ``None``."""
    return _scope.get()


def current_participant(key: StoreKey, owner: object, *, write: bool) -> Participant | None:
    """Return the participant of ``key`` in the scope published here, through its checkpoint.

    Args:
        key: Store the repository belongs to.
        owner: The repository's session manager or client.
        write: Whether the repository is about to write.

    Returns:
        The participant, or ``None`` without a scope or when ``key`` is not
        enlisted: the repository then uses a session per call.

    Raises:
        ScopeOwnershipError: See :meth:`InvocationScope.participant_for`.
        ForeignSessionError: See :meth:`InvocationScope.participant_for`.
    """
    scope = _scope.get()
    return None if scope is None else scope.participant_for(key, owner, write=write)


@contextmanager
def unscoped() -> Iterator[None]:
    """Publish no scope for the block: what runs in it uses a session per call."""
    token = _scope.set(None)
    try:
        yield
    finally:
        _scope.reset(token)


def refuse_inside_open_scope(unit_of_work: object) -> None:
    """Refuse to enter ``unit_of_work`` inside an open invocation scope.

    A closed scope is not refused: a task that outlived its use case still
    sees it, and may open a transaction of its own.

    Args:
        unit_of_work: The unit of work being entered.

    Raises:
        UnitOfWorkInsideScope: When the scope published in this context is
            still open.
    """
    scope = _scope.get()
    if scope is not None and not scope.closed:
        raise UnitOfWorkInsideScope.entering(unit_of_work)


@asynccontextmanager
async def invocation(coordinator: Coordinator) -> AsyncIterator[InvocationScope]:
    """Open an invocation scope owned by the current task and publish it.

    On exit the coordinator ends the participants the caller has not ended,
    with the exception that left the block if any, and the scope is
    unpublished. Neither re-raises that exception here: it propagates.

    Args:
        coordinator: Enlists the stores of the invocation and ends them.

    Yields:
        The open scope.

    Raises:
        asyncio.CancelledError: If ending the participants was interrupted.
    """
    scope = InvocationScope(owner_task=asyncio.current_task(), coordinator=coordinator)
    token = _scope.set(scope)
    try:
        try:
            yield scope
        except BaseException as exc:
            await _finish_if_open(scope, exc)
            raise
        await _finish_if_open(scope, None)
    finally:
        _scope.reset(token)


@asynccontextmanager
async def single_store(key: StoreKey, unit_of_work: UnitOfWork) -> AsyncIterator[InvocationScope]:
    """Run ``unit_of_work`` as the only participant of an invocation of its own.

    What ``async with uow:`` and ``@transactional`` do outside a scope: the
    repositories of ``key`` join the unit of work through the scope, and on
    exit the coordinator commits it, or rolls it back if the block raised.

    Args:
        key: Store the unit of work belongs to.
        unit_of_work: Not yet begun; it is begun on entry.

    Yields:
        The open scope.

    Raises:
        Exception: What the commit failed with, when the block did not raise.
    """
    coordinator = OrderedCommits({key: _Given(unit_of_work)})
    async with invocation(coordinator) as scope:
        async with scope.frame(Frame(key)):
            yield scope
        outcome = await coordinator.finish(scope, None)
    failure = invocation_error(outcome, None)
    if failure is not None:
        raise failure


@dataclass(frozen=True, slots=True)
class _Given:
    """A factory that hands out one unit of work already built."""

    unit_of_work: UnitOfWork

    def create(self) -> UnitOfWork:
        return self.unit_of_work


async def _finish_if_open(scope: InvocationScope, error: BaseException | None) -> None:
    if not scope.closed:
        await scope.coordinator.finish(scope, error)


def invocation_error(
    outcome: InvocationOutcome, error: BaseException | None
) -> BaseException | None:
    """Return what an invocation that ended with ``outcome`` raises, if anything.

    The business ``error`` wins over the stores' own failures. With two or
    more stores the invocation raises :class:`StoresNotCommitted` instead
    when the stores did not end the same way: one is ``Uncertain``, or one
    committed or wrote without a transaction while another failed. An
    interruption (``asyncio.CancelledError`` and the like) is never
    translated. With one store the original exception is always returned.

    Args:
        outcome: The outcome of every store, in commit order.
        error: The exception the invocation's body raised, or ``None``.

    Returns:
        The exception to raise, or ``None`` when the invocation succeeded.
    """
    failure = error if error is not None else _first_failure(outcome.stores)
    if not isinstance(failure, Exception) or not _split(outcome.stores):
        return failure
    stores_error = StoresNotCommitted.of(outcome)
    stores_error.__cause__ = failure
    return stores_error


def _first_failure(stores: tuple[StoreOutcome, ...]) -> BaseException | None:
    return next(
        (store.cause for store in stores if isinstance(store, RolledBack | Uncertain)), None
    )


def _split(stores: tuple[StoreOutcome, ...]) -> bool:
    """Whether two or more stores ended in states the caller cannot treat as one."""
    if len(stores) < 2:
        return False
    failed = any(isinstance(store, RolledBack | Uncertain) for store in stores)
    durable = any(_is_durable(store) for store in stores)
    return any(isinstance(store, Uncertain) for store in stores) or (failed and durable)


def _is_durable(store: StoreOutcome) -> bool:
    return isinstance(store, Committed) or (isinstance(store, NonTransactional) and store.wrote)
