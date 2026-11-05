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
import inspect
import itertools
from collections.abc import AsyncIterator, Iterable, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from loom.core.errors.codes import ErrorCode
from loom.core.errors.errors import ForeignSessionError, SystemError
from loom.core.identity import ANONYMOUS, Identity, current_identity
from loom.core.persistence.stores import StoreKey
from loom.core.uow.coordinator import (
    Committed,
    Coordinator,
    InvocationOutcome,
    NonTransactional,
    Participant,
    RolledBack,
    StoreOutcome,
    Uncertain,
)

if TYPE_CHECKING:
    # Only typed here: ``loom.core.engine`` and ``loom.core.repository`` import this module.
    from loom.core.engine.post_commit import PostCommitChannel
    from loom.core.repository.mutation import MutationEvent


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


class UndeclaredStoreWrite(SystemError):
    """A write targeted a store the running execution did not declare.

    An execution writes to its primary store, the ``__store__`` of its main
    model, and to the stores of its ``Joined[...]`` repositories; it reads
    from any store. The write is refused before the driver is called.

    Args:
        message: Names the store written to and the stores declared.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.UNDECLARED_STORE_WRITE)


class NestedIdentityMismatch(SystemError):
    """A nested execution joined its parent's unit of work as another identity.

    The shared transaction filters rows as one identity, so it cannot serve
    the child's as well; when it has not begun yet, the child's identity would
    be the one it begins with, for the parent too. Build it with :meth:`joining`.

    Args:
        message: Names the nested use case.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.NESTED_IDENTITY_MISMATCH)

    @classmethod
    def joining(cls, use_case: str) -> NestedIdentityMismatch:
        """The error for ``use_case`` joining the open invocation as another identity."""
        return cls(
            f"{use_case} would join the open unit of work as another identity; "
            "invoke it with the same identity, or dispatch a job that runs as the other one"
        )


class NestedElevation(SystemError):
    """A nested execution was started after its parent elevated a scope.

    The elevation flag is set on the shared transaction, and an elevation is
    never inherited. Build it with :meth:`opening`.

    Args:
        message: Names the nested use case and the elevated scopes.
    """

    retryable = False
    """A programming error: a retry meets it again."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.NESTED_ELEVATION)

    @classmethod
    def opening(cls, use_case: str, scopes: frozenset[str]) -> NestedElevation:
        """The error for starting ``use_case`` inside an execution holding ``scopes`` elevated."""
        return cls(
            f"{use_case} cannot run inside an execution that elevated "
            f"{', '.join(sorted(scopes))}; elevate inside {use_case} instead, or dispatch a job"
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
        primary: The store the execution writes to; ``None`` for a read-only
            execution, which may write to none.
        joined: Further stores it may write to (``Joined[...]``).
        stores: The primary store, then the joined stores in name order.
    """

    primary: StoreKey | None
    joined: frozenset[StoreKey] = frozenset()
    stores: tuple[StoreKey, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        stores = (
            () if self.primary is None else (self.primary, *sorted(self.joined - {self.primary}))
        )
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
        identity: The ambient identity when the scope opened; an execution
            joining it must run as this identity.
        post_commit: The channel its owner bound, holding the actions no
            store holds; while set, the others wait in :attr:`store_channels`
            for :meth:`settle_post_commit`.
        store_channels: The post-commit actions, by the stores they depend on.
        read_only: Whether a read-only execution opened it: nothing run in it
            may write.
        mutations: What the repositories wrote in it, once ``@transactional``
            set it to collect them for its hooks.
    """

    owner_task: asyncio.Task[Any] | None
    coordinator: Coordinator
    closed: bool = False
    participants: dict[StoreKey, Participant] = field(default_factory=dict)
    frames: list[Frame] = field(default_factory=list)
    outcome: InvocationOutcome | None = None
    identity: Identity = ANONYMOUS
    post_commit: PostCommitChannel | None = None
    store_channels: dict[frozenset[StoreKey], PostCommitChannel] = field(default_factory=dict)
    read_only: bool = False
    mutations: list[MutationEvent] | None = None
    _writes: itertools.count[int] = field(default_factory=itertools.count, init=False, repr=False)

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

    def participant_for(
        self, key: StoreKey, owner: object, *, write: bool, record: bool = True
    ) -> Participant | None:
        """Return the participant of ``key`` a repository of ``owner`` joins, if any.

        The single checkpoint every access to a participant goes through.
        A read of a store whose participant holds no session (the store is
        not enlisted, or its unit of work is single-phase, such as Mongo with
        ``transactions: false``) returns ``None`` from any task, and the
        repository reads with a session per call (D3). Anything else checks
        ownership. A write must target a store of the innermost frame: a
        write to any other store is refused, because the execution did not
        declare it, and so is any write while no frame is pushed.

        Args:
            key: Store the repository belongs to.
            owner: The repository's session manager or client.
            write: Whether the repository is about to write.
            record: Whether to record the write now; a repository that
                records it once the driver answered passes ``False`` and
                calls :meth:`record_write` then.

        Returns:
            The participant of ``key``, or ``None`` when it is not enlisted
            or, for a read, holds no session.

        Raises:
            ScopeOwnershipError: When the caller may not touch the scope
                (:meth:`require_owner`).
            ForeignSessionError: When the participant is phased and its
                sessions come from another owner.
            UndeclaredStoreWrite: When a write targets a store outside the
                innermost frame, or no frame is pushed.
        """
        participant = self.participants.get(key)
        if not write and (participant is None or not participant.phased):
            return None
        self.require_owner()
        if participant is not None and not participant.accepts(owner):
            raise ForeignSessionError.between(
                ForeignSessionError.side(key, _described(owner)),
                ForeignSessionError.side(key, participant.physical.target),
            )
        if write:
            self._require_in_frame(key, owner)
            if participant is not None and record:
                self.record_write(participant)
        return participant

    def record_write(self, participant: Participant) -> None:
        """Record a write through ``participant`` for the commit order."""
        participant.record_write(next(self._writes))

    def _require_in_frame(self, key: StoreKey, owner: object) -> None:
        if self.frames and key in self.frames[-1].stores:
            return
        written = ForeignSessionError.side(key, _described(owner))
        if not self.frames:
            raise UndeclaredStoreWrite(
                f"a write to {written} is refused: no execution of this invocation is running"
            )
        stores = self.frames[-1].stores
        declared = (
            f"declares only store {', '.join(repr(store) for store in stores)}"
            if stores
            else "is read-only"
        )
        raise UndeclaredStoreWrite(
            f"a write to {written} is refused: the execution running now {declared}; "
            "inject its repository as Joined[...] to write to it"
        )

    def poison(self, keys: Iterable[StoreKey], cause: BaseException) -> None:
        """Forbid the invocation to commit, through the participants of ``keys`` that are enlisted.

        See :meth:`~loom.core.uow.coordinator.Participant.poison`.

        Args:
            keys: Stores whose transaction can no longer be trusted.
            cause: Why; the stores' ``RolledBack`` outcomes carry it.

        Raises:
            ScopeOwnershipError: When the caller may not touch the scope
                (:meth:`require_owner`).
        """
        self.require_owner()
        for key in keys:
            if key in self.participants:
                self.participants[key].poison(cause)

    @property
    def poisoned(self) -> BaseException | None:
        """Why the invocation must not succeed: the first poisoned participant's cause."""
        return next((p.poisoned for p in self.participants.values() if p.poisoned), None)

    def settle_post_commit(self, into: PostCommitChannel, error: BaseException | None) -> None:
        """Move into ``into`` the post-commit actions the stores' outcomes let run.

        Each group of actions runs by the stores it depends on (a cache
        invalidation on its repository's store, a job dispatch on every store
        of the execution that queued it): every lane if all of them
        committed, or wrote without a transaction, in an invocation that
        neither failed nor was poisoned; only the priority lane if one is
        ``Uncertain`` or non-transactional in a failed invocation, since a
        cache invalidation is safe whether or not the write landed; nothing
        if one rolled back, and when the body did not raise, its publish and
        plain actions are logged as never ran. Actions no store holds, on
        :attr:`post_commit`, run only if the whole invocation succeeded.
        What reaches :attr:`post_commit` after this call never runs.

        Args:
            into: The channel the owner drains next.
            error: The exception the invocation's body raised, or ``None``.
        """
        outcome, unowned = self.outcome, self.post_commit
        if outcome is None or unowned is None:
            return
        failed = error is not None or self.poisoned is not None
        if not failed and outcome.succeeded:
            into.adopt(unowned)
        lanes = {store.key: _lanes(store, failed=failed) for store in outcome.stores}
        for owners, channel in self.store_channels.items():
            allowed = min(lanes.get(key, _NO_LANE) for key in owners)
            if allowed != _NO_LANE:
                into.adopt(channel, priority_only=allowed == _PRIORITY_LANE)
            elif error is None:
                channel.discard_dispatches(log=True)

    def owners_of(self, store: StoreKey | None) -> frozenset[StoreKey] | None:
        """Return the stores an action that depends on ``store`` waits for, while this task settles.

        A cache invalidation names ``store`` and waits only while that
        store's participant holds the write in a transaction it can still
        roll back: a write anywhere else is already durable. A job dispatch
        names none and waits on every store of the running execution, its
        primary and ``Joined`` stores.

        Args:
            store: Store the action depends on; ``None`` for the running
                execution's stores.

        Returns:
            Those stores, or ``None`` when no owner settles this scope by
            store (:attr:`post_commit` unset), another task asks, or the
            action need not wait.
        """
        if self.post_commit is None or not self.owned_here():
            return None
        if store is not None:
            participant = self.participants.get(store)
            holds = participant is not None and participant.transactional and participant.phased
            return frozenset((store,)) if holds else None
        owners = frozenset(next((f.stores for f in reversed(self.frames) if f.primary), ()))
        return owners if owners and owners <= self.participants.keys() else None

    def channels(self) -> list[PostCommitChannel]:
        """Return the channels of :attr:`store_channels`, while this task settles."""
        settles = self.post_commit is not None and self.owned_here()
        return list(self.store_channels.values()) if settles else []

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


_NO_LANE, _PRIORITY_LANE, _EVERY_LANE = 0, 1, 2


def _lanes(store: StoreOutcome, *, failed: bool) -> int:
    """Which lanes of the actions depending on ``store`` may run, given its outcome."""
    if isinstance(store, RolledBack):
        return _NO_LANE
    if failed or isinstance(store, Uncertain):
        return _PRIORITY_LANE
    return _EVERY_LANE


def _described(owner: object) -> str:
    """Name ``owner`` for an error: its credential-free ``target``, else only its type.

    ``target`` counts only as a ``str`` property declared on the owner's
    class, looked up without calling ``__getattr__``: a pymongo client answers
    any attribute name with a database whose repr prints the client's options,
    passwords included.
    """
    if isinstance(inspect.getattr_static(type(owner), "target", None), property):
        target = getattr(owner, "target")  # noqa: B009 - the property is not typed on object
        if isinstance(target, str):
            return target
    return f"another {type(owner).__qualname__}"


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
        UndeclaredStoreWrite: See :meth:`InvocationScope.participant_for`.
    """
    scope = _scope.get()
    return None if scope is None else scope.participant_for(key, owner, write=write)


def current_handle(key: StoreKey, owner: object, *, write: bool) -> object | None:
    """Return the session the participant of ``key`` holds, through the checkpoint.

    What a backend's repositories join; each backend narrows it to its own
    session type once.

    Args:
        key: Store the repository belongs to.
        owner: The repository's session manager or client.
        write: Whether the repository is about to write.

    Returns:
        The session, or ``None`` when :func:`current_participant` returns
        none or the participant holds no session.

    Raises:
        ScopeOwnershipError: See :meth:`InvocationScope.participant_for`.
        ForeignSessionError: See :meth:`InvocationScope.participant_for`.
        UndeclaredStoreWrite: See :meth:`InvocationScope.participant_for`.
    """
    participant = current_participant(key, owner, write=write)
    return None if participant is None else participant.handle


def refuse_write_under_read_only(what: str) -> None:
    """Refuse ``what``, which writes, while a read-only execution runs in this context.

    Raises:
        UndeclaredStoreWrite: When the published scope was opened by a
            read-only execution, or one runs in it now, in any task.
    """
    scope = _scope.get()
    if scope is None:
        return
    if scope.read_only or (scope.frames and scope.frames[-1].primary is None):
        raise UndeclaredStoreWrite(f"{what} is refused: the enclosing execution is read-only")


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
async def invocation(
    coordinator: Coordinator, *, read_only: bool = False
) -> AsyncIterator[InvocationScope]:
    """Open an invocation scope owned by the current task, as its ambient identity, and publish it.

    On exit the coordinator ends the participants the caller has not ended,
    with the exception that left the block if any, and the scope is
    unpublished. Neither re-raises that exception here: it propagates.

    Args:
        coordinator: Enlists the stores of the invocation and ends them.
        read_only: Whether a read-only execution opens it.

    Yields:
        The open scope.

    Raises:
        asyncio.CancelledError: If ending the participants was interrupted.
    """
    scope = InvocationScope(
        owner_task=asyncio.current_task(),
        coordinator=coordinator,
        identity=current_identity(),
        read_only=read_only,
    )
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


async def _finish_if_open(scope: InvocationScope, error: BaseException | None) -> None:
    if not scope.participants:
        scope.closed = True
    elif not scope.closed:
        await scope.coordinator.finish(scope, error)


def invocation_error(
    outcome: InvocationOutcome, error: BaseException | None
) -> BaseException | None:
    """Return what an invocation that ended with ``outcome`` raises, if anything.

    The business ``error`` wins over the stores' own failures. The
    invocation raises :class:`StoresNotCommitted` instead, which no retry
    repeats, when a store is ``Uncertain`` (one store alone too), or when one
    committed or wrote without a transaction while another failed. An
    interruption (``asyncio.CancelledError`` and the like) is never
    translated.

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
    """Whether a store may have committed while the invocation failed."""
    failed = any(isinstance(store, RolledBack | Uncertain) for store in stores)
    durable = any(_is_durable(store) for store in stores)
    return any(isinstance(store, Uncertain) for store in stores) or (failed and durable)


def _is_durable(store: StoreOutcome) -> bool:
    return isinstance(store, Committed) or (isinstance(store, NonTransactional) and store.wrote)
