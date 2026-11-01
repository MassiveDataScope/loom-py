"""Ordered commits across the stores of one invocation (ADR 0008).

The coordinator turns the units of work of one invocation into a typed
outcome per store. It has two seams: :meth:`OrderedCommits.enlist` creates
and begins the participant of a store, and :meth:`OrderedCommits.finish`
ends every participant and returns the :class:`InvocationOutcome`.

``finish`` runs separable phases:

1. **Flush** every phased participant, in commit order. A failure rolls
   every participant back.
2. **Commit** the phased participants in commit order: first those that
   wrote, by first write, then the rest, by enlistment. The first failure
   stops the sequence and the participants not yet committed roll back.
3. **End** every participant in reverse enlistment order (LIFO): roll back
   what did not commit, then close. ``ContextVar`` tokens a unit of work
   published in ``begin`` are therefore reset in the reverse order they were
   set.

A single-phase participant (a third-party unit of work with only the
:class:`~loom.core.uow.abc.UnitOfWork` protocol) commits and closes in the
same ``__aexit__`` call, so it commits at its LIFO position in phase 3,
after every phased participant. Its writes are never recorded by loom's
repositories, so it would sit among "the rest" of the commit order anyway.

Nothing here is atomic across stores, and the coordinator raises no
invocation error: the caller reads :class:`InvocationOutcome` and decides.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable

from loom.core.logger import get_logger
from loom.core.persistence.stores import StoreKey
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory

_log = get_logger(__name__).bind(component="uow")


@dataclass(frozen=True, slots=True)
class PhysicalStore:
    """Where a participant's writes physically land, without credentials.

    Attributes:
        backend: Backend name, such as ``"sqlalchemy"`` or ``"mongo"``.
        target: Connection target without credentials (URL or cluster).
    """

    backend: str
    target: str


UNKNOWN_PHYSICAL = PhysicalStore(backend="unknown", target="")
"""Physical identity of a unit of work that does not declare one."""


@dataclass(frozen=True, slots=True)
class Committed:
    """The store's transaction committed."""

    key: StoreKey


@dataclass(frozen=True, slots=True)
class RolledBack:
    """The store's transaction did not commit; ``cause`` is why."""

    key: StoreKey
    cause: BaseException


@dataclass(frozen=True, slots=True)
class Uncertain:
    """The store may or may not have committed; ``cause`` is what interrupted it."""

    key: StoreKey
    cause: BaseException


@dataclass(frozen=True, slots=True)
class NonTransactional:
    """The store cannot roll back: whatever it ``wrote`` is already durable."""

    key: StoreKey
    wrote: bool


StoreOutcome = Committed | RolledBack | Uncertain | NonTransactional


@dataclass(frozen=True, slots=True)
class InvocationOutcome:
    """The outcome of every participant of one invocation.

    Attributes:
        stores: One outcome per participant, in commit order.
    """

    stores: tuple[StoreOutcome, ...]

    @property
    def succeeded(self) -> bool:
        """Whether every store committed or is non-transactional."""
        return all(isinstance(store, Committed | NonTransactional) for store in self.stores)


class CommitUncertain(Exception):
    """Raised by a unit of work's ``commit`` when the driver cannot tell whether it committed.

    Internal signal from a unit of work to the coordinator; it never reaches
    a transport.

    Args:
        cause: The driver error that left the commit's result unknown.
    """

    def __init__(self, cause: BaseException) -> None:
        super().__init__(f"commit result unknown: {cause!r}")
        self.cause = cause


@runtime_checkable
class PhasedUnitOfWork(UnitOfWork, Protocol):
    """A unit of work whose flush, commit, rollback and close are separate calls.

    ``begin``, ``commit`` and ``rollback`` leave the session open; ``close``
    releases it and resets any ``ContextVar`` token ``begin`` published.
    Each call shields its own driver I/O, as ``__aexit__`` does.
    """

    physical: PhysicalStore

    async def flush(self) -> None:
        """Send pending writes to the store without committing them."""
        ...

    async def close(self) -> None:
        """Release the session and reset the tokens ``begin`` published."""
        ...


class Participant(ABC):
    """One store's unit of work inside an invocation.

    Records the bookkeeping the coordinator orders commits by. Subclasses
    adapt a unit of work to the five phases.

    Args:
        key: Store the unit of work belongs to.
        uow: The store's unit of work, not yet begun.
        physical: Where its writes land.

    Attributes:
        key: Store the participant belongs to.
        physical: Where its writes land.
        transactional: Whether its unit of work can roll back.
        first_write: Invocation-wide order of its first write, or ``None``.
        wrote: Whether a write went through it.
    """

    phased: ClassVar[bool]

    def __init__(self, key: StoreKey, uow: UnitOfWork, physical: PhysicalStore) -> None:
        self.key = key
        self.physical = physical
        self.transactional = type(uow).transactional
        self.first_write: int | None = None
        self.wrote = False

    def record_write(self, order: int) -> None:
        """Note a write; only the first one sets :attr:`first_write`.

        Args:
            order: Invocation-wide sequence number of the write.
        """
        if self.first_write is None:
            self.first_write = order
        self.wrote = True

    @abstractmethod
    async def begin(self) -> None:
        """Open the unit of work."""

    @abstractmethod
    async def flush(self) -> None:
        """Send pending writes without committing them."""

    @abstractmethod
    async def commit(self) -> None:
        """Commit the unit of work."""

    @abstractmethod
    async def rollback(self, cause: BaseException) -> None:
        """Discard the unit of work's writes.

        Args:
            cause: Why the invocation does not commit this store.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release the unit of work."""


class _PhasedParticipant(Participant):
    """Drives a :class:`PhasedUnitOfWork` one phase per call."""

    phased: ClassVar[bool] = True

    def __init__(self, key: StoreKey, uow: PhasedUnitOfWork) -> None:
        super().__init__(key, uow, uow.physical)
        self._uow = uow

    async def begin(self) -> None:
        await self._uow.begin()

    async def flush(self) -> None:
        await self._uow.flush()

    async def commit(self) -> None:
        await self._uow.commit()

    async def rollback(self, cause: BaseException) -> None:
        await self._uow.rollback()

    async def close(self) -> None:
        await self._uow.close()


class _SinglePhaseParticipant(Participant):
    """Drives a protocol-only unit of work through its context manager.

    ``__aenter__`` begins it, ``__aexit__(None, ...)`` commits and closes it,
    and ``__aexit__(exc, ...)`` rolls back and closes it. It is exited once:
    after that, ``rollback`` and ``close`` do nothing.
    """

    phased: ClassVar[bool] = False

    def __init__(self, key: StoreKey, uow: UnitOfWork) -> None:
        super().__init__(key, uow, UNKNOWN_PHYSICAL)
        self._uow = uow
        self._exited = False

    async def begin(self) -> None:
        await self._uow.__aenter__()

    async def flush(self) -> None:
        """Nothing to flush: the protocol has no such phase."""

    async def commit(self) -> None:
        self._exited = True
        await self._uow.__aexit__(None, None, None)

    async def rollback(self, cause: BaseException) -> None:
        if self._exited:
            return
        self._exited = True
        await self._uow.__aexit__(type(cause), cause, cause.__traceback__)

    async def close(self) -> None:
        """Nothing left to close: ``__aexit__`` already did."""


def _participant(key: StoreKey, uow: UnitOfWork) -> Participant:
    if isinstance(uow, PhasedUnitOfWork):
        return _PhasedParticipant(key, uow)
    return _SinglePhaseParticipant(key, uow)


class ParticipantScope(Protocol):
    """What the coordinator needs from an invocation scope.

    Attributes:
        participants: Participants by store, in enlistment order.
        closed: Set once :meth:`Coordinator.finish` has ended the scope.
    """

    participants: dict[StoreKey, Participant]
    closed: bool


class Coordinator(Protocol):
    """Enlists the stores of an invocation and ends them together."""

    async def enlist(self, scope: ParticipantScope, key: StoreKey) -> Participant:
        """Create, begin and register the participant of ``key``."""
        ...

    async def finish(
        self, scope: ParticipantScope, error: BaseException | None
    ) -> InvocationOutcome:
        """End every participant of ``scope`` and report each store's outcome."""
        ...


class OrderedCommits:
    """Commits the stores of an invocation one after another (see the module docstring).

    Args:
        factories: Unit-of-work factory of every store that may be enlisted.

    Example::

        coordinator = OrderedCommits({DEFAULT_STORE: factory})
        participant = await coordinator.enlist(scope, DEFAULT_STORE)
        outcome = await coordinator.finish(scope, error=None)
    """

    def __init__(self, factories: Mapping[StoreKey, UnitOfWorkFactory]) -> None:
        self._factories = dict(factories)

    async def enlist(self, scope: ParticipantScope, key: StoreKey) -> Participant:
        """Create, begin and register the participant of ``key``.

        Enlisting a store that is already a participant returns that
        participant. A participant whose ``begin`` fails is not registered.

        Args:
            scope: Scope the participant joins.
            key: Store to enlist.

        Returns:
            The participant of ``key`` in ``scope``.

        Raises:
            KeyError: If no factory is registered for ``key``.
        """
        existing = scope.participants.get(key)
        if existing is not None:
            return existing
        participant = _participant(key, self._factories[key].create())
        await participant.begin()
        scope.participants[key] = participant
        return participant

    async def finish(
        self, scope: ParticipantScope, error: BaseException | None
    ) -> InvocationOutcome:
        """End every participant of ``scope`` and report each store's outcome.

        A cancellation or failure during flush or commit becomes the cause
        in the outcome, not an exception: the caller must re-raise it. A
        cancellation that arrives while participants roll back or close does
        not stop the others; it is raised once all of them have ended.

        Args:
            scope: Scope whose participants end.
            error: Exception the invocation failed with, or ``None``.

        Returns:
            One outcome per participant, in commit order.

        Raises:
            asyncio.CancelledError: If the task was cancelled while the
                participants rolled back or closed.
        """
        settlement = _Settlement(tuple(scope.participants.values()), error)
        try:
            outcome = await settlement.run()
        finally:
            scope.closed = True
        _log_outcome(outcome, scope.participants)
        settlement.raise_interrupt()
        return outcome


def _commit_order(participants: Iterable[Participant]) -> list[Participant]:
    return sorted(participants, key=lambda p: (p.first_write is None, p.first_write or 0))


def _succeeded(participant: Participant) -> StoreOutcome:
    if participant.transactional:
        return Committed(participant.key)
    return NonTransactional(participant.key, participant.wrote)


def _undone(participant: Participant, cause: BaseException) -> StoreOutcome:
    if participant.transactional:
        return RolledBack(participant.key, cause)
    return NonTransactional(participant.key, participant.wrote)


def _failed(participant: Participant, error: BaseException) -> RolledBack | Uncertain:
    """Classify a commit that raised ``error``."""
    if isinstance(error, CommitUncertain):
        return Uncertain(participant.key, error.cause)
    if isinstance(error, Exception) and participant.transactional:
        return RolledBack(participant.key, error)
    # A cancellation, or a failure of a store whose writes cannot be undone.
    return Uncertain(participant.key, error)


class _Settlement:
    """One run of :meth:`OrderedCommits.finish` over the enlisted participants."""

    def __init__(self, enlisted: tuple[Participant, ...], error: BaseException | None) -> None:
        self._enlisted = enlisted
        self._failure = error
        self._outcomes: dict[StoreKey, StoreOutcome] = {}
        self._interrupt: asyncio.CancelledError | None = None

    async def run(self) -> InvocationOutcome:
        phased = _commit_order(p for p in self._enlisted if p.phased)
        if self._failure is None:
            await self._flush(phased)
        if self._failure is None:
            await self._commit(phased)
        for participant in reversed(self._enlisted):
            await self._end(participant)
        single = [p for p in reversed(self._enlisted) if not p.phased]
        return InvocationOutcome(tuple(self._outcomes[p.key] for p in [*phased, *single]))

    def raise_interrupt(self) -> None:
        if self._interrupt is not None:
            raise self._interrupt

    async def _flush(self, phased: list[Participant]) -> None:
        for participant in phased:
            try:
                await participant.flush()
            except BaseException as exc:
                self._failure = exc
                return

    async def _commit(self, phased: list[Participant]) -> None:
        for participant in phased:
            if not await self._commit_one(participant):
                return

    async def _commit_one(self, participant: Participant) -> bool:
        try:
            await participant.commit()
        except BaseException as exc:
            failed = _failed(participant, exc)
            self._failure = failed.cause
            self._outcomes[participant.key] = failed
            return False
        self._outcomes[participant.key] = _succeeded(participant)
        return True

    async def _end(self, participant: Participant) -> None:
        if self._failure is None and participant.key not in self._outcomes:
            await self._commit_one(participant)
        failure = self._failure
        if failure is not None and not isinstance(
            self._outcomes.get(participant.key), Committed | NonTransactional
        ):
            self._outcomes.setdefault(participant.key, _undone(participant, failure))
            await self._quietly(
                lambda: participant.rollback(failure), "UoWRollbackFailed", participant
            )
        await self._quietly(participant.close, "UoWCloseFailed", participant)

    async def _quietly(
        self, step: Callable[[], Awaitable[None]], event: str, participant: Participant
    ) -> None:
        """Run a teardown step; log its failure and defer its cancellation.

        A cancellation also becomes the failure, so no single-phase
        participant still to end commits after it.
        """
        try:
            await step()
        except asyncio.CancelledError as exc:
            # A single-phase participant later in the sweep must not commit.
            self._failure = self._failure or exc
            self._interrupt = self._interrupt or exc
        except Exception:
            _log.exception(event, store=participant.key)


def _log_outcome(outcome: InvocationOutcome, participants: Mapping[StoreKey, Participant]) -> None:
    stores = [
        {
            "store": store.key,
            "outcome": type(store).__name__,
            "backend": participants[store.key].physical.backend,
        }
        for store in outcome.stores
    ]
    if outcome.succeeded:
        _log.debug("InvocationOutcome", stores=stores)
    else:
        _log.warning("InvocationOutcome", stores=stores)
