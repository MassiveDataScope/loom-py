"""Ordered commits across the stores of one invocation (ADR 0008).

The coordinator turns the units of work of one invocation into a typed
outcome per store. It has two seams: :meth:`OrderedCommits.enlist` creates
and begins the participant of a store, and :meth:`OrderedCommits.finish`
ends every participant and records the :class:`InvocationOutcome`.

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

A single-phase participant (any unit of work that does not declare the
``phased`` marker of :class:`PhasedUnitOfWork`) commits and closes in the
same ``__aexit__`` call, so it commits at its LIFO position in phase 3,
after every phased participant. Its writes are never recorded by loom's
repositories, so it would sit among "the rest" of the commit order anyway.

Interruptions (``asyncio.CancelledError``, ``KeyboardInterrupt``,
``SystemExit``) never cut the sequence short: the first one becomes the
failure, every participant still ends, and ``finish`` raises it once the
outcome is recorded on the scope and logged.

Nothing here is atomic across stores, and the coordinator raises no
invocation error: the caller reads :class:`InvocationOutcome` and decides.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import ClassVar, Protocol, TypeGuard

from loom.core._async import run_to_completion
from loom.core.errors.codes import ErrorCode
from loom.core.errors.errors import SystemError
from loom.core.logger import get_logger
from loom.core.persistence.stores import StoreKey
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory

_log = get_logger(__name__).bind(component="uow")

_ROLLBACK_TIMEOUT_S = 30.0
"""How long a phased rollback may take before it counts as failed.

A rollback runs to completion even when the owning task is cancelled, so a
rollback stuck on a dead socket would leave that task uncancellable. Thirty
seconds outlasts any rollback a live server answers, yet stays well under the
minutes a TCP keepalive or driver socket timeout may take to give up. A
rollback that times out is logged as ``UoWRollbackFailed``, like any other
failed rollback, and ``close`` runs next; the server discards the
transaction once the connection is gone.
"""


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
        return all(_is_success(store) for store in self.stores)


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


class UncomposedStore(SystemError):
    """A use case enlisted a store that has no unit of work in this application.

    The store is not declared, or its backend persists nothing.

    Args:
        key: The store enlisted.
        composed: The stores that have a unit of work.
    """

    retryable = False
    """A configuration error: a retry meets it again."""

    def __init__(self, key: StoreKey, composed: Iterable[StoreKey]) -> None:
        super().__init__(
            f"store {key!r} has no unit of work in this application; "
            f"the stores with one are {sorted(composed)}",
            code=ErrorCode.UNCOMPOSED_STORE,
        )


class PhasedUnitOfWork(UnitOfWork, Protocol):
    """A unit of work whose flush, commit, rollback and close are separate calls.

    A unit of work opts in explicitly with ``phased: ClassVar[bool] = True``
    and then implements every member below. Without the marker it is driven
    through its context manager in a single phase, whatever methods it
    happens to define, so a third-party ``flush`` or ``close`` changes nothing.

    - ``begin``, ``commit`` and ``rollback`` leave the session open.
    - ``rollback`` runs in a task of its own that is awaited to completion,
      however often the owning task is cancelled, but for at most
      :data:`_ROLLBACK_TIMEOUT_S`; it must not touch a ``ContextVar``.
    - ``close`` releases the session, shields its own driver I/O and resets
      every ``ContextVar`` token ``begin`` published in a ``finally``, so a
      cancellation never leaves one set. It is also called when ``begin``
      fails.
    """

    phased: ClassVar[bool]
    """``True``: the coordinator drives this unit of work phase by phase."""

    @property
    def owner(self) -> object:
        """The session manager or client every session of this unit of work comes from."""
        ...

    @property
    def physical(self) -> PhysicalStore:
        """Where this unit of work's writes land."""
        ...

    @property
    def session(self) -> object | None:
        """The session its store's repositories join; ``None`` outside ``begin``..``close``."""
        ...

    async def flush(self) -> None:
        """Send pending writes to the store without committing them."""
        ...

    async def close(self) -> None:
        """Release the session and reset the tokens ``begin`` published."""
        ...


def _is_phased(uow: UnitOfWork) -> TypeGuard[PhasedUnitOfWork]:
    """Whether ``uow`` opted into phased driving with the ``phased`` marker."""
    return getattr(type(uow), "phased", False) is True


class _Driver(Protocol):
    """The five phases, as one kind of unit of work runs them, and what it declares."""

    phased: ClassVar[bool]

    @property
    def physical(self) -> PhysicalStore: ...
    @property
    def handle(self) -> object | None: ...
    def accepts(self, owner: object) -> bool: ...
    async def begin(self) -> None: ...
    async def flush(self) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self, cause: BaseException) -> None: ...
    async def close(self) -> None: ...


class _PhasedDriver:
    """Drives a :class:`PhasedUnitOfWork` one phase per call."""

    phased: ClassVar[bool] = True

    def __init__(self, key: StoreKey, uow: PhasedUnitOfWork) -> None:
        self._key = key
        self._uow = uow

    @property
    def physical(self) -> PhysicalStore:
        return self._uow.physical

    @property
    def handle(self) -> object | None:
        return self._uow.session

    def accepts(self, owner: object) -> bool:
        """Only a repository whose sessions come from the unit of work's own owner joins it."""
        return owner is self._uow.owner

    async def begin(self) -> None:
        try:
            await self._uow.begin()
        except BaseException:
            # ``begin`` may have published tokens before it failed.
            await self._close_logging_failure()
            raise

    async def _close_logging_failure(self) -> None:
        try:
            await self._uow.close()
        except Exception:
            _log.exception("UoWCloseFailed", store=self._key)

    async def flush(self) -> None:
        await self._uow.flush()

    async def commit(self) -> None:
        await self._uow.commit()

    async def rollback(self, cause: BaseException) -> None:
        """Roll back to completion or to :data:`_ROLLBACK_TIMEOUT_S`, even if cancelled meanwhile.

        ``close`` never runs while the rollback is in flight: a cancellation
        waits for it, and the timeout bounds that wait, so a rollback hung on
        a dead socket cannot leave the owning task uncancellable. ``cause`` is
        unused: a phased ``rollback`` takes no exception, unlike the
        ``__aexit__`` a single-phase unit of work rolls back with.

        Raises:
            TimeoutError: If the rollback outlasted :data:`_ROLLBACK_TIMEOUT_S`.
            asyncio.CancelledError: If the task was cancelled meanwhile, once
                the rollback has ended.
        """
        del cause
        await run_to_completion(self._uow.rollback(), timeout=_ROLLBACK_TIMEOUT_S)

    async def close(self) -> None:
        await self._uow.close()


class _SinglePhaseDriver:
    """Drives a protocol-only unit of work through its context manager.

    ``__aenter__`` begins it, ``__aexit__(None, ...)`` commits and closes it,
    and ``__aexit__(exc, ...)`` rolls back and closes it. It is exited once:
    after that, ``rollback`` does nothing. ``__aexit__`` resets its tokens,
    so it runs in the caller's task; the protocol has it shield its own I/O.
    """

    phased: ClassVar[bool] = False

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow
        self._exited = False

    @property
    def physical(self) -> PhysicalStore:
        """Unknown: the protocol declares none."""
        return UNKNOWN_PHYSICAL

    @property
    def handle(self) -> None:
        """None: the protocol exposes no session, so repositories open their own."""
        return None

    def accepts(self, owner: object) -> bool:
        """Any repository of the store joins it: the unit of work declares no owner."""
        del owner
        return True

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


class Participant:
    """One store's unit of work inside an invocation.

    Records the bookkeeping the coordinator orders commits by and runs the
    five phases through the driver that fits its unit of work.

    Args:
        key: Store the unit of work belongs to.
        uow: The store's unit of work, not yet begun.

    Attributes:
        key: Store the participant belongs to.
        transactional: Whether its unit of work can roll back.
        first_write: Invocation-wide order of its first write, or ``None``.
        wrote: Whether a write went through it.
        poisoned: Why the invocation must not commit, once :meth:`poison` set it.
    """

    def __init__(self, key: StoreKey, uow: UnitOfWork) -> None:
        self.key = key
        self.transactional = type(uow).transactional
        self.first_write: int | None = None
        self.wrote = False
        self.poisoned: BaseException | None = None
        self._driver: _Driver = (
            _PhasedDriver(key, uow) if _is_phased(uow) else _SinglePhaseDriver(uow)
        )

    @property
    def phased(self) -> bool:
        """Whether its unit of work declares the :class:`PhasedUnitOfWork` marker."""
        return self._driver.phased

    @property
    def physical(self) -> PhysicalStore:
        """Where its writes land, read when asked: only logging and errors need it."""
        return self._driver.physical

    @property
    def handle(self) -> object | None:
        """The session its store's repositories join; ``None`` for a single-phase unit of work."""
        return self._driver.handle

    def accepts(self, owner: object) -> bool:
        """Whether a repository whose sessions come from ``owner`` may join this participant.

        A phased participant accepts only its unit of work's own owner; a
        single-phase one declares no owner and accepts any repository of its
        store.

        Args:
            owner: The repository's session manager or client.
        """
        return self._driver.accepts(owner)

    def record_write(self, order: int) -> None:
        """Note a write; only the first one sets :attr:`first_write`.

        Args:
            order: Invocation-wide sequence number of the write.
        """
        if self.first_write is None:
            self.first_write = order
        self.wrote = True

    def poison(self, cause: BaseException) -> None:
        """Forbid the invocation to commit: :meth:`OrderedCommits.finish` rolls every store back.

        For a transaction whose state can no longer be trusted, such as one
        whose elevation flag could not be cleared. It holds even when the
        caller catches the error that reported it.

        Args:
            cause: Why; the stores' :class:`RolledBack` outcomes carry it.
        """
        self.poisoned = self.poisoned or cause

    async def begin(self) -> None:
        """Open the unit of work; a phased one whose ``begin`` fails is closed."""
        await self._driver.begin()

    async def flush(self) -> None:
        """Send pending writes without committing them."""
        await self._driver.flush()

    async def commit(self) -> None:
        """Commit the unit of work."""
        await self._driver.commit()

    async def rollback(self, cause: BaseException) -> None:
        """Discard the unit of work's writes.

        Args:
            cause: Why the invocation does not commit this store.
        """
        await self._driver.rollback(cause)

    async def close(self) -> None:
        """Release the unit of work."""
        await self._driver.close()


class ParticipantScope(Protocol):
    """What the coordinator needs from an invocation scope.

    Attributes:
        participants: Participants by store, in enlistment order.
        closed: Set once :meth:`Coordinator.finish` has ended the scope.
        outcome: Set by :meth:`Coordinator.finish`, also when it raises.
    """

    participants: dict[StoreKey, Participant]
    closed: bool
    outcome: InvocationOutcome | None


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
            UncomposedStore: If no factory is registered for ``key``.
        """
        existing = scope.participants.get(key)
        if existing is not None:
            return existing
        factory = self._factories.get(key)
        if factory is None:
            raise UncomposedStore(key, self._factories)
        participant = Participant(key, factory.create())
        await participant.begin()
        scope.participants[key] = participant
        return participant

    async def finish(
        self, scope: ParticipantScope, error: BaseException | None
    ) -> InvocationOutcome:
        """End every participant of ``scope`` and report each store's outcome.

        ``scope.outcome`` is set before anything is raised, so a caller that
        catches the interruption still reads it. ``error`` itself is never
        raised here: re-raising it is the caller's job.

        Args:
            scope: Scope whose participants end.
            error: Exception the invocation failed with, or ``None``. Without
                one, a poisoned participant's cause fails the invocation:
                every store rolls back.

        Returns:
            One outcome per participant, in commit order.

        Raises:
            asyncio.CancelledError: If the task was cancelled while finishing.
            BaseException: Likewise, for any other interruption that is not an
                ``Exception`` (``KeyboardInterrupt``, ``SystemExit``).
        """
        settlement = _Settlement(tuple(scope.participants.values()), error)
        outcome = await settlement.run()
        scope.outcome, scope.closed = outcome, True
        _log_outcome(outcome, scope.participants)
        settlement.raise_interrupt()
        return outcome


def _commit_order(participants: Iterable[Participant]) -> list[Participant]:
    return sorted(participants, key=lambda p: (p.first_write is None, p.first_write or 0))


def _cause(error: BaseException) -> BaseException:
    return error.cause if isinstance(error, CommitUncertain) else error


def _is_success(outcome: StoreOutcome | None) -> bool:
    return isinstance(outcome, Committed | NonTransactional)


def _succeeded(participant: Participant) -> StoreOutcome:
    if participant.transactional:
        return Committed(participant.key)
    return NonTransactional(participant.key, participant.wrote)


def _undone(participant: Participant, cause: BaseException) -> StoreOutcome:
    if participant.transactional:
        return RolledBack(participant.key, cause)
    return NonTransactional(participant.key, participant.wrote)


def _failed(participant: Participant, error: BaseException) -> StoreOutcome:
    """Classify a commit that raised ``error``.

    A defined failure is a rollback when the store can roll back or never
    wrote. A lost acknowledgement, an interruption, or a failure after a
    non-transactional write is uncertain.
    """
    defined = isinstance(error, Exception) and not isinstance(error, CommitUncertain)
    if defined and (participant.transactional or not participant.wrote):
        return RolledBack(participant.key, error)
    return Uncertain(participant.key, _cause(error))


class _Settlement:
    """One run of :meth:`OrderedCommits.finish` over the enlisted participants."""

    def __init__(self, enlisted: tuple[Participant, ...], error: BaseException | None) -> None:
        self._enlisted = enlisted
        # A poisoned participant fails a clean invocation, as its business error would.
        self._failure = error or next(
            (p.poisoned for p in enlisted if p.poisoned is not None), None
        )
        self._outcomes: dict[StoreKey, StoreOutcome] = {}
        self._interrupt: BaseException | None = None

    async def run(self) -> InvocationOutcome:
        phased = _commit_order(p for p in self._enlisted if p.phased)
        if self._failure is None:
            await self._flush(phased)
        if self._failure is None:
            await self._commit(phased)
        for participant in reversed(self._enlisted):
            await self._commit_if_pending(participant)
            await self._teardown(participant)
        single = [p for p in reversed(self._enlisted) if not p.phased]
        return InvocationOutcome(tuple(self._outcomes[p.key] for p in [*phased, *single]))

    def raise_interrupt(self) -> None:
        if self._interrupt is not None:
            raise self._interrupt

    async def _flush(self, phased: list[Participant]) -> None:
        for participant in phased:
            error = await self._attempt(participant.flush, participant)
            if error is not None:
                self._failure = error
                return

    async def _commit(self, phased: list[Participant]) -> None:
        for participant in phased:
            await self._commit_one(participant)
            if self._failure is not None:
                return

    async def _commit_one(self, participant: Participant) -> None:
        error = await self._attempt(participant.commit, participant)
        if error is None:
            self._outcomes[participant.key] = _succeeded(participant)
            return
        self._failure = _cause(error)
        self._outcomes[participant.key] = _failed(participant, error)

    async def _commit_if_pending(self, participant: Participant) -> None:
        """Commit a single-phase participant at its LIFO position, if nothing failed."""
        if self._failure is None and participant.key not in self._outcomes:
            await self._commit_one(participant)

    async def _teardown(self, participant: Participant) -> None:
        failure = self._failure
        if failure is not None and not _is_success(self._outcomes.get(participant.key)):
            self._outcomes.setdefault(participant.key, _undone(participant, failure))
            await self._attempt(
                lambda: participant.rollback(failure), participant, "UoWRollbackFailed"
            )
        await self._attempt(participant.close, participant, "UoWCloseFailed")

    async def _attempt(
        self,
        step: Callable[[], Awaitable[None]],
        participant: Participant,
        log_failure_as: str | None = None,
    ) -> BaseException | None:
        """Run one step and return what it raised.

        An ``Exception`` is returned, and logged when ``log_failure_as`` is
        given. Anything else is an interruption: the first one becomes the
        failure, so no later participant commits, and :meth:`raise_interrupt`
        raises it once every participant has ended.
        """
        try:
            await step()
        except Exception as exc:
            if log_failure_as is not None:
                _log.exception(log_failure_as, store=participant.key)
            return exc
        except BaseException as exc:  # NOSONAR S5754 - re-raised by raise_interrupt()
            self._interrupt = self._interrupt or exc
            self._failure = self._failure or exc
            return exc
        return None


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
