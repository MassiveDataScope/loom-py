"""Recording units of work for the ordered-commits coordinator tests.

Every fake writes into a shared :class:`Ledger`: the calls each store
received, in order, and what each store persisted. A :class:`Fault` makes one
phase of one store raise, lose its commit acknowledgement, or block until the
test cancels the task running ``finish``.
"""

from __future__ import annotations

import asyncio
import enum
from collections import defaultdict
from collections.abc import Callable, Mapping
from contextvars import Token
from dataclasses import dataclass, field
from typing import ClassVar

from loom.core.persistence.stores import StoreKey
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import (
    CommitUncertain,
    InvocationOutcome,
    Participant,
    PhysicalStore,
)


class Boom(Exception):
    """A defined failure injected by a :class:`Fault`."""


class Effect(enum.Enum):
    """What a fault does when its phase runs."""

    RAISE = "raise"
    """Raise :class:`Boom` before anything is persisted."""
    UNCERTAIN = "uncertain"
    """Persist, then raise :class:`CommitUncertain`: the acknowledgement was lost."""
    CANCEL = "cancel"
    """Block before persisting until the test cancels the task."""
    CANCEL_LATE = "cancel_late"
    """Persist, then block until the test cancels the task."""


@dataclass(frozen=True)
class Fault:
    """One injected fault: ``effect`` when ``phase`` runs."""

    phase: str
    effect: Effect

    @property
    def cancels(self) -> bool:
        return self.effect in {Effect.CANCEL, Effect.CANCEL_LATE}


@dataclass
class Ledger:
    """Calls received and rows persisted, shared by every store of one test."""

    calls: list[tuple[StoreKey, str]] = field(default_factory=list)
    persisted: defaultdict[StoreKey, list[str]] = field(default_factory=lambda: defaultdict(list))
    blocked: asyncio.Event = field(default_factory=asyncio.Event)
    gate: asyncio.Event = field(default_factory=asyncio.Event)

    def calls_of(self, key: StoreKey) -> list[str]:
        return [call for store, call in self.calls if store == key]

    def terminal_calls(self) -> list[tuple[StoreKey, str]]:
        """Calls that end a store: ``close`` or a third-party ``__aexit__``."""
        return [(key, call) for key, call in self.calls if call in TERMINAL_CALLS]


TERMINAL_CALLS = frozenset({"close", "exit-commit", "exit-rollback"})


class _RecordingStore:
    """Shared behaviour: record calls, stage writes, apply the fault."""

    transactional: ClassVar[bool] = True

    def __init__(self, key: StoreKey, ledger: Ledger, fault: Fault | None) -> None:
        self.key = key
        self.ledger = ledger
        self.fault = fault
        self.staged: list[str] = []

    def write(self, row: str) -> None:
        if self.transactional:
            self.staged.append(row)
        else:
            self.ledger.persisted[self.key].append(row)

    def _record(self, call: str) -> None:
        self.ledger.calls.append((self.key, call))

    def _persist(self) -> None:
        self.ledger.persisted[self.key].extend(self.staged)
        self.staged.clear()

    async def _run(self, phase: str, persist: Callable[[], None] = lambda: None) -> None:
        """Record ``phase``, apply the fault aimed at it, persist when it gets that far."""
        self._record(phase)
        effect = self.fault.effect if self.fault and self.fault.phase == phase else None
        if effect is Effect.RAISE:
            raise Boom(f"{phase} failed on {self.key}")
        if effect is Effect.CANCEL:
            await self._block()
        persist()
        if effect is Effect.UNCERTAIN:
            raise CommitUncertain(ConnectionError(f"lost {phase} ack from {self.key}"))
        if effect is Effect.CANCEL_LATE:
            await self._block()

    async def _block(self) -> None:
        """Wait at the gate; a cancellation interrupts the wait unless the step is drained."""
        self.ledger.blocked.set()
        await self.ledger.gate.wait()
        self._record("unblocked")


class PhasedStore(_RecordingStore):
    """A loom-owned transactional unit of work with separate phases."""

    phased: ClassVar[bool] = True

    def __init__(self, key: StoreKey, ledger: Ledger, fault: Fault | None = None) -> None:
        super().__init__(key, ledger, fault)
        self.physical = PhysicalStore(backend="fake", target=f"memory://{key}")
        self.owner: object = object()
        self._token: Token[bool] | None = None

    async def begin(self) -> None:
        if self.transactional:
            self._token = open_atomic_transaction()
        await self._run("begin")

    async def flush(self) -> None:
        await self._run("flush")

    async def commit(self) -> None:
        await self._run("commit", self._persist)

    async def rollback(self) -> None:
        self.staged.clear()
        await self._run("rollback")

    async def close(self) -> None:
        try:
            await self._run("close")
        finally:
            if self._token is not None:
                close_atomic_transaction(self._token)
                self._token = None

    async def __aenter__(self) -> PhasedStore:
        raise AssertionError("the coordinator drives a phased unit of work phase by phase")

    async def __aexit__(self, *exc: object) -> None:
        raise AssertionError("the coordinator drives a phased unit of work phase by phase")


class NonTransactionalStore(PhasedStore):
    """A loom-owned unit of work whose writes autocommit (no-op commit and rollback)."""

    transactional: ClassVar[bool] = False


class ThirdPartyStore(_RecordingStore):
    """A protocol-only unit of work; a transactional one opens the atomic signal."""

    def __init__(self, key: StoreKey, ledger: Ledger, fault: Fault | None = None) -> None:
        super().__init__(key, ledger, fault)
        self._token: Token[bool] | None = None

    async def begin(self) -> None:
        await self._run("begin")
        if self.transactional:
            self._token = open_atomic_transaction()

    async def commit(self) -> None:
        await self._run("commit", self._persist)

    async def rollback(self) -> None:
        self.staged.clear()
        await self._run("rollback")

    async def __aenter__(self) -> ThirdPartyStore:
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        try:
            if exc_type is None:
                self._record("exit-commit")
                await self.commit()
            else:
                self._record("exit-rollback")
                await self.rollback()
        finally:
            if self._token is not None:
                close_atomic_transaction(self._token)
                self._token = None


class AutocommitStore(ThirdPartyStore):
    """A third-party unit of work whose writes autocommit, like ``NoOpMongoUnitOfWork``.

    It declares no ``phased`` marker, so the coordinator drives it in a single
    phase and its participant has no owner; it opens no atomic signal.
    """

    transactional: ClassVar[bool] = False


StoreFactory = Callable[[StoreKey, Ledger, Fault | None], _RecordingStore]


class Kind(enum.Enum):
    """The three kinds of participant the matrix combines."""

    PHASED = "phased"
    NON_TRANSACTIONAL = "nontx"
    THIRD_PARTY = "third"

    @property
    def store(self) -> StoreFactory:
        return _STORES[self]

    @property
    def phased(self) -> bool:
        return self is not Kind.THIRD_PARTY


_STORES: Mapping[Kind, StoreFactory] = {
    Kind.PHASED: PhasedStore,
    Kind.NON_TRANSACTIONAL: NonTransactionalStore,
    Kind.THIRD_PARTY: ThirdPartyStore,
}


class Factory:
    """A unit-of-work factory that hands out one prepared store."""

    def __init__(self, store: _RecordingStore) -> None:
        self.store = store

    def create(self) -> UnitOfWork:
        assert isinstance(self.store, UnitOfWork)
        return self.store


@dataclass
class Scope:
    """Stand-in for the invocation scope: participants and the closed flag."""

    participants: dict[StoreKey, Participant] = field(default_factory=dict)
    closed: bool = False
    outcome: InvocationOutcome | None = None
