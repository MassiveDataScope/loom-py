"""Fault injection at every position of :meth:`OrderedCommits.finish`.

Every layout of one, two and three participants built from the three kinds
(phased transactional, non-transactional, third-party single-phase) meets
every fault the kind can take:

- a business error or a cancellation before the flush;
- a failure or a cancellation during each flush;
- a failure, a lost acknowledgement, or a cancellation before or after each
  commit persists;
- a failure or a cancellation during each rollback (with a business error)
  and each close (on a clean run).

Each layout also gets a write order, cycled through every permutation and
every permutation that leaves the last store without a write, so the commit
order differs from the LIFO order.

The expected values come from :func:`_expected`, which restates the rules of
ADR 0008 and design-us7 §5 for the fakes. ``test_coordinator.py`` pins some
of the same cases by hand.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from loom.core.persistence.stores import StoreKey
from loom.core.transaction import in_atomic_transaction
from loom.core.uow.coordinator import (
    Committed,
    InvocationOutcome,
    NonTransactional,
    OrderedCommits,
    RolledBack,
    StoreOutcome,
    Uncertain,
)

from ._coordinator_fakes import Boom, Effect, Factory, Fault, Kind, Ledger, Scope

Shape = tuple[str, StoreKey, object]
"""An outcome reduced to comparable values: name, store, cause type or ``wrote``."""

_TWO_WAY = (Effect.RAISE, Effect.CANCEL)
_FAULTS: dict[Kind, dict[str, tuple[Effect, ...]]] = {
    Kind.PHASED: {
        "flush": _TWO_WAY,
        "commit": tuple(Effect),
        "rollback": _TWO_WAY,
        "close": _TWO_WAY,
    },
    Kind.NON_TRANSACTIONAL: {"flush": _TWO_WAY, "commit": tuple(Effect), "close": _TWO_WAY},
    Kind.THIRD_PARTY: {"commit": tuple(Effect), "rollback": _TWO_WAY},
}
"""The phases each kind runs, and the faults each phase can take there."""


@dataclass(frozen=True)
class Case:
    """One layout, one write order and at most one error and one fault."""

    kinds: tuple[Kind, ...]
    writes: tuple[int, ...]
    error: type[BaseException] | None = None
    target: int | None = None
    fault: Fault | None = None

    @property
    def keys(self) -> list[StoreKey]:
        return [StoreKey(f"s{index}") for index in range(len(self.kinds))]

    @property
    def id(self) -> str:
        parts = ["+".join(kind.value for kind in self.kinds), "w" + "".join(map(str, self.writes))]
        if self.error is not None:
            parts.append(f"error:{self.error.__name__}")
        if self.fault is not None:
            parts.append(f"{self.fault.phase}:{self.fault.effect.value}@s{self.target}")
        return "-".join(parts)

    @property
    def cancels(self) -> bool:
        return self.fault is not None and self.fault.cancels

    def phase_is(self, *phases: str) -> bool:
        return self.fault is not None and self.fault.phase in phases


def _write_orders(size: int) -> Iterator[tuple[int, ...]]:
    orders = list(itertools.permutations(range(size)))
    return itertools.cycle(orders + [order[:-1] for order in orders])


def _faulted(kinds: tuple[Kind, ...], writes: Iterator[tuple[int, ...]]) -> Iterator[Case]:
    for target, kind in enumerate(kinds):
        for phase, effects in _FAULTS[kind].items():
            error = Boom if phase == "rollback" else None
            for effect in effects:
                yield Case(kinds, next(writes), error, target, Fault(phase, effect))


def _cases() -> Iterator[Case]:
    for size in (1, 2, 3):
        writes = _write_orders(size)
        for kinds in itertools.product(Kind, repeat=size):
            yield Case(kinds, next(writes))
            yield Case(kinds, next(writes), error=Boom)
            yield Case(kinds, next(writes), error=asyncio.CancelledError)
            yield from _faulted(kinds, writes)


CASES = list(_cases())


def _shape(outcome: StoreOutcome) -> Shape:
    match outcome:
        case Committed(key):
            return ("Committed", key, None)
        case RolledBack(key, cause) | Uncertain(key, cause):
            return (type(outcome).__name__, key, type(cause))
        case NonTransactional(key, wrote):
            return ("NonTransactional", key, wrote)


def _commit_order(case: Case) -> list[int]:
    """Phased writers by first write, other phased by enlistment, then third-party LIFO."""
    phased = [i for i, kind in enumerate(case.kinds) if kind.phased]
    writers = [i for i in case.writes if i in phased]
    lifo = reversed(range(len(case.kinds)))
    return (
        writers
        + [i for i in phased if i not in case.writes]
        + [i for i in lifo if not case.kinds[i].phased]
    )


def _cause(case: Case) -> type[BaseException] | None:
    """What makes stores roll back: the error, or a flush or commit fault."""
    if case.error is not None:
        return case.error
    if not case.phase_is("flush", "commit"):
        return None
    assert case.fault is not None
    if case.fault.cancels:
        return asyncio.CancelledError
    return ConnectionError if case.fault.effect is Effect.UNCERTAIN else Boom


class _Oracle:
    """The expected outcome of one case, in commit order."""

    def __init__(self, case: Case) -> None:
        self.case = case
        self.cause = _cause(case)

    def wrote(self, index: int) -> bool:
        return index in self.case.writes

    def nontx(self, index: int) -> bool:
        return self.case.kinds[index] is Kind.NON_TRANSACTIONAL

    def succeeded(self, index: int, key: StoreKey) -> Shape:
        if self.nontx(index):
            return ("NonTransactional", key, self.wrote(index))
        return ("Committed", key, None)

    def undone(self, index: int, key: StoreKey, cause: type[BaseException] | None) -> Shape:
        if self.nontx(index):
            return ("NonTransactional", key, self.wrote(index))
        return ("RolledBack", key, cause)

    def failed(self, index: int, key: StoreKey) -> Shape:
        defined = self.case.fault is not None and self.case.fault.effect is Effect.RAISE
        if defined and not (self.nontx(index) and self.wrote(index)):
            return ("RolledBack", key, Boom)
        return ("Uncertain", key, self.cause)

    def shapes(self) -> list[Shape]:
        keys, case = self.case.keys, self.case
        failed = self.cause is not None and not case.phase_is("commit")
        shapes: list[Shape] = []
        for index in _commit_order(case):
            if failed:
                shapes.append(self.undone(index, keys[index], self.cause))
            elif case.phase_is("commit") and index == case.target:
                shapes.append(self.failed(index, keys[index]))
                failed = True
            elif self._cancelled_close_stops(index):
                shapes.append(self.undone(index, keys[index], asyncio.CancelledError))
            else:
                shapes.append(self.succeeded(index, keys[index]))
        return shapes

    def _cancelled_close_stops(self, index: int) -> bool:
        """A third-party store swept after a cancelled close rolls back."""
        case = self.case
        return (
            case.cancels
            and case.phase_is("close")
            and not case.kinds[index].phased
            and index < (case.target or 0)
        )

    def persisted(self, shapes: list[Shape]) -> dict[StoreKey, list[str]]:
        """Rows each store holds afterwards.

        A non-transactional store keeps its write whatever happens; a
        transactional one only once it committed, or got past persisting
        before its commit fault.
        """
        late = self.case.fault is not None and self.case.fault.effect in {
            Effect.UNCERTAIN,
            Effect.CANCEL_LATE,
        }
        durable = {"Committed"} | ({"Uncertain"} if late else set())
        rows: dict[StoreKey, list[str]] = {}
        for name, key, _ in shapes:
            index = self.case.keys.index(key)
            kept = self.wrote(index) and (name in durable or self.nontx(index))
            rows[key] = [f"row-{key}"] if kept else []
        return rows


@dataclass
class Run:
    """What one scenario left behind."""

    outcome: InvocationOutcome | None
    scope: Scope
    ledger: Ledger
    cancelled: bool
    signal_after: bool


async def _scenario(case: Case, ledger: Ledger) -> Run:
    """Enlist, write and finish in one task, as an invocation does."""
    stores = [
        kind.store(key, ledger, case.fault if index == case.target else None)
        for index, (kind, key) in enumerate(zip(case.kinds, case.keys, strict=True))
    ]
    coordinator = OrderedCommits({store.key: Factory(store) for store in stores})
    scope = Scope()
    for store in stores:
        await coordinator.enlist(scope, store.key)
    for order, index in enumerate(case.writes):
        stores[index].write(f"row-{stores[index].key}")
        scope.participants[stores[index].key].record_write(order)
    cancelled = False
    try:
        await coordinator.finish(scope, case.error() if case.error is not None else None)
    except asyncio.CancelledError:
        cancelled = True
    return Run(scope.outcome, scope, ledger, cancelled, in_atomic_transaction())


async def _run(case: Case) -> Run:
    ledger = Ledger()
    task = asyncio.create_task(_scenario(case, ledger))
    if case.cancels:
        await ledger.blocked.wait()
        task.cancel()
        await asyncio.sleep(0)
        ledger.gate.set()
    return await task


@pytest.mark.parametrize("case", CASES, ids=[case.id for case in CASES])
async def test_finish_reports_each_store_and_persists_only_what_committed(case: Case) -> None:
    run = await _run(case)

    oracle = _Oracle(case)
    expected = oracle.shapes()
    assert run.outcome is not None
    assert [_shape(store) for store in run.outcome.stores] == expected
    assert {key: run.ledger.persisted[key] for key in case.keys} == oracle.persisted(expected)


@pytest.mark.parametrize("case", CASES, ids=[case.id for case in CASES])
async def test_finish_ends_every_store_once_in_lifo_order_and_raises_a_cancellation(
    case: Case,
) -> None:
    run = await _run(case)

    assert [key for key, _ in run.ledger.terminal_calls()] == list(reversed(case.keys))
    assert run.scope.closed is True
    assert run.signal_after is False
    assert run.cancelled is case.cancels
