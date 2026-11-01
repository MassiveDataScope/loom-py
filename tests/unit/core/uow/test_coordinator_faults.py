"""Fault injection at every position of :meth:`OrderedCommits.finish`.

Every layout of one, two and three participants built from the three kinds
(phased transactional, non-transactional, third-party single-phase) meets
every fault: a business error or cancellation before the flush, a failure or
cancellation during each flush, and a failure, a lost acknowledgement or a
cancellation before and after each commit persists.

Each case asserts four things: the per-store outcome, what each store
persisted, that every store was ended exactly once in LIFO order, and that
the scope is closed. The expected values come from :func:`_expected`, which
restates the rules of ADR 0008 and design-us7 §5 for the fakes:

- participants write in reverse enlistment order, so phased participants
  commit in reverse enlistment order; third-party ones commit after them, at
  their LIFO position;
- a commit failure stops the sequence: earlier stores keep their outcome,
  later ones roll back;
- a non-transactional store persists at write time and never rolls back.
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


@dataclass(frozen=True)
class Case:
    """One layout and one fault position."""

    kinds: tuple[Kind, ...]
    error: type[BaseException] | None = None
    target: int | None = None
    fault: Fault | None = None

    @property
    def keys(self) -> list[StoreKey]:
        return [StoreKey(f"s{index}") for index in range(len(self.kinds))]

    @property
    def id(self) -> str:
        layout = "+".join(kind.value for kind in self.kinds)
        if self.error is not None:
            return f"{layout}-error:{self.error.__name__}"
        if self.fault is None:
            return f"{layout}-clean"
        return f"{layout}-{self.fault.phase}:{self.fault.effect.value}@s{self.target}"


def _faults(kind: Kind) -> Iterator[Fault]:
    if kind.phased:
        yield from (Fault("flush", effect) for effect in (Effect.RAISE, Effect.CANCEL))
    yield from (Fault("commit", effect) for effect in Effect)


def _cases() -> Iterator[Case]:
    for size in (1, 2, 3):
        for kinds in itertools.product(Kind, repeat=size):
            yield Case(kinds)
            yield Case(kinds, error=Boom)
            yield Case(kinds, error=asyncio.CancelledError)
            for target, kind in enumerate(kinds):
                yield from (Case(kinds, target=target, fault=fault) for fault in _faults(kind))


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
    lifo = list(reversed(range(len(case.kinds))))
    return [i for i in lifo if case.kinds[i].phased] + [i for i in lifo if not case.kinds[i].phased]


def _cause(case: Case) -> type[BaseException] | None:
    if case.error is not None:
        return case.error
    if case.fault is None:
        return None
    if case.fault.cancels:
        return asyncio.CancelledError
    if case.fault.effect is Effect.UNCERTAIN:
        return ConnectionError
    return Boom


def _succeeded(kind: Kind, key: StoreKey) -> Shape:
    if kind is Kind.NON_TRANSACTIONAL:
        return ("NonTransactional", key, True)
    return ("Committed", key, None)


def _undone(kind: Kind, key: StoreKey, cause: type[BaseException] | None) -> Shape:
    if kind is Kind.NON_TRANSACTIONAL:
        return ("NonTransactional", key, True)
    return ("RolledBack", key, cause)


def _failed(case: Case, kind: Kind, key: StoreKey) -> Shape:
    """The outcome of the store whose commit was faulted."""
    defined = case.fault is not None and case.fault.effect is Effect.RAISE
    if defined and kind is not Kind.NON_TRANSACTIONAL:
        return ("RolledBack", key, Boom)
    return ("Uncertain", key, _cause(case))


def _expected(case: Case) -> list[Shape]:
    keys, cause = case.keys, _cause(case)
    committing = case.error is None and (case.fault is None or case.fault.phase == "commit")
    shapes: list[Shape] = []
    failed = not committing
    for index in _commit_order(case):
        kind, key = case.kinds[index], keys[index]
        if failed:
            shapes.append(_undone(kind, key, cause))
        elif index == case.target:
            shapes.append(_failed(case, kind, key))
            failed = True
        else:
            shapes.append(_succeeded(kind, key))
    return shapes


def _persisted(case: Case, shapes: list[Shape]) -> dict[StoreKey, list[str]]:
    """Rows each store holds afterwards.

    A non-transactional store keeps its write whatever happens; a
    transactional one only once it committed, or reached the point where its
    commit persisted before the fault.
    """
    late = case.fault is not None and case.fault.effect in {Effect.UNCERTAIN, Effect.CANCEL_LATE}
    durable = {"Committed"} | ({"Uncertain"} if late else set())
    kinds = dict(zip(case.keys, case.kinds, strict=True))
    return {
        key: [f"row-{key}"] if name in durable or kinds[key] is Kind.NON_TRANSACTIONAL else []
        for name, key, _ in shapes
    }


@dataclass
class Run:
    """What one scenario left behind."""

    outcome: InvocationOutcome
    scope: Scope
    ledger: Ledger
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
    for order, store in enumerate(reversed(stores)):
        store.write(f"row-{store.key}")
        scope.participants[store.key].record_write(order)
    outcome = await coordinator.finish(scope, case.error() if case.error is not None else None)
    return Run(outcome, scope, ledger, in_atomic_transaction())


async def _run(case: Case) -> Run:
    ledger = Ledger()
    task = asyncio.create_task(_scenario(case, ledger))
    if case.fault is not None and case.fault.cancels:
        await ledger.blocked.wait()
        task.cancel()
    return await task


@pytest.mark.parametrize("case", CASES, ids=[case.id for case in CASES])
async def test_finish_reports_each_store_and_persists_only_what_committed(case: Case) -> None:
    run = await _run(case)

    expected = _expected(case)
    assert [_shape(store) for store in run.outcome.stores] == expected
    assert {key: run.ledger.persisted[key] for key in case.keys} == _persisted(case, expected)


@pytest.mark.parametrize("case", CASES, ids=[case.id for case in CASES])
async def test_finish_ends_every_store_once_in_lifo_order_and_closes_the_scope(
    case: Case,
) -> None:
    run = await _run(case)

    assert [key for key, _ in run.ledger.terminal_calls()] == list(reversed(case.keys))
    assert run.scope.closed is True
    assert run.signal_after is False
