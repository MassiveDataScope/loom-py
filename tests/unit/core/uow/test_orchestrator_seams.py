"""The seams a future cross-store orchestrator relies on (ADR 0008, T065)."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, get_args

import pytest

import loom.core.uow
from loom.core.bootstrap import create_kernel
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory
from loom.core.uow.coordinator import (
    Committed,
    Coordinator,
    InvocationOutcome,
    NonTransactional,
    OrderedCommits,
    Participant,
    ParticipantScope,
    PhysicalStore,
    RolledBack,
    StoreOutcome,
    Uncertain,
)
from loom.core.uow.scope import Frame, invocation
from loom.core.use_case import UseCase

from ._coordinator_fakes import Factory, Ledger, PhasedStore

pytestmark = pytest.mark.timeout(30)

S0, S1 = StoreKey("s0"), StoreKey("s1")


class _ExternalCoordinator:
    """A coordinator written outside loom from the protocol and the participant alone."""

    def __init__(self, factories: Mapping[StoreKey, UnitOfWorkFactory]) -> None:
        self._factories = factories
        self.physical: dict[StoreKey, PhysicalStore] = {}

    async def enlist(self, scope: ParticipantScope, key: StoreKey) -> Participant:
        participant = Participant(key, self._factories[key].create())
        await participant.begin()
        scope.participants[key] = participant
        return participant

    async def finish(
        self, scope: ParticipantScope, error: BaseException | None
    ) -> InvocationOutcome:
        enlisted = list(scope.participants.values())
        for phase in (Participant.flush, Participant.commit, Participant.close):
            for participant in enlisted:
                await phase(participant)
        self.physical = {p.key: p.physical for p in enlisted}
        scope.outcome = InvocationOutcome(tuple(Committed(p.key) for p in enlisted))
        scope.closed = True
        return scope.outcome


async def test_loom_drives_an_invocation_through_a_coordinator_written_outside_it() -> None:
    ledger = Ledger()
    stores = {key: PhasedStore(key, ledger) for key in (S0, S1)}
    external = _ExternalCoordinator({key: Factory(store) for key, store in stores.items()})

    async with invocation(external) as opened, opened.frame(Frame(S0, frozenset({S1}))):
        pass

    phases = ("begin", "flush", "commit", "close")  # each phase called on its own
    assert ledger.calls == [(key, phase) for phase in phases for key in (S0, S1)]
    assert external.physical == {key: store.physical for key, store in stores.items()}
    assert opened.outcome == InvocationOutcome((Committed(S0), Committed(S1)))


class _Noop(UseCase[Any, None]):
    async def execute(self) -> None:
        return None


async def test_the_kernel_runs_each_execution_through_the_coordinator_it_is_given() -> None:
    ledger = Ledger()
    external = _ExternalCoordinator({DEFAULT_STORE: Factory(PhasedStore(DEFAULT_STORE, ledger))})
    kernel = create_kernel(config=object(), use_cases=[_Noop], coordinator=external)

    await kernel.app.invoke(_Noop)

    phases = ("begin", "flush", "commit", "close")
    assert ledger.calls == [(DEFAULT_STORE, phase) for phase in phases]


class _FreshStores:
    def __init__(self, ledger: Ledger) -> None:
        self._ledger = ledger

    def create(self) -> UnitOfWork:
        return Factory(PhasedStore(S0, self._ledger)).create()


async def test_concurrent_invocations_sharing_one_coordinator_keep_their_own_participants() -> None:
    shared = OrderedCommits({S0: _FreshStores(Ledger())})
    both_open = asyncio.Barrier(2)

    async def run() -> tuple[ParticipantScope, Participant]:
        async with invocation(shared) as invocation_scope, invocation_scope.frame(Frame(S0)):
            await both_open.wait()
            return invocation_scope, invocation_scope.participants[S0]

    (first, first_participant), (second, second_participant) = await asyncio.gather(run(), run())

    assert first_participant is not second_participant
    assert first.outcome == second.outcome == InvocationOutcome((Committed(S0),))


def test_the_coordinator_has_exactly_two_seams() -> None:
    assert {name for name in vars(Coordinator) if not name.startswith("_")} == {"enlist", "finish"}


def test_each_store_ends_in_one_of_four_typed_outcomes() -> None:
    cause = ValueError("boom")
    rolled_back, uncertain = RolledBack(S0, cause), Uncertain(S0, cause)
    non_transactional = NonTransactional(S0, wrote=True)

    assert get_args(StoreOutcome) == (Committed, RolledBack, Uncertain, NonTransactional)
    assert all(o.key == S0 for o in (Committed(S0), rolled_back, uncertain, non_transactional))
    assert rolled_back.cause is uncertain.cause is cause
    assert non_transactional.wrote


def test_the_unit_of_work_package_exports_only_the_errors_of_the_coordinator_or_the_scope() -> None:
    symbols = [getattr(loom.core.uow, name) for name in loom.core.uow.__all__]
    exported = {s.__module__ for s in symbols if not issubclass(s, BaseException)}

    assert exported.isdisjoint({"loom.core.uow.coordinator", "loom.core.uow.scope"})
