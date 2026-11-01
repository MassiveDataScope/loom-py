"""Ordered commits: enlistment, commit order, outcomes, teardown and logging.

The fault matrix lives in ``test_coordinator_faults.py``; these tests pin
the rules it relies on, one at a time.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import pytest

from loom.core.persistence.stores import StoreKey
from loom.core.transaction import in_atomic_transaction
from loom.core.uow.coordinator import (
    UNKNOWN_PHYSICAL,
    Committed,
    CommitUncertain,
    InvocationOutcome,
    NonTransactional,
    OrderedCommits,
    PhysicalStore,
    RolledBack,
    StoreOutcome,
    Uncertain,
)

from ._coordinator_fakes import (
    Boom,
    Effect,
    Factory,
    Fault,
    Ledger,
    NonTransactionalStore,
    PhasedStore,
    Scope,
    ThirdPartyStore,
)

S0, S1, S2, S3 = (StoreKey(f"s{index}") for index in range(4))


def _coordinator(*stores: PhasedStore | ThirdPartyStore) -> OrderedCommits:
    return OrderedCommits({store.key: Factory(store) for store in stores})


async def _enlist_all(coordinator: OrderedCommits, scope: Scope, *keys: StoreKey) -> None:
    for key in keys:
        await coordinator.enlist(scope, key)


def _calls(ledger: Ledger, call: str) -> list[StoreKey]:
    return [key for key, made in ledger.calls if made == call]


class TestEnlist:
    async def test_begins_the_unit_of_work_and_registers_the_participant(self) -> None:
        ledger = Ledger()
        store = PhasedStore(S0, ledger)
        scope = Scope()

        participant = await _coordinator(store).enlist(scope, S0)

        assert scope.participants == {S0: participant}
        assert ledger.calls == [(S0, "begin")]

    async def test_enlisting_a_store_twice_returns_its_participant_without_a_second_begin(
        self,
    ) -> None:
        ledger = Ledger()
        coordinator = _coordinator(PhasedStore(S0, ledger))
        scope = Scope()
        first = await coordinator.enlist(scope, S0)

        second = await coordinator.enlist(scope, S0)

        assert second is first
        assert ledger.calls == [(S0, "begin")]

    async def test_a_participant_whose_begin_fails_is_not_registered(self) -> None:
        store = PhasedStore(S0, Ledger(), Fault("begin", Effect.RAISE))
        coordinator, scope = _coordinator(store), Scope()

        with pytest.raises(Boom):
            await coordinator.enlist(scope, S0)

        assert scope.participants == {}

    async def test_a_store_without_a_factory_is_a_key_error(self) -> None:
        coordinator, scope = OrderedCommits({}), Scope()

        with pytest.raises(KeyError):
            await coordinator.enlist(scope, S0)

    @pytest.mark.parametrize(
        ("store_type", "physical"),
        [
            (PhasedStore, PhysicalStore(backend="fake", target="memory://s0")),
            (ThirdPartyStore, UNKNOWN_PHYSICAL),
        ],
        ids=["phased-declares-it", "third-party-unknown"],
    )
    async def test_exposes_the_physical_identity_of_the_store(
        self, store_type: type[PhasedStore | ThirdPartyStore], physical: PhysicalStore
    ) -> None:
        store = store_type(S0, Ledger())

        participant = await _coordinator(store).enlist(Scope(), S0)

        assert participant.physical == physical


class TestCommitOrder:
    async def test_writers_commit_by_first_write_then_the_rest_by_enlistment(self) -> None:
        ledger = Ledger()
        stores = [PhasedStore(key, ledger) for key in (S0, S1, S2, S3)]
        coordinator, scope = _coordinator(*stores), Scope()
        await _enlist_all(coordinator, scope, S0, S1, S2, S3)
        scope.participants[S2].record_write(0)
        scope.participants[S0].record_write(1)
        scope.participants[S2].record_write(2)

        outcome = await coordinator.finish(scope, None)

        assert _calls(ledger, "commit") == [S2, S0, S1, S3]
        assert [store.key for store in outcome.stores] == [S2, S0, S1, S3]

    async def test_the_first_write_is_kept_and_any_write_marks_the_participant(self) -> None:
        participant = await _coordinator(PhasedStore(S0, Ledger())).enlist(Scope(), S0)

        participant.record_write(4)
        participant.record_write(1)

        assert (participant.first_write, participant.wrote) == (4, True)

    async def test_flush_runs_on_every_phased_store_before_any_commit(self) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(PhasedStore(S0, ledger), PhasedStore(S1, ledger)), Scope()
        await _enlist_all(coordinator, scope, S0, S1)

        await coordinator.finish(scope, None)

        phases = [call for _, call in ledger.calls if call in {"flush", "commit"}]
        assert phases == ["flush", "flush", "commit", "commit"]

    async def test_a_third_party_unit_of_work_commits_at_its_lifo_position_after_phased_ones(
        self,
    ) -> None:
        ledger = Ledger()
        third_a, phased, third_b = (
            ThirdPartyStore(S0, ledger),
            PhasedStore(S1, ledger),
            ThirdPartyStore(S2, ledger),
        )
        coordinator, scope = _coordinator(third_a, phased, third_b), Scope()
        await _enlist_all(coordinator, scope, S0, S1, S2)

        outcome = await coordinator.finish(scope, None)

        assert _calls(ledger, "commit") == [S1, S2, S0]
        assert outcome.stores == (Committed(S1), Committed(S2), Committed(S0))
        assert in_atomic_transaction() is False

    async def test_a_scope_without_participants_succeeds_with_no_outcomes(self) -> None:
        scope = Scope()

        outcome = await OrderedCommits({}).finish(scope, None)

        assert outcome == InvocationOutcome(stores=())
        assert outcome.succeeded is True
        assert scope.closed is True


class TestOutcome:
    @pytest.mark.parametrize(
        ("store", "succeeded"),
        [
            (Committed(S0), True),
            (NonTransactional(S0, wrote=True), True),
            (RolledBack(S0, Boom()), False),
            (Uncertain(S0, ConnectionError()), False),
        ],
        ids=["committed", "non-transactional", "rolled-back", "uncertain"],
    )
    def test_succeeded_only_when_every_store_committed_or_is_non_transactional(
        self, store: StoreOutcome, succeeded: bool
    ) -> None:
        outcome = InvocationOutcome(stores=(Committed(S1), store))

        assert outcome.succeeded is succeeded

    def test_commit_uncertain_carries_the_driver_error(self) -> None:
        cause = ConnectionError("ack lost")

        assert CommitUncertain(cause).cause is cause

    async def test_a_non_transactional_store_that_never_wrote_says_so(self) -> None:
        store = NonTransactionalStore(S0, Ledger())
        coordinator, scope = _coordinator(store), Scope()
        await coordinator.enlist(scope, S0)

        outcome = await coordinator.finish(scope, None)

        assert outcome.stores == (NonTransactional(S0, wrote=False),)


class TestTeardownFailures:
    @pytest.mark.parametrize(
        ("store_type", "phase", "event"),
        [
            (PhasedStore, "rollback", "UoWRollbackFailed"),
            (ThirdPartyStore, "rollback", "UoWRollbackFailed"),
            (PhasedStore, "close", "UoWCloseFailed"),
        ],
        ids=["phased-rollback", "third-party-rollback", "phased-close"],
    )
    async def test_a_failed_teardown_step_is_logged_and_the_others_still_end(
        self,
        store_type: type[PhasedStore | ThirdPartyStore],
        phase: str,
        event: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        ledger = Ledger()
        faulty = store_type(S1, ledger, Fault(phase, Effect.RAISE))
        coordinator = _coordinator(PhasedStore(S0, ledger), faulty, PhasedStore(S2, ledger))
        scope = Scope()
        await _enlist_all(coordinator, scope, S0, S1, S2)
        error = Boom("business")

        with caplog.at_level(logging.ERROR):
            outcome = await coordinator.finish(scope, error)

        assert {store.key: store for store in outcome.stores} == {
            key: RolledBack(key, error) for key in (S0, S1, S2)
        }
        assert [key for key, _ in ledger.terminal_calls()] == [S2, S1, S0]
        assert event in caplog.text


@dataclass
class _Teardown:
    """State observed once a cancelled teardown has finished."""

    closed: bool = False
    signal: bool = True
    calls: list[tuple[StoreKey, str]] = field(default_factory=list)


class TestCancellationDuringTeardown:
    @pytest.mark.parametrize(
        ("store_type", "phase"),
        [(PhasedStore, "rollback"), (ThirdPartyStore, "rollback"), (PhasedStore, "close")],
        ids=["phased-rollback", "third-party-rollback", "phased-close"],
    )
    async def test_every_store_still_ends_and_the_cancellation_is_raised_after(
        self, store_type: type[PhasedStore | ThirdPartyStore], phase: str
    ) -> None:
        ledger, seen = Ledger(), _Teardown()

        async def _invocation() -> None:
            faulty = store_type(S1, ledger, Fault(phase, Effect.CANCEL))
            coordinator = _coordinator(PhasedStore(S0, ledger), faulty, PhasedStore(S2, ledger))
            scope = Scope()
            await _enlist_all(coordinator, scope, S0, S1, S2)
            try:
                await coordinator.finish(scope, Boom("business"))
            finally:
                seen.closed, seen.signal = scope.closed, in_atomic_transaction()

        task = asyncio.create_task(_invocation())
        await ledger.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert [key for key, _ in ledger.terminal_calls()] == [S2, S1, S0]
        assert _calls(ledger, "rollback") == [S2, S1, S0]
        assert (seen.closed, seen.signal) == (True, False)

    async def test_a_cancellation_while_closing_stops_a_third_party_store_from_committing(
        self,
    ) -> None:
        ledger, seen = Ledger(), _Teardown()
        outcomes: list[InvocationOutcome] = []

        async def _invocation() -> None:
            third = ThirdPartyStore(S0, ledger)
            phased = PhasedStore(S1, ledger, Fault("close", Effect.CANCEL))
            coordinator, scope = _coordinator(third, phased), Scope()
            await _enlist_all(coordinator, scope, S0, S1)
            third.write("row-s0")
            phased.write("row-s1")
            try:
                outcomes.append(await coordinator.finish(scope, None))
            finally:
                seen.closed, seen.signal = scope.closed, in_atomic_transaction()

        task = asyncio.create_task(_invocation())
        await ledger.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert outcomes == []
        assert ledger.terminal_calls() == [(S1, "close"), (S0, "exit-rollback")]
        assert dict(ledger.persisted) == {S1: ["row-s1"]}
        assert (seen.closed, seen.signal) == (True, False)


class TestOutcomeLog:
    async def test_a_failed_invocation_logs_each_store_with_its_backend_as_a_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ledger = Ledger()
        coordinator = _coordinator(PhasedStore(S0, ledger), ThirdPartyStore(S1, ledger))
        scope = Scope()
        await _enlist_all(coordinator, scope, S0, S1)

        with caplog.at_level(logging.WARNING):
            await coordinator.finish(scope, Boom("business"))

        records = [r for r in caplog.records if "InvocationOutcome" in r.getMessage()]
        assert [record.levelno for record in records] == [logging.WARNING]
        assert "fake" in records[0].getMessage()

    async def test_a_successful_invocation_logs_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        coordinator, scope = _coordinator(PhasedStore(S0, Ledger())), Scope()
        await coordinator.enlist(scope, S0)

        with caplog.at_level(logging.WARNING):
            await coordinator.finish(scope, None)

        assert not [r for r in caplog.records if "InvocationOutcome" in r.getMessage()]
