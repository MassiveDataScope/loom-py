"""Ordered commits: enlistment, commit order, outcomes, interruptions and logging.

The generated fault matrix lives in ``test_coordinator_faults.py``; these
tests pin its rules one at a time, and some of its cases by hand.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest
from sqlalchemy.engine import make_url

from loom.core._async import run_to_completion
from loom.core.errors import UncomposedStore
from loom.core.errors.codes import ErrorCode
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.mongo.uow import MongoUnitOfWork
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.uow import coordinator as coordinator_module
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import (
    UNKNOWN_PHYSICAL,
    Committed,
    CommitUncertain,
    InvocationOutcome,
    NonTransactional,
    OrderedCommits,
    Participant,
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
    Kind,
    Ledger,
    NonTransactionalStore,
    PhasedStore,
    Scope,
    ThirdPartyStore,
)

S0, S1, S2, S3 = (StoreKey(f"s{index}") for index in range(4))

AnyStore = PhasedStore | ThirdPartyStore


def _coordinator(*stores: AnyStore, rollback_timeout: float = 30.0) -> OrderedCommits:
    factories = {store.key: Factory(store) for store in stores}
    return OrderedCommits(factories, rollback_timeout=rollback_timeout)


async def _enlist_all(coordinator: OrderedCommits, scope: Scope, *keys: StoreKey) -> None:
    for key in keys:
        await coordinator.enlist(scope, key)


def _calls(ledger: Ledger, call: str) -> list[StoreKey]:
    return [key for key, made in ledger.calls if made == call]


def _ends(ledger: Ledger) -> list[StoreKey]:
    return [key for key, _ in ledger.terminal_calls()]


class _Interrupt(BaseException):
    """An interruption that is not an ``Exception`` and that asyncio does not special-case."""


class _InterruptedCommit(PhasedStore):
    async def commit(self) -> None:
        self._record("commit")
        raise _Interrupt


class _CloseFails(PhasedStore):
    async def close(self) -> None:
        await super().close()
        raise Boom("close failed")


class _FlushOnly(ThirdPartyStore):
    async def flush(self) -> None:
        """A phase method without the phased marker."""


class _CloseOnly(ThirdPartyStore):
    async def close(self) -> None:
        """A phase method without the phased marker."""


class TestEnlist:
    async def test_begins_the_unit_of_work_and_registers_the_participant(self) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(PhasedStore(S0, ledger)), Scope()

        participant = await coordinator.enlist(scope, S0)

        assert scope.participants == {S0: participant}
        assert ledger.calls == [(S0, "begin")]

    async def test_enlisting_a_store_twice_returns_its_participant_without_a_second_begin(
        self,
    ) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(PhasedStore(S0, ledger)), Scope()
        first = await coordinator.enlist(scope, S0)

        second = await coordinator.enlist(scope, S0)

        assert second is first
        assert ledger.calls == [(S0, "begin")]

    @pytest.mark.parametrize(
        "store_type", [PhasedStore, _CloseFails], ids=["closes", "close-fails"]
    )
    async def test_a_failed_phased_begin_is_closed_and_not_registered(
        self, store_type: type[PhasedStore]
    ) -> None:
        ledger = Ledger()
        coordinator = _coordinator(store_type(S0, ledger, Fault("begin", Effect.RAISE)))
        scope = Scope()

        with pytest.raises(Boom, match="begin failed"):
            await coordinator.enlist(scope, S0)

        assert scope.participants == {}
        assert ledger.calls == [(S0, "begin"), (S0, "close")]

    async def test_a_close_failing_after_a_failed_begin_is_logged_with_its_store(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        coordinator = _coordinator(_CloseFails(S0, Ledger(), Fault("begin", Effect.RAISE)))
        scope = Scope()

        with caplog.at_level(logging.ERROR), pytest.raises(Boom, match="begin failed"):
            await coordinator.enlist(scope, S0)

        assert "UoWCloseFailed" in caplog.text
        assert "'store': 's0'" in caplog.text

    async def test_a_store_without_a_factory_is_refused_naming_it_and_the_composed_ones(
        self,
    ) -> None:
        coordinator = OrderedCommits({S1: Factory(PhasedStore(S1, Ledger()))})
        scope = Scope()

        with pytest.raises(UncomposedStore, match=r"store 's0' .* are \['s1'\]") as caught:
            await coordinator.enlist(scope, S0)

        assert (caught.value.code, caught.value.retryable) == (ErrorCode.UNCOMPOSED_STORE, False)

    @pytest.mark.parametrize("store_type", [_FlushOnly, _CloseOnly], ids=["flush", "close"])
    async def test_a_unit_of_work_without_the_phased_marker_runs_in_a_single_phase(
        self, store_type: type[ThirdPartyStore]
    ) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(store_type(S0, ledger)), Scope()

        participant = await coordinator.enlist(scope, S0)
        outcome = await coordinator.finish(scope, None)

        assert participant.phased is False
        assert outcome.stores == (Committed(S0),)
        assert ledger.calls_of(S0) == ["begin", "exit-commit", "commit"]

    @pytest.mark.parametrize(
        ("store_type", "phased", "physical"),
        [
            (PhasedStore, True, PhysicalStore(backend="fake", target="memory://s0")),
            (ThirdPartyStore, False, UNKNOWN_PHYSICAL),
        ],
        ids=["phased-declares-it", "third-party-unknown"],
    )
    async def test_exposes_whether_it_is_phased_and_its_physical_identity(
        self, store_type: type[AnyStore], phased: bool, physical: PhysicalStore
    ) -> None:
        coordinator = _coordinator(store_type(S0, Ledger()))

        participant = await coordinator.enlist(Scope(), S0)

        assert participant.phased is phased
        assert participant.physical == physical

    async def test_a_phased_participant_accepts_the_owner_of_its_unit_of_work(self) -> None:
        store = PhasedStore(S0, Ledger())

        participant = await _coordinator(store).enlist(Scope(), S0)

        assert participant.accepts(store.owner) is True

    async def test_a_phased_participant_refuses_another_owner(self) -> None:
        participant = await _coordinator(PhasedStore(S0, Ledger())).enlist(Scope(), S0)

        assert participant.accepts(object()) is False

    async def test_a_single_phase_participant_accepts_any_owner(self) -> None:
        participant = await _coordinator(ThirdPartyStore(S0, Ledger())).enlist(Scope(), S0)

        assert participant.accepts(object()) is True

    async def test_a_phased_participant_hands_out_the_session_of_its_unit_of_work(
        self,
    ) -> None:
        store = PhasedStore(S0, Ledger())

        participant = await _coordinator(store).enlist(Scope(), S0)

        assert participant.handle is store.session

    async def test_a_single_phase_participant_hands_out_no_session(self) -> None:
        participant = await _coordinator(ThirdPartyStore(S0, Ledger())).enlist(Scope(), S0)

        assert participant.handle is None


def _session_manager() -> MagicMock:
    manager = MagicMock()
    manager.engine.url = make_url("sqlite+aiosqlite:///loom.db")
    return manager


@pytest.mark.parametrize(
    "uow",
    [SQLAlchemyUnitOfWork(_session_manager()), MongoUnitOfWork(MagicMock(), store=DEFAULT_STORE)],
    ids=["sqlalchemy", "mongo"],
)
def test_loom_transactional_units_of_work_are_phased(uow: UnitOfWork) -> None:
    assert Participant(S0, uow).phased


class TestCommitOrder:
    async def test_writers_commit_by_first_write_then_the_rest_by_enlistment(self) -> None:
        ledger = Ledger()
        coordinator = _coordinator(*(PhasedStore(key, ledger) for key in (S0, S1, S2, S3)))
        scope = Scope()
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

        assert participant.first_write == 4
        assert participant.wrote is True

    async def test_flush_runs_on_every_phased_store_before_any_commit(self) -> None:
        ledger = Ledger()
        coordinator = _coordinator(PhasedStore(S0, ledger), PhasedStore(S1, ledger))
        scope = Scope()
        await _enlist_all(coordinator, scope, S0, S1)

        await coordinator.finish(scope, None)

        phases = [call for _, call in ledger.calls if call in {"flush", "commit"}]
        assert phases == ["flush", "flush", "commit", "commit"]

    async def test_a_third_party_unit_of_work_commits_at_its_lifo_position_after_phased_ones(
        self,
    ) -> None:
        ledger = Ledger()
        coordinator = _coordinator(
            ThirdPartyStore(S0, ledger), PhasedStore(S1, ledger), ThirdPartyStore(S2, ledger)
        )
        scope = Scope()
        await _enlist_all(coordinator, scope, S0, S1, S2)

        outcome = await coordinator.finish(scope, None)

        assert _calls(ledger, "commit") == [S1, S2, S0]
        assert outcome.stores == (Committed(S1), Committed(S2), Committed(S0))

    async def test_a_scope_without_participants_succeeds_with_no_outcomes(self) -> None:
        scope = Scope()

        outcome = await OrderedCommits({}).finish(scope, None)

        assert outcome == InvocationOutcome(stores=())
        assert scope.outcome == outcome
        assert scope.closed is True


@dataclass(frozen=True)
class _KeyCase:
    """A case of the matrix with its expectations written out by hand."""

    kinds: tuple[Kind, ...]
    writes: tuple[int, ...]
    fault: tuple[int, Fault] | None
    outcome: list[tuple[str, StoreKey, object]]
    persisted: dict[StoreKey, list[str]]


_KEY_CASES = {
    "second-commit-fails-after-a-non-transactional-write": _KeyCase(
        (Kind.PHASED, Kind.NON_TRANSACTIONAL, Kind.THIRD_PARTY),
        writes=(1, 0, 2),
        fault=(0, Fault("commit", Effect.RAISE)),
        outcome=[
            ("NonTransactional", S1, True),
            ("RolledBack", S0, Boom),
            ("RolledBack", S2, Boom),
        ],
        persisted={S0: [], S1: ["row-s1"], S2: []},
    ),
    "lost-ack-on-the-second-commit": _KeyCase(
        (Kind.PHASED, Kind.PHASED),
        writes=(0, 1),
        fault=(1, Fault("commit", Effect.UNCERTAIN)),
        outcome=[("Committed", S0, None), ("Uncertain", S1, ConnectionError)],
        persisted={S0: ["row-s0"], S1: ["row-s1"]},
    ),
    "flush-fails-before-a-third-party-commit": _KeyCase(
        (Kind.THIRD_PARTY, Kind.PHASED),
        writes=(0, 1),
        fault=(1, Fault("flush", Effect.RAISE)),
        outcome=[("RolledBack", S1, Boom), ("RolledBack", S0, Boom)],
        persisted={S0: [], S1: []},
    ),
    "non-transactional-commit-fails-without-a-write": _KeyCase(
        (Kind.NON_TRANSACTIONAL,),
        writes=(),
        fault=(0, Fault("commit", Effect.RAISE)),
        outcome=[("RolledBack", S0, Boom)],
        persisted={S0: []},
    ),
    "non-transactional-commit-fails-after-a-write": _KeyCase(
        (Kind.NON_TRANSACTIONAL,),
        writes=(0,),
        fault=(0, Fault("commit", Effect.RAISE)),
        outcome=[("Uncertain", S0, Boom)],
        persisted={S0: ["row-s0"]},
    ),
    "write-order-differs-from-lifo": _KeyCase(
        (Kind.PHASED, Kind.THIRD_PARTY, Kind.PHASED),
        writes=(0, 2, 1),
        fault=None,
        outcome=[("Committed", S0, None), ("Committed", S2, None), ("Committed", S1, None)],
        persisted={S0: ["row-s0"], S1: ["row-s1"], S2: ["row-s2"]},
    ),
}


def _shape(outcome: StoreOutcome) -> tuple[str, StoreKey, object]:
    match outcome:
        case Committed(key):
            return ("Committed", key, None)
        case RolledBack(key, cause) | Uncertain(key, cause):
            return (type(outcome).__name__, key, type(cause))
        case NonTransactional(key, wrote):
            return ("NonTransactional", key, wrote)


@pytest.mark.parametrize("case", _KEY_CASES.values(), ids=_KEY_CASES.keys())
async def test_key_cases_written_out_by_hand(case: _KeyCase) -> None:
    ledger = Ledger()
    target, fault = case.fault if case.fault is not None else (None, None)
    keys = [StoreKey(f"s{index}") for index in range(len(case.kinds))]
    stores = [
        kind.store(key, ledger, fault if index == target else None)
        for index, (kind, key) in enumerate(zip(case.kinds, keys, strict=True))
    ]
    coordinator, scope = OrderedCommits({s.key: Factory(s) for s in stores}), Scope()
    await _enlist_all(coordinator, scope, *keys)
    for order, index in enumerate(case.writes):
        stores[index].write(f"row-{keys[index]}")
        scope.participants[keys[index]].record_write(order)

    outcome = await coordinator.finish(scope, None)

    assert [_shape(store) for store in outcome.stores] == case.outcome
    assert {key: ledger.persisted[key] for key in keys} == case.persisted


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
        coordinator, scope = _coordinator(NonTransactionalStore(S0, Ledger())), Scope()
        await coordinator.enlist(scope, S0)

        outcome = await coordinator.finish(scope, None)

        assert outcome.stores == (NonTransactional(S0, wrote=False),)


class TestPoisoned:
    async def test_a_poisoned_participant_rolls_every_store_back_on_a_clean_finish(
        self,
    ) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(PhasedStore(S0, ledger), PhasedStore(S1, ledger)), Scope()
        await _enlist_all(coordinator, scope, S0, S1)
        cause = Boom("flag left on")
        scope.participants[S1].poison(cause)

        outcome = await coordinator.finish(scope, None)

        assert outcome.stores == (RolledBack(S0, cause), RolledBack(S1, cause))
        assert _calls(ledger, "commit") == []

    async def test_the_first_poison_is_the_cause(self) -> None:
        participant = Participant(S0, PhasedStore(S0, Ledger()))
        first = Boom("first")

        participant.poison(first)
        participant.poison(Boom("second"))

        assert participant.poisoned is first


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
        store_type: type[AnyStore],
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
        assert _ends(ledger) == [S2, S1, S0]
        assert event in caplog.text


class TestHungRollback:
    """A rollback that never returns is bounded: it fails, and close still runs."""

    async def test_the_store_is_still_closed(self) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(_hung_rollback(ledger), rollback_timeout=0.01), Scope()
        await coordinator.enlist(scope, S0)

        await _finish_within_a_second(coordinator, scope, Boom("business"), ledger)

        assert ledger.calls_of(S0) == ["begin", "rollback", "close"]

    async def test_the_store_is_reported_rolled_back(self) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(_hung_rollback(ledger), rollback_timeout=0.01), Scope()
        await coordinator.enlist(scope, S0)
        error = Boom("business")

        outcome = await _finish_within_a_second(coordinator, scope, error, ledger)

        assert outcome.stores == (RolledBack(S0, error),)

    async def test_the_timeout_is_logged_as_a_failed_rollback(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ledger = Ledger()
        coordinator, scope = _coordinator(_hung_rollback(ledger), rollback_timeout=0.01), Scope()
        await coordinator.enlist(scope, S0)

        with caplog.at_level(logging.ERROR):
            await _finish_within_a_second(coordinator, scope, Boom("business"), ledger)

        assert "UoWRollbackFailed" in caplog.text

    async def test_the_owning_task_can_still_be_cancelled(self) -> None:
        ledger = Ledger()

        async def _invocation() -> None:
            coordinator, scope = (
                _coordinator(_hung_rollback(ledger), rollback_timeout=0.01),
                Scope(),
            )
            await coordinator.enlist(scope, S0)
            await coordinator.finish(scope, Boom("business"))

        task = asyncio.create_task(_invocation())
        await ledger.blocked.wait()
        task.cancel()
        done, _ = await asyncio.wait([task], timeout=1.0)
        ledger.gate.set()  # Unbounded, the rollback would still be waiting here.
        await asyncio.gather(task, return_exceptions=True)

        assert done == {task}

    async def test_every_rollback_of_the_invocation_shares_one_deadline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        budgets: list[float] = []

        async def _recording(step: Coroutine[Any, Any, None], *, timeout: float) -> None:
            budgets.append(timeout)
            await run_to_completion(step, timeout=timeout)

        monkeypatch.setattr(coordinator_module, "run_to_completion", _recording)
        ledger = Ledger()
        hung = [PhasedStore(key, ledger, Fault("rollback", Effect.CANCEL)) for key in (S0, S1)]
        coordinator, scope = _coordinator(*hung, rollback_timeout=0.1), Scope()
        await _enlist_all(coordinator, scope, S0, S1)
        error = Boom("business")

        outcome = await _finish_within_a_second(coordinator, scope, error, ledger)

        assert budgets == [pytest.approx(0.1), pytest.approx(0.0, abs=0.05)]
        assert outcome.stores == (RolledBack(S0, error), RolledBack(S1, error))
        assert ledger.calls_of(S0)[-1] == "close"


async def _finish_within_a_second(
    coordinator: OrderedCommits, scope: Scope, error: BaseException, ledger: Ledger
) -> InvocationOutcome:
    """Finish, failing rather than hanging the suite if the rollback is unbounded.

    ``finish`` waits through cancellation, so the bound is a separate wait:
    on expiry the gate opens to let the rollback end, then the test fails.
    """
    finishing = asyncio.ensure_future(coordinator.finish(scope, error))
    done, _ = await asyncio.wait([finishing], timeout=1.0)
    if not done:
        ledger.gate.set()
        await asyncio.gather(finishing, return_exceptions=True)
        pytest.fail("the rollback was not bounded: finish waited for it past one second")
    return finishing.result()


def _hung_rollback(ledger: Ledger) -> PhasedStore:
    """A store whose rollback waits at a gate the test never opens in time."""
    return PhasedStore(S0, ledger, Fault("rollback", Effect.CANCEL))


@dataclass
class _Seen:
    """State observed in the invocation's own task once ``finish`` has returned or raised."""

    closed: bool = False
    outcome: InvocationOutcome | None = None


async def _cancel_when_blocked(ledger: Ledger, cancel: Callable[[], object]) -> None:
    """Cancel once a store blocks, keep cancelling for a few turns, then open the gate."""
    await ledger.blocked.wait()
    for _ in range(3):
        cancel()
        await asyncio.sleep(0)
    ledger.gate.set()


class TestInterruptions:
    async def test_a_cancelled_phased_rollback_still_runs_to_completion(self) -> None:
        ledger = Ledger()

        async def _invocation() -> None:
            faulty = PhasedStore(S1, ledger, Fault("rollback", Effect.CANCEL))
            coordinator, scope = _coordinator(PhasedStore(S0, ledger), faulty), Scope()
            await _enlist_all(coordinator, scope, S0, S1)
            await coordinator.finish(scope, Boom("business"))

        task = asyncio.create_task(_invocation())
        await _cancel_when_blocked(ledger, task.cancel)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert ledger.calls_of(S1)[-3:] == ["rollback", "unblocked", "close"]

    @pytest.mark.parametrize(
        ("store_type", "phase"),
        [(PhasedStore, "rollback"), (ThirdPartyStore, "rollback"), (PhasedStore, "close")],
        ids=["phased-rollback", "third-party-rollback", "phased-close"],
    )
    async def test_a_cancelled_teardown_ends_every_store_then_raises(
        self, store_type: type[AnyStore], phase: str
    ) -> None:
        ledger, seen = Ledger(), _Seen()

        async def _invocation() -> None:
            faulty = store_type(S1, ledger, Fault(phase, Effect.CANCEL))
            coordinator = _coordinator(PhasedStore(S0, ledger), faulty, PhasedStore(S2, ledger))
            scope = Scope()
            await _enlist_all(coordinator, scope, S0, S1, S2)
            try:
                await coordinator.finish(scope, Boom("business"))
            finally:
                seen.closed = scope.closed

        task = asyncio.create_task(_invocation())
        await _cancel_when_blocked(ledger, task.cancel)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert _ends(ledger) == [S2, S1, S0]
        assert _calls(ledger, "rollback") == [S2, S1, S0]
        assert seen.closed is True

    async def test_a_cancellation_while_closing_stops_a_third_party_store_from_committing(
        self,
    ) -> None:
        ledger, seen = Ledger(), _Seen()

        async def _invocation() -> None:
            third = ThirdPartyStore(S0, ledger)
            phased = PhasedStore(S1, ledger, Fault("close", Effect.CANCEL))
            coordinator, scope = _coordinator(third, phased), Scope()
            await _enlist_all(coordinator, scope, S0, S1)
            third.write("row-s0")
            phased.write("row-s1")
            try:
                await coordinator.finish(scope, None)
            finally:
                seen.outcome = scope.outcome

        task = asyncio.create_task(_invocation())
        await _cancel_when_blocked(ledger, task.cancel)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert seen.outcome is not None
        assert [_shape(store) for store in seen.outcome.stores] == [
            ("Committed", S1, None),
            ("RolledBack", S0, asyncio.CancelledError),
        ]
        assert ledger.terminal_calls() == [(S1, "close"), (S0, "exit-rollback")]
        assert dict(ledger.persisted) == {S1: ["row-s1"]}

    async def test_an_interruption_that_is_not_an_exception_ends_every_store_then_propagates(
        self,
    ) -> None:
        ledger = Ledger()
        coordinator = _coordinator(
            PhasedStore(S0, ledger), _InterruptedCommit(S1, ledger), ThirdPartyStore(S2, ledger)
        )
        scope = Scope()
        await _enlist_all(coordinator, scope, S0, S1, S2)

        with pytest.raises(_Interrupt):
            await coordinator.finish(scope, None)

        assert scope.outcome is not None
        assert [_shape(store) for store in scope.outcome.stores] == [
            ("Committed", S0, None),
            ("Uncertain", S1, _Interrupt),
            ("RolledBack", S2, _Interrupt),
        ]
        assert _ends(ledger) == [S2, S1, S0]

    async def test_level_cancellation_still_rolls_back_and_closes_every_store_once(self) -> None:
        ledger, seen = Ledger(), _Seen()
        coordinator = _coordinator(
            PhasedStore(S0, ledger),
            PhasedStore(S1, ledger, Fault("rollback", Effect.CANCEL)),
            ThirdPartyStore(S2, ledger),
        )
        scope, cancel_scope = Scope(), anyio.CancelScope()
        releaser = asyncio.create_task(_cancel_when_blocked(ledger, cancel_scope.cancel))

        with cancel_scope:
            await _enlist_all(coordinator, scope, S0, S1, S2)
            try:
                await coordinator.finish(scope, Boom("business"))
            finally:
                seen.closed = scope.closed
        await releaser

        assert cancel_scope.cancelled_caught is True
        assert _calls(ledger, "unblocked") == [S1]
        assert _calls(ledger, "rollback") == [S2, S1, S0]
        assert _ends(ledger) == [S2, S1, S0]
        assert seen.closed is True


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
