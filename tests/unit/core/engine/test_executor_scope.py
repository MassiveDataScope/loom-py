"""The executor opens, joins and ends the invocation scope (ADR 0008, T045a).

Every scenario runs through ``RuntimeExecutor.execute`` over recording
phased stores, so what is asserted is what a use case gets: atomicity, the
nested rules, ownership, the error each outcome raises, cancellation at every
point and no leak across invocations.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import (
    Committed,
    InvocationOutcome,
    Participant,
    RolledBack,
    Uncertain,
)
from loom.core.uow.scope import (
    InvocationScope,
    ScopeOwnershipError,
    StoresNotCommitted,
    current_scope,
)
from loom.core.use_case.use_case import UseCase

from ..uow._coordinator_fakes import (
    Boom,
    Effect,
    Fault,
    Ledger,
    NonTransactionalStore,
    PhasedStore,
)
from ._lifecycle_doubles import Metrics, context_is_clean

AUDIT = StoreKey("audit")


class _BusinessError(Exception):
    pass


class _Stores:
    """A unit-of-work factory handing out a fresh phased store per invocation."""

    def __init__(self, ledger: Ledger, fault: Fault | None = None) -> None:
        self._ledger = ledger
        self._fault = fault
        self.created: list[PhasedStore] = []

    def create(self) -> UnitOfWork:
        store = PhasedStore(DEFAULT_STORE, self._ledger, self._fault)
        self.created.append(store)
        return cast(UnitOfWork, store)

    @property
    def last(self) -> PhasedStore:
        return self.created[-1]


Body = Callable[[], Awaitable[Any]]


class _Run(UseCase[Any, Any]):
    """Runs whatever body the test hands it."""

    def __init__(self, body: Body) -> None:
        self._body = body

    async def execute(self) -> Any:
        return await self._body()


def _executor(stores: _Stores, metrics: Metrics | None = None) -> RuntimeExecutor:
    return RuntimeExecutor(UseCaseCompiler(), uow_factory=stores, metrics=metrics)


def _scope() -> InvocationScope:
    scope = current_scope()
    assert scope is not None
    return scope


def _write(stores: _Stores, row: str) -> None:
    """Write ``row`` through the default store the way a repository will (PR 3)."""
    _scope().participant_for(DEFAULT_STORE, stores.last.owner, write=True)
    stores.last.write(row)


async def _second_store(store: PhasedStore) -> None:
    """Enlist ``store`` by hand: before PR 11 the executor knows only the default store."""
    scope = _scope()
    participant = Participant(store.key, cast(UnitOfWork, store))
    await participant.begin()
    scope.participants[store.key] = participant


def _done(value: Any = "ok") -> Body:
    async def body() -> Any:
        return value

    return body


class TestAtomicity:
    async def test_a_clean_execution_persists_its_write(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)

        async def body() -> None:
            _write(stores, "row")

        await _executor(stores).execute(_Run(body))

        assert ledger.persisted[DEFAULT_STORE] == ["row"]

    async def test_a_failure_after_writing_persists_nothing(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)

        async def body() -> None:
            _write(stores, "row")
            raise _BusinessError

        with pytest.raises(_BusinessError):
            await _executor(stores).execute(_Run(body))

        assert ledger.persisted[DEFAULT_STORE] == []

    async def test_a_failure_after_writing_rolls_back_and_closes(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)

        async def body() -> None:
            _write(stores, "row")
            raise _BusinessError

        with pytest.raises(_BusinessError):
            await _executor(stores).execute(_Run(body))

        assert ledger.calls_of(DEFAULT_STORE) == ["begin", "rollback", "close"]

    async def test_a_nested_execution_opens_no_second_unit_of_work(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)

        async def outer() -> None:
            await executor.execute(_Run(_done()))

        await executor.execute(_Run(outer))

        assert len(stores.created) == 1

    async def test_a_nested_execution_does_not_commit(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        executor = _executor(stores)
        calls_after_nested: list[list[str]] = []

        async def nested() -> None:
            _write(stores, "nested-row")

        async def outer() -> None:
            await executor.execute(_Run(nested))
            calls_after_nested.append(ledger.calls_of(DEFAULT_STORE))

        await executor.execute(_Run(outer))

        assert calls_after_nested == [["begin"]]

    async def test_a_failure_after_a_nested_write_persists_nothing(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        executor = _executor(stores)

        async def nested() -> None:
            _write(stores, "nested-row")

        async def outer() -> None:
            await executor.execute(_Run(nested))
            raise _BusinessError

        with pytest.raises(_BusinessError):
            await executor.execute(_Run(outer))

        assert ledger.persisted[DEFAULT_STORE] == []

    async def test_a_nested_execution_pushes_its_frame_on_the_same_scope(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)
        seen: list[tuple[InvocationScope, int]] = []

        async def nested() -> None:
            seen.append((_scope(), len(_scope().frames)))

        async def outer() -> None:
            seen.append((_scope(), len(_scope().frames)))
            await executor.execute(_Run(nested))

        await executor.execute(_Run(outer))

        assert [(scope is seen[0][0], depth) for scope, depth in seen] == [(True, 1), (True, 2)]

    async def test_a_read_only_execution_opens_no_scope(self) -> None:
        stores = _Stores(Ledger())
        seen: list[InvocationScope | None] = []

        async def body() -> None:
            seen.append(current_scope())

        await _executor(stores).execute(_Run(body), read_only=True)

        assert (seen, stores.created) == ([None], [])

    async def test_a_read_only_nested_execution_pushes_no_frame(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)
        depths: list[int] = []

        async def nested() -> None:
            depths.append(len(_scope().frames))

        async def outer() -> None:
            await executor.execute(_Run(nested), read_only=True)

        await executor.execute(_Run(outer))

        assert depths == [1]

    async def test_an_executor_without_a_factory_opens_no_scope(self) -> None:
        seen: list[InvocationScope | None] = []

        async def body() -> None:
            seen.append(current_scope())

        await RuntimeExecutor(UseCaseCompiler()).execute(_Run(body))

        assert seen == [None]


class TestOwnership:
    async def test_a_gather_child_running_a_nested_execution_is_refused(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)

        async def outer() -> None:
            await asyncio.gather(executor.execute(_Run(_done())))

        with pytest.raises(ScopeOwnershipError, match="task other than the one that owns it"):
            await executor.execute(_Run(outer))

    async def test_a_refused_gather_child_rolls_the_invocation_back(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        executor = _executor(stores)

        async def outer() -> None:
            _write(stores, "row")
            await asyncio.gather(executor.execute(_Run(_done())))

        with pytest.raises(ScopeOwnershipError):
            await executor.execute(_Run(outer))

        assert ledger.persisted[DEFAULT_STORE] == []

    async def test_a_gather_child_running_a_read_only_execution_reads_per_call(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)

        async def outer() -> Any:
            return await asyncio.gather(executor.execute(_Run(_done("read")), read_only=True))

        assert await executor.execute(_Run(outer)) == ["read"]

    async def test_a_gather_child_touching_the_participant_is_refused(self) -> None:
        stores = _Stores(Ledger())

        async def touch() -> None:
            _scope().participant_for(DEFAULT_STORE, stores.last.owner, write=False)

        async def outer() -> None:
            await asyncio.gather(touch())

        with pytest.raises(ScopeOwnershipError):
            await _executor(stores).execute(_Run(outer))

    async def test_a_thread_touching_the_participant_is_refused(self) -> None:
        stores = _Stores(Ledger())

        async def outer() -> None:
            scope = _scope()
            await asyncio.to_thread(
                scope.participant_for, DEFAULT_STORE, stores.last.owner, write=False
            )

        with pytest.raises(ScopeOwnershipError, match="thread"):
            await _executor(stores).execute(_Run(outer))

    async def test_a_task_that_outlives_its_use_case_is_refused(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)
        go = asyncio.Event()
        survivors: list[asyncio.Task[Any]] = []

        async def late() -> Any:
            await go.wait()
            return await executor.execute(_Run(_done()))

        async def outer() -> None:
            survivors.append(asyncio.create_task(late()))

        await executor.execute(_Run(outer))
        go.set()

        with pytest.raises(ScopeOwnershipError, match="already closed"):
            await survivors[0]

    async def test_a_task_that_outlives_its_use_case_opens_no_unit_of_work(self) -> None:
        stores = _Stores(Ledger())
        executor = _executor(stores)
        go = asyncio.Event()
        survivors: list[asyncio.Task[Any]] = []

        async def late() -> Any:
            await go.wait()
            return await executor.execute(_Run(_done()))

        async def outer() -> None:
            survivors.append(asyncio.create_task(late()))

        await executor.execute(_Run(outer))
        go.set()
        await asyncio.gather(*survivors, return_exceptions=True)

        assert len(stores.created) == 1


class TestOneStoreRaisesAsToday:
    async def test_the_business_error_propagates_as_the_same_object(self) -> None:
        error = _BusinessError()

        async def body() -> None:
            raise error

        with pytest.raises(_BusinessError) as raised:
            await _executor(_Stores(Ledger())).execute(_Run(body))

        assert raised.value is error

    async def test_a_commit_failure_propagates_as_the_driver_error(self) -> None:
        stores = _Stores(Ledger(), Fault("commit", Effect.RAISE))

        with pytest.raises(Boom, match="commit failed on default"):
            await _executor(stores).execute(_Run(_done()))

    async def test_a_lost_commit_acknowledgement_propagates_its_cause(self) -> None:
        stores = _Stores(Ledger(), Fault("commit", Effect.UNCERTAIN))

        with pytest.raises(ConnectionError, match="lost commit ack"):
            await _executor(stores).execute(_Run(_done()))

    @pytest.mark.parametrize("phase", ["flush", "commit"])
    async def test_a_failure_ending_the_scope_is_reported_as_the_commit(self, phase: str) -> None:
        metrics = Metrics()
        stores = _Stores(Ledger(), Fault(phase, Effect.RAISE))

        with pytest.raises(Boom):
            await _executor(stores, metrics).execute(_Run(_done()))

        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "commit"

    async def test_a_failed_begin_is_reported_as_the_begin(self) -> None:
        metrics = Metrics()
        stores = _Stores(Ledger(), Fault("begin", Effect.RAISE))

        with pytest.raises(Boom):
            await _executor(stores, metrics).execute(_Run(_done()))

        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "begin"

    async def test_a_business_failure_is_reported_as_business(self) -> None:
        metrics = Metrics()

        async def body() -> None:
            raise _BusinessError

        with pytest.raises(_BusinessError):
            await _executor(_Stores(Ledger()), metrics).execute(_Run(body))

        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "business"

    async def test_a_clean_execution_times_the_commit(self) -> None:
        metrics = Metrics()

        await _executor(_Stores(Ledger()), metrics).execute(_Run(_done()))

        assert metrics.only(EventKind.EXEC_DONE).commit_ms is not None


class TestStoresNotCommitted:
    async def test_a_store_that_committed_beside_one_that_failed_raises_it(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = PhasedStore(AUDIT, ledger, Fault("commit", Effect.RAISE))

        async def body() -> None:
            await _second_store(audit)
            _write(stores, "row")

        with pytest.raises(StoresNotCommitted) as raised:
            await _executor(stores).execute(_Run(body))

        assert raised.value.outcome == InvocationOutcome(
            (Committed(DEFAULT_STORE), RolledBack(AUDIT, _cause(raised.value)))
        )

    async def test_it_is_caused_by_the_failed_commit(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = PhasedStore(AUDIT, ledger, Fault("commit", Effect.RAISE))

        async def body() -> None:
            await _second_store(audit)
            _write(stores, "row")

        with pytest.raises(StoresNotCommitted) as raised:
            await _executor(stores).execute(_Run(body))

        assert isinstance(raised.value.__cause__, Boom)

    async def test_the_committed_store_keeps_its_write(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = PhasedStore(AUDIT, ledger, Fault("commit", Effect.RAISE))

        async def body() -> None:
            await _second_store(audit)
            _write(stores, "row")

        with pytest.raises(StoresNotCommitted):
            await _executor(stores).execute(_Run(body))

        assert ledger.persisted[DEFAULT_STORE] == ["row"]

    async def test_a_business_error_after_a_non_transactional_write_raises_it(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = NonTransactionalStore(AUDIT, ledger)
        error = _BusinessError()

        async def body() -> None:
            await _second_store(audit)
            _scope().participant_for(AUDIT, audit.owner, write=True)
            audit.write("audit-row")
            raise error

        with pytest.raises(StoresNotCommitted) as raised:
            await _executor(stores).execute(_Run(body))

        assert raised.value.__cause__ is error

    async def test_an_uncertain_store_raises_it(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger, Fault("commit", Effect.UNCERTAIN))
        audit = PhasedStore(AUDIT, ledger)

        async def body() -> None:
            await _second_store(audit)

        with pytest.raises(StoresNotCommitted) as raised:
            await _executor(stores).execute(_Run(body))

        assert raised.value.outcome is not None
        assert any(isinstance(store, Uncertain) for store in raised.value.outcome.stores)

    async def test_a_business_error_with_two_stores_rolled_back_is_re_raised(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = PhasedStore(AUDIT, ledger)
        error = _BusinessError()

        async def body() -> None:
            await _second_store(audit)
            raise error

        with pytest.raises(_BusinessError) as raised:
            await _executor(stores).execute(_Run(body))

        assert raised.value is error

    async def test_a_clean_non_transactional_write_beside_a_commit_succeeds(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = NonTransactionalStore(AUDIT, ledger)

        async def body() -> str:
            await _second_store(audit)
            _scope().participant_for(AUDIT, audit.owner, write=True)
            return "ok"

        assert await _executor(stores).execute(_Run(body)) == "ok"

    async def test_a_stores_not_committed_failure_discards_the_channel(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        audit = PhasedStore(AUDIT, ledger, Fault("commit", Effect.RAISE))
        ran: list[str] = []

        async def body() -> None:
            await _second_store(audit)
            channel = active_channel()
            assert channel is not None
            channel.enqueue(lambda: ran.append("action"))

        with pytest.raises(StoresNotCommitted):
            await _executor(stores).execute(_Run(body))

        assert ran == []


class TestChannel:
    async def test_a_business_error_over_a_non_transactional_store_discards_the_channel(
        self,
    ) -> None:
        """As today with a no-op unit of work: nothing committed, so nothing drains."""
        ran: list[str] = []

        async def body() -> None:
            channel = active_channel()
            assert channel is not None
            channel.enqueue(lambda: ran.append("action"))
            raise _BusinessError

        with pytest.raises(_BusinessError):
            await _executor(_NonTransactionalStores()).execute(_Run(body))

        assert ran == []

    async def test_a_clean_execution_drains_after_the_scope_is_unpublished(self) -> None:
        seen: list[InvocationScope | None] = []

        async def body() -> None:
            channel = active_channel()
            assert channel is not None
            channel.enqueue(lambda: seen.append(current_scope()))

        await _executor(_Stores(Ledger())).execute(_Run(body))

        assert seen == [None]


class _NonTransactionalStores:
    def create(self) -> UnitOfWork:
        return cast(UnitOfWork, NonTransactionalStore(DEFAULT_STORE, Ledger()))


def _cause(error: BaseException) -> BaseException:
    assert error.__cause__ is not None
    return error.__cause__


def _fault_id(fault: Fault) -> str:
    return f"{fault.phase}-{fault.effect.value}"


_CANCEL_POINTS = [
    Fault("begin", Effect.CANCEL),
    Fault("flush", Effect.CANCEL),
    Fault("commit", Effect.CANCEL),
    Fault("commit", Effect.CANCEL_LATE),
    Fault("close", Effect.CANCEL),
    Fault("rollback", Effect.CANCEL),
]
"""Every point a cancellation can hit; a rollback fault makes the body fail first."""


class TestCancellation:
    @pytest.mark.parametrize("fault", _CANCEL_POINTS, ids=_fault_id)
    async def test_a_cancellation_propagates(self, fault: Fault) -> None:
        run = _Cancelled.at(fault)

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

    @pytest.mark.parametrize("fault", _CANCEL_POINTS, ids=_fault_id)
    async def test_the_task_ends_with_a_clean_context(self, fault: Fault) -> None:
        run = _Cancelled.at(fault)

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert run.observed == [True]

    @pytest.mark.parametrize("fault", _CANCEL_POINTS, ids=_fault_id)
    async def test_the_store_is_closed_once(self, fault: Fault) -> None:
        run = _Cancelled.at(fault)

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert run.ledger.calls_of(DEFAULT_STORE).count("close") == 1

    async def test_a_cancelled_rollback_still_runs_to_completion(self) -> None:
        run = _Cancelled.at(Fault("rollback", Effect.CANCEL))

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert run.ledger.calls_of(DEFAULT_STORE) == ["begin", "rollback", "unblocked", "close"]

    @pytest.mark.parametrize("fault", [*_CANCEL_POINTS[:3], _CANCEL_POINTS[-1]], ids=_fault_id)
    async def test_nothing_is_persisted_before_the_commit_lands(self, fault: Fault) -> None:
        run = _Cancelled.at(fault)

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert run.ledger.persisted[DEFAULT_STORE] == []

    async def test_a_cancelled_begin_closes_the_store_without_a_rollback(self) -> None:
        run = _Cancelled.at(Fault("begin", Effect.CANCEL))

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert run.ledger.calls_of(DEFAULT_STORE) == ["begin", "close"]

    async def test_a_cancelled_body_rolls_back(self) -> None:
        ledger = Ledger()
        stores = _Stores(ledger)
        started = asyncio.Event()

        async def body() -> None:
            _write(stores, "row")
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(_executor(stores).execute(_Run(body)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert (ledger.calls_of(DEFAULT_STORE), ledger.persisted[DEFAULT_STORE]) == (
            ["begin", "rollback", "close"],
            [],
        )


@dataclass
class _Cancelled:
    """An execution whose store blocks at a fault until the test cancels it."""

    task: asyncio.Task[None]
    ledger: Ledger
    observed: list[bool]

    @classmethod
    def at(cls, fault: Fault) -> _Cancelled:
        """Start the execution; its task records whether its own context ended clean."""
        ledger = Ledger()
        stores = _Stores(ledger, fault)
        observed: list[bool] = []

        async def body() -> None:
            _write(stores, "row")
            if fault.phase == "rollback":
                raise _BusinessError

        async def run() -> None:
            try:
                await _executor(stores).execute(_Run(body))
            finally:
                observed.append(context_is_clean())

        return cls(asyncio.create_task(run()), ledger, observed)

    async def cancel_when_blocked(self) -> None:
        """Cancel once the store blocks, then let the blocked step finish.

        The gate opens before the task is awaited: a rollback runs to
        completion however often the task is cancelled, so the task waits
        for it.
        """
        await self.ledger.blocked.wait()
        self.task.cancel()
        await asyncio.sleep(0)
        self.ledger.gate.set()
        await self.task


class TestNoLeaks:
    async def test_a_hundred_mixed_invocations_leave_every_scope_closed(self) -> None:
        scopes, _, _ = await _hundred_invocations()

        assert all(scope.closed for scope in scopes)

    async def test_a_hundred_mixed_invocations_close_every_store_they_began(self) -> None:
        _, ledger, _ = await _hundred_invocations()

        assert _count(ledger, "close") == _count(ledger, "begin") == 100

    async def test_a_hundred_mixed_invocations_leave_the_context_clean(self) -> None:
        _, _, clean = await _hundred_invocations()

        assert clean == [True] * 100

    async def test_a_hundred_mixed_invocations_persist_only_the_clean_ones(self) -> None:
        _, ledger, _ = await _hundred_invocations()

        assert len(ledger.persisted[DEFAULT_STORE]) == 34


def _count(ledger: Ledger, call: str) -> int:
    return sum(1 for _, made in ledger.calls if made == call)


async def _hundred_invocations() -> tuple[list[InvocationScope], Ledger, list[bool]]:
    """Run 100 invocations: clean, failing and cancelled in turn, each in its own task."""
    ledger = Ledger()
    stores = _Stores(ledger)
    executor = _executor(stores)
    scopes: list[InvocationScope] = []
    clean: list[bool] = []

    async def invoke(index: int) -> None:
        async def body() -> None:
            scopes.append(_scope())
            _write(stores, f"row-{index}")
            if index % 3 == 1:
                raise _BusinessError
            if index % 3 == 2:
                await asyncio.Event().wait()

        try:
            await executor.execute(_Run(body))
        except _BusinessError:
            pass
        finally:
            clean.append(context_is_clean())

    for index in range(100):
        task = asyncio.create_task(invoke(index))
        await asyncio.sleep(0)
        if index % 3 == 2:
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    return scopes, ledger, clean
