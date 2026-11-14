"""The unit of work each execution runs in, and the post-commit actions it settles.

``loom.core.engine.boundaries``: a top-level writer opens and commits an
invocation scope, a nested one joins it, one with no coordinator runs with no
unit of work, a read-only one enlists no store.
Every scenario runs through ``RuntimeExecutor.execute`` over recording phased
stores, so what is asserted is what a use case gets: atomicity, the nested
rules, ownership, the error each outcome raises, cancellation at every point
and no leak across invocations (ADR 0008, T045a).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

import pytest

from loom.core.authz.elevation import elevated_scopes
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import active_channel
from loom.core.errors import (
    PostCommitError,
    ScopeOwnershipError,
    StoresNotCommitted,
    UndeclaredStoreWrite,
)
from loom.core.job.context import add_pending_dispatch
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import RepoFor
from loom.core.uow.coordinator import (
    Committed,
    InvocationOutcome,
    OrderedCommits,
    RolledBack,
    Uncertain,
)
from loom.core.uow.scope import InvocationScope, current_scope
from loom.core.use_case import Joined, UseCase
from loom.core.use_case.errors import JoinedAutocommitStore

from ...authz._elevation_doubles import FakeSink
from ...uow._coordinator_fakes import (
    Boom,
    Effect,
    Factory,
    Fault,
    Ledger,
    NonTransactionalStore,
    PhasedStore,
)
from .._lifecycle_doubles import (
    BusinessError,
    Log,
    Metrics,
    Run,
    StubUnitOfWorkFactory,
    context_is_clean,
)
from .conftest import MakeExecutor
from .doubles import (
    AUDIT,
    AcmeAudited,
    AcmeAuditEntry,
    AcmeElevating,
    AcmeNonTransactionalStores,
    AcmeSeesElevation,
    AcmeStores,
    done,
    scope_now,
    second_store,
    write,
    write_joined,
)


@pytest.fixture
def executor(transactional_executor: RuntimeExecutor) -> RuntimeExecutor:
    """In this module the executor opens an invocation scope over the phased stores."""
    return transactional_executor


class TestAtomicity:
    async def test_a_clean_execution_persists_its_write(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def body() -> None:
            write(stores, "row")

        await executor.execute(Run(body))

        assert ledger.persisted[DEFAULT_STORE] == ["row"]

    async def test_a_failure_after_writing_rolls_back_closes_and_persists_nothing(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def body() -> None:
            write(stores, "row")
            raise BusinessError

        use_case = Run(body)

        with pytest.raises(BusinessError):
            await executor.execute(use_case)

        assert (ledger.calls_of(DEFAULT_STORE), ledger.persisted[DEFAULT_STORE]) == (
            ["begin", "rollback", "close"],
            [],
        )

    async def test_a_failure_after_a_nested_write_persists_nothing(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def nested() -> None:
            write(stores, "nested-row")

        async def outer() -> None:
            await executor.execute(Run(nested))
            raise BusinessError

        use_case = Run(outer)

        with pytest.raises(BusinessError):
            await executor.execute(use_case)

        assert ledger.persisted[DEFAULT_STORE] == []

    async def test_a_nested_execution_pushes_its_frame_on_the_same_scope(
        self, executor: RuntimeExecutor
    ) -> None:
        seen: list[tuple[InvocationScope, int]] = []

        async def nested() -> None:
            seen.append((scope_now(), len(scope_now().frames)))

        async def outer() -> None:
            seen.append((scope_now(), len(scope_now().frames)))
            await executor.execute(Run(nested))

        await executor.execute(Run(outer))

        assert [(scope is seen[0][0], depth) for scope, depth in seen] == [(True, 1), (True, 2)]

    async def test_a_read_only_execution_opens_a_read_only_scope_it_closes_unsettled(
        self,
        stores: AcmeStores,
        executor: RuntimeExecutor,
    ) -> None:
        """It enlists no store, so ending it settles and logs no outcome."""
        seen: list[tuple[InvocationScope, tuple[StoreKey, ...]]] = []

        async def body() -> None:
            seen.append((scope_now(), scope_now().frames[-1].stores))

        await executor.execute(Run(body), read_only=True)

        [(scope, frame)] = seen
        assert (scope.read_only, frame, scope.participants, stores.created) == (True, (), {}, [])
        assert (scope.closed, scope.outcome) == (True, None)

    async def test_a_read_only_nested_execution_pushes_a_frame_writing_to_no_store(
        self, executor: RuntimeExecutor
    ) -> None:
        depths: list[tuple[int, tuple[object, ...]]] = []

        async def nested() -> None:
            frames = scope_now().frames
            depths.append((len(frames), frames[-1].stores))

        async def outer() -> None:
            await executor.execute(Run(nested), read_only=True)

        await executor.execute(Run(outer))

        assert depths == [(2, ())]

    async def test_a_writer_under_a_read_only_nested_execution_is_refused(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def read() -> None:
            await executor.execute(Run(done()))

        async def outer() -> None:
            write(stores, "row")
            await executor.execute(Run(read), read_only=True)

        use_case = Run(outer)

        with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
            await executor.execute(use_case)

        assert ledger.persisted[DEFAULT_STORE] == []


class TestOwnership:
    async def test_a_gather_child_running_a_nested_execution_is_refused(
        self, executor: RuntimeExecutor
    ) -> None:

        async def outer() -> None:
            await asyncio.gather(executor.execute(Run(done())))

        use_case = Run(outer)

        with pytest.raises(ScopeOwnershipError, match="task other than the one that owns it"):
            await executor.execute(use_case)

    async def test_a_refused_gather_child_rolls_the_invocation_back(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def outer() -> None:
            write(stores, "row")
            await asyncio.gather(executor.execute(Run(done())))

        use_case = Run(outer)

        with pytest.raises(ScopeOwnershipError):
            await executor.execute(use_case)

        assert ledger.persisted[DEFAULT_STORE] == []

    async def test_a_gather_child_running_a_read_only_execution_is_not_refused(
        self, executor: RuntimeExecutor
    ) -> None:
        """A read-only execution joins no scope, so no ownership check refuses it."""

        async def outer() -> Any:
            return await asyncio.gather(executor.execute(Run(done("read")), read_only=True))

        assert await executor.execute(Run(outer)) == ["read"]

    async def test_a_gather_child_running_a_read_only_execution_owns_a_scope_of_its_own(
        self,
        executor: RuntimeExecutor,
    ) -> None:
        """Its repositories read per call instead of meeting a scope they may not touch."""
        seen: list[tuple[bool, int]] = []

        async def read() -> None:
            seen.append((scope_now().owned_here(), len(scope_now().participants)))

        async def outer() -> None:
            await asyncio.gather(executor.execute(Run(read), read_only=True))

        await executor.execute(Run(outer))

        assert seen == [(True, 0)]

    @pytest.mark.parametrize("coordinated", [True, False], ids=["coordinator", "none"])
    async def test_a_gather_child_of_a_read_only_execution_may_not_run_a_writer(
        self, stores: AcmeStores, executor: RuntimeExecutor, coordinated: bool
    ) -> None:
        executor = executor if coordinated else RuntimeExecutor(UseCaseCompiler())

        async def outer() -> None:
            await asyncio.gather(executor.execute(Run(done())))

        use_case = Run(outer)

        with pytest.raises(UndeclaredStoreWrite, match="the enclosing execution is read-only"):
            await executor.execute(use_case, read_only=True)

        assert stores.created == []

    async def test_a_read_only_execution_in_the_owner_task_still_sees_the_scope(
        self, executor: RuntimeExecutor
    ) -> None:
        """Its reads join the invocation's sessions and see its uncommitted writes."""
        seen: list[bool] = []

        async def read() -> None:
            seen.append(current_scope() is not None)

        async def outer() -> None:
            await executor.execute(Run(read), read_only=True)

        await executor.execute(Run(outer))

        assert seen == [True]

    async def test_a_gather_child_touching_the_participant_is_refused(
        self, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def touch() -> None:
            scope_now().participant_for(DEFAULT_STORE, stores.last.owner, write=False)

        async def outer() -> None:
            await asyncio.gather(touch())

        use_case = Run(outer)

        with pytest.raises(ScopeOwnershipError):
            await executor.execute(use_case)

    async def test_a_thread_touching_the_participant_is_refused(
        self, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:

        async def outer() -> None:
            scope = scope_now()
            await asyncio.to_thread(
                scope.participant_for, DEFAULT_STORE, stores.last.owner, write=False
            )

        use_case = Run(outer)

        with pytest.raises(ScopeOwnershipError, match="thread"):
            await executor.execute(use_case)

    async def test_a_task_that_outlives_its_use_case_cannot_join_its_closed_scope(
        self, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:
        go = asyncio.Event()
        survivors: list[asyncio.Task[Any]] = []

        async def late() -> Any:
            await go.wait()
            return scope_now().participant_for(DEFAULT_STORE, stores.last.owner, write=False)

        async def outer() -> None:
            survivors.append(asyncio.create_task(late()))

        await executor.execute(Run(outer))
        go.set()

        with pytest.raises(ScopeOwnershipError, match="already closed"):
            await survivors[0]


class TestOneStoreRaisesAsToday:
    async def test_the_business_error_propagates_as_the_same_object(
        self, executor: RuntimeExecutor
    ) -> None:
        error = BusinessError()

        async def body() -> None:
            raise error

        use_case = Run(body)

        with pytest.raises(BusinessError) as raised:
            await executor.execute(use_case)

        assert raised.value is error

    async def test_a_commit_failure_propagates_as_the_driver_error(
        self, make_executor: MakeExecutor
    ) -> None:
        stores = AcmeStores(Ledger(), Fault("commit", Effect.RAISE))

        executor = make_executor(coordinator=OrderedCommits({DEFAULT_STORE: stores}))
        use_case = Run(done())

        with pytest.raises(Boom, match="commit failed on default"):
            await executor.execute(use_case)

    async def test_a_lost_commit_acknowledgement_raises_stores_not_committed(
        self, make_executor: MakeExecutor
    ) -> None:
        """T101: never the driver error, which a job runner would retry."""
        stores = AcmeStores(Ledger(), Fault("commit", Effect.UNCERTAIN))

        executor = make_executor(coordinator=OrderedCommits({DEFAULT_STORE: stores}))
        use_case = Run(done())

        with pytest.raises(StoresNotCommitted) as raised:
            await executor.execute(use_case)

        assert isinstance(raised.value.__cause__, ConnectionError)

    async def test_a_failed_flush_is_reported_as_the_commit(
        self, metrics: Metrics, make_executor: MakeExecutor
    ) -> None:
        stores = AcmeStores(Ledger(), Fault("flush", Effect.RAISE))

        executor = make_executor(
            coordinator=OrderedCommits({DEFAULT_STORE: stores}), metrics=metrics
        )
        use_case = Run(done())

        with pytest.raises(Boom):
            await executor.execute(use_case)

        assert metrics.only(EventKind.EXEC_ERROR).error_kind == "commit"


class TestStoresNotCommitted:
    async def test_a_store_that_committed_beside_one_that_failed_raises_it(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:
        """Caused by the failed commit, naming both stores; the committed one keeps its write."""
        audit = PhasedStore(AUDIT, ledger, Fault("commit", Effect.RAISE))

        async def body() -> None:
            await second_store(audit)
            write(stores, "row")

        use_case = Run(body)

        with pytest.raises(StoresNotCommitted) as raised:
            await executor.execute(use_case)

        cause = _cause(raised.value)
        assert (type(cause), raised.value.outcome, ledger.persisted[DEFAULT_STORE]) == (
            Boom,
            InvocationOutcome((Committed(DEFAULT_STORE), RolledBack(AUDIT, cause))),
            ["row"],
        )

    async def test_a_business_error_after_a_non_transactional_write_raises_it(
        self, ledger: Ledger, executor: RuntimeExecutor
    ) -> None:
        audit = NonTransactionalStore(AUDIT, ledger)
        error = BusinessError()

        async def body() -> None:
            await second_store(audit)
            await write_joined(audit, "audit-row")
            raise error

        use_case = Run(body)

        with pytest.raises(StoresNotCommitted) as raised:
            await executor.execute(use_case)

        assert raised.value.__cause__ is error

    async def test_an_uncertain_store_raises_it(
        self, ledger: Ledger, make_executor: MakeExecutor
    ) -> None:
        stores = AcmeStores(ledger, Fault("commit", Effect.UNCERTAIN))
        audit = PhasedStore(AUDIT, ledger)

        async def body() -> None:
            await second_store(audit)

        executor = make_executor(coordinator=OrderedCommits({DEFAULT_STORE: stores}))
        use_case = Run(body)

        with pytest.raises(StoresNotCommitted) as raised:
            await executor.execute(use_case)

        assert raised.value.outcome is not None
        assert any(isinstance(store, Uncertain) for store in raised.value.outcome.stores)

    async def test_a_business_error_with_two_stores_rolled_back_is_re_raised(
        self, ledger: Ledger, executor: RuntimeExecutor
    ) -> None:
        audit = PhasedStore(AUDIT, ledger)
        error = BusinessError()

        async def body() -> None:
            await second_store(audit)
            raise error

        use_case = Run(body)

        with pytest.raises(BusinessError) as raised:
            await executor.execute(use_case)

        assert raised.value is error

    async def test_a_clean_non_transactional_write_beside_a_commit_succeeds(
        self, ledger: Ledger, executor: RuntimeExecutor
    ) -> None:
        audit = NonTransactionalStore(AUDIT, ledger)

        async def body() -> str:
            await second_store(audit)
            await write_joined(audit, "audit-row")
            return "ok"

        assert await executor.execute(Run(body)) == "ok"

    async def test_a_stores_not_committed_failure_discards_the_channel(
        self, ledger: Ledger, executor: RuntimeExecutor
    ) -> None:
        audit = PhasedStore(AUDIT, ledger, Fault("commit", Effect.RAISE))
        ran: list[str] = []

        async def body() -> None:
            await second_store(audit)
            channel = active_channel()
            assert channel is not None
            channel.enqueue(lambda: ran.append("action"))

        use_case = Run(body)

        with pytest.raises(StoresNotCommitted):
            await executor.execute(use_case)

        assert ran == []


class TestChannel:
    async def test_a_business_error_over_a_non_transactional_store_discards_the_channel(
        self,
        make_executor: MakeExecutor,
    ) -> None:
        """As today with a no-op unit of work: nothing committed, so nothing drains."""
        ran: list[str] = []

        async def body() -> None:
            channel = active_channel()
            assert channel is not None
            channel.enqueue(lambda: ran.append("action"))
            raise BusinessError

        executor = make_executor(
            coordinator=OrderedCommits({DEFAULT_STORE: AcmeNonTransactionalStores()})
        )
        use_case = Run(body)

        with pytest.raises(BusinessError):
            await executor.execute(use_case)

        assert ran == []

    async def test_a_clean_execution_drains_after_the_scope_is_unpublished(
        self, executor: RuntimeExecutor
    ) -> None:
        seen: list[InvocationScope | None] = []

        async def body() -> None:
            channel = active_channel()
            assert channel is not None
            channel.enqueue(lambda: seen.append(current_scope()))

        await executor.execute(Run(body))

        assert seen == [None]


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
    async def test_a_cancellation_propagates_with_a_clean_context_and_one_close(
        self, fault: Fault
    ) -> None:
        run = _Cancelled.at(fault)

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert (run.observed, run.ledger.calls_of(DEFAULT_STORE).count("close")) == ([True], 1)

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

    async def test_a_cancelled_body_rolls_back(
        self, ledger: Ledger, stores: AcmeStores, executor: RuntimeExecutor
    ) -> None:
        started = asyncio.Event()

        async def body() -> None:
            write(stores, "row")
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(executor.execute(Run(body)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert (ledger.calls_of(DEFAULT_STORE), ledger.persisted[DEFAULT_STORE]) == (
            ["begin", "rollback", "close"],
            [],
        )

    async def test_a_cancelled_close_after_the_commit_still_runs_the_priority_actions(
        self,
    ) -> None:
        """Every store committed: the post-commit channel describes a durable write."""
        ran: list[str] = []

        def enqueue() -> None:
            channel = active_channel()
            assert channel is not None
            channel.enqueue_priority(lambda: ran.append("bump"))

        run = _Cancelled.at(Fault("close", Effect.CANCEL), enqueue)

        with pytest.raises(asyncio.CancelledError):
            await run.cancel_when_blocked()

        assert ran == ["bump"]


@dataclass
class _Cancelled:
    """An execution whose store blocks at a fault until the test cancels it."""

    task: asyncio.Task[None]
    ledger: Ledger
    observed: list[bool]

    @classmethod
    def at(cls, fault: Fault, also: Callable[[], None] = lambda: None) -> _Cancelled:
        """Start the execution; its task records whether its own context ended clean.

        Args:
            fault: Where the store blocks.
            also: Run by the body after it writes, before anything fails.
        """
        ledger = Ledger()
        stores = AcmeStores(ledger, fault)
        observed: list[bool] = []

        async def body() -> None:
            write(stores, "row")
            also()
            if fault.phase == "rollback":
                raise BusinessError

        async def run() -> None:
            try:
                await RuntimeExecutor(
                    UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: stores})
                ).execute(Run(body))
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
    async def test_a_hundred_mixed_invocations_leak_nothing(self) -> None:
        """Every scope closed, every store begun closed, a clean context, only clean ones kept."""
        scopes, ledger, clean = await _hundred_invocations()

        assert (
            all(scope.closed for scope in scopes),
            _count(ledger, "begin"),
            _count(ledger, "close"),
            clean,
            len(ledger.persisted[DEFAULT_STORE]),
        ) == (True, 100, 100, [True] * 100, 34)


def _count(ledger: Ledger, call: str) -> int:
    return sum(1 for _, made in ledger.calls if made == call)


async def _hundred_invocations() -> tuple[list[InvocationScope], Ledger, list[bool]]:
    """Run 100 invocations: clean, failing and cancelled in turn, each in its own task."""
    ledger = Ledger()
    stores = AcmeStores(ledger)
    executor = RuntimeExecutor(
        UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: stores})
    )
    scopes: list[InvocationScope] = []
    clean: list[bool] = []

    async def invoke(index: int) -> None:
        async def body() -> None:
            scopes.append(scope_now())
            write(stores, f"row-{index}")
            if index % 3 == 1:
                raise BusinessError
            if index % 3 == 2:
                await asyncio.Event().wait()

        try:
            await executor.execute(Run(body))
        except BusinessError:
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


class TestJoinedStoreThatCannotRollBack:
    @pytest.mark.parametrize("nested", [False, True], ids=["top-level", "nested"])
    async def test_it_is_refused_before_the_use_case_runs(
        self, ledger: Ledger, nested: bool
    ) -> None:
        ran: list[str] = []
        audit = Factory(NonTransactionalStore(AUDIT, ledger))
        coordinator = OrderedCommits({DEFAULT_STORE: AcmeStores(ledger), AUDIT: audit})
        executor = RuntimeExecutor(UseCaseCompiler(), coordinator=coordinator)

        async def body() -> None:
            ran.append("ran")

        audited = AcmeAudited(body, cast(Joined[RepoFor[AcmeAuditEntry]], None))

        async def outer() -> None:
            await executor.execute(audited)

        with pytest.raises(JoinedAutocommitStore, match="store audit, which cannot roll back"):
            await executor.execute(Run(outer) if nested else audited)

        assert ran == []


class TestWithoutAUnitOfWork:
    """No coordinator, or a plan declaring ``read_only``: no unit of work is opened."""

    async def test_an_executor_without_a_factory_opens_no_scope(self) -> None:
        seen: list[InvocationScope | None] = []

        async def body() -> None:
            seen.append(current_scope())

        await RuntimeExecutor(UseCaseCompiler()).execute(Run(body))

        assert seen == [None]

    async def test_dispatch_runs_at_the_end_without_uow_factory(self) -> None:
        """FR-003: with no factory configured the dispatches still run, once, at the end."""
        executor = RuntimeExecutor(UseCaseCompiler())
        ran: list[str] = []

        class _DispatchNoUoW(UseCase[Any, str]):
            async def execute(self) -> str:
                add_pending_dispatch(lambda: ran.append("ran"))
                ran.append("executed")
                return "ok"

        await executor.execute(_DispatchNoUoW())

        assert ran == ["executed", "ran"]

    async def test_read_only_declared_on_the_class_skips_the_unit_of_work(self) -> None:
        """``UseCase.read_only = True`` reaches the executor through the compiled plan."""

        class _ReadOnlyUseCase(UseCase[Any, str]):
            read_only = True

            async def execute(self) -> str:
                return "read"

        log = Log()
        factory = StubUnitOfWorkFactory(log)
        executor = RuntimeExecutor(
            UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: factory})
        )

        assert await executor.execute(_ReadOnlyUseCase()) == "read"
        assert factory.created == []
        assert log.entries == []

    async def test_a_failing_action_without_a_unit_of_work_reports_nothing_committed(
        self,
    ) -> None:
        """The drain tells the caller no transaction committed, so it may retry."""
        executor = RuntimeExecutor(UseCaseCompiler())

        def fail() -> None:
            raise ConnectionError("acme broker down")

        async def body() -> None:
            add_pending_dispatch(fail)

        with pytest.raises(PostCommitError) as raised:
            await executor.execute(Run(body))

        assert raised.value.committed is False


class TestElevationFrame:
    """Each execution runs in an elevation frame that wraps its flush and commit."""

    async def test_an_execution_runs_inside_a_frame_that_closes_with_it(self) -> None:
        sink = FakeSink(open=True)
        executor = RuntimeExecutor(
            UseCaseCompiler(),
            coordinator=OrderedCommits({DEFAULT_STORE: StubUnitOfWorkFactory(Log())}),
            elevation_sink=sink,
        )

        result = await executor.execute(AcmeElevating(), params={"value": "x"})  # type: ignore[arg-type]

        assert result == ["editor"]
        assert sink.set == ["editor"]
        assert elevated_scopes() == frozenset()

    async def test_without_a_sink_the_frame_still_exists_and_elevation_waits_for_the_provider(
        self,
    ) -> None:
        executor = RuntimeExecutor(UseCaseCompiler())

        result = await executor.execute(AcmeElevating(), params={"value": "x"})  # type: ignore[arg-type]

        assert result == ["editor"]
        assert elevated_scopes() == frozenset()

    @pytest.mark.parametrize(
        "phase",
        [
            pytest.param(
                "flush", id="the_elevated_scopes_are_still_visible_when_the_coordinator_flushes"
            ),
            pytest.param(
                "commit", id="the_elevated_scopes_are_still_visible_when_the_coordinator_commits"
            ),
        ],
    )
    async def test_the_elevated_scopes_are_still_visible_when_the_coordinator_finishes(
        self, phase: str
    ) -> None:
        store = AcmeSeesElevation()
        executor = RuntimeExecutor(
            UseCaseCompiler(), coordinator=OrderedCommits({DEFAULT_STORE: Factory(store)})
        )

        await executor.execute(AcmeElevating(), params={"value": "x"})  # type: ignore[arg-type]

        assert store.seen[phase] == ["editor"]
