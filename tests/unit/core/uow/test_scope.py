"""The invocation scope: frames, the ownership checkpoint, publishing and the invocation error.

Ownership is the security boundary of ADR 0008: a participant is touched
only from the task that owns the scope, never from a ``gather`` child, a
thread or once the scope is closed, while a read of a store that is not
enlisted, or whose unit of work holds no session, works from anywhere.
"""

from __future__ import annotations

import asyncio
import pickle
from contextlib import nullcontext
from typing import Any

import pytest
from celery.backends.base import BaseBackend
from pymongo import AsyncMongoClient

from loom.core.errors.codes import ErrorCode
from loom.core.errors.errors import ForeignSessionError, SystemError
from loom.core.persistence.stores import StoreKey
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import (
    Committed,
    InvocationOutcome,
    NonTransactional,
    OrderedCommits,
    RolledBack,
    Uncertain,
)
from loom.core.uow.scope import (
    Frame,
    InvocationScope,
    NestedElevation,
    NestedIdentityMismatch,
    ScopeOwnershipError,
    StoresNotCommitted,
    UnitOfWorkInsideScope,
    current_handle,
    current_participant,
    current_scope,
    invocation,
    invocation_error,
    refuse_inside_open_scope,
    single_store,
    unscoped,
)

from ._coordinator_fakes import (
    AutocommitStore,
    Boom,
    Effect,
    Factory,
    Fault,
    Ledger,
    PhasedStore,
    ThirdPartyStore,
)

S0, S1, S2 = StoreKey("s0"), StoreKey("s1"), StoreKey("s2")


def _coordinator(*stores: PhasedStore | ThirdPartyStore | AutocommitStore) -> OrderedCommits:
    return OrderedCommits({store.key: Factory(store) for store in stores})


def _stores(ledger: Ledger, *keys: StoreKey) -> dict[StoreKey, PhasedStore]:
    return {key: PhasedStore(key, ledger) for key in keys}


def _scope_for(*stores: PhasedStore) -> InvocationScope:
    """A scope owned by the current task over ``stores``, none enlisted yet."""
    return InvocationScope(asyncio.current_task(), _coordinator(*stores))


async def _enlisted(*keys: StoreKey) -> tuple[InvocationScope, dict[StoreKey, PhasedStore]]:
    """A scope owned by the current task with ``keys`` enlisted."""
    stores = _stores(Ledger(), *keys)
    scope = InvocationScope(asyncio.current_task(), _coordinator(*stores.values()))
    for key in keys:
        await scope.coordinator.enlist(scope, key)
    return scope, stores


async def _in_child_task(scope: InvocationScope, key: StoreKey, *, write: bool) -> Any:
    owner = object()
    return await asyncio.gather(asyncio.create_task(_lookup(scope, key, owner, write)))


async def _lookup(scope: InvocationScope, key: StoreKey, owner: object, write: bool) -> Any:
    return scope.participant_for(key, owner, write=write)


class _TargetedOwner:
    def __init__(self, target: object) -> None:
        self._target = target

    @property
    def target(self) -> object:
        return self._target


class _TargetAttribute:
    def __init__(self) -> None:
        self.target = "client(password='secret')"


class _ReprLeaks:
    def __repr__(self) -> str:
        return "client(password='secret')"


class TestFrame:
    def test_stores_are_the_primary_then_the_joined_by_name(self) -> None:
        assert Frame(S1, frozenset({S2, S0})).stores == (S1, S0, S2)

    def test_a_primary_listed_as_joined_is_not_repeated(self) -> None:
        assert Frame(S0, frozenset({S0, S1})).stores == (S0, S1)

    def test_a_frame_joins_nothing_by_default(self) -> None:
        assert Frame(S0).stores == (S0,)


class TestParticipantForFromTheOwner:
    async def test_a_read_of_an_enlisted_store_returns_its_participant(self) -> None:
        scope, stores = await _enlisted(S0)

        participant = scope.participant_for(S0, stores[S0].owner, write=False)

        assert participant is scope.participants[S0]

    async def test_a_read_marks_no_write(self) -> None:
        scope, stores = await _enlisted(S0)

        scope.participant_for(S0, stores[S0].owner, write=False)

        assert scope.participants[S0].wrote is False

    async def test_a_store_that_is_not_enlisted_returns_none_for_a_read(self) -> None:
        scope, _ = await _enlisted(S0)

        assert scope.participant_for(S1, object(), write=False) is None

    async def test_a_store_that_is_not_enlisted_returns_none_for_a_write(self) -> None:
        scope, _ = await _enlisted(S0)

        assert scope.participant_for(S1, object(), write=True) is None

    async def test_a_write_marks_the_participant(self) -> None:
        scope, stores = await _enlisted(S0)

        scope.participant_for(S0, stores[S0].owner, write=True)

        assert scope.participants[S0].wrote is True

    async def test_writes_are_numbered_across_the_invocation(self) -> None:
        scope, stores = await _enlisted(S0, S1)

        scope.participant_for(S1, stores[S1].owner, write=True)
        scope.participant_for(S0, stores[S0].owner, write=True)

        assert [scope.participants[key].first_write for key in (S1, S0)] == [0, 1]

    async def test_a_later_write_keeps_the_first_write_order(self) -> None:
        scope, stores = await _enlisted(S0, S1)
        scope.participant_for(S0, stores[S0].owner, write=True)
        scope.participant_for(S1, stores[S1].owner, write=True)

        scope.participant_for(S0, stores[S0].owner, write=True)

        assert scope.participants[S0].first_write == 0

    async def test_the_first_write_order_drives_the_commit_order(self) -> None:
        ledger = Ledger()
        stores = _stores(ledger, S0, S1)
        scope = InvocationScope(asyncio.current_task(), _coordinator(*stores.values()))
        async with scope.frame(Frame(S0, frozenset({S1}))):
            scope.participant_for(S1, stores[S1].owner, write=True)

        await scope.coordinator.finish(scope, None)

        assert [key for key, call in ledger.calls if call == "commit"] == [S1, S0]

    @pytest.mark.parametrize("write", [False, True], ids=["read", "write"])
    async def test_another_owner_of_the_same_store_is_refused(self, write: bool) -> None:
        scope, _ = await _enlisted(S0)

        with pytest.raises(ForeignSessionError, match=r"store 's0' \(memory://s0\)"):
            scope.participant_for(S0, object(), write=write)

    async def test_the_refusal_names_another_owner_by_its_target(self) -> None:
        scope, _ = await _enlisted(S0)
        owner = _TargetedOwner("postgresql://db-b:5432/app")

        with pytest.raises(ForeignSessionError, match=r"store 's0' \(postgresql://db-b:5432/app\)"):
            scope.participant_for(S0, owner, write=False)

    @pytest.mark.parametrize(
        "owner",
        [
            pytest.param(_TargetedOwner(b"secret"), id="target-not-a-str"),
            pytest.param(_TargetAttribute(), id="target-not-a-property"),
        ],
    )
    async def test_the_refusal_ignores_a_target_that_is_not_a_str_property(
        self, owner: object
    ) -> None:
        scope, _ = await _enlisted(S0)

        with pytest.raises(ForeignSessionError) as raised:
            scope.participant_for(S0, owner, write=False)

        assert "secret" not in str(raised.value)

    async def test_the_refusal_never_prints_a_mongo_client_s_options(self) -> None:
        scope, _ = await _enlisted(S0)
        client: AsyncMongoClient[Any] = AsyncMongoClient(
            "mongodb://loom:hunter2@db.internal/?tlsCertificateKeyFilePassword=keypass",
            connect=False,
        )
        try:
            with pytest.raises(ForeignSessionError) as raised:
                scope.participant_for(S0, client, write=False)
        finally:
            await client.close()

        assert "keypass" not in str(raised.value)

    async def test_the_refusal_names_an_owner_without_a_target_by_its_type_only(self) -> None:
        scope, _ = await _enlisted(S0)
        owner = _ReprLeaks()

        with pytest.raises(ForeignSessionError, match="another _ReprLeaks") as raised:
            scope.participant_for(S0, owner, write=False)

        assert "secret" not in str(raised.value)

    async def test_a_refused_write_marks_nothing(self) -> None:
        scope, _ = await _enlisted(S0)

        with pytest.raises(ForeignSessionError):
            scope.participant_for(S0, object(), write=True)

        assert scope.participants[S0].wrote is False


class TestParticipantForASinglePhaseStore:
    """A single-phase participant has no owner: any repository of its store joins it."""

    async def test_a_read_returns_none_as_there_is_no_session_to_join(self) -> None:
        scope = await _with_autocommit()

        assert scope.participant_for(S1, object(), write=False) is None

    async def test_a_read_from_a_gather_child_returns_none(self) -> None:
        scope = await _with_autocommit()

        assert await _in_child_task(scope, S1, write=False) == [None]

    async def test_a_write_from_a_gather_child_is_refused(self) -> None:
        scope = await _with_autocommit()

        with pytest.raises(ScopeOwnershipError):
            await _in_child_task(scope, S1, write=True)

    async def test_a_write_returns_its_participant(self) -> None:
        scope = await _with_autocommit()

        assert scope.participant_for(S1, object(), write=True) is scope.participants[S1]

    async def test_a_write_is_recorded(self) -> None:
        scope = await _with_autocommit()

        scope.participant_for(S1, object(), write=True)

        assert scope.participants[S1].wrote is True

    async def test_its_write_is_reported_as_non_transactional(self) -> None:
        scope = await _with_autocommit()
        scope.participant_for(S1, object(), write=True)

        outcome = await scope.coordinator.finish(scope, Boom("business"))

        assert NonTransactional(S1, wrote=True) in outcome.stores

    async def test_a_failure_after_its_write_raises_stores_not_committed(self) -> None:
        scope = await _with_autocommit()
        scope.participant_for(S1, object(), write=True)
        error = Boom("business")

        outcome = await scope.coordinator.finish(scope, error)

        assert isinstance(invocation_error(outcome, error), StoresNotCommitted)


async def _with_autocommit() -> InvocationScope:
    """A scope with a phased store ``S0`` and a single-phase autocommit store ``S1``."""
    ledger = Ledger()
    coordinator = _coordinator(PhasedStore(S0, ledger), AutocommitStore(S1, ledger))
    scope = InvocationScope(asyncio.current_task(), coordinator)
    for key in (S0, S1):
        await coordinator.enlist(scope, key)
    return scope


class TestOwnership:
    @pytest.mark.parametrize("write", [False, True], ids=["read", "write"])
    async def test_a_gather_child_touching_a_participant_is_refused(self, write: bool) -> None:
        scope, _ = await _enlisted(S0)

        with pytest.raises(ScopeOwnershipError, match="task other than the one that owns it"):
            await _in_child_task(scope, S0, write=write)

    async def test_a_gather_child_reading_a_store_that_is_not_enlisted_reads_per_call(
        self,
    ) -> None:
        scope, _ = await _enlisted(S0)

        assert await _in_child_task(scope, S1, write=False) == [None]

    async def test_a_gather_child_writing_a_store_that_is_not_enlisted_is_refused(self) -> None:
        scope, _ = await _enlisted(S0)

        with pytest.raises(ScopeOwnershipError):
            await _in_child_task(scope, S1, write=True)

    async def test_a_thread_touching_a_participant_is_refused(self) -> None:
        scope, stores = await _enlisted(S0)

        with pytest.raises(ScopeOwnershipError, match="thread without its event loop"):
            await asyncio.to_thread(scope.participant_for, S0, stores[S0].owner, write=False)

    async def test_a_thread_reading_a_store_that_is_not_enlisted_reads_per_call(self) -> None:
        scope, _ = await _enlisted(S0)

        assert await asyncio.to_thread(scope.participant_for, S1, object(), write=False) is None

    async def test_a_closed_scope_is_refused(self) -> None:
        scope, stores = await _enlisted(S0)
        await scope.coordinator.finish(scope, None)

        with pytest.raises(ScopeOwnershipError, match="already closed"):
            scope.participant_for(S0, stores[S0].owner, write=False)

    async def test_a_scope_without_an_owner_task_is_refused(self) -> None:
        scope, stores = await _enlisted(S0)
        scope.owner_task = None

        with pytest.raises(ScopeOwnershipError, match="task other than the one that owns it"):
            scope.participant_for(S0, stores[S0].owner, write=False)

    async def test_a_scope_without_an_owner_task_is_refused_from_a_loop_callback(self) -> None:
        """No task is running in a callback either: ``None is None`` must not pass."""
        scope, stores = await _enlisted(S0)
        scope.owner_task = None
        looked_up: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

        def lookup() -> None:
            try:
                looked_up.set_result(scope.participant_for(S0, stores[S0].owner, write=False))
            except ScopeOwnershipError as error:
                looked_up.set_exception(error)

        asyncio.get_running_loop().call_soon(lookup)

        with pytest.raises(ScopeOwnershipError):
            await looked_up

    async def test_the_ownership_error_is_a_system_error_with_its_code(self) -> None:
        scope, _ = await _enlisted(S0)
        scope.owner_task = None

        with pytest.raises(ScopeOwnershipError) as raised:
            scope.participant_for(S0, object(), write=False)

        assert raised.value.code == ErrorCode.INVOCATION_SCOPE_FOREIGN_TASK


class TestWritesStayInTheInnermostFrame:
    async def test_a_write_to_a_store_outside_it_is_refused(self) -> None:
        stores = _stores(Ledger(), S0, S1)

        async with invocation(_coordinator(*stores.values())) as scope, scope.frame(Frame(S0)):
            with pytest.raises(
                ForeignSessionError,
                match=r"a write to store 's1' .* is refused: .* declares only store 's0'$",
            ):
                scope.participant_for(S1, stores[S1].owner, write=True)

    async def test_an_enlisted_store_of_an_outer_frame_refuses_the_write_and_records_nothing(
        self,
    ) -> None:
        stores = _stores(Ledger(), S0, S1)

        async with invocation(_coordinator(*stores.values())) as scope:
            async with scope.frame(Frame(S1)), scope.frame(Frame(S0)):
                with pytest.raises(ForeignSessionError):
                    scope.participant_for(S1, stores[S1].owner, write=True)
            wrote = scope.participants[S1].wrote

        assert wrote is False

    async def test_a_read_of_an_enlisted_store_of_an_outer_frame_joins_it(self) -> None:
        stores = _stores(Ledger(), S0, S1)

        async with invocation(_coordinator(*stores.values())) as scope:
            async with scope.frame(Frame(S1)), scope.frame(Frame(S0)):
                found = scope.participant_for(S1, stores[S1].owner, write=False)
            enlisted = scope.participants[S1]

        assert found is enlisted

    async def test_a_write_to_a_store_of_it_is_recorded(self) -> None:
        stores = _stores(Ledger(), S0)

        async with invocation(_coordinator(*stores.values())) as scope, scope.frame(Frame(S0)):
            participant = scope.participant_for(S0, stores[S0].owner, write=True)

        assert participant is not None
        assert participant.wrote is True


class TestOwnedHere:
    async def test_the_owner_task_owns_the_scope(self) -> None:
        assert _scope_for().owned_here() is True

    async def test_a_gather_child_does_not(self) -> None:
        scope = _scope_for()

        async def owned() -> bool:
            return scope.owned_here()

        assert await asyncio.gather(owned()) == [False]

    async def test_a_closed_scope_is_owned_by_no_one(self) -> None:
        scope = _scope_for()
        scope.closed = True

        assert scope.owned_here() is False


class TestCurrentHandle:
    def test_without_a_scope_there_is_none(self) -> None:
        assert current_handle(S0, object(), write=False) is None

    async def test_it_is_the_session_of_the_published_scope_s_participant(self) -> None:
        stores = _stores(Ledger(), S0)

        async with invocation(_coordinator(*stores.values())) as scope:
            enlisted = await scope.coordinator.enlist(scope, S0)
            found = current_handle(S0, stores[S0].owner, write=False)
            held = enlisted.handle

        assert found is held

    async def test_a_participant_s_session_is_not_none(self) -> None:
        stores = _stores(Ledger(), S0)

        async with invocation(_coordinator(*stores.values())) as scope:
            await scope.coordinator.enlist(scope, S0)
            found = current_handle(S0, stores[S0].owner, write=False)

        assert found is not None


class TestCurrentParticipant:
    def test_without_a_scope_there_is_none(self) -> None:
        assert current_participant(S0, object(), write=True) is None

    async def test_it_is_the_participant_of_the_published_scope(self) -> None:
        stores = _stores(Ledger(), S0)

        async with invocation(_coordinator(*stores.values())) as scope:
            enlisted = await scope.coordinator.enlist(scope, S0)
            found = current_participant(S0, stores[S0].owner, write=True)

        assert found is enlisted
        assert enlisted.wrote is True

    async def test_it_goes_through_the_ownership_checkpoint(self) -> None:
        stores = _stores(Ledger(), S0)

        async def lookup() -> object:
            return current_participant(S0, stores[S0].owner, write=False)

        async with invocation(_coordinator(*stores.values())) as scope:
            await scope.coordinator.enlist(scope, S0)
            child = asyncio.gather(lookup())
            with pytest.raises(ScopeOwnershipError):
                await child


class TestUnscoped:
    async def test_no_scope_is_published_inside(self) -> None:
        async with invocation(_coordinator()):
            with unscoped():
                inside = current_scope()

        assert inside is None

    async def test_the_scope_is_published_again_afterwards(self) -> None:
        async with invocation(_coordinator()) as scope:
            with unscoped():
                pass
            after = current_scope()

        assert after is scope


def _unit_of_work(store: PhasedStore) -> UnitOfWork:
    assert isinstance(store, UnitOfWork)
    return store


class TestSingleStore:
    async def test_a_clean_block_commits_the_unit_of_work(self) -> None:
        ledger = Ledger()
        store = PhasedStore(S0, ledger)

        async with single_store(S0, _unit_of_work(store)):
            store.write("row")

        assert ledger.persisted[S0] == ["row"]

    async def test_the_unit_of_work_is_the_participant_of_its_store(self) -> None:
        store = PhasedStore(S0, Ledger())

        async with single_store(S0, _unit_of_work(store)) as scope:
            handle = scope.participants[S0].handle

        assert handle is not None

    async def test_a_failing_block_rolls_back_and_propagates(self) -> None:
        ledger = Ledger()
        store = PhasedStore(S0, ledger)

        async def fail() -> None:
            async with single_store(S0, _unit_of_work(store)):
                store.write("row")
                raise Boom("body")

        with pytest.raises(Boom, match="body"):
            await fail()

        assert ledger.calls_of(S0) == ["begin", "rollback", "close"]

    async def test_a_failed_commit_raises_its_cause(self) -> None:
        store = PhasedStore(S0, Ledger(), Fault("commit", Effect.RAISE))

        async def commit() -> None:
            async with single_store(S0, _unit_of_work(store)):
                pass

        with pytest.raises(Boom, match="commit failed"):
            await commit()

    async def test_the_scope_is_unpublished_afterwards(self) -> None:
        async with single_store(S0, _unit_of_work(PhasedStore(S0, Ledger()))):
            pass

        assert current_scope() is None


class TestFramePush:
    async def test_the_frame_is_on_top_while_the_execution_runs(self) -> None:
        scope, _ = await _enlisted(S0)
        frame = Frame(S0)

        async with scope.frame(frame):
            assert scope.frames == [frame]

    async def test_frames_stack_innermost_last(self) -> None:
        scope = _scope_for(*_stores(Ledger(), S0, S1).values())
        outer, inner = Frame(S0), Frame(S1)

        async with scope.frame(outer), scope.frame(inner):
            assert scope.frames == [outer, inner]

    async def test_it_enlists_the_stores_of_the_frame_in_order(self) -> None:
        scope = _scope_for(*_stores(Ledger(), S0, S1, S2).values())

        async with scope.frame(Frame(S1, frozenset({S0}))):
            pass

        assert list(scope.participants) == [S1, S0]

    async def test_a_store_already_enlisted_is_not_begun_again(self) -> None:
        ledger = Ledger()
        scope = _scope_for(PhasedStore(S0, ledger))

        async with scope.frame(Frame(S0)), scope.frame(Frame(S0)):
            pass

        assert ledger.calls_of(S0) == ["begin"]

    @pytest.mark.parametrize(
        ("fault", "fails", "enlisted"),
        [
            (None, False, [S0]),
            (None, True, [S0]),
            (Fault("begin", Effect.RAISE), False, []),
        ],
        ids=["clean", "execution-fails", "enlisting-fails"],
    )
    async def test_the_frame_is_popped_afterwards(
        self, fault: Fault | None, fails: bool, enlisted: list[StoreKey]
    ) -> None:
        """A store whose enlistment failed is not registered either."""
        scope = _scope_for(PhasedStore(S0, Ledger(), fault))
        raised = pytest.raises(Boom) if fault is not None or fails else nullcontext()

        with raised:
            async with scope.frame(Frame(S0)):
                if fails:
                    raise Boom

        assert (scope.frames, list(scope.participants)) == ([], enlisted)

    async def test_a_cancelled_enlistment_pops_the_frame_and_registers_nothing(self) -> None:
        ledger = Ledger()
        store = PhasedStore(S0, ledger, Fault("begin", Effect.CANCEL))
        scope = InvocationScope(None, _coordinator(store))

        async def run() -> None:
            scope.owner_task = asyncio.current_task()
            async with scope.frame(Frame(S0)):
                pass

        task = asyncio.create_task(run())
        await ledger.blocked.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert (scope.frames, scope.participants) == ([], {})

    async def test_a_frame_pushed_from_another_task_is_refused_before_anything_changes(
        self,
    ) -> None:
        scope = _scope_for(PhasedStore(S0, Ledger()))

        async def push() -> None:
            async with scope.frame(Frame(S0)):
                pass

        pushing = asyncio.create_task(push())  # runs only once awaited below

        with pytest.raises(ScopeOwnershipError):
            await pushing

        assert (scope.frames, scope.participants) == ([], {})


class TestInvocation:
    async def test_no_scope_is_published_outside_an_invocation(self) -> None:
        assert current_scope() is None

    async def test_the_scope_is_published_inside(self) -> None:
        async with invocation(_coordinator()) as scope:
            assert current_scope() is scope

    async def test_the_current_task_owns_it(self) -> None:
        async with invocation(_coordinator()) as scope:
            assert scope.owner_task is asyncio.current_task()

    async def test_the_scope_is_unpublished_after_a_clean_exit(self) -> None:
        async with invocation(_coordinator()):
            pass

        assert current_scope() is None

    async def test_the_scope_is_unpublished_after_a_failure(self) -> None:
        coordinator = _coordinator()

        with pytest.raises(Boom):
            async with invocation(coordinator):
                raise Boom

        assert current_scope() is None

    async def test_a_clean_exit_commits_what_the_caller_did_not_finish(self) -> None:
        async with invocation(_coordinator(PhasedStore(S0, Ledger()))) as scope:
            await scope.coordinator.enlist(scope, S0)

        assert scope.outcome == InvocationOutcome((Committed(S0),))

    async def test_a_failure_rolls_back_what_the_caller_did_not_finish(self) -> None:
        error = Boom()
        scopes: list[InvocationScope] = []
        coordinator = _coordinator(PhasedStore(S0, Ledger()))

        async def enlist_then_fail() -> None:
            async with invocation(coordinator) as scope:
                scopes.append(scope)
                await scope.coordinator.enlist(scope, S0)
                raise error

        with pytest.raises(Boom):
            await enlist_then_fail()

        assert scopes[0].outcome == InvocationOutcome((RolledBack(S0, error),))

    async def test_the_failure_itself_propagates(self) -> None:
        error = Boom()

        coordinator = _coordinator()

        with pytest.raises(Boom) as raised:
            async with invocation(coordinator):
                raise error

        assert raised.value is error

    async def test_a_scope_the_caller_finished_is_not_finished_again(self) -> None:
        ledger = Ledger()
        async with invocation(_coordinator(PhasedStore(S0, ledger))) as scope:
            await scope.coordinator.enlist(scope, S0)
            await scope.coordinator.finish(scope, None)

        assert ledger.calls_of(S0) == ["begin", "flush", "commit", "close"]

    @pytest.mark.parametrize(
        ("outcome", "committed"),
        [
            (None, False),
            (InvocationOutcome((Committed(S0),)), True),
            (InvocationOutcome((RolledBack(S0, Boom()),)), False),
        ],
        ids=["no-outcome", "succeeded", "rolled-back"],
    )
    def test_committed_is_whether_a_recorded_outcome_succeeded(
        self, outcome: InvocationOutcome | None, committed: bool
    ) -> None:
        scope = InvocationScope(None, _coordinator(), outcome=outcome)

        assert scope.committed is committed

    async def test_the_scope_is_closed_on_exit(self) -> None:
        async with invocation(_coordinator()) as scope:
            pass

        assert scope.closed is True

    async def test_a_cancelled_body_rolls_back_and_the_cancellation_propagates(self) -> None:
        ledger = Ledger()
        scopes: list[InvocationScope] = []

        async def run() -> None:
            async with invocation(_coordinator(PhasedStore(S0, ledger))) as scope:
                scopes.append(scope)
                await scope.coordinator.enlist(scope, S0)
                await asyncio.Event().wait()

        task = asyncio.create_task(run())
        while not scopes or S0 not in scopes[0].participants:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert ledger.calls_of(S0) == ["begin", "rollback", "close"]

    async def test_a_scope_opened_in_a_child_task_is_not_seen_by_the_parent(self) -> None:
        async def open_one() -> None:
            async with invocation(_coordinator()):
                await asyncio.sleep(0)

        await asyncio.create_task(open_one())

        assert current_scope() is None


class TestRefuseInsideOpenScope:
    def test_outside_a_scope_nothing_is_refused(self) -> None:
        assert _passes_the_guard() is True

    async def test_inside_an_open_scope_the_unit_of_work_is_refused(self) -> None:
        async with invocation(_coordinator(PhasedStore(S0, Ledger()))):
            with pytest.raises(UnitOfWorkInsideScope, match="`async with object`"):
                refuse_inside_open_scope(object())

    async def test_a_closed_scope_a_task_still_sees_refuses_nothing(self) -> None:
        go = asyncio.Event()
        survivors: list[asyncio.Task[bool]] = []

        async def late() -> bool:
            await go.wait()
            return _passes_the_guard()

        async with invocation(_coordinator(PhasedStore(S0, Ledger()))):
            survivors.append(asyncio.create_task(late()))
        go.set()

        assert await survivors[0] is True


def _passes_the_guard() -> bool:
    refuse_inside_open_scope(object())
    return True


def _outcome(*stores: Any) -> InvocationOutcome:
    return InvocationOutcome(tuple(stores))


class TestInvocationError:
    def test_a_clean_invocation_raises_nothing(self) -> None:
        assert invocation_error(_outcome(Committed(S0), Committed(S1)), None) is None

    def test_a_non_transactional_write_beside_a_commit_raises_nothing(self) -> None:
        outcome = _outcome(Committed(S0), NonTransactional(S1, wrote=True))

        assert invocation_error(outcome, None) is None

    def test_one_store_re_raises_its_commit_failure(self) -> None:
        cause = Boom("commit")

        assert invocation_error(_outcome(RolledBack(S0, cause)), None) is cause

    def test_one_uncertain_store_re_raises_its_cause(self) -> None:
        cause = ConnectionError("ack lost")

        assert invocation_error(_outcome(Uncertain(S0, cause)), None) is cause

    def test_one_store_re_raises_the_business_error(self) -> None:
        error = Boom("business")

        assert invocation_error(_outcome(RolledBack(S0, error)), error) is error

    def test_a_business_error_beside_a_non_transactional_store_is_re_raised(self) -> None:
        error = Boom("business")
        outcome = _outcome(NonTransactional(S0, wrote=True))

        assert invocation_error(outcome, error) is error

    def test_two_stores_that_both_rolled_back_re_raise_the_first_failure(self) -> None:
        first, second = Boom("first"), Boom("second")

        assert invocation_error(_outcome(RolledBack(S0, first), RolledBack(S1, second)), None) is (
            first
        )

    def test_a_non_transactional_store_that_never_wrote_does_not_split(self) -> None:
        cause = Boom("commit")
        outcome = _outcome(RolledBack(S0, cause), NonTransactional(S1, wrote=False))

        assert invocation_error(outcome, None) is cause

    @pytest.mark.parametrize(
        "stores",
        [
            (Committed(S0), RolledBack(S1, Boom("commit"))),
            (NonTransactional(S0, wrote=True), RolledBack(S1, Boom("commit"))),
            (RolledBack(S0, Boom("commit")), Uncertain(S1, ConnectionError("ack"))),
            (Uncertain(S0, ConnectionError("ack")), NonTransactional(S1, wrote=False)),
        ],
        ids=["committed-and-rolled-back", "wrote-and-rolled-back", "uncertain", "uncertain-only"],
    )
    def test_stores_that_ended_apart_raise_stores_not_committed(self, stores: Any) -> None:
        assert isinstance(invocation_error(_outcome(*stores), None), StoresNotCommitted)

    def test_stores_not_committed_carries_the_outcome(self) -> None:
        outcome = _outcome(Committed(S0), RolledBack(S1, Boom("commit")))

        error = invocation_error(outcome, None)

        assert isinstance(error, StoresNotCommitted)
        assert error.outcome is outcome

    def test_stores_not_committed_is_caused_by_the_first_failure(self) -> None:
        cause = Boom("commit")

        error = invocation_error(_outcome(Committed(S0), RolledBack(S1, cause)), None)

        assert error is not None
        assert error.__cause__ is cause

    def test_a_business_error_after_a_non_transactional_write_raises_stores_not_committed(
        self,
    ) -> None:
        error = Boom("business")
        outcome = _outcome(NonTransactional(S0, wrote=True), RolledBack(S1, error))

        translated = invocation_error(outcome, error)

        assert translated is not None
        assert translated.__cause__ is error

    def test_an_interruption_is_never_translated(self) -> None:
        cancelled = asyncio.CancelledError()
        outcome = _outcome(NonTransactional(S0, wrote=True), RolledBack(S1, cancelled))

        assert invocation_error(outcome, cancelled) is cancelled


class TestErrors:
    @pytest.mark.parametrize(
        "error",
        [
            ScopeOwnershipError("closed"),
            UnitOfWorkInsideScope.entering(object()),
            StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1")))),
        ],
        ids=["ownership", "inside-scope", "stores"],
    )
    def test_each_error_is_not_retryable(self, error: SystemError) -> None:
        assert error.retryable is False

    def test_stores_not_committed_names_every_store_and_how_it_ended(self) -> None:
        error = StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1"))))

        assert str(error) == (
            "the stores of this invocation did not all commit: s0=Committed, s1=RolledBack"
        )

    def test_stores_not_committed_has_its_code(self) -> None:
        error = StoresNotCommitted.of(_outcome(Committed(S0)))

        assert error.code == ErrorCode.STORES_NOT_COMMITTED

    def test_stores_not_committed_pickles_its_message(self) -> None:
        error = StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1"))))

        restored = pickle.loads(pickle.dumps(error))

        assert (type(restored), str(restored)) == (StoresNotCommitted, str(error))

    def test_stores_not_committed_pickles_its_code(self) -> None:
        error = StoresNotCommitted.of(_outcome(Committed(S0)))

        assert pickle.loads(pickle.dumps(error)).code == ErrorCode.STORES_NOT_COMMITTED

    def test_stores_not_committed_leaves_the_outcome_behind(self) -> None:
        error = StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1"))))

        assert pickle.loads(pickle.dumps(error)).outcome is None

    def test_stores_not_committed_pickles_no_cause_text(self) -> None:
        error = StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1"))))

        assert b"pw=1" not in pickle.dumps(error)

    def test_stores_not_committed_pickles_with_an_unpicklable_cause(self) -> None:
        cause = Boom()
        cause.socket = _Unpicklable()  # type: ignore[attr-defined]
        error = StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, cause)))

        assert pickle.loads(pickle.dumps(error)).message == error.message

    @pytest.mark.parametrize(
        "error",
        [
            ScopeOwnershipError("closed"),
            UnitOfWorkInsideScope.entering(object()),
            StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1")))),
        ],
        ids=["ownership", "inside-scope", "stores"],
    )
    def test_each_error_pickles_to_its_type_and_message(self, error: Exception) -> None:
        restored = pickle.loads(pickle.dumps(error))

        assert (type(restored), str(restored)) == (type(error), str(error))

    @pytest.mark.parametrize(
        "error",
        [
            ScopeOwnershipError("closed"),
            UnitOfWorkInsideScope.entering(object()),
            StoresNotCommitted.of(_outcome(Committed(S0), RolledBack(S1, Boom("pw=1")))),
            ForeignSessionError.between("store 's0' (b.db)", "store 's0' (a.db)"),
            NestedIdentityMismatch.joining("Child", "Parent"),
            NestedElevation.opening("Child", "Parent", frozenset({"editor"})),
        ],
        ids=[
            "ownership",
            "inside-scope",
            "stores",
            "foreign-session",
            "nested-identity",
            "nested-elevation",
        ],
    )
    def test_each_error_crosses_the_celery_json_result_backend(self, error: Exception) -> None:
        backend = BaseBackend(app=_celery_app(), serializer="json")

        restored = backend.exception_to_python(backend.prepare_exception(error))

        assert (type(restored), str(restored)) == (type(error), str(error))

    def test_uow_inside_scope_names_the_unit_of_work(self) -> None:
        error = UnitOfWorkInsideScope.entering(PhasedStore(S0, Ledger()))

        assert "`async with PhasedStore` inside an invocation scope" in str(error)

    def test_uow_inside_scope_has_its_code(self) -> None:
        assert UnitOfWorkInsideScope.entering(object()).code == ErrorCode.UOW_INSIDE_SCOPE


class _Unpicklable:
    def __reduce__(self) -> Any:
        raise TypeError("cannot pickle a socket")


def _celery_app() -> Any:
    from celery import Celery

    return Celery("loom-scope-tests", set_as_current=False)
